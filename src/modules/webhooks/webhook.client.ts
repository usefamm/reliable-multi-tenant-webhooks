import { request as httpRequest, type IncomingMessage } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { truncateToBytes } from '../../common/bytes';
import type { AppConfig } from '../../config/env';
import { signWebhook } from './signing';
import type { DeliveryAttemptResult } from '../../domain/attempt';
import {
  DestinationNotAllowedError,
  isAllowedDestination,
} from './destination-policy';
import {
  classifyStatus,
  describeStatus,
  headerOf,
  isTimeoutLike,
  parseRetryAfterMs,
} from './response-classification';

// Re-exported so existing importers keep one stable entry point.
export { DestinationNotAllowedError, isAllowedDestination } from './destination-policy';
export { parseRetryAfterMs } from './response-classification';

type WebhookConfig = Pick<
  AppConfig,
  'WEBHOOK_TIMEOUT_MS' | 'WEBHOOK_MAX_RESPONSE_BYTES' | 'WEBHOOK_ALLOWED_HOSTS'
>;

/** Everything needed to dispatch one attempt. The secret never leaves this call. */
export interface DispatchInput {
  url: string;
  secret: string;
  eventId: string;
  deliveryId: string;
  attemptId: string;
  /** Exact persisted envelope bytes - signed and sent verbatim. */
  body: Buffer;
  /** Unix seconds for this attempt (fresh per attempt). */
  timestampUnixSec: number;
}

/**
 * The outbound webhook client. This is the ONLY code that makes HTTP calls to
 * tenant destinations. It uses node:http directly (no redirect-following agent
 * semantics, precise control of the total timeout across connect + response +
 * body). Safety properties (PDF section 18):
 *
 *  - POST only, JSON content type, the exact persisted envelope bytes as body.
 *  - Hard TOTAL timeout (WEBHOOK_TIMEOUT_MS, default 2s) covering connect,
 *    response, and body read; expiry destroys the socket.
 *  - Redirects are NEVER followed: a 3xx response is returned as-is and
 *    classified NON_RETRYABLE.
 *  - Response bodies are captured bounded to WEBHOOK_MAX_RESPONSE_BYTES (4 KiB)
 *    and the connection is cancelled once that bound is reached.
 *  - The ONLY headers sent are content-type plus the five identity headers -
 *    never the inbound Authorization token, never the endpoint secret.
 *
 * Outcome mapping:
 *  - 2xx                    -> SUCCESS
 *  - timeout                -> RETRYABLE ('timeout'; httpStatus may exist if
 *                               headers arrived before the stall)
 *  - transport error        -> UNKNOWN ('transport_error'): the request may have
 *                               reached the receiver before the connection
 *                               broke, so we preserve the uncertainty instead of
 *                               inventing a result.
 *  - 408 / 429 / 5xx        -> RETRYABLE (429 parses delta-seconds Retry-After)
 *  - other 4xx / any 3xx    -> NON_RETRYABLE
 */
export class WebhookClient {
  constructor(private readonly config: WebhookConfig) {}

  dispatch(input: DispatchInput): Promise<DeliveryAttemptResult> {
    if (!isAllowedDestination(input.url, this.config.WEBHOOK_ALLOWED_HOSTS)) {
      throw new DestinationNotAllowedError(input.url);
    }

    let url: URL;
    try {
      url = new URL(input.url);
    } catch {
      return Promise.resolve({
        outcome: 'NON_RETRYABLE',
        httpStatus: null,
        errorCode: 'bad_destination_url',
        responseSnippet: null,
        retryAfterMs: null,
      });
    }
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      return Promise.resolve({
        outcome: 'NON_RETRYABLE',
        httpStatus: null,
        errorCode: 'unsupported_scheme',
        responseSnippet: null,
        retryAfterMs: null,
      });
    }

    const signature = signWebhook(input.secret, input.timestampUnixSec, input.body);

    return new Promise<DeliveryAttemptResult>((resolve) => {
      const chunks: Buffer[] = [];
      let captured = 0;
      let settled = false;
      let timedOut = false;
      let response: IncomingMessage | null = null;

      const finish = (result: DeliveryAttemptResult) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        req.destroy();
        resolve(result);
      };

      const timeoutResult = (): DeliveryAttemptResult => {
        const res = response;
        if (!res) {
          // Nothing arrived: the request may have reached the receiver, but no
          // status exists to classify. Record a timeout retry with no status
          // rather than inventing one.
          return {
            outcome: 'RETRYABLE',
            httpStatus: null,
            errorCode: 'timeout',
            responseSnippet: null,
            retryAfterMs: null,
          };
        }
        // Headers arrived but the response never completed. A rejection status
        // is already authoritative; a 2xx whose body was cut short is NOT - the
        // receiver may or may not have committed, so preserve the uncertainty.
        return resultFromStatus(res, this.boundedText(chunks), 'timeout');
      };

      const transportResult = (res: IncomingMessage | null, err: Error): DeliveryAttemptResult => {
        // A timeout destroys the socket, which can surface as a request error
        // after we already resolved the timeout result - keep them distinct.
        if (timedOut || isTimeoutLike(err)) {
          return timeoutResult();
        }
        // If status+headers were received, the receiver's answer is known even
        // though the body stream broke: classify on the observed status.
        if (res && res.statusCode) {
          return resultFromStatus(res, chunks.length ? this.boundedText(chunks) : null, 'stream_error');
        }
        // Nothing arrived: the request MAY have been processed before the
        // connection broke, so record the uncertainty - never invent a result.
        return {
          outcome: 'UNKNOWN',
          httpStatus: null,
          errorCode: 'transport_error',
          responseSnippet: null,
          retryAfterMs: null,
        };
      };

      const options = {
        protocol: url.protocol,
        hostname: url.hostname,
        port: url.port ? Number(url.port) : undefined,
        path: `${url.pathname}${url.search}`,
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'content-length': String(input.body.byteLength),
          'x-event-id': input.eventId,
          'x-delivery-id': input.deliveryId,
          'x-attempt-id': input.attemptId,
          'x-webhook-timestamp': String(input.timestampUnixSec),
          'x-webhook-signature': signature,
        },
      };

      const req = (url.protocol === 'https:' ? httpsRequest : httpRequest)(options, (res) => {
        response = res;
        res.on('data', (c: Buffer) => {
          if (settled || captured >= this.config.WEBHOOK_MAX_RESPONSE_BYTES) {
            // Bound already reached: accept nothing more, even if body bytes are
            // still in flight ahead of the socket teardown.
            return;
          }
          chunks.push(c);
          captured += c.byteLength;
          if (captured >= this.config.WEBHOOK_MAX_RESPONSE_BYTES) {
            // Bound reached: resolve with what we have and stop the stream at the
            // source, so the peer's remaining body is never read into memory.
            const bounded = resultFromStatus(res, this.boundedText(chunks));
            res.destroy();
            finish(bounded);
          }
        });
        res.on('end', () => {
          if (!settled) finish(resultFromStatus(res, this.boundedText(chunks)));
        });
        res.on('error', (err) => {
          finish(transportResult(res, err));
        });
      });

      const timer = setTimeout(() => {
        timedOut = true;
        finish(timeoutResult());
      }, this.config.WEBHOOK_TIMEOUT_MS);

      req.on('error', (err) => {
        finish(transportResult(response, err));
      });

      req.setTimeout(this.config.WEBHOOK_TIMEOUT_MS, () => {
        timedOut = true;
        finish(timeoutResult());
      });

      req.write(input.body);
      req.end();
    });
  }

  private boundedText(chunks: Buffer[]): string {
    // Decode first (a partial multibyte tail becomes U+FFFD), then cap bytes on
    // a character boundary so the stored snippet never splits an escape.
    return truncateToBytes(Buffer.concat(chunks).toString('utf8'), this.config.WEBHOOK_MAX_RESPONSE_BYTES);
  }
}

/**
 * Build the result from an observed HTTP status.
 *
 * `incomplete` marks a response that never finished (timeout / broken stream).
 * A non-2xx rejection is authoritative even then; a 2xx with an unfinished body
 * is recorded as UNKNOWN because the receiver's commit cannot be confirmed -
 * the attempt stays uncertain rather than falsely successful.
 */
function resultFromStatus(
  res: IncomingMessage,
  snippet: string | null,
  incomplete?: 'timeout' | 'stream_error',
): DeliveryAttemptResult {
  const status = res.statusCode ?? 0;
  const outcome = classifyStatus(status);
  if (incomplete && outcome === 'SUCCESS') {
    return {
      outcome: 'UNKNOWN',
      httpStatus: status,
      errorCode: incomplete,
      responseSnippet: snippet,
      retryAfterMs: null,
    };
  }
  return {
    outcome,
    httpStatus: res.statusCode ?? null,
    errorCode: outcome === 'SUCCESS' ? null : describeStatus(status, incomplete),
    responseSnippet: snippet,
    retryAfterMs: status === 429 ? parseRetryAfterMs(headerOf(res, 'retry-after')) : null,
  };
}

