import { createServer, type Server, type ServerResponse } from 'node:http';
import { type AddressInfo } from 'node:net';
import { verifyWebhookSignature } from '../../src/modules/webhooks/signing';

/**
 * A real HTTP endpoint that behaves like a customer webhook receiver while
 * recording everything the sender actually put on the wire.
 *
 * The mock receiver (src/receiver) proves the *receiver-side* semantics:
 * verification, freshness, deduplication, failure modes. This harness answers
 * the complementary question - "what did OUR service send?" - because it keeps
 * the raw bytes, parses the identity headers, recomputes the HMAC with the
 * configured secret and counts genuinely concurrent in-flight requests.
 *
 * Nothing here is simulated: sockets, delays and timeouts are real, which is
 * what makes the concurrency and timeout bounds measurable.
 */
export interface CaptureResponse {
  status: number;
  body?: string;
  headers?: Record<string, string>;
  /** Hold the reply open this long, so timeouts and concurrency overlap are real. */
  delayMs?: number;
}

export type Responder = (req: CapturedRequest, index: number) => CaptureResponse;

export interface CapturedRequest {
  method: string;
  path: string;
  headers: Record<string, string | string[] | undefined>;
  /** Exact bytes received, unmodified - the only thing a signature can be checked against. */
  body: Buffer;
  eventId: string | null;
  deliveryId: string | null;
  attemptId: string | null;
  timestampSec: number | null;
  signature: string | null;
  /** True when the HMAC verifies over these bytes with this endpoint's secret. */
  signatureValid: boolean;
  /** In-flight requests (including this one) at arrival: the observed concurrency. */
  concurrent: number;
}

export interface CaptureEndpoint {
  port: number;
  secret: string;
  url(path?: string): string;
  readonly requests: CapturedRequest[];
  /** Highest concurrency ever observed by this endpoint. */
  maxConcurrent(): number;
  setResponder(next: Responder): void;
  /** Forget recorded requests (the port and endpoint row stay). */
  clear(): void;
  close(): Promise<void>;
}

const acceptAny: Responder = () => ({ status: 200, body: '{"ok":true}' });

export async function startCaptureEndpoint(
  responder: Responder = acceptAny,
  secret = 'capture-secret',
): Promise<CaptureEndpoint> {
  let current = responder;
  const requests: CapturedRequest[] = [];
  let inFlight = 0;
  let peak = 0;

  const server: Server = createServer((req, res) => {
    const arrived = ++inFlight;
    peak = Math.max(peak, arrived);
    // The count only means something if it comes back down: release when the
    // reply is written (or the socket dies trying).
    const release = singleShot(() => {
      inFlight -= 1;
    });
    res.once('finish', release);
    res.once('close', release);

    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      const body = Buffer.concat(chunks);
      const headers = req.headers;
      const timestampHeader = first(headers['x-webhook-timestamp']);
      const signature = first(headers['x-webhook-signature']);
      const timestampSec = timestampHeader !== null ? Number(timestampHeader) : null;
      const recorded: CapturedRequest = {
        method: req.method ?? '',
        path: req.url ?? '',
        headers,
        body,
        eventId: first(headers['x-event-id']),
        deliveryId: first(headers['x-delivery-id']),
        attemptId: first(headers['x-attempt-id']),
        timestampSec: timestampSec !== null && Number.isInteger(timestampSec) ? timestampSec : null,
        signature,
        signatureValid:
          signature !== null &&
          timestampSec !== null &&
          Number.isInteger(timestampSec) &&
          verifyWebhookSignature(secret, timestampSec, body, signature),
        concurrent: arrived,
      };
      requests.push(recorded);
      reply(res, current(recorded, requests.length - 1));
    });
    req.on('error', () => reply(res, { status: 500, body: '{"code":"capture_error"}' }));
  });
  server.setMaxListeners(0);

  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve());
  });
  const port = (server.address() as AddressInfo).port;

  return {
    port,
    secret,
    url: (path = '/hook') => `http://127.0.0.1:${port}${path}`,
    requests,
    maxConcurrent: () => peak,
    setResponder: (next) => {
      current = next;
    },
    clear: () => {
      requests.length = 0;
      peak = 0;
      inFlight = 0;
    },
    close() {
      return new Promise<void>((resolve) => {
        server.close(() => resolve());
        server.closeAllConnections?.();
      });
    },
  };
}

function reply(res: ServerResponse, spec: CaptureResponse): void {
  const send = () => {
    if (res.writableEnded || res.destroyed) return;
    const payload = Buffer.from(spec.body ?? '', 'utf8');
    res.writeHead(spec.status, {
      'content-type': 'application/json',
      'content-length': String(payload.byteLength),
      ...spec.headers,
    });
    res.end(payload);
  };
  if (spec.delayMs) setTimeout(send, spec.delayMs).unref();
  else send();
}

function singleShot(fn: () => void): () => void {
  let done = false;
  return () => {
    if (done) return;
    done = true;
    fn();
  };
}

function first(value: string | string[] | undefined): string | null {
  if (Array.isArray(value)) return value[0] ?? null;
  return value ?? null;
}
