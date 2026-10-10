import type { Database } from '../db/pool';
import { EndpointRepository } from '../db/repositories/endpoint.repository';
import type { Clock } from '../common/clock';
import { truncateToBytes } from '../common/bytes';
import { DestinationNotAllowedError, WebhookClient } from '../modules/webhooks/webhook.client';
import type { WebhookEnvelope } from '../domain/types';
import type { ClaimedWork, DeliveryAttemptResult, DeliveryProcessor } from '../domain/attempt';

/**
 * Build the worker's delivery processor.
 *
 * Per attempt it: reads the endpoint destination (server-side configuration,
 * never caller-supplied), reuses the exact persisted envelope bytes, and signs
 * a FRESH timestamp for those bytes - so eventId/deliveryId stay stable while
 * attemptId/timestamp/signature change every attempt.
 *
 * The secret is only ever passed to the signer; it never reaches logs (best
 * effort: the captured response snippet is bounded and secret-ish strings are
 * redacted) nor the outbound request.
 */
export function createWebhookProcessor(deps: {
  db: Database;
  clock: Clock;
  client: WebhookClient;
  endpoints?: EndpointRepository;
}): DeliveryProcessor {
  const endpoints = deps.endpoints ?? new EndpointRepository();
  return async (work: ClaimedWork): Promise<DeliveryAttemptResult> => {
    const target = await endpoints.findDispatchTarget(deps.db, work.endpointId, work.tenantId);
    if (!target) {
      // Endpoint vanished - nothing to deliver to. Not retryable by policy.
      return {
        outcome: 'NON_RETRYABLE',
        httpStatus: null,
        errorCode: 'endpoint_missing',
        responseSnippet: null,
        retryAfterMs: null,
      };
    }

    const envelope = JSON.parse(work.envelopeBytes.toString('utf8')) as WebhookEnvelope;

    let result: DeliveryAttemptResult;
    try {
      result = await deps.client.dispatch({
        url: target.url,
        secret: target.secret,
        eventId: envelope.eventId,
        deliveryId: envelope.deliveryId,
        attemptId: work.attemptId,
        body: work.envelopeBytes,
        timestampUnixSec: deps.clock.nowUnixSeconds(),
      });
    } catch (err) {
      // A destination rejected by the allowlist is a configuration/security
      // fact, not a transient network condition: it must not burn the retry
      // budget and must never reach the network.
      if (err instanceof DestinationNotAllowedError) {
        return {
          outcome: 'NON_RETRYABLE',
          httpStatus: null,
          errorCode: 'destination_not_allowed',
          responseSnippet: null,
          retryAfterMs: null,
        };
      }
      throw err;
    }

    return { ...result, responseSnippet: sanitizeSnippet(result.responseSnippet) };
  };
}

const SECRETISH = /secret|token|authorization/gi;

/** PDF bound on captured response details, matching the WEBHOOK_MAX_RESPONSE_BYTES default. */
const MAX_SNIPPET_BYTES = 4096;

/**
 * Scrub accidental secret echoes, then bound the result.
 *
 * The order matters: "[redacted:token]" is longer than the "token" it replaces,
 * so bounding first and redacting second lets redaction push the stored snippet
 * past the 4 KiB limit (a body of nothing but "token" measured 11,587 bytes).
 * Redacting first makes the bound unconditional.
 */
function sanitizeSnippet(snippet: string | null): string | null {
  if (snippet === null) return null;
  const redacted = snippet.replace(SECRETISH, (m: string) => `[redacted:${m}]`);
  return truncateToBytes(redacted, MAX_SNIPPET_BYTES);
}
