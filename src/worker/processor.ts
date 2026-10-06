import type { Database } from '../db/pool';
import type { Clock } from '../common/clock';
import { truncateToBytes } from '../common/bytes';
import { DestinationNotAllowedError, WebhookClient } from '../modules/webhooks/webhook.client';
import type { WebhookEnvelope } from '../domain/types';
import type { ClaimedWork, DeliveryAttemptResult, DeliveryProcessor } from './types';

interface EndpointTarget {
  url: string;
  secret: string;
}

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
}): DeliveryProcessor {
  return async (work: ClaimedWork): Promise<DeliveryAttemptResult> => {
    const { rows } = await deps.db.query<EndpointTarget>(
      'SELECT url, secret FROM endpoints WHERE id = $1 AND tenant_id = $2',
      [work.endpointId, work.tenantId],
    );
    const target = rows[0];
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

/** Bound the snippet again defensively and scrub accidental secret echoes. */
function sanitizeSnippet(snippet: string | null): string | null {
  if (snippet === null) return null;
  const bounded = truncateToBytes(snippet, 4096);
  return bounded.replace(SECRETISH, (m: string) => `[redacted:${m}]`);
}
