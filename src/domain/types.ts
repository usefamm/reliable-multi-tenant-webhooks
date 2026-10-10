/**
 * Core domain types shared across the API, workers, and tests.
 * Keeping these framework-agnostic means the same shapes are used by the Nest
 * controllers, the standalone worker process, and the repository layer.
 */

/** Delivery state machine states (PDF: READY, IN_FLIGHT, RETRY_WAIT, DELIVERED, DEAD). */
export const DeliveryState = {
  READY: 'READY',
  IN_FLIGHT: 'IN_FLIGHT',
  RETRY_WAIT: 'RETRY_WAIT',
  DELIVERED: 'DELIVERED',
  DEAD: 'DEAD',
} as const;
export type DeliveryState = (typeof DeliveryState)[keyof typeof DeliveryState];

/** Terminal states: no automatic work happens and no lease is held. */
export const TERMINAL_STATES: readonly DeliveryState[] = [DeliveryState.DELIVERED, DeliveryState.DEAD];

/** Attempt outcome classification (persisted in delivery_attempts.outcome). */
export const AttemptOutcome = {
  SUCCESS: 'SUCCESS',
  RETRYABLE: 'RETRYABLE',
  NON_RETRYABLE: 'NON_RETRYABLE',
  UNKNOWN: 'UNKNOWN',
} as const;
export type AttemptOutcome = (typeof AttemptOutcome)[keyof typeof AttemptOutcome];

/** The webhook envelope delivered to receivers (PDF contract). */
export interface WebhookEnvelope {
  eventId: string;
  deliveryId: string;
  eventType: string;
  occurredAt: string; // ISO-8601 UTC, fixed at publication
  payload: unknown; // caller-supplied JSON object
}

/**
 * Maximum size, in bytes, of the serialized envelope a receiver accepts: the
 * same 64 KiB bound the receiver enforces on its inbound body.
 *
 * The envelope is always strictly LARGER than the request body that produced it
 * (`eventId`, `deliveryId`, `eventType` and `occurredAt` are added around the
 * payload), so the parser bound on `POST /events` does not bound the delivery.
 * Publication therefore measures the built bytes against this limit: a request
 * that would produce an over-sized envelope is refused with 413 instead of being
 * accepted with 202 and then failing permanently at the receiver.
 */
export const MAX_ENVELOPE_BYTES = 64 * 1024;

/** A row from the deliveries table (subset used across services). */
export interface DeliveryRow {
  id: string;
  event_id: string;
  tenant_id: string;
  endpoint_id: string;
  state: DeliveryState;
  envelope_bytes: Buffer;
  envelope_hash: string;
  attempt_count: number;
  cycle: number;
  attempts_in_cycle: number;
  next_attempt_at: Date | null;
  lease_owner: string | null;
  lease_generation: string; // bigint comes back as string from pg
  lease_expires_at: Date | null;
  last_http_status: number | null;
  last_error_code: string | null;
  created_at: Date;
  updated_at: Date;
}
