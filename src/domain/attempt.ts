import type { DeliveryState } from './types';

/**
 * Outcome classification for a single dispatch. Mirrors the attempt_outcome
 * enum so it can be persisted verbatim.
 *  - SUCCESS: 2xx from the receiver.
 *  - RETRYABLE: timeout, connection failure, 408, 429, or 5xx.
 *  - NON_RETRYABLE: any other status, or a redirect.
 *  - UNKNOWN: the outcome could not be determined (e.g. the response was lost
 *    after the receiver may have processed it). Never invent a result.
 */
export type AttemptOutcomeKind = 'SUCCESS' | 'RETRYABLE' | 'NON_RETRYABLE' | 'UNKNOWN';

/** The result a processor returns after attempting delivery. */
export interface DeliveryAttemptResult {
  outcome: AttemptOutcomeKind;
  /** Observed HTTP status, or null when no response was received. */
  httpStatus: number | null;
  /** Bounded error/classification code (no secrets, no full bodies). */
  errorCode: string | null;
  /** Captured response detail, already truncated to <= 4 KiB. */
  responseSnippet: string | null;
  /** Parsed Retry-After delta in ms for 429 responses; null otherwise. */
  retryAfterMs: number | null;
}

/**
 * A delivery claimed under a lease, plus the pre-allocated attempt record.
 * Everything the processor needs to sign and dispatch, and everything the
 * completion step needs to fence its write.
 */
export interface ClaimedWork {
  deliveryId: string;
  eventId: string;
  tenantId: string;
  endpointId: string;
  /** Exact persisted envelope bytes - reused byte-for-byte on every attempt. */
  envelopeBytes: Buffer;
  /** delivery_attempts row PK. */
  attemptRowId: string;
  /** The X-Attempt-Id sent to the receiver; fresh per attempt. */
  attemptId: string;
  /** Lifetime attempt number (1..N), not reset by redrive. */
  attemptNumber: number;
  /** Current automatic cycle number. */
  cycle: number;
  /** Attempts used in the current cycle AFTER this claim's increment. */
  attemptsInCycle: number;
  /** Lease identity captured at claim time; the fencing predicate. */
  leaseOwner: string;
  /** bigint comes back from pg as a string. */
  leaseGeneration: string;
}

/** Performs the actual outbound delivery. Implemented by the webhook client (M9). */
export type DeliveryProcessor = (work: ClaimedWork) => Promise<DeliveryAttemptResult>;

/** The delivery-state decision produced by the retry policy for one attempt. */
export interface RetryDecision {
  nextState: DeliveryState;
  /** Due time for a retry; null for terminal states. */
  nextAttemptAt: Date | null;
}
