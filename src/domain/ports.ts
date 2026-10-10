import type { AttemptOutcomeKind, ClaimedWork } from './attempt';
import type { DeliveryState } from './types';

/** Parameters for the fenced completion of one attempt. */
export interface CompleteAttemptParams {
  deliveryId: string;
  attemptRowId: string;
  leaseOwner: string;
  leaseGeneration: string;
  outcome: AttemptOutcomeKind;
  httpStatus: number | null;
  errorCode: string | null;
  responseSnippet: string | null;
  nextState: DeliveryState;
  nextAttemptAt: Date | null;
}

export interface CompleteAttemptResult {
  /** True when this worker still held the lease and its state write was applied. */
  applied: boolean;
}

/**
 * The work queue the delivery worker depends on. The shipped implementation is
 * PostgreSQL (`PgDeliveryQueue`); a broker-backed queue would implement the same
 * two operations. The contract any implementation must keep:
 *
 *  - claimNext hands a due delivery to at most one owner at a time, bumps a
 *    fencing token, and records the attempt BEFORE the caller dispatches.
 *  - completeAttempt records the attempt outcome truthfully, but moves the
 *    delivery only if the caller still holds the lease it claimed under.
 *  - neither call is held open across the network dispatch.
 */
export interface WorkQueue {
  claimNext(owner: string, leaseTtlMs: number): Promise<ClaimedWork | null>;
  completeAttempt(params: CompleteAttemptParams): Promise<CompleteAttemptResult>;
}
