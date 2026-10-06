import type { Database } from '../db/pool';
import type { Clock } from '../common/clock';
import { newAttemptId, newUuid } from '../common/ids';
import type { DeliveryState } from '../domain/types';
import type { AttemptOutcomeKind, ClaimedWork } from './types';

interface ClaimRow {
  id: string;
  event_id: string;
  tenant_id: string;
  endpoint_id: string;
  envelope_bytes: Buffer;
  attempt_count: number;
  cycle: number;
  attempts_in_cycle: number;
  lease_owner: string;
  lease_generation: string; // bigint -> string
}

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
 * The durable, database-backed delivery queue.
 *
 * This is the heart of the concurrency-safety story. It owns exactly two
 * operations, each a single short transaction that NEVER spans an HTTP call:
 *
 *   claimNext()      - atomically find due work, take a bounded lease, bump the
 *                      fencing generation, and pre-allocate the attempt record.
 *   completeAttempt() - record the attempt outcome and transition the delivery
 *                      state, fenced on (lease_owner, lease_generation) so a
 *                      stale worker can never overwrite newer state.
 */
export class DeliveryQueue {
  constructor(
    private readonly db: Database,
    private readonly clock: Clock,
  ) {}

  /**
   * Claim a single due delivery for `owner`.
   *
   * Due work is either:
   *   - READY / RETRY_WAIT whose next_attempt_at has arrived, or
   *   - IN_FLIGHT whose lease has expired (crashed / paused worker recovery).
   *
   * SELECT ... FOR UPDATE SKIP LOCKED makes the claim concurrency-safe across
   * workers: two workers scanning simultaneously lock different rows and never
   * block each other or double-claim. lease_generation is incremented on every
   * claim, which is the fencing token that invalidates any previous holder.
   *
   * The attempt row is inserted in the SAME transaction, BEFORE any HTTP, so a
   * crash after commit but before dispatch still leaves a durable record (and a
   * recoverable lease) rather than silently losing the delivery.
   *
   * Returns null when no work is currently claimable.
   */
  async claimNext(owner: string, leaseTtlMs: number): Promise<ClaimedWork | null> {
    const now = this.clock.now();
    const expiresAt = new Date(now.getTime() + leaseTtlMs);

    return this.db.withTransaction(async (client) => {
      const { rows } = await client.query<ClaimRow>(
        `WITH candidate AS (
           SELECT id
             FROM deliveries
            WHERE (
                    (state IN ('READY','RETRY_WAIT') AND next_attempt_at <= $1)
                 OR (state = 'IN_FLIGHT' AND lease_expires_at IS NOT NULL AND lease_expires_at <= $1)
                  )
            ORDER BY next_attempt_at ASC NULLS LAST, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
         )
         UPDATE deliveries d
            SET state             = 'IN_FLIGHT',
                lease_owner       = $2,
                lease_generation  = d.lease_generation + 1,
                lease_expires_at  = $3,
                attempt_count     = d.attempt_count + 1,
                attempts_in_cycle = d.attempts_in_cycle + 1,
                updated_at        = $1
           FROM candidate
          WHERE d.id = candidate.id
         RETURNING d.id, d.event_id, d.tenant_id, d.endpoint_id, d.envelope_bytes,
                   d.attempt_count, d.cycle, d.attempts_in_cycle,
                   d.lease_owner, d.lease_generation`,
        [now, owner, expiresAt],
      );

      const row = rows[0];
      if (!row) return null;

      const attemptRowId = newUuid();
      const attemptId = newAttemptId();

      await client.query(
        `INSERT INTO delivery_attempts
           (id, delivery_id, attempt_number, cycle, attempt_id,
            lease_owner, lease_generation, started_at, outcome)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'UNKNOWN')`,
        [
          attemptRowId,
          row.id,
          row.attempt_count, // lifetime attempt number after increment
          row.cycle,
          attemptId,
          row.lease_owner,
          row.lease_generation,
          now,
        ],
      );

      return {
        deliveryId: row.id,
        eventId: row.event_id,
        tenantId: row.tenant_id,
        endpointId: row.endpoint_id,
        envelopeBytes: row.envelope_bytes,
        attemptRowId,
        attemptId,
        attemptNumber: row.attempt_count,
        cycle: row.cycle,
        attemptsInCycle: row.attempts_in_cycle,
        leaseOwner: row.lease_owner,
        leaseGeneration: row.lease_generation,
      };
    });
  }

  /**
   * Record the attempt outcome and transition the delivery, fenced on the lease.
   *
   * The attempt row (this worker's own, identified by attemptRowId) is always
   * updated with the true outcome so history stays accurate even for a stale
   * worker. The delivery-state UPDATE additionally requires
   * (lease_owner, lease_generation) to still match: if another worker recovered
   * the expired lease and bumped the generation, this UPDATE affects zero rows
   * and the stale worker cannot overwrite the newer state.
   *
   * On a terminal transition the lease is released (owner/expiry cleared).
   */
  async completeAttempt(params: CompleteAttemptParams): Promise<CompleteAttemptResult> {
    const now = this.clock.now();
    const terminal = params.nextAttemptAt === null;

    return this.db.withTransaction(async (client) => {
      await client.query(
        `UPDATE delivery_attempts
            SET finished_at       = $2,
                outcome           = $3,
                http_status       = $4,
                error_code        = $5,
                response_snippet  = $6
          WHERE id = $1`,
        [
          params.attemptRowId,
          now,
          params.outcome,
          params.httpStatus,
          params.errorCode,
          params.responseSnippet,
        ],
      );

      const { rowCount } = await client.query(
        `UPDATE deliveries
            SET state            = $2,
                next_attempt_at  = $3,
                last_http_status = $4,
                last_error_code  = $5,
                lease_owner      = CASE WHEN $6 THEN NULL ELSE lease_owner END,
                lease_expires_at = CASE WHEN $6 THEN NULL ELSE lease_expires_at END,
                updated_at       = $7
          WHERE id = $1
            AND lease_owner = $8
            AND lease_generation = $9`,
        [
          params.deliveryId,
          params.nextState,
          params.nextAttemptAt,
          params.httpStatus,
          params.errorCode,
          terminal,
          now,
          params.leaseOwner,
          params.leaseGeneration,
        ],
      );

      return { applied: (rowCount ?? 0) > 0 };
    });
  }
}
