import type { Database } from './pool';
import type { Clock } from '../common/clock';
import { newAttemptId, newUuid } from '../common/ids';
import type { ClaimedWork } from '../domain/attempt';
import { isValidCompletion, isTerminal } from '../domain/delivery-state';
import type {
  CompleteAttemptParams,
  CompleteAttemptResult,
  WorkQueue,
} from '../domain/ports';
import { AttemptRepository } from './repositories/attempt.repository';
import { DeliveryRepository } from './repositories/delivery.repository';

/**
 * The PostgreSQL-backed `WorkQueue`: the `deliveries` table IS the queue.
 *
 * This class owns the two transaction boundaries of the worker path and nothing
 * else - the SQL lives in the repositories, the transition rules in the domain.
 * Each operation is ONE short transaction that never spans an HTTP call:
 *
 *   claimNext        - take a bounded lease, bump the fencing generation and
 *                      pre-allocate the attempt record, atomically.
 *   completeAttempt  - record the attempt outcome and transition the delivery,
 *                      fenced on (lease_owner, lease_generation) so a stale
 *                      worker can never overwrite newer state.
 */
export class PgDeliveryQueue implements WorkQueue {
  constructor(
    private readonly db: Database,
    private readonly clock: Clock,
    private readonly deliveries: DeliveryRepository = new DeliveryRepository(),
    private readonly attempts: AttemptRepository = new AttemptRepository(),
  ) {}

  /**
   * Claim a single due delivery for `owner`, or null when nothing is claimable.
   *
   * The attempt row is inserted in the SAME transaction, BEFORE any HTTP, so a
   * crash after commit but before dispatch still leaves a durable record (and a
   * recoverable lease) rather than silently losing the delivery.
   */
  async claimNext(owner: string, leaseTtlMs: number): Promise<ClaimedWork | null> {
    const now = this.clock.now();
    const leaseExpiresAt = new Date(now.getTime() + leaseTtlMs);

    return this.db.withTransaction(async (tx) => {
      const claimed = await this.deliveries.claimNextDue(tx, { now, owner, leaseExpiresAt });
      if (!claimed) return null;

      const attemptRowId = newUuid();
      const attemptId = newAttemptId();

      await this.attempts.insertPending(tx, {
        id: attemptRowId,
        deliveryId: claimed.id,
        attemptNumber: claimed.attemptCount, // lifetime attempt number after increment
        cycle: claimed.cycle,
        attemptId,
        leaseOwner: claimed.leaseOwner,
        leaseGeneration: claimed.leaseGeneration,
        startedAt: now,
      });

      return {
        deliveryId: claimed.id,
        eventId: claimed.eventId,
        tenantId: claimed.tenantId,
        endpointId: claimed.endpointId,
        envelopeBytes: claimed.envelopeBytes,
        attemptRowId,
        attemptId,
        attemptNumber: claimed.attemptCount,
        cycle: claimed.cycle,
        attemptsInCycle: claimed.attemptsInCycle,
        leaseOwner: claimed.leaseOwner,
        leaseGeneration: claimed.leaseGeneration,
      };
    });
  }

  /**
   * Record the attempt outcome and transition the delivery, fenced on the lease.
   *
   * The attempt row is always updated with the true outcome so history stays
   * accurate even for a stale worker. The delivery-state write additionally
   * requires the lease to still match: if another worker recovered the expired
   * lease and bumped the generation it affects zero rows and `applied` is false.
   * On a terminal transition the lease is released in the same statement.
   */
  async completeAttempt(params: CompleteAttemptParams): Promise<CompleteAttemptResult> {
    if (!isValidCompletion(params.nextState, params.nextAttemptAt)) {
      throw new Error(
        `illegal completion: state ${params.nextState} with nextAttemptAt ${
          params.nextAttemptAt === null ? 'null' : 'set'
        }`,
      );
    }

    const now = this.clock.now();

    return this.db.withTransaction(async (tx) => {
      await this.attempts.recordResult(tx, params.attemptRowId, {
        outcome: params.outcome,
        httpStatus: params.httpStatus,
        errorCode: params.errorCode,
        responseSnippet: params.responseSnippet,
        finishedAt: now,
      });

      const applied = await this.deliveries.completeFenced(tx, {
        deliveryId: params.deliveryId,
        leaseOwner: params.leaseOwner,
        leaseGeneration: params.leaseGeneration,
        nextState: params.nextState,
        nextAttemptAt: params.nextAttemptAt,
        httpStatus: params.httpStatus,
        errorCode: params.errorCode,
        releaseLease: isTerminal(params.nextState),
        now,
      });

      return { applied };
    });
  }
}
