import type { Database } from '../../db/pool';
import { DeliveryState } from '../../domain/types';
import { QueueStatsRepository } from '../../db/repositories/queue-stats.repository';

/**
 * Operational counters, whole-system view (operator only).
 *
 * Everything here answers one question: is the queue keeping up? The values are
 * derived from durable state (the `deliveries` table), not from worker memory,
 * so a restarted process reports the same numbers and no counter depends on any
 * single worker being alive to report it.
 */
export interface DeliveryCounts {
  ready: number;
  inFlight: number;
  retryWait: number;
  delivered: number;
  dead: number;
}

/** The delivery the queue has failed to attempt for the longest time. */
export interface OldestPending {
  deliveryId: string;
  state: DeliveryState;
  /** When this attempt was scheduled (ISO-8601). */
  scheduledFor: string;
  /** How long past that instant we are, clamped at 0. */
  overdueMs: number;
}

export interface WorkerActivity {
  owner: string;
  inFlight: number;
}

export interface OpsStatus {
  snapshotAt: string;
  counts: DeliveryCounts;
  oldestPending: OldestPending | null;
  /**
   * In-flight rows whose lease has already lapsed: a worker died mid-attempt and
   * the lease-recovery path has not picked the work up yet. Non-zero means work is
   * parked, not lost - the delivery is still durable and its attempt history is
   * already recorded.
   */
  expiredLeases: number;
  workers: WorkerActivity[];
}

/**
 * State -> response field. Listing all five states explicitly means a state that
 * is absent from the table reports 0 instead of disappearing from the response,
 * and a future state added without updating this table shows up as a type error.
 */
const COUNT_FIELDS: ReadonlyArray<readonly [DeliveryState, keyof DeliveryCounts]> = [
  [DeliveryState.READY, 'ready'],
  [DeliveryState.IN_FLIGHT, 'inFlight'],
  [DeliveryState.RETRY_WAIT, 'retryWait'],
  [DeliveryState.DELIVERED, 'delivered'],
  [DeliveryState.DEAD, 'dead'],
];

/**
 * Time comparisons are done by the database clock (`now()`), never by comparing a
 * row timestamped by one process against another process's clock: that is how an
 * "age" metric goes negative on a box with clock skew. The application does not
 * read a clock at all for this endpoint, so there is exactly one time source per
 * computation.
 *
 * These are whole-table aggregates on purpose: the question is about the shared
 * queue, not one tenant's slice of it. Past the volume where one grouped read is
 * cheap, the same shape would be served from a periodically refreshed rollup -
 * the response contract would not change, only how it is computed.
 */
export class StatusService {
  constructor(
    private readonly db: Database,
    private readonly stats: QueueStatsRepository = new QueueStatsRepository(),
  ) {}

  async snapshot(): Promise<OpsStatus> {
    // One MVCC snapshot for every counter: a delivery that transitions between
    // two statements must not be counted as both DEAD and READY in one response.
    // Sequential, not concurrent: these share ONE connection, and a PoolClient
    // is a serial protocol channel.
    return this.db.withTransaction(
      async (tx) => {
        const counts = await this.stats.countByState(tx);
        const oldest = await this.stats.oldestPending(tx);
        const leases = await this.stats.expiredLeases(tx);
        const workers = await this.stats.inFlightByOwner(tx);

        const result: DeliveryCounts = { ready: 0, inFlight: 0, retryWait: 0, delivered: 0, dead: 0 };
        for (const [state, field] of COUNT_FIELDS) {
          result[field] = counts.get(state) ?? 0;
        }

        return {
          snapshotAt: leases.snapshotAt.toISOString(),
          counts: result,
          oldestPending: oldest
            ? {
                deliveryId: oldest.id,
                state: oldest.state,
                scheduledFor: oldest.nextAttemptAt.toISOString(),
                overdueMs: Math.round(oldest.overdueMs),
              }
            : null,
          expiredLeases: leases.count,
          workers,
        };
      },
      { isolation: 'REPEATABLE READ' },
    );
  }
}
