import type { Database } from '../../db/pool';
import { DeliveryState } from '../../domain/types';

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

interface StateCountRow {
  state: DeliveryState;
  n: number;
}

interface OldestPendingRow {
  id: string;
  state: DeliveryState;
  next_attempt_at: Date;
  overdue_ms: number;
}

interface LeaseRow {
  expired_leases: number;
  snapshot_at: Date;
}

interface WorkerRow {
  owner: string;
  in_flight: number;
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
  constructor(private readonly db: Database) {}

  async snapshot(): Promise<OpsStatus> {
    return this.db.withTransaction(async (client) => {
      // One MVCC snapshot for every counter: a delivery that transitions between
      // two statements must not be counted as both DEAD and READY in one response.
      await client.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');

      // Sequential, not concurrent: these share ONE connection, and a PoolClient
      // is a serial protocol channel. Each statement is one aggregate or one
      // bounded read over a predicate the schema indexes on purpose.
      const counts = await client.query<StateCountRow>(
        `SELECT state, count(*)::int AS n FROM deliveries GROUP BY state`,
      );

      // Shaped to match deliveries_due_idx (state IN ('READY','RETRY_WAIT') ordered
      // by next_attempt_at) so the hottest operational question - "what is the
      // queue stuck on?" - reads the partial index instead of the whole table.
      const oldest = await client.query<OldestPendingRow>(
        `SELECT id, state, next_attempt_at,
                greatest((extract(epoch FROM (now() - next_attempt_at)) * 1000)::double precision, 0)
                  AS overdue_ms
           FROM deliveries
          WHERE state IN ('READY', 'RETRY_WAIT')
          ORDER BY next_attempt_at
          LIMIT 1`,
      );

      const leases = await client.query<LeaseRow>(
        `SELECT count(*)::int AS expired_leases, now() AS snapshot_at
           FROM deliveries
          WHERE state = 'IN_FLIGHT' AND lease_expires_at < now()`,
      );

      const workers = await client.query<WorkerRow>(
        `SELECT lease_owner AS owner, count(*)::int AS in_flight
           FROM deliveries
          WHERE state = 'IN_FLIGHT' AND lease_owner IS NOT NULL
          GROUP BY lease_owner
          ORDER BY lease_owner`,
      );

      const stateCounts = new Map<string, number>(counts.rows.map((r) => [r.state, r.n]));
      const result: DeliveryCounts = { ready: 0, inFlight: 0, retryWait: 0, delivered: 0, dead: 0 };
      for (const [state, field] of COUNT_FIELDS) {
        result[field] = stateCounts.get(state) ?? 0;
      }

      const head = oldest.rows[0];
      const lease = leases.rows[0];

      return {
        snapshotAt: lease.snapshot_at.toISOString(),
        counts: result,
        oldestPending: head
          ? {
              deliveryId: head.id,
              state: head.state,
              scheduledFor: head.next_attempt_at.toISOString(),
              overdueMs: Math.round(head.overdue_ms),
            }
          : null,
        expiredLeases: lease.expired_leases,
        workers: workers.rows.map((w) => ({ owner: w.owner, inFlight: w.in_flight })),
      };
    });
  }
}
