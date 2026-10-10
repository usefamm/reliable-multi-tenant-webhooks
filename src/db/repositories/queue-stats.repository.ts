import type { Queryable } from '../pool';
import type { DeliveryState } from '../../domain/types';

export interface OldestPendingRecord {
  id: string;
  state: DeliveryState;
  nextAttemptAt: Date;
  overdueMs: number;
}

/**
 * Whole-queue aggregates for the operator status endpoint. Time is compared with
 * the database's `now()`, never with an application clock, so there is a single
 * time source per computation and no skew-induced negative ages.
 */
export class QueueStatsRepository {
  async countByState(q: Queryable): Promise<Map<DeliveryState, number>> {
    const { rows } = await q.query<{ state: DeliveryState; n: number }>(
      'SELECT state, count(*)::int AS n FROM deliveries GROUP BY state',
    );
    return new Map(rows.map((r) => [r.state, r.n]));
  }

  /**
   * Shaped to match deliveries_due_idx (state IN ('READY','RETRY_WAIT') ordered
   * by next_attempt_at) so "what is the queue stuck on?" reads the partial index
   * instead of the whole table.
   */
  async oldestPending(q: Queryable): Promise<OldestPendingRecord | null> {
    const { rows } = await q.query<{
      id: string;
      state: DeliveryState;
      next_attempt_at: Date;
      overdue_ms: number;
    }>(
      `SELECT id, state, next_attempt_at,
              greatest((extract(epoch FROM (now() - next_attempt_at)) * 1000)::double precision, 0)
                AS overdue_ms
         FROM deliveries
        WHERE state IN ('READY', 'RETRY_WAIT')
        ORDER BY next_attempt_at
        LIMIT 1`,
    );
    const r = rows[0];
    return r
      ? { id: r.id, state: r.state, nextAttemptAt: r.next_attempt_at, overdueMs: r.overdue_ms }
      : null;
  }

  /** IN_FLIGHT rows whose lease already lapsed, plus the database's current time. */
  async expiredLeases(q: Queryable): Promise<{ count: number; snapshotAt: Date }> {
    const { rows } = await q.query<{ expired_leases: number; snapshot_at: Date }>(
      `SELECT count(*)::int AS expired_leases, now() AS snapshot_at
         FROM deliveries
        WHERE state = 'IN_FLIGHT' AND lease_expires_at < now()`,
    );
    return { count: rows[0].expired_leases, snapshotAt: rows[0].snapshot_at };
  }

  async inFlightByOwner(q: Queryable): Promise<Array<{ owner: string; inFlight: number }>> {
    const { rows } = await q.query<{ owner: string; in_flight: number }>(
      `SELECT lease_owner AS owner, count(*)::int AS in_flight
         FROM deliveries
        WHERE state = 'IN_FLIGHT' AND lease_owner IS NOT NULL
        GROUP BY lease_owner
        ORDER BY lease_owner`,
    );
    return rows.map((r) => ({ owner: r.owner, inFlight: r.in_flight }));
  }
}
