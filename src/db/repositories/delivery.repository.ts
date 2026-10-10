import type { Queryable } from '../pool';
import type { DeliveryState } from '../../domain/types';

export interface NewDelivery {
  id: string;
  eventId: string;
  tenantId: string;
  endpointId: string;
  state: DeliveryState;
  envelopeBytes: Buffer;
  envelopeHash: string;
  nextAttemptAt: Date;
  createdAt: Date;
}

/** A delivery as shown in tenant listings. Never carries envelope bytes or secrets. */
export interface DeliverySummary {
  id: string;
  eventId: string;
  endpointId: string;
  state: DeliveryState;
  attemptCount: number;
  cycle: number;
  attemptsInCycle: number;
  nextAttemptAt: Date | null;
  lastHttpStatus: number | null;
  lastErrorCode: string | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface DeliveryListFilter {
  state?: DeliveryState;
  /** Keyset position: rows strictly after this (createdAt, id) in listing order. */
  after?: { createdAt: string; id: string };
  limit: number;
}

/** What a redrive needs from the delivery row it locked. */
export interface LockedDelivery {
  id: string;
  eventId: string;
  state: DeliveryState;
  cycle: number;
  attemptCount: number;
}

/** A delivery taken under a lease by `claimNextDue`. */
export interface ClaimedDelivery {
  id: string;
  eventId: string;
  tenantId: string;
  endpointId: string;
  envelopeBytes: Buffer;
  attemptCount: number;
  cycle: number;
  attemptsInCycle: number;
  leaseOwner: string;
  /** bigint comes back from pg as a string. */
  leaseGeneration: string;
}

export interface FencedCompletion {
  deliveryId: string;
  leaseOwner: string;
  leaseGeneration: string;
  nextState: DeliveryState;
  nextAttemptAt: Date | null;
  httpStatus: number | null;
  errorCode: string | null;
  /** Release the lease in the same statement (terminal transitions). */
  releaseLease: boolean;
  now: Date;
}

interface DeliverySummaryRow {
  id: string;
  event_id: string;
  endpoint_id: string;
  state: DeliveryState;
  attempt_count: number;
  cycle: number;
  attempts_in_cycle: number;
  next_attempt_at: Date | null;
  last_http_status: number | null;
  last_error_code: string | null;
  created_at: Date;
  updated_at: Date;
}

interface LockedDeliveryRow {
  id: string;
  event_id: string;
  state: DeliveryState;
  cycle: number;
  attempt_count: number;
}

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
  lease_generation: string;
}

/**
 * All SQL against `deliveries`. This class executes state changes; WHICH changes
 * are legal is decided in `domain/delivery-state` and `domain/retry-policy`.
 */
export class DeliveryRepository {
  async insert(q: Queryable, d: NewDelivery): Promise<void> {
    await q.query(
      `INSERT INTO deliveries
         (id, event_id, tenant_id, endpoint_id, state, envelope_bytes, envelope_hash,
          next_attempt_at, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`,
      [
        d.id,
        d.eventId,
        d.tenantId,
        d.endpointId,
        d.state,
        d.envelopeBytes,
        d.envelopeHash,
        d.nextAttemptAt,
        d.createdAt,
      ],
    );
  }

  /**
   * Tenant-scoped page in (created_at DESC, id DESC) order. `tenant_id` is the
   * first predicate and a required argument, so the isolation filter cannot be
   * forgotten by a caller. Returns up to `limit` rows; the caller asks for one
   * extra to detect a next page.
   */
  async listForTenant(
    q: Queryable,
    tenantId: string,
    filter: DeliveryListFilter,
  ): Promise<DeliverySummary[]> {
    const params: unknown[] = [tenantId];
    const clauses: string[] = ['tenant_id = $1'];

    if (filter.state) {
      params.push(filter.state);
      clauses.push(`state = $${params.length}`);
    }

    if (filter.after) {
      params.push(filter.after.createdAt, filter.after.id);
      // Row comparison matches the (created_at DESC, id DESC) ordering.
      clauses.push(`(created_at, id) < ($${params.length - 1}, $${params.length})`);
    }

    params.push(filter.limit);

    const { rows } = await q.query<DeliverySummaryRow>(
      `SELECT id, event_id, endpoint_id, state, attempt_count, cycle, attempts_in_cycle,
              next_attempt_at, last_http_status, last_error_code, created_at, updated_at
         FROM deliveries
        WHERE ${clauses.join(' AND ')}
        ORDER BY created_at DESC, id DESC
        LIMIT $${params.length}`,
      params,
    );
    return rows.map((r) => ({
      id: r.id,
      eventId: r.event_id,
      endpointId: r.endpoint_id,
      state: r.state,
      attemptCount: r.attempt_count,
      cycle: r.cycle,
      attemptsInCycle: r.attempts_in_cycle,
      nextAttemptAt: r.next_attempt_at,
      lastHttpStatus: r.last_http_status,
      lastErrorCode: r.last_error_code,
      createdAt: r.created_at,
      updatedAt: r.updated_at,
    }));
  }

  /** The owning tenant of a delivery, or null when it does not exist. */
  async findTenantId(q: Queryable, deliveryId: string): Promise<string | null> {
    const { rows } = await q.query<{ tenant_id: string }>(
      'SELECT tenant_id FROM deliveries WHERE id = $1',
      [deliveryId],
    );
    return rows[0]?.tenant_id ?? null;
  }

  /** Row-lock a delivery for the rest of the transaction. Null when it does not exist. */
  async lockForRedrive(q: Queryable, deliveryId: string): Promise<LockedDelivery | null> {
    const { rows } = await q.query<LockedDeliveryRow>(
      `SELECT id, event_id, state, cycle, attempt_count
         FROM deliveries
        WHERE id = $1
          FOR UPDATE`,
      [deliveryId],
    );
    const r = rows[0];
    return r
      ? {
          id: r.id,
          eventId: r.event_id,
          state: r.state,
          cycle: r.cycle,
          attemptCount: r.attempt_count,
        }
      : null;
  }

  /**
   * Put a locked DEAD delivery back in the queue as a fresh automatic cycle.
   * Identity, envelope bytes and lifetime attempt_count are untouched.
   */
  async startNewCycle(q: Queryable, deliveryId: string, cycle: number, now: Date): Promise<void> {
    await q.query(
      `UPDATE deliveries
          SET state = 'READY',
              cycle = $2,
              attempts_in_cycle = 0,
              next_attempt_at = $3,
              lease_owner = NULL,
              lease_expires_at = NULL,
              updated_at = $3
        WHERE id = $1`,
      [deliveryId, cycle, now],
    );
  }

  /**
   * Take the next due delivery under a lease, or null when nothing is due.
   *
   * Due work is READY / RETRY_WAIT whose next_attempt_at has arrived, or
   * IN_FLIGHT whose lease has expired (crashed / paused worker recovery).
   * FOR UPDATE SKIP LOCKED lets concurrent workers lock different rows instead
   * of blocking or double-claiming. `lease_generation + 1` is the fencing token,
   * minted by the database so no worker (or clock) can choose its own.
   *
   * The state names are literals on purpose: they must match the partial index
   * predicates in migration 001 for the planner to use `deliveries_due_idx`.
   */
  async claimNextDue(
    q: Queryable,
    claim: { now: Date; owner: string; leaseExpiresAt: Date },
  ): Promise<ClaimedDelivery | null> {
    const { rows } = await q.query<ClaimRow>(
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
      [claim.now, claim.owner, claim.leaseExpiresAt],
    );
    const r = rows[0];
    return r
      ? {
          id: r.id,
          eventId: r.event_id,
          tenantId: r.tenant_id,
          endpointId: r.endpoint_id,
          envelopeBytes: r.envelope_bytes,
          attemptCount: r.attempt_count,
          cycle: r.cycle,
          attemptsInCycle: r.attempts_in_cycle,
          leaseOwner: r.lease_owner,
          leaseGeneration: r.lease_generation,
        }
      : null;
  }

  /**
   * Fenced state write: applies only while `(lease_owner, lease_generation)` still
   * match the claim. Returns false when another worker has since recovered the
   * lease - the stale caller must not overwrite newer state.
   */
  async completeFenced(q: Queryable, c: FencedCompletion): Promise<boolean> {
    const { rowCount } = await q.query(
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
        c.deliveryId,
        c.nextState,
        c.nextAttemptAt,
        c.httpStatus,
        c.errorCode,
        c.releaseLease,
        c.now,
        c.leaseOwner,
        c.leaseGeneration,
      ],
    );
    return (rowCount ?? 0) > 0;
  }
}
