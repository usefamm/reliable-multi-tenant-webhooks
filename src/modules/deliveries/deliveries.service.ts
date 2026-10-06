import type { Database } from '../../db/pool';
import { DeliveryState } from '../../domain/types';
import { encodeCursor, type DeliveryQuery } from './dto';

/** A single delivery in the list response. Never contains secrets or envelopes. */
export interface DeliveryListItem {
  deliveryId: string;
  eventId: string;
  endpointId: string;
  state: DeliveryState;
  attemptCount: number;
  cycle: number;
  attemptsInCycle: number;
  nextAttemptAt: string | null;
  lastHttpStatus: number | null;
  lastErrorCode: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface DeliveryListResult {
  data: DeliveryListItem[];
  pagination: {
    limit: number;
    nextCursor: string | null;
  };
}

interface DeliveryListRow {
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

/**
 * Tenant-scoped delivery listing.
 *
 * Isolation: the WHERE clause always pins tenant_id to the authenticated
 * principal's tenant, so another tenant's deliveries are never returned.
 *
 * Pagination: stable keyset pagination over (created_at DESC, id DESC). We fetch
 * limit+1 rows to detect whether a further page exists without a COUNT(*), then
 * hand back an opaque cursor pointing at the last returned row. Keyset (not
 * OFFSET) keeps ordering stable and cost constant as new deliveries arrive.
 *
 * The response never includes envelope bytes, endpoint URLs, or secrets.
 */
export class DeliveriesService {
  constructor(private readonly db: Database) {}

  async list(tenantId: string, query: DeliveryQuery): Promise<DeliveryListResult> {
    const params: unknown[] = [tenantId];
    const clauses: string[] = ['tenant_id = $1'];

    if (query.state) {
      params.push(query.state);
      clauses.push(`state = $${params.length}`);
    }

    if (query.cursor) {
      params.push(query.cursor.createdAt, query.cursor.id);
      // Row comparison matches the (created_at DESC, id DESC) ordering.
      clauses.push(`(created_at, id) < ($${params.length - 1}, $${params.length})`);
    }

    // Fetch one extra row to decide if there is a next page.
    params.push(query.limit + 1);

    const sql = `
      SELECT id, event_id, endpoint_id, state, attempt_count, cycle, attempts_in_cycle,
             next_attempt_at, last_http_status, last_error_code, created_at, updated_at
        FROM deliveries
       WHERE ${clauses.join(' AND ')}
       ORDER BY created_at DESC, id DESC
       LIMIT $${params.length}`;

    const { rows } = await this.db.query<DeliveryListRow>(sql, params);

    const hasMore = rows.length > query.limit;
    const pageRows = hasMore ? rows.slice(0, query.limit) : rows;

    const data = pageRows.map(toListItem);
    const lastRow = pageRows[pageRows.length - 1];
    const nextCursor = hasMore && lastRow ? encodeCursor(lastRow.created_at, lastRow.id) : null;

    return { data, pagination: { limit: query.limit, nextCursor } };
  }
}

function toListItem(row: DeliveryListRow): DeliveryListItem {
  return {
    deliveryId: row.id,
    eventId: row.event_id,
    endpointId: row.endpoint_id,
    state: row.state,
    attemptCount: row.attempt_count,
    cycle: row.cycle,
    attemptsInCycle: row.attempts_in_cycle,
    nextAttemptAt: row.next_attempt_at ? row.next_attempt_at.toISOString() : null,
    lastHttpStatus: row.last_http_status,
    lastErrorCode: row.last_error_code,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}
