import type { Database } from '../../db/pool';
import { DeliveryState } from '../../domain/types';
import { DeliveryRepository, type DeliverySummary } from '../../db/repositories/delivery.repository';
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
  constructor(
    private readonly db: Database,
    private readonly deliveries: DeliveryRepository = new DeliveryRepository(),
  ) {}

  async list(tenantId: string, query: DeliveryQuery): Promise<DeliveryListResult> {
    // Fetch one extra row to decide if there is a next page.
    const rows = await this.deliveries.listForTenant(this.db, tenantId, {
      state: query.state,
      after: query.cursor,
      limit: query.limit + 1,
    });

    const hasMore = rows.length > query.limit;
    const pageRows = hasMore ? rows.slice(0, query.limit) : rows;

    const data = pageRows.map(toListItem);
    const lastRow = pageRows[pageRows.length - 1];
    const nextCursor = hasMore && lastRow ? encodeCursor(lastRow.createdAt, lastRow.id) : null;

    return { data, pagination: { limit: query.limit, nextCursor } };
  }
}

function toListItem(row: DeliverySummary): DeliveryListItem {
  return {
    deliveryId: row.id,
    eventId: row.eventId,
    endpointId: row.endpointId,
    state: row.state,
    attemptCount: row.attemptCount,
    cycle: row.cycle,
    attemptsInCycle: row.attemptsInCycle,
    nextAttemptAt: row.nextAttemptAt ? row.nextAttemptAt.toISOString() : null,
    lastHttpStatus: row.lastHttpStatus,
    lastErrorCode: row.lastErrorCode,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
  };
}
