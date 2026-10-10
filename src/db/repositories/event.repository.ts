import type { Queryable } from '../pool';
import type { DeliveryState } from '../../domain/types';

export interface NewEvent {
  id: string;
  tenantId: string;
  endpointId: string;
  eventType: string;
  payload: unknown;
  occurredAt: Date;
}

/** Event joined with its single logical delivery (the GET /events/:id read model). */
export interface EventWithDelivery {
  eventId: string;
  tenantId: string;
  endpointId: string;
  eventType: string;
  occurredAt: Date;
  createdAt: Date;
  deliveryId: string;
  state: DeliveryState;
  attemptCount: number;
  cycle: number;
  nextAttemptAt: Date | null;
  lastHttpStatus: number | null;
  lastErrorCode: string | null;
}

interface EventWithDeliveryRow {
  event_id: string;
  tenant_id: string;
  endpoint_id: string;
  event_type: string;
  occurred_at: Date;
  event_created_at: Date;
  delivery_id: string;
  state: DeliveryState;
  attempt_count: number;
  cycle: number;
  next_attempt_at: Date | null;
  last_http_status: number | null;
  last_error_code: string | null;
}

export class EventRepository {
  async insert(q: Queryable, event: NewEvent): Promise<void> {
    await q.query(
      `INSERT INTO events (id, tenant_id, endpoint_id, event_type, payload, occurred_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        event.id,
        event.tenantId,
        event.endpointId,
        event.eventType,
        JSON.stringify(event.payload),
        event.occurredAt,
      ],
    );
  }

  /**
   * Looks the event up by id ONLY. Tenant ownership is the caller's decision, so
   * "unknown" and "someone else's" can be mapped to one indistinguishable 404.
   */
  async findWithDelivery(q: Queryable, eventId: string): Promise<EventWithDelivery | null> {
    const { rows } = await q.query<EventWithDeliveryRow>(
      `SELECT
         e.id            AS event_id,
         e.tenant_id     AS tenant_id,
         e.endpoint_id   AS endpoint_id,
         e.event_type    AS event_type,
         e.occurred_at   AS occurred_at,
         e.created_at    AS event_created_at,
         d.id            AS delivery_id,
         d.state         AS state,
         d.attempt_count AS attempt_count,
         d.cycle         AS cycle,
         d.next_attempt_at AS next_attempt_at,
         d.last_http_status AS last_http_status,
         d.last_error_code  AS last_error_code
       FROM events e
       JOIN deliveries d ON d.event_id = e.id
       WHERE e.id = $1`,
      [eventId],
    );
    const r = rows[0];
    if (!r) return null;
    return {
      eventId: r.event_id,
      tenantId: r.tenant_id,
      endpointId: r.endpoint_id,
      eventType: r.event_type,
      occurredAt: r.occurred_at,
      createdAt: r.event_created_at,
      deliveryId: r.delivery_id,
      state: r.state,
      attemptCount: r.attempt_count,
      cycle: r.cycle,
      nextAttemptAt: r.next_attempt_at,
      lastHttpStatus: r.last_http_status,
      lastErrorCode: r.last_error_code,
    };
  }
}
