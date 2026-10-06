import type { PoolClient } from 'pg';
import type { Database } from '../../db/pool';
import type { Clock } from '../../common/clock';
import { newUuid } from '../../common/ids';
import { notFound } from '../../common/errors';
import { buildEnvelope } from '../webhooks/envelope';
import { DeliveryState } from '../../domain/types';
import type { PublishEventInput } from './dto';

/** Result of a successful (or replayed) publication. */
export interface PublishResult {
  eventId: string;
  deliveryId: string;
  status: DeliveryState;
  statusUrl: string;
}

interface EndpointRow {
  id: string;
  tenant_id: string;
  url: string;
  secret: string;
}

/**
 * Event publication and read model.
 *
 * The publish path commits the event AND its single logical delivery in ONE
 * transaction, so there is never an event without a delivery or vice versa
 * (deliveries.event_id is UNIQUE). The webhook envelope bytes are built once here
 * and persisted for byte-identical reuse on every attempt.
 */
export class EventsService {
  constructor(
    private readonly db: Database,
    private readonly clock: Clock,
  ) {}

  /**
   * Load and verify endpoint ownership. Unknown endpoint OR an endpoint owned by
   * another tenant both yield 404 so cross-tenant existence is never leaked.
   */
  private async loadOwnedEndpoint(
    client: PoolClient,
    endpointId: string,
    tenantId: string,
  ): Promise<EndpointRow> {
    const { rows } = await client.query<EndpointRow>(
      'SELECT id, tenant_id, url, secret FROM endpoints WHERE id = $1',
      [endpointId],
    );
    const ep = rows[0];
    if (!ep || ep.tenant_id !== tenantId) {
      throw notFound('Endpoint not found');
    }
    return ep;
  }

  /**
   * Atomically create an event and its delivery. Returns the ids and the initial
   * state. Idempotency (M5) wraps this within the same transaction.
   */
  async publish(tenantId: string, input: PublishEventInput): Promise<PublishResult> {
    return this.db.withTransaction(async (client) => {
      await this.loadOwnedEndpoint(client, input.endpointId, tenantId);
      return this.insertEventAndDelivery(client, tenantId, input);
    });
  }

  /** Insert event + delivery inside an existing transaction client. */
  async insertEventAndDelivery(
    client: PoolClient,
    tenantId: string,
    input: PublishEventInput,
  ): Promise<PublishResult> {
    const eventId = newUuid();
    const deliveryId = newUuid();
    const occurredAt = this.clock.now();

    const { bytes, hash } = buildEnvelope({
      eventId,
      deliveryId,
      eventType: input.eventType,
      occurredAt,
      payload: input.payload,
    });

    await client.query(
      `INSERT INTO events (id, tenant_id, endpoint_id, event_type, payload, occurred_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [eventId, tenantId, input.endpointId, input.eventType, JSON.stringify(input.payload), occurredAt],
    );

    await client.query(
      `INSERT INTO deliveries
         (id, event_id, tenant_id, endpoint_id, state, envelope_bytes, envelope_hash,
          next_attempt_at, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`,
      [
        deliveryId,
        eventId,
        tenantId,
        input.endpointId,
        DeliveryState.READY,
        bytes,
        hash,
        occurredAt, // due immediately
        occurredAt,
      ],
    );

    return {
      eventId,
      deliveryId,
      status: DeliveryState.READY,
      statusUrl: `/events/${eventId}`,
    };
  }

  /**
   * GET /events/:id - tenant-scoped event metadata + delivery state.
   * Unknown id OR another tenant's id both yield 404.
   */
  async getEvent(tenantId: string, eventId: string): Promise<EventDetail> {
    const { rows } = await this.db.query<EventDetailRow>(
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
    const row = rows[0];
    // Ownership check: unknown and other-tenant both map to 404 (no existence leak).
    if (!row || row.tenant_id !== tenantId) {
      throw notFound('Event not found');
    }
    return {
      eventId: row.event_id,
      endpointId: row.endpoint_id,
      eventType: row.event_type,
      occurredAt: row.occurred_at.toISOString(),
      createdAt: row.event_created_at.toISOString(),
      delivery: {
        deliveryId: row.delivery_id,
        state: row.state as DeliveryState,
        totalAttempts: row.attempt_count,
        cycle: row.cycle,
        nextAttemptAt: row.next_attempt_at ? row.next_attempt_at.toISOString() : null,
        lastHttpStatus: row.last_http_status,
        lastErrorCode: row.last_error_code,
      },
    };
  }
}

interface EventDetailRow {
  event_id: string;
  tenant_id: string;
  endpoint_id: string;
  event_type: string;
  occurred_at: Date;
  event_created_at: Date;
  delivery_id: string;
  state: string;
  attempt_count: number;
  cycle: number;
  next_attempt_at: Date | null;
  last_http_status: number | null;
  last_error_code: string | null;
}

export interface EventDetail {
  eventId: string;
  endpointId: string;
  eventType: string;
  occurredAt: string;
  createdAt: string;
  delivery: {
    deliveryId: string;
    state: DeliveryState;
    totalAttempts: number;
    cycle: number;
    nextAttemptAt: string | null;
    lastHttpStatus: number | null;
    lastErrorCode: string | null;
  };
}
