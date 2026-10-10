import type { PoolClient } from 'pg';
import type { Database } from '../../db/pool';
import type { Clock } from '../../common/clock';
import { newUuid } from '../../common/ids';
import { notFound, payloadTooLarge } from '../../common/errors';
import { buildEnvelope } from '../webhooks/envelope';
import { DeliveryState, MAX_ENVELOPE_BYTES } from '../../domain/types';
import { DeliveryRepository } from '../../db/repositories/delivery.repository';
import { EndpointRepository } from '../../db/repositories/endpoint.repository';
import { EventRepository } from '../../db/repositories/event.repository';
import {
  IdempotencyOperation,
  IdempotencyService,
  requestFingerprint,
} from '../idempotency/idempotency.service';
import type { PublishEventInput } from './dto';

/** Result of a successful (or replayed) publication. */
export interface PublishResult {
  eventId: string;
  deliveryId: string;
  status: DeliveryState;
  statusUrl: string;
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
    private readonly idempotency: IdempotencyService,
    private readonly endpoints: EndpointRepository = new EndpointRepository(),
    private readonly events: EventRepository = new EventRepository(),
    private readonly deliveries: DeliveryRepository = new DeliveryRepository(),
  ) {}

  /**
   * Verify endpoint ownership. Unknown endpoint OR an endpoint owned by another
   * tenant both yield 404 so cross-tenant existence is never leaked.
   */
  private async assertEndpointOwned(
    client: PoolClient,
    endpointId: string,
    tenantId: string,
  ): Promise<void> {
    const owner = await this.endpoints.findOwnerTenantId(client, endpointId);
    if (owner === null || owner !== tenantId) {
      throw notFound('Endpoint not found');
    }
  }

  /**
   * Atomically create an event and its delivery, guarded by publication
   * idempotency.
   *
   * The idempotency fingerprint covers exactly the fields the PDF says must
   * match for a replay: endpointId, eventType and payload. Canonical JSON makes
   * object key order irrelevant while array order stays significant. The
   * event + delivery insert AND the idempotency record insert happen in the SAME
   * transaction, so:
   *   - a successful publish commits both atomically;
   *   - concurrent duplicates serialize on the UNIQUE(tenant, op, key) index and
   *     losers roll back their duplicate rows, then replay the winner's response;
   *   - a request that fails validation/ownership never writes a record, so it
   *     does not consume the key - the envelope-size refusal works the same way,
   *     because it throws inside this same transaction.
   */
  async publish(
    tenantId: string,
    input: PublishEventInput,
    idempotencyKey: string,
  ): Promise<PublishResult> {
    const requestHash = requestFingerprint({
      endpointId: input.endpointId,
      eventType: input.eventType,
      payload: input.payload,
    });

    const outcome = await this.idempotency.execute<PublishResult>(
      {
        tenantId,
        operation: IdempotencyOperation.PUBLISH_EVENT,
        key: idempotencyKey,
        requestHash,
      },
      async (client) => {
        await this.assertEndpointOwned(client, input.endpointId, tenantId);
        const result = await this.insertEventAndDelivery(client, tenantId, input);
        return { status: 202, body: result, resourceId: result.eventId };
      },
    );

    return outcome.body;
  }

  /**
   * Insert event + delivery inside an existing transaction client.
   *
   * The bytes validated here are the envelope, not the request body: the
   * receiver bounds what it receives, and the envelope is larger than the body
   * that produced it. The check runs before the first INSERT so a refusal rolls
   * back the event, the delivery AND the idempotency claim as one unit.
   */
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

    if (bytes.length > MAX_ENVELOPE_BYTES) {
      throw payloadTooLarge(
        `The delivery envelope would be ${bytes.length} bytes, over the ${MAX_ENVELOPE_BYTES}-byte limit a receiver accepts; reduce the payload size`,
      );
    }

    await this.events.insert(client, {
      id: eventId,
      tenantId,
      endpointId: input.endpointId,
      eventType: input.eventType,
      payload: input.payload,
      occurredAt,
    });

    await this.deliveries.insert(client, {
      id: deliveryId,
      eventId,
      tenantId,
      endpointId: input.endpointId,
      state: DeliveryState.READY,
      envelopeBytes: bytes,
      envelopeHash: hash,
      nextAttemptAt: occurredAt, // due immediately
      createdAt: occurredAt,
    });

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
    const row = await this.events.findWithDelivery(this.db, eventId);
    // Ownership check: unknown and other-tenant both map to 404 (no existence leak).
    if (!row || row.tenantId !== tenantId) {
      throw notFound('Event not found');
    }
    return {
      eventId: row.eventId,
      endpointId: row.endpointId,
      eventType: row.eventType,
      occurredAt: row.occurredAt.toISOString(),
      createdAt: row.createdAt.toISOString(),
      delivery: {
        deliveryId: row.deliveryId,
        state: row.state,
        totalAttempts: row.attemptCount,
        cycle: row.cycle,
        nextAttemptAt: row.nextAttemptAt ? row.nextAttemptAt.toISOString() : null,
        lastHttpStatus: row.lastHttpStatus,
        lastErrorCode: row.lastErrorCode,
      },
    };
  }
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
