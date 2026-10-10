import type { Database } from '../../db/pool';
import type { Clock } from '../../common/clock';
import { newUuid } from '../../common/ids';
import { conflict, notFound } from '../../common/errors';
import { canRedrive } from '../../domain/delivery-state';
import { DeliveryState } from '../../domain/types';
import { DeliveryRepository } from '../../db/repositories/delivery.repository';
import { RedriveAuditRepository } from '../../db/repositories/redrive-audit.repository';
import {
  IdempotencyOperation,
  IdempotencyService,
  requestFingerprint,
} from '../idempotency/idempotency.service';

export interface RedriveResult {
  deliveryId: string;
  eventId: string;
  state: DeliveryState;
  cycle: number;
  attemptCount: number;
  attemptsInCycle: number;
  nextAttemptAt: string;
}

export interface RedriveParams {
  deliveryId: string;
  reason: string;
  idempotencyKey: string;
  /** Operator label from the authenticated principal - never the raw token. */
  operator: string;
}

/**
 * Operator redrive: put a DEAD delivery back into the queue with a fresh
 * automatic cycle, without inventing a new event or pretending the earlier
 * attempts never happened.
 *
 * What is preserved and why:
 *  - eventId/deliveryId and envelope bytes: the receiver's dedup identity is the
 *    event, so a redrive that changed identity would apply the business effect
 *    twice.
 *  - attempt history and lifetime attempt_count: the operational record of what
 *    already was tried.
 *  - cycle is incremented and attempts_in_cycle reset to 0: the retry budget is
 *    per automatic cycle, so a redrive buys exactly RETRY_MAX_ATTEMPTS_PER_CYCLE
 *    more attempts and nothing more.
 *
 * Concurrency: the idempotency key is claimed first, then the delivery row is
 * locked FOR UPDATE. Two operators with different keys therefore cannot start
 * two cycles - the second blocks, sees a non-DEAD row and gets 409. Two requests
 * with the same key replay one response.
 */
export class RedriveService {
  constructor(
    private readonly db: Database,
    private readonly clock: Clock,
    private readonly idempotency: IdempotencyService,
    private readonly deliveries: DeliveryRepository = new DeliveryRepository(),
    private readonly audits: RedriveAuditRepository = new RedriveAuditRepository(),
  ) {}

  async redrive(params: RedriveParams): Promise<RedriveResult> {
    // The key is scoped to the tenant that owns the delivery: an operator acts on
    // a tenant's resource, and tenant_id is part of idempotency_unique. This read
    // is only key scoping; the authoritative state check happens under the row
    // lock inside the transaction below.
    const tenantId = await this.deliveries.findTenantId(this.db, params.deliveryId);
    if (!tenantId) {
      throw notFound('Delivery not found');
    }

    const requestHash = requestFingerprint({
      deliveryId: params.deliveryId,
      reason: params.reason,
    });

    const outcome = await this.idempotency.execute<RedriveResult>(
      {
        tenantId,
        operation: IdempotencyOperation.REDRIVE,
        key: params.idempotencyKey,
        requestHash,
      },
      async (client) => {
        const delivery = await this.deliveries.lockForRedrive(client, params.deliveryId);
        if (!delivery) {
          throw notFound('Delivery not found');
        }
        if (!canRedrive(delivery.state)) {
          throw conflict(
            `Only a DEAD delivery can be redriven (current state: ${delivery.state})`,
          );
        }

        const now = this.clock.now();
        const cycle = delivery.cycle + 1;
        await this.deliveries.startNewCycle(client, delivery.id, cycle, now);

        await this.audits.insert(client, {
          id: newUuid(),
          deliveryId: delivery.id,
          operator: params.operator,
          reason: params.reason,
          idempotencyKey: params.idempotencyKey,
        });

        return {
          status: 202,
          resourceId: delivery.id,
          body: {
            deliveryId: delivery.id,
            eventId: delivery.eventId,
            state: DeliveryState.READY,
            cycle,
            attemptCount: delivery.attemptCount,
            attemptsInCycle: 0,
            nextAttemptAt: now.toISOString(),
          },
        };
      },
    );

    return outcome.body;
  }
}
