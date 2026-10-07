/**
 * PDF acceptance test 6 - crash boundaries.
 *
 * Verbatim requirement: "Crash boundaries: Crash after publication commit and
 * before dispatch; recover accepted work. Crash after dispatch but before
 * completion commit; recover without losing history or duplicating receiver
 * effects."
 *
 * A crash is modelled the only honest way in a test: work is durably committed,
 * then the code that would have moved it on simply never runs - no completion
 * write, no shutdown drain, nothing. Recovery is then done by a DIFFERENT worker
 * identity that finds the row through the queue's own rules (due READY work, or
 * IN_FLIGHT work whose lease has expired).
 */
import type { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { DeliveryState } from '../../src/domain/types';
import { WebhookClient } from '../../src/modules/webhooks/webhook.client';
import { createWebhookProcessor } from '../../src/worker/processor';
import { DeliveryQueue } from '../../src/worker/delivery-queue';
import { createTestApp } from '../helpers/app';
import { q, resetDatabase } from '../helpers/db';
import { startReceiver, type ReceiverApp } from '../helpers/receiver';
import { startLoop, type DeliveryLoop, type LoopOptions } from '../helpers/delivery-loop';
import { BASE_MS } from '../helpers/worker';
import { TEST_DATABASE_URL, TEST_TOKENS } from '../helpers/test-env';

const LEASE_TTL_MS = 30_000;

describe('PDF test 6: crash boundaries', () => {
  let receiver: ReceiverApp;
  let loops: DeliveryLoop[];
  let app: INestApplication | null;

  beforeEach(async () => {
    await resetDatabase();
    receiver = await startReceiver({ databaseUrl: TEST_DATABASE_URL });
    receiver.clock.set(BASE_MS);
    loops = [];
    app = null;
  });

  afterEach(async () => {
    for (const loop of loops) await loop.close();
    if (app) await app.close();
    await receiver.close();
  });

  async function newLoop(opts: LoopOptions = {}): Promise<DeliveryLoop> {
    const loop = await startLoop(receiver, opts);
    loops.push(loop);
    return loop;
  }

  async function attemptRows(deliveryId: string) {
    return q<{
      attempt_number: number;
      outcome: string;
      http_status: number | null;
      finished_at: Date | null;
      lease_owner: string | null;
      lease_generation: string;
    }>(
      `SELECT attempt_number, outcome, http_status, finished_at, lease_owner, lease_generation
         FROM delivery_attempts WHERE delivery_id = $1 ORDER BY attempt_number`,
      [deliveryId],
    );
  }

  async function publishThroughApi(endpointId: string, key: string) {
    app = await createTestApp({ clock: receiver.clock });
    const res = await request(app.getHttpServer())
      .post('/events')
      .set({ Authorization: `Bearer ${TEST_TOKENS.tenantA}` })
      .set('Idempotency-Key', key)
      .send({ endpointId, eventType: 'order.created', payload: { orderId: `ord_${key}` } })
      .expect(202);
    return res.body as { eventId: string; deliveryId: string };
  }

  it('recovers accepted work when the API process dies after the publication commit', async () => {
    // No worker has ever run: the publication is the only thing on record.
    const loop = await newLoop({ owner: 'worker-after-crash' });
    const { eventId, deliveryId } = await publishThroughApi(loop.endpointId, 'crash-before-dispatch');

    const queued = await q<{ state: string; next_attempt_at: Date; attempt_count: number }>(
      'SELECT state, next_attempt_at, attempt_count FROM deliveries WHERE id = $1',
      [deliveryId],
    );
    expect(queued[0]).toMatchObject({ state: DeliveryState.READY, attempt_count: 0 });
    expect(queued[0].next_attempt_at.getTime()).toBe(BASE_MS);
    expect(await attemptRows(deliveryId)).toHaveLength(0);
    expect(await receiver.repo.listRequests(loop.endpointId)).toHaveLength(0);

    // The caller's 202 was a durable promise, not a memory: a worker that starts
    // after the crash finds the work and delivers it. Nothing was silently lost.
    loop.start();
    const done = await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);
    expect(done).toMatchObject({ state: DeliveryState.DELIVERED, attempt_count: 1, cycle: 1 });
    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
    const effects = await q<{ event_id: string }>('SELECT event_id FROM receiver_effects');
    expect(effects.map((e) => e.event_id)).toEqual([eventId]);
  });

  it('keeps history and a single effect when the worker dies after dispatch but before the completion commit', async () => {
    const loop = await newLoop({ owner: 'worker-crashed' });
    const { deliveryId } = await loop.publish();

    // Dispatch for real, then stop before the second transaction: this is exactly
    // what a kill -9 between the HTTP call and the completion write looks like to
    // the queue.
    const queue = new DeliveryQueue(loop.db, receiver.clock);
    const processor = createWebhookProcessor({
      db: loop.db,
      clock: receiver.clock,
      client: new WebhookClient({
        WEBHOOK_TIMEOUT_MS: 2_000,
        WEBHOOK_MAX_RESPONSE_BYTES: 4_096,
        WEBHOOK_ALLOWED_HOSTS: '',
      }),
    });
    const work = await queue.claimNext('worker-crashed', LEASE_TTL_MS);
    expect(work).not.toBeNull();
    const result = await processor(work!);
    expect(result.outcome).toBe('SUCCESS');

    // Mid-crash durable state: leased IN_FLIGHT, attempt allocated and unfinished.
    const mid = await q<{ state: string; lease_owner: string; lease_expires_at: Date; attempt_count: number }>(
      'SELECT state, lease_owner, lease_expires_at, attempt_count FROM deliveries WHERE id = $1',
      [deliveryId],
    );
    expect(mid[0]).toMatchObject({ state: DeliveryState.IN_FLIGHT, lease_owner: 'worker-crashed', attempt_count: 1 });
    const abandoned = await attemptRows(deliveryId);
    expect(abandoned).toHaveLength(1);
    expect(abandoned[0]).toMatchObject({ attempt_number: 1, outcome: 'UNKNOWN', lease_owner: 'worker-crashed' });
    expect(abandoned[0].finished_at).toBeNull();
    // The receiver did apply the effect; the response died with the worker's record of it.
    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
    expect(await receiver.repo.listRequests(loop.endpointId)).toHaveLength(1);

    // The dead worker never comes back: its lease simply expires, and another
    // identity recovers the row through the queue's own due rules.
    receiver.clock.advance(LEASE_TTL_MS + 1_000);
    const recovery = await newLoop({ owner: 'worker-recovery' });
    recovery.start();
    await recovery.waitForClaim(deliveryId, 2);
    const done = await recovery.waitForState(deliveryId, [DeliveryState.DELIVERED]);

    const attempts = await attemptRows(deliveryId);
    expect(attempts.map((a) => a.attempt_number)).toEqual([1, 2]);
    expect(attempts.map((a) => a.lease_owner)).toEqual(['worker-crashed', 'worker-recovery']);
    expect(attempts[0].outcome).toBe('UNKNOWN');
    expect(attempts[0].finished_at).toBeNull();
    expect(attempts[1]).toMatchObject({ outcome: 'SUCCESS', http_status: 200 });
    expect(Number(attempts[0].lease_generation)).toBe(1);
    expect(Number(attempts[1].lease_generation)).toBe(2);
    // History preserved (both dispatches recorded), schedule and cycle untouched.
    expect(done).toMatchObject({ state: DeliveryState.DELIVERED, attempt_count: 2, cycle: 1, lease_owner: null });

    // The duplicate dispatch reached the receiver, but the business effect did
    // not happen twice.
    expect(await receiver.repo.listRequests(loop.endpointId)).toHaveLength(2);
    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
  });

  it('recovers work whose completion write was lost with the process, without re-issuing the budget', async () => {
    // Same crash, but this time the delivery was already deep into a cycle: the
    // recovery must continue the SAME cycle, not start a fresh attempt budget.
    const loop = await newLoop({ owner: 'worker-mid-cycle' });
    const { deliveryId } = await loop.publish({ payload: { orderId: 'ord_t6' } });
    const queue = new DeliveryQueue(loop.db, receiver.clock);

    const first = await queue.claimNext('worker-mid-cycle', LEASE_TTL_MS);
    await queue.completeAttempt({
      deliveryId,
      attemptRowId: first!.attemptRowId,
      leaseOwner: first!.leaseOwner,
      leaseGeneration: first!.leaseGeneration,
      outcome: 'RETRYABLE',
      httpStatus: 503,
      errorCode: 'http_503',
      responseSnippet: null,
      nextState: DeliveryState.RETRY_WAIT,
      nextAttemptAt: new Date(BASE_MS + 1_000),
    });
    // Time reaches the scheduled retry, so attempt 2 is genuinely claimable.
    receiver.clock.set(BASE_MS + 1_000);
    const second = await queue.claimNext('worker-mid-cycle', LEASE_TTL_MS);
    expect(second).toMatchObject({ attemptNumber: 2, attemptsInCycle: 2, cycle: 1 });
    // Process dies here: the second attempt was allocated, dispatch may or may
    // not have happened, and nothing was committed about it.

    receiver.clock.advance(LEASE_TTL_MS + 1_000);
    const recovery = await newLoop({ owner: 'worker-recovers-cycle' });
    recovery.start();
    const claimed = await recovery.waitForClaim(deliveryId, 3);
    expect(claimed[2]).toMatchObject({ attempt_number: 3, cycle: 1 });

    const state = await recovery.waitForState(deliveryId, [
      DeliveryState.DELIVERED,
      DeliveryState.RETRY_WAIT,
      DeliveryState.DEAD,
    ]);
    // The lifetime counter never restarted at 1, and the cycle stayed 1: the
    // automatic budget is a fact about the row, not about the process.
    expect(state.attempt_count).toBe(3);
    expect(state.cycle).toBe(1);
    expect(state.attempts_in_cycle).toBe(3);
    expect(claimed.map((a) => Number(a.lease_generation))).toEqual([1, 2, 3]);
  });
});
