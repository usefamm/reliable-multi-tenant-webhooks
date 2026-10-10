/**
 * PDF acceptance test 5 - retry scheduling.
 *
 * Verbatim requirement: "Retry scheduling: Verify 503 recovery, 429 delay,
 * five-attempt exhaustion and immediate DEAD on 400. Restart during RETRY_WAIT
 * and preserve schedule and budget."
 *
 * Each of those is proven individually, against exact timestamps, in
 * test/integration/delivery-loop.spec.ts (the 1s/2s/4s/8s ladder, Retry-After
 * honoured and capped at 60s, five attempts then DEAD, 400 straight to DEAD,
 * restart during RETRY_WAIT). This file adds the two combinations that suite
 * does not cover: ONE cycle whose attempts see different failure classes, and a
 * retry-wait restart for work that entered the queue through the real
 * publication transaction rather than a fixture row.
 */
import type { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { DeliveryState } from '../../src/domain/types';
import { ReceiverMode } from '../../src/receiver/repository';
import { createTestApp } from '../helpers/app';
import { resetDatabase } from '../helpers/db';
import { startReceiver, type ReceiverApp } from '../helpers/receiver';
import { startLoop, type DeliveryLoop, type LoopOptions } from '../helpers/delivery-loop';
import { BASE_MS } from '../helpers/worker';
import { TEST_DATABASE_URL, TEST_TOKENS } from '../helpers/test-env';

const SECOND = 1_000;

describe('PDF test 5: retry scheduling', () => {
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

  it('re-times one cycle that mixes a 429 and a 503, then recovers on the ladder', async () => {
    const loop = await newLoop({ owner: 'worker-t5' });
    const published = await loop.publish();
    const { deliveryId, eventId } = published;
    // Event-scoped "rate limited, once" beats the endpoint-wide "always 503",
    // so attempt 1 is a 429 and every later attempt a 503 until it recovers:
    // one cycle that exercises both retry classes in a known order.
    await receiver.repo.setMode({
      endpointId: loop.endpointId,
      eventId,
      mode: ReceiverMode.RATE_LIMITED,
      remaining: 1,
      retryAfter: 20,
    });
    await receiver.repo.setMode({ endpointId: loop.endpointId, mode: ReceiverMode.TEMP_FAILURE });
    loop.start();

    // Attempt 1: max(normal 1s backoff, Retry-After 20s) = 20s from the attempt.
    const one = await loop.waitForAttempts(deliveryId, 1);
    expect(one[0]).toMatchObject({ outcome: 'RETRYABLE', http_status: 429, error_code: 'http_429' });
    expect((await loop.delivery(deliveryId)).next_attempt_at?.getTime()).toBe(BASE_MS + 20 * SECOND);

    await loop.advanceToDue(deliveryId);
    // Attempt 2: the counted 429 is spent, so the 503 answers. The ladder is
    // indexed by the LIFETIME attempt number (2 -> 2s), not reset by the 429.
    const two = await loop.waitForAttempts(deliveryId, 2);
    expect(two[1]).toMatchObject({ attempt_number: 2, outcome: 'RETRYABLE', http_status: 503 });
    const after2 = await loop.delivery(deliveryId);
    expect(after2).toMatchObject({ state: DeliveryState.RETRY_WAIT, attempts_in_cycle: 2, cycle: 1 });
    expect(after2.next_attempt_at?.getTime()).toBe(BASE_MS + 22 * SECOND);

    await loop.advanceToDue(deliveryId);
    // Attempt 3: receiver healthy again. The budget was never re-issued.
    await receiver.repo.clearModes();
    const done = await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);
    const attempts = await loop.attempts(deliveryId);

    expect(done.attempt_count).toBe(3);
    expect(attempts.map((a) => a.outcome)).toEqual(['RETRYABLE', 'RETRYABLE', 'SUCCESS']);
    expect(attempts.map((a) => a.http_status)).toEqual([429, 503, 200]);
    expect(attempts.map((a) => a.cycle)).toEqual([1, 1, 1]);
    // Three dispatches, one business effect: the retries were never re-applied.
    expect(await receiver.repo.listRequests(loop.endpointId)).toHaveLength(3);
    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
  });

  it('preserves schedule and budget across a worker restart for API-published work', async () => {
    const loop = await newLoop({ owner: 'worker-before' });
    // The API schedules next_attempt_at with its own clock, so the app must share
    // the loop's: two clocks and the retry is permanently underived.
    app = await createTestApp({ clock: receiver.clock });
    const res = await request(app.getHttpServer())
      .post('/events')
      .set({ Authorization: `Bearer ${TEST_TOKENS.tenantA}` })
      .set('Idempotency-Key', 'restart-during-retry-wait')
      .send({
        endpointId: loop.endpointId,
        eventType: 'order.created',
        payload: { orderId: 'ord_t5' },
      })
      .expect(202);
    const { deliveryId, eventId } = res.body;

    await receiver.repo.setMode({
      endpointId: loop.endpointId,
      mode: ReceiverMode.TEMP_FAILURE,
      remaining: 2,
    });
    loop.start();

    await loop.waitForAttempts(deliveryId, 1);
    const waiting = await loop.delivery(deliveryId);
    expect(waiting).toMatchObject({ state: DeliveryState.RETRY_WAIT, attempts_in_cycle: 1, cycle: 1 });
    expect(waiting.next_attempt_at?.getTime()).toBe(BASE_MS + SECOND);

    // Kill the worker while the delivery waits, then replace it.
    await loop.worker.stop();
    const replacement = await newLoop({ owner: 'worker-after' });
    // The replacement must not attempt early: the due gate survived the restart.
    // It is only "not early" if the worker really looked, so wait for several
    // completed claim passes instead of guessing a wall-clock pause.
    replacement.start();
    await replacement.waitForIdlePolls();
    expect(await replacement.attempts(deliveryId)).toHaveLength(1);

    await replacement.advanceToDue(deliveryId);
    await replacement.waitForAttempts(deliveryId, 2);
    const second = (await replacement.attempts(deliveryId))[1];
    expect(second).toMatchObject({ attempt_number: 2, cycle: 1, outcome: 'RETRYABLE' });
    expect((await replacement.delivery(deliveryId)).attempts_in_cycle).toBe(2);

    await replacement.advanceToDue(deliveryId);
    const done = await replacement.waitForState(deliveryId, [DeliveryState.DELIVERED]);
    expect(done).toMatchObject({ state: DeliveryState.DELIVERED, attempt_count: 3, cycle: 1 });
    expect((await replacement.attempts(deliveryId))[2].lease_owner).toBe('worker-after');

    // Still the same event the publication transaction committed, now readable
    // as delivered through the tenant API.
    const stored = await request(app.getHttpServer())
      .get(`/events/${eventId}`)
      .set({ Authorization: `Bearer ${TEST_TOKENS.tenantA}` })
      .expect(200);
    expect(stored.body.delivery).toMatchObject({
      deliveryId,
      state: DeliveryState.DELIVERED,
      totalAttempts: 3,
      cycle: 1,
    });
    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
  });
});
