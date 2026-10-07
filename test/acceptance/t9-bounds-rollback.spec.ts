/**
 * PDF acceptance test 9 - bounds and rollback.
 *
 * Verbatim requirement: "Bounds and rollback: Prove per-worker concurrency and
 * timeout bounds. Reject redirects without contacting their target. Inject a
 * local write failure and prove atomic rollback of event creation or completion
 * state."
 *
 * Bounds are measured against real sockets - the counter is maintained by the
 * destination, not by the sender - and the injected failures are real database
 * errors raised by a trigger, so the rollback is Postgres' own, not a stub.
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
import { startCaptureEndpoint, type CaptureEndpoint } from '../helpers/capture-endpoint';
import { BASE_MS } from '../helpers/worker';
import { SEED, TEST_DATABASE_URL, TEST_TOKENS } from '../helpers/test-env';

const TRIGGERS = [
  { fn: 'inject_delivery_write_failure', trigger: 'inject_delivery_write_failure' },
  { fn: 'inject_completion_write_failure', trigger: 'inject_completion_write_failure' },
];

describe('PDF test 9: bounds and rollback', () => {
  let receiver: ReceiverApp;
  let loops: DeliveryLoop[];
  let captures: CaptureEndpoint[];
  let app: INestApplication | null;

  beforeEach(async () => {
    await resetDatabase();
    receiver = await startReceiver({ databaseUrl: TEST_DATABASE_URL });
    receiver.clock.set(BASE_MS);
    loops = [];
    captures = [];
    app = null;
  });

  afterEach(async () => {
    for (const loop of loops) await loop.close();
    for (const capture of captures) await capture.close();
    if (app) await app.close();
    await receiver.close();
    await dropTriggers();
  });

  async function newCapture(responder?: Parameters<typeof startCaptureEndpoint>[0]): Promise<CaptureEndpoint> {
    const capture = await startCaptureEndpoint(responder, SEED.endpointSecretA1);
    captures.push(capture);
    return capture;
  }

  async function newLoop(opts: LoopOptions = {}): Promise<DeliveryLoop> {
    const loop = await startLoop(receiver, opts);
    loops.push(loop);
    return loop;
  }

  async function dropTriggers(): Promise<void> {
    for (const t of TRIGGERS) {
      await q(`DROP TRIGGER IF EXISTS ${t.trigger} ON deliveries`);
      await q(`DROP FUNCTION IF EXISTS ${t.fn}()`);
    }
  }

  it('never lets one worker exceed its configured outbound concurrency', async () => {
    // Every request is held open for 120ms, so a worker with 4 permits can only
    // ever have 4 in flight. The destination counts what it actually sees.
    const capture = await newCapture(() => ({ status: 200, body: '{"ok":true}', delayMs: 120 }));
    const loop = await newLoop({ owner: 'worker-bounded', concurrency: 4, destinationUrl: capture.url('/hook') });

    const ids: string[] = [];
    for (let i = 0; i < 12; i += 1) {
      ids.push((await loop.publish({ payload: { orderId: `ord_bounded_${i}` } })).deliveryId);
    }
    loop.start();
    for (const id of ids) await loop.waitForState(id, [DeliveryState.DELIVERED]);

    expect(capture.requests).toHaveLength(12);
    expect(capture.maxConcurrent()).toBeLessThanOrEqual(4);
    // ...and the bound was genuinely contended, otherwise the assertion is empty.
    expect(capture.maxConcurrent()).toBeGreaterThan(1);

    for (const id of ids) expect(await loop.attempts(id)).toHaveLength(1);
    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(0);
  });

  it('gives up on a stalled destination inside the configured timeout, then recovers', async () => {
    const capture = await newCapture(() => ({ status: 200, body: '{"ok":true}', delayMs: 1_500 }));
    const loop = await newLoop({
      owner: 'worker-timed-out',
      timeoutMs: 150,
      destinationUrl: capture.url('/hook'),
    });
    const { deliveryId } = await loop.publish();

    const startedAt = Date.now();
    loop.start();
    const first = await loop.waitForAttempts(deliveryId, 1);
    const elapsed = Date.now() - startedAt;

    expect(first[0]).toMatchObject({ outcome: 'RETRYABLE', error_code: 'timeout', http_status: null });
    // The client abandoned the call at its own deadline instead of waiting 1.5s
    // for a reply it could not use. Generous ceiling: this proves the bound, not
    // the exact millisecond.
    expect(elapsed).toBeLessThan(1_000);
    expect((await loop.delivery(deliveryId)).state).toBe(DeliveryState.RETRY_WAIT);

    // A timeout is uncertainty, not a failure: once the destination answers
    // promptly the retry delivers, and the late reply changed nothing.
    capture.setResponder(() => ({ status: 200, body: '{"ok":true}' }));
    await loop.advanceToDue(deliveryId);
    const done = await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);
    expect(done).toMatchObject({ state: DeliveryState.DELIVERED, attempt_count: 2 });
    expect((await loop.attempts(deliveryId)).map((a) => a.outcome)).toEqual(['RETRYABLE', 'SUCCESS']);
  });

  it('rejects a redirect without contacting the target it points at', async () => {
    // A LIVE redirect target: "we did not follow it" has to be proven against a
    // server that would have answered.
    const target = await newCapture();
    const origin = await newCapture(() => ({
      status: 302,
      body: '',
      headers: { location: target.url('/should-never-be-fetched') },
    }));
    const loop = await newLoop({ owner: 'worker-redirect', destinationUrl: origin.url('/hook') });
    const { deliveryId } = await loop.publish();
    loop.start();

    const dead = await loop.waitForState(deliveryId, [DeliveryState.DEAD]);
    expect(dead).toMatchObject({
      state: DeliveryState.DEAD,
      attempt_count: 1,
      last_http_status: 302,
      last_error_code: 'redirect',
    });
    expect(origin.requests).toHaveLength(1);
    // The caller cannot turn the delivery worker into an SSRF proxy: the
    // Location value was never fetched.
    expect(target.requests).toHaveLength(0);
  });

  it('rolls the whole publication back when a local write fails, without consuming the key', async () => {
    await q(
      `CREATE OR REPLACE FUNCTION inject_delivery_write_failure() RETURNS trigger AS $plpgsql$
         BEGIN
           IF EXISTS (SELECT 1 FROM events e WHERE e.id = NEW.event_id AND e.payload ? '__injectFailure') THEN
             RAISE EXCEPTION 'injected write failure' USING ERRCODE = 'XX000';
           END IF;
           RETURN NEW;
         END;
       $plpgsql$ LANGUAGE plpgsql`,
    );
    await q(
      `CREATE TRIGGER inject_delivery_write_failure BEFORE INSERT ON deliveries
         FOR EACH ROW EXECUTE FUNCTION inject_delivery_write_failure()`,
    );

    app = await createTestApp({ clock: receiver.clock });
    const http = app.getHttpServer();
    const auth = { Authorization: `Bearer ${TEST_TOKENS.tenantA}` };

    const failed = await request(http)
      .post('/events')
      .set(auth)
      .set('Idempotency-Key', 'rollback-key')
      .send({
        endpointId: SEED.endpointA1,
        eventType: 'order.created',
        payload: { orderId: 'ord_rollback', __injectFailure: true },
      });
    expect(failed.status).toBe(500);
    expect(failed.body).toMatchObject({ code: 'internal_error' });
    expect(failed.body.stack).toBeUndefined();
    expect(JSON.stringify(failed.body)).not.toMatch(/plpgsql|trigger|inject_delivery/);

    // Atomic: the event row that was inserted earlier in the SAME transaction is
    // gone as well, and the idempotency claim rolled back with it.
    const durable = await q<{ events: number; deliveries: number; claims: number }>(
      `SELECT (SELECT count(*) FROM events)::int        AS events,
              (SELECT count(*) FROM deliveries)::int    AS deliveries,
              (SELECT count(*) FROM idempotency_records)::int AS claims`,
    );
    expect(durable[0]).toEqual({ events: 0, deliveries: 0, claims: 0 });

    await dropTriggers();

    // The key was never consumed, so the same call now succeeds for real.
    const ok = await request(http)
      .post('/events')
      .set(auth)
      .set('Idempotency-Key', 'rollback-key')
      .send({ endpointId: SEED.endpointA1, eventType: 'order.created', payload: { orderId: 'ord_rollback' } })
      .expect(202);
    expect(ok.body.eventId).toBeTruthy();
    expect(
      await q<{ n: number }>('SELECT count(*)::int AS n FROM deliveries WHERE event_id = $1', [ok.body.eventId]),
    ).toEqual([{ n: 1 }]);
  });

  it('rolls a completion write back entirely, leaving the lease and history untouched', async () => {
    await q(
      `CREATE OR REPLACE FUNCTION inject_completion_write_failure() RETURNS trigger AS $plpgsql$
         BEGIN
           IF NEW.state = 'DELIVERED'
              AND EXISTS (SELECT 1 FROM events e WHERE e.id = NEW.event_id AND e.payload ? '__injectFailure') THEN
             RAISE EXCEPTION 'injected completion write failure' USING ERRCODE = 'XX000';
           END IF;
           RETURN NEW;
         END;
       $plpgsql$ LANGUAGE plpgsql`,
    );
    await q(
      `CREATE TRIGGER inject_completion_write_failure BEFORE UPDATE ON deliveries
         FOR EACH ROW EXECUTE FUNCTION inject_completion_write_failure()`,
    );

    const loop = await newLoop({ owner: 'worker-rollback' });
    const { deliveryId } = await loop.publish({ payload: { orderId: 'ord_t9', __injectFailure: true } });

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
    const work = await queue.claimNext('worker-rollback', 30_000);
    const result = await processor(work!);
    expect(result.outcome).toBe('SUCCESS');

    // The completion transaction writes the attempt row FIRST and the delivery
    // state SECOND. Failing on the second write must undo the first.
    await expect(
      queue.completeAttempt({
        deliveryId,
        attemptRowId: work!.attemptRowId,
        leaseOwner: work!.leaseOwner,
        leaseGeneration: work!.leaseGeneration,
        outcome: result.outcome,
        httpStatus: result.httpStatus,
        errorCode: result.errorCode,
        responseSnippet: result.responseSnippet,
        nextState: DeliveryState.DELIVERED,
        nextAttemptAt: null,
      }),
    ).rejects.toThrow(/injected completion write failure/);

    const mid = await q<{ state: string; lease_owner: string; attempt_count: number }>(
      'SELECT state, lease_owner, attempt_count FROM deliveries WHERE id = $1',
      [deliveryId],
    );
    expect(mid[0]).toMatchObject({ state: DeliveryState.IN_FLIGHT, lease_owner: 'worker-rollback' });
    const attempts = await q<{ outcome: string; finished_at: Date | null; http_status: number | null }>(
      'SELECT outcome, finished_at, http_status FROM delivery_attempts WHERE delivery_id = $1',
      [deliveryId],
    );
    expect(attempts).toHaveLength(1);
    expect(attempts[0]).toMatchObject({ outcome: 'UNKNOWN', http_status: null });
    expect(attempts[0].finished_at).toBeNull();

    // With the failure removed, the same lease can be completed normally: the
    // work was never lost, only un-acknowledged.
    await dropTriggers();
    const completed = await queue.completeAttempt({
      deliveryId,
      attemptRowId: work!.attemptRowId,
      leaseOwner: work!.leaseOwner,
      leaseGeneration: work!.leaseGeneration,
      outcome: result.outcome,
      httpStatus: result.httpStatus,
      errorCode: result.errorCode,
      responseSnippet: result.responseSnippet,
      nextState: DeliveryState.DELIVERED,
      nextAttemptAt: null,
    });
    expect(completed.applied).toBe(true);
    expect(await loop.delivery(deliveryId)).toMatchObject({ state: DeliveryState.DELIVERED, lease_owner: null });
  });
});
