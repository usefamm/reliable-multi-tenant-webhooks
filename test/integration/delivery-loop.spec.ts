/**
 * Retry engine, exercised through the real delivery loop (PDF sections 16-17 and
 * tests 4-6): durable queue + worker + outbound client + mock receiver + Postgres.
 *
 * The schedule is asserted against exact timestamps because time itself is
 * injected: a shared FakeClock drives the retry policy, the lease comparisons
 * and the signature timestamps, so "1s, 2s, 4s, 8s" is verified as data rather
 * than waited for in wall-clock time.
 */
import { DeliveryState } from '../../src/domain/types';
import { ReceiverMode } from '../../src/receiver/repository';
import { BASE_MS } from '../helpers/worker';
import { startReceiver, type ReceiverApp } from '../helpers/receiver';
import { startLoop, type DeliveryLoop, type LoopOptions } from '../helpers/delivery-loop';
import { SEED, TEST_DATABASE_URL } from '../helpers/test-env';

const SECOND = 1_000;

describe('delivery loop: retry scheduling and recovery', () => {
  let receiver: ReceiverApp;
  let loops: DeliveryLoop[] = [];

  beforeEach(async () => {
    receiver = await startReceiver({ databaseUrl: TEST_DATABASE_URL });
    await receiver.repo.resetAll();
    receiver.clock.set(BASE_MS);
    loops = [];
  });

  afterEach(async () => {
    for (const loop of loops) await loop.close();
    loops = [];
    await receiver.close();
  });

  /** Create a loop bound to this receiver; always closed by afterEach. */
  async function newLoop(opts: LoopOptions = {}): Promise<DeliveryLoop> {
    const loop = await startLoop(receiver, opts);
    loops.push(loop);
    return loop;
  }

  async function openLoop(opts: LoopOptions = {}): Promise<DeliveryLoop> {
    const loop = await newLoop(opts);
    loop.start();
    return loop;
  }

  /** Wait until the receiver has logged at least `count` requests (durable fact, not a sleep). */
  async function waitForRequests(loop: DeliveryLoop, count: number): Promise<void> {
    const deadline = Date.now() + 3_000;
    while (Date.now() < deadline) {
      if ((await receiver.repo.listRequests(loop.endpointId)).length >= count) return;
      await new Promise((r) => setTimeout(r, 5));
    }
    throw new Error(`receiver did not observe ${count} requests`);
  }

  describe('happy path', () => {
    it('delivers once, stops, and leaves a truthful attempt history', async () => {
      const loop = await openLoop();
      const { deliveryId, eventId } = await loop.publish();

      const done = await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);
      const attempts = await loop.attempts(deliveryId);

      expect(done).toMatchObject({
        state: DeliveryState.DELIVERED,
        next_attempt_at: null,
        lease_owner: null,
        attempt_count: 1,
      });
      expect(attempts).toHaveLength(1);
      expect(attempts[0]).toMatchObject({
        attempt_number: 1,
        cycle: 1,
        outcome: 'SUCCESS',
        http_status: 200,
      });
      expect(attempts[0].finished_at).not.toBeNull();

      const effects = await receiver.repo.listEffects(loop.endpointId);
      expect(effects).toHaveLength(1);
      expect(effects[0].event_id).toBe(eventId);
    });

    it('re-signs every attempt over the same bytes and never repeats an attempt id', async () => {
      const loop = await openLoop();
      await receiver.repo.setMode({
        endpointId: loop.endpointId,
        mode: ReceiverMode.TEMP_FAILURE,
        remaining: 1,
      });
      const { deliveryId, eventId } = await loop.publish();

      await loop.waitForAttempts(deliveryId, 1);
      await loop.advanceToDue(deliveryId);
      await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);

      // Both attempts passed HMAC verification; a mutated body or a replayed
      // signature would have been answered 401 instead.
      const requests = await receiver.repo.listRequests(loop.endpointId);
      expect(requests).toHaveLength(2);
      expect(requests.every((r) => r.signature_ok)).toBe(true);
      expect(requests.map((r) => r.event_id)).toEqual([eventId, eventId]);

      const attempts = await loop.attempts(deliveryId);
      expect(new Set(attempts.map((a) => a.attempt_id)).size).toBe(2);
      expect(new Set(attempts.map((a) => a.lease_generation)).size).toBe(2);
      expect(attempts.every((a) => a.lease_owner === 'loop-worker')).toBe(true);
    });
  });

  describe('backoff schedule', () => {
    it('waits 1s, 2s, 4s, 8s between the attempts of one cycle', async () => {
      const loop = await openLoop();
      await receiver.repo.setMode({
        endpointId: loop.endpointId,
        mode: ReceiverMode.TEMP_FAILURE,
        remaining: 4,
      });
      const { deliveryId } = await loop.publish();

      // Cumulative due times for attempts 2..5 given a 1s base and zero jitter.
      const expectedDue = [
        BASE_MS + 1 * SECOND,
        BASE_MS + 3 * SECOND,
        BASE_MS + 7 * SECOND,
        BASE_MS + 15 * SECOND,
      ];
      for (let n = 1; n <= 4; n += 1) {
        const attempts = await loop.waitForAttempts(deliveryId, n);
        expect(attempts[n - 1]).toMatchObject({ outcome: 'RETRYABLE', http_status: 503 });
        const current = await loop.delivery(deliveryId);
        expect(current.state).toBe(DeliveryState.RETRY_WAIT);
        expect(current.next_attempt_at?.getTime()).toBe(expectedDue[n - 1]);
        expect(current.attempts_in_cycle).toBe(n);
        await loop.advanceToDue(deliveryId);
      }

      // Attempt 5: the receiver's failure budget is spent, so it recovers.
      const done = await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);
      const attempts = await loop.attempts(deliveryId);
      expect(done.attempt_count).toBe(5);
      expect(attempts).toHaveLength(5);
      expect(attempts[4]).toMatchObject({ attempt_number: 5, outcome: 'SUCCESS', http_status: 200 });
      // Only the successful attempt changed receiver state.
      expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
    });

    it('honours Retry-After when it exceeds the normal backoff', async () => {
      const loop = await openLoop();
      await receiver.repo.setMode({
        endpointId: loop.endpointId,
        mode: ReceiverMode.RATE_LIMITED,
        retryAfter: 30,
        remaining: 1,
      });
      const { deliveryId } = await loop.publish();

      const first = await loop.waitForAttempts(deliveryId, 1);
      expect(first[0]).toMatchObject({ outcome: 'RETRYABLE', http_status: 429, error_code: 'http_429' });
      // max(normal 1s, Retry-After 30s)
      const current = await loop.delivery(deliveryId);
      expect(current.next_attempt_at?.getTime()).toBe(BASE_MS + 30 * SECOND);

      await loop.advanceToDue(deliveryId);
      await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);
    });

    it('caps a long Retry-After at the configured ceiling', async () => {
      const loop = await openLoop();
      await receiver.repo.setMode({
        endpointId: loop.endpointId,
        mode: ReceiverMode.RATE_LIMITED,
        retryAfter: 3_600,
        remaining: 1,
      });
      const { deliveryId } = await loop.publish();

      await loop.waitForAttempts(deliveryId, 1);
      const current = await loop.delivery(deliveryId);
      expect(current.next_attempt_at?.getTime()).toBe(BASE_MS + 60 * SECOND);
    });

    it('gives up after exactly five attempts in a cycle and marks the delivery DEAD', async () => {
      const loop = await openLoop();
      await receiver.repo.setMode({ endpointId: loop.endpointId, mode: ReceiverMode.PERM_FAILURE });
      const { deliveryId } = await loop.publish();

      for (let n = 1; n < 5; n += 1) {
        const attempts = await loop.waitForAttempts(deliveryId, n);
        expect(attempts[n - 1].outcome).toBe('RETRYABLE');
        await loop.advanceToDue(deliveryId);
      }
      const dead = await loop.waitForState(deliveryId, [DeliveryState.DEAD]);
      const attempts = await loop.attempts(deliveryId);

      expect(attempts).toHaveLength(5);
      expect(attempts.map((a) => a.attempt_number)).toEqual([1, 2, 3, 4, 5]);
      expect(dead).toMatchObject({
        state: DeliveryState.DEAD,
        attempt_count: 5,
        attempts_in_cycle: 5,
        cycle: 1,
        next_attempt_at: null,
        last_http_status: 500,
        // A terminal delivery holds no lease, so it is not queue work any more.
        lease_owner: null,
      });
    });

    it('sends a non-retryable rejection straight to DEAD after one attempt', async () => {
      const loop = await openLoop();
      await receiver.repo.setMode({ endpointId: loop.endpointId, mode: ReceiverMode.REJECT_400 });
      const { deliveryId } = await loop.publish();

      const dead = await loop.waitForState(deliveryId, [DeliveryState.DEAD]);
      expect(dead).toMatchObject({ attempt_count: 1, last_error_code: 'http_400' });
      expect((await loop.attempts(deliveryId))[0].outcome).toBe('NON_RETRYABLE');

      // No retry storm: the receiver saw exactly one request, and applied nothing.
      await waitForRequests(loop, 1);
      expect(await receiver.repo.listRequests(loop.endpointId)).toHaveLength(1);
      expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(0);
    });

    it('treats a redirect as non-retryable instead of following it', async () => {
      const loop = await openLoop();
      await receiver.repo.setMode({ endpointId: loop.endpointId, mode: ReceiverMode.REDIRECT });
      const { deliveryId } = await loop.publish();

      const dead = await loop.waitForState(deliveryId, [DeliveryState.DEAD]);
      expect(dead).toMatchObject({ last_http_status: 302, last_error_code: 'redirect' });
      // Exactly one request: the sender did not chase the Location header.
      expect(await receiver.repo.listRequests(loop.endpointId)).toHaveLength(1);
    });
  });

  describe('uncertain outcomes', () => {
    it('keeps a durably applied effect when the response is lost, and converges on one application', async () => {
      const loop = await openLoop();
      await receiver.repo.setMode({
        endpointId: loop.endpointId,
        mode: ReceiverMode.LOST_RESPONSE,
        remaining: 1,
      });
      const { deliveryId } = await loop.publish();

      const attempts = await loop.waitForAttempts(deliveryId, 1);
      expect(attempts[0]).toMatchObject({
        outcome: 'UNKNOWN',
        http_status: null,
        error_code: 'transport_error',
      });
      // The worker refused to invent a result, and the effect is already durable.
      expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);

      await loop.advanceToDue(deliveryId);
      await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);

      const history = await loop.attempts(deliveryId);
      expect(history.map((a) => a.outcome)).toEqual(['UNKNOWN', 'SUCCESS']);
      // At-least-once delivery, exactly-once business effect.
      expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
      expect(await receiver.repo.listRequests(loop.endpointId)).toHaveLength(2);
    });

    it('classifies a stalled receiver as a timeout, then deduplicates the retry', async () => {
      // Outbound timeout far shorter than the receiver's deliberate delay: the
      // sender gives up while the receiver is still applying the effect.
      const loop = await openLoop({ timeoutMs: 100 });
      await receiver.repo.setMode({
        endpointId: loop.endpointId,
        mode: ReceiverMode.SLOW,
        delayMs: 400,
        remaining: 1,
      });
      const { deliveryId } = await loop.publish();

      const first = await loop.waitForAttempts(deliveryId, 1);
      expect(first[0]).toMatchObject({ outcome: 'RETRYABLE', error_code: 'timeout', http_status: null });
      expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);

      await loop.advanceToDue(deliveryId);
      await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);
      expect((await loop.attempts(deliveryId)).map((a) => a.error_code)).toEqual(['timeout', null]);
      expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
    });
  });

  describe('restart during retry wait', () => {
    it('preserves the schedule and the attempt budget across a worker restart', async () => {
      const first = await openLoop();
      await receiver.repo.setMode({
        endpointId: first.endpointId,
        mode: ReceiverMode.TEMP_FAILURE,
        remaining: 2,
      });
      const { deliveryId } = await first.publish();

      await first.waitForAttempts(deliveryId, 1);
      const before = await first.delivery(deliveryId);
      expect(before.state).toBe(DeliveryState.RETRY_WAIT);
      expect(before.next_attempt_at?.getTime()).toBe(BASE_MS + SECOND);

      // Kill the worker mid retry-wait, then start a replacement over the same rows.
      await first.worker.stop();

      const replacement = await newLoop({ owner: 'worker-b' });
      expect((await replacement.delivery(deliveryId)).next_attempt_at?.getTime()).toBe(BASE_MS + SECOND);
      replacement.start();

      // The replacement must not attempt early: the due gate survived the restart.
      await new Promise((r) => setTimeout(r, 120));
      expect(await replacement.attempts(deliveryId)).toHaveLength(1);

      await replacement.advanceToDue(deliveryId);
      await replacement.waitForAttempts(deliveryId, 2);
      const second = (await replacement.attempts(deliveryId))[1];
      // Lifetime numbering and cycle are unchanged, so the budget survived too.
      expect(second).toMatchObject({ attempt_number: 2, cycle: 1, outcome: 'RETRYABLE' });
      expect((await replacement.delivery(deliveryId)).attempts_in_cycle).toBe(2);

      await replacement.advanceToDue(deliveryId);
      await replacement.waitForState(deliveryId, [DeliveryState.DELIVERED]);
      expect((await replacement.attempts(deliveryId)).map((a) => a.attempt_number)).toEqual([1, 2, 3]);
      expect((await replacement.attempts(deliveryId))[2].lease_owner).toBe('worker-b');
    });

    it('recovers an in-flight delivery whose worker stalls past its lease', async () => {
      // The receiver answers slower than the lease lives, so the row is still
      // IN_FLIGHT when its lease expires - exactly what a paused or wedged
      // worker looks like from the queue's point of view.
      const slowWorker = await newLoop({ owner: 'worker-slow', leaseTtlMs: 300 });
      await receiver.repo.setMode({
        endpointId: slowWorker.endpointId,
        mode: ReceiverMode.SLOW,
        delayMs: 1_200,
      });
      const { deliveryId } = await slowWorker.publish();

      slowWorker.start();
      const claimed = await slowWorker.waitForClaim(deliveryId, 1);
      expect(claimed[0].finished_at).toBeNull();
      expect((await slowWorker.delivery(deliveryId)).state).toBe(DeliveryState.IN_FLIGHT);

      // Time passes as it would for a stalled worker: the lease lapses while the
      // original dispatch is still running.
      slowWorker.clock.advance(30_000);
      const recovery = await newLoop({ owner: 'worker-recovery' });
      recovery.start();
      await recovery.waitForClaim(deliveryId, 2);

      const done = await recovery.waitForAttempts(deliveryId, 2);
      const state = await recovery.waitForState(deliveryId, [
        DeliveryState.DELIVERED,
        DeliveryState.DEAD,
      ]);

      // Two dispatches under two fencing tokens; the stale completion cannot
      // overwrite the newer transition (proved in worker-fencing coverage).
      expect(done.map((a) => a.outcome)).toEqual(['SUCCESS', 'SUCCESS']);
      expect(done[0].lease_generation).not.toBe(done[1].lease_generation);
      expect(new Set(done.map((a) => a.lease_owner))).toEqual(new Set(['worker-slow', 'worker-recovery']));
      // The overlapping dispatch changed receiver state exactly once.
      expect(await receiver.repo.listEffects(slowWorker.endpointId)).toHaveLength(1);
      expect(await receiver.repo.listRequests(slowWorker.endpointId)).toHaveLength(2);
      expect(state.state).toBe(DeliveryState.DELIVERED);
    });
  });

  describe('two workers, one queue', () => {
    it('drains a batch with no delivery dispatched twice', async () => {
      const a = await newLoop({ owner: 'worker-a', concurrency: 2 });
      const b = await newLoop({ owner: 'worker-b', concurrency: 2 });

      const ids: string[] = [];
      for (let i = 0; i < 4; i += 1) {
        ids.push((await a.publish({ payload: { via: 'a', seq: i } })).deliveryId);
        ids.push((await b.publish({ payload: { via: 'b', seq: i } })).deliveryId);
      }
      a.start();
      b.start();

      // Both workers scan the same due set concurrently; FOR UPDATE SKIP LOCKED
      // guarantees they take different rows rather than blocking or colliding.
      for (const id of ids) await a.waitForState(id, [DeliveryState.DELIVERED]);
      for (const id of ids) {
        const attempts = await a.attempts(id);
        expect(attempts).toHaveLength(1);
        expect(attempts[0].outcome).toBe('SUCCESS');
      }
      expect(await receiver.repo.listEffects()).toHaveLength(8);
      expect(await receiver.repo.listRequests()).toHaveLength(8);
    });
  });

  describe('destination configuration', () => {
    it('pins dispatch to the configured allowlist rather than any caller-supplied target', async () => {
      const loop = await newLoop({ allowedHosts: 'webhook.invalid' });
      const { deliveryId } = await loop.publish();
      loop.start();

      const dead = await loop.waitForState(deliveryId, [DeliveryState.DEAD]);
      expect(dead).toMatchObject({ last_error_code: 'destination_not_allowed', attempt_count: 1 });
      // Nothing left this process: the receiver saw no request at all.
      expect(await receiver.repo.listRequests(loop.endpointId)).toHaveLength(0);
    });

    it('records endpoint_missing when the destination stops resolving for the tenant', async () => {
      const loop = await newLoop();
      const { deliveryId } = await loop.publish();
      // The endpoint row itself cannot be deleted (events reference it), but a
      // destination that no longer belongs to this tenant is the same failure:
      // the processor's tenant-scoped lookup resolves nothing.
      await loop.db.query('UPDATE endpoints SET tenant_id = $2 WHERE id = $1', [
        loop.endpointId,
        SEED.tenantBId,
      ]);
      loop.start();

      const dead = await loop.waitForState(deliveryId, [DeliveryState.DEAD]);
      expect(dead).toMatchObject({ last_error_code: 'endpoint_missing', attempt_count: 1 });
      expect(await receiver.repo.listRequests(loop.endpointId)).toHaveLength(0);
    });
  });
});
