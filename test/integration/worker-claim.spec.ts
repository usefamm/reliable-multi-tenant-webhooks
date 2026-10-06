import { DeliveryQueue } from '../../src/worker/delivery-queue';
import { Database } from '../../src/db/pool';
import { FakeClock } from '../../src/common/clock';
import { q, resetDatabase, testDb } from '../helpers/db';
import {
  BASE_MS,
  createWorkerStack,
  insertDelivery,
  sleep,
  type WorkerStack,
} from '../helpers/worker';
import type { DeliveryAttemptResult } from '../../src/worker/types';

const OK: DeliveryAttemptResult = {
  outcome: 'SUCCESS',
  httpStatus: 200,
  errorCode: null,
  responseSnippet: null,
  retryAfterMs: null,
};

/**
 * M7: worker claim loop, leases, and bounded concurrency.
 * These tests exercise the durable queue directly (claim/complete) and the
 * worker orchestration (concurrency bound, graceful shutdown) against the real
 * database with a fake clock for deterministic lease timing.
 */
describe('M7 worker claim loop + leases + bounded concurrency', () => {
  const stacks: WorkerStack[] = [];
  const extraDbs: Database[] = [];

  afterEach(async () => {
    await Promise.all(stacks.splice(0).map((s) => s.db.close()));
    await Promise.all(extraDbs.splice(0).map((d) => d.close()));
    await resetDatabase();
  });

  function track(stack: WorkerStack): WorkerStack {
    stacks.push(stack);
    return stack;
  }

  describe('DeliveryQueue.claimNext', () => {
    it('claims a due delivery, takes a lease, bumps generation, and pre-allocates the attempt', async () => {
      const { deliveryId } = await insertDelivery(testDb, { state: 'READY' });
      const clock = new FakeClock(BASE_MS);
      const queue = new DeliveryQueue(testDb, clock);

      const work = await queue.claimNext('worker-a', 30_000);
      expect(work).not.toBeNull();
      expect(work!.deliveryId).toBe(deliveryId);
      expect(work!.leaseOwner).toBe('worker-a');
      expect(work!.leaseGeneration).toBe('1'); // 0 -> 1 on first claim
      expect(work!.attemptNumber).toBe(1);
      expect(work!.attemptsInCycle).toBe(1);

      // Delivery is now IN_FLIGHT with a bounded lease.
      const [d] = await q(
        'SELECT state, lease_owner, lease_generation, lease_expires_at, attempt_count, attempts_in_cycle FROM deliveries WHERE id = $1',
        [deliveryId],
      );
      expect(d.state).toBe('IN_FLIGHT');
      expect(d.lease_owner).toBe('worker-a');
      expect(Number(d.lease_generation)).toBe(1);
      expect(d.lease_expires_at).not.toBeNull();
      expect(d.attempt_count).toBe(1);

      // The attempt row was persisted BEFORE any dispatch, outcome UNKNOWN.
      const attempts = await q(
        'SELECT id, attempt_id, attempt_number, outcome, finished_at, lease_owner, lease_generation FROM delivery_attempts WHERE delivery_id = $1',
        [deliveryId],
      );
      expect(attempts).toHaveLength(1);
      expect(attempts[0].outcome).toBe('UNKNOWN');
      expect(attempts[0].finished_at).toBeNull();
      expect(attempts[0].attempt_id).toBe(work!.attemptId);
      expect(Number(attempts[0].lease_generation)).toBe(1);
    });

    it('returns null when no work is due (future next_attempt_at)', async () => {
      await insertDelivery(testDb, {
        state: 'RETRY_WAIT',
        nextAttemptAt: new Date(BASE_MS + 60_000),
      });
      const queue = new DeliveryQueue(testDb, new FakeClock(BASE_MS));
      expect(await queue.claimNext('worker-a', 30_000)).toBeNull();
    });

    it('does not double-claim: two concurrent claims on one delivery yield one winner', async () => {
      await insertDelivery(testDb, { state: 'READY' });
      const queue = new DeliveryQueue(testDb, new FakeClock(BASE_MS));

      const [a, b] = await Promise.all([
        queue.claimNext('worker-a', 30_000),
        queue.claimNext('worker-b', 30_000),
      ]);
      const winners = [a, b].filter((w) => w !== null);
      expect(winners).toHaveLength(1);
    });

    it('recovers an IN_FLIGHT delivery whose lease has expired, bumping the generation', async () => {
      const { deliveryId } = await insertDelivery(testDb, {
        state: 'IN_FLIGHT',
        leaseOwner: 'crashed-worker',
        leaseGeneration: 3,
        leaseExpiresAt: new Date(BASE_MS - 1_000), // expired
        attemptCount: 1,
        attemptsInCycle: 1,
        nextAttemptAt: null,
      });
      const queue = new DeliveryQueue(testDb, new FakeClock(BASE_MS));

      const work = await queue.claimNext('worker-b', 30_000);
      expect(work).not.toBeNull();
      expect(work!.deliveryId).toBe(deliveryId);
      expect(work!.leaseGeneration).toBe('4'); // fencing token advanced
      expect(work!.leaseOwner).toBe('worker-b');
    });

    it('does NOT recover an IN_FLIGHT delivery whose lease is still valid', async () => {
      await insertDelivery(testDb, {
        state: 'IN_FLIGHT',
        leaseOwner: 'active-worker',
        leaseGeneration: 1,
        leaseExpiresAt: new Date(BASE_MS + 60_000), // still valid
        nextAttemptAt: null,
      });
      const queue = new DeliveryQueue(testDb, new FakeClock(BASE_MS));
      expect(await queue.claimNext('worker-b', 30_000)).toBeNull();
    });
  });

  describe('DeliveryWorker loop', () => {
    it('delivers all due work and transitions each to DELIVERED', async () => {
      const ids = [] as string[];
      for (let i = 0; i < 6; i += 1) {
        const { deliveryId } = await insertDelivery(testDb, { state: 'READY' });
        ids.push(deliveryId);
      }

      const stack = track(createWorkerStack(async () => OK, { concurrency: 4 }));
      stack.worker.start();

      // Poll until all six are terminal (bounded wait).
      const deadline = Date.now() + 3_000;
      let delivered = 0;
      while (Date.now() < deadline) {
        const rows = await q("SELECT id FROM deliveries WHERE state = 'DELIVERED'");
        delivered = rows.length;
        if (delivered === ids.length) break;
        await sleep(20);
      }
      await stack.worker.stop();

      expect(delivered).toBe(ids.length);
      // Each delivery recorded exactly one successful attempt.
      const attempts = await q(
        "SELECT delivery_id, outcome FROM delivery_attempts WHERE outcome = 'SUCCESS'",
      );
      expect(attempts).toHaveLength(ids.length);
    });

    it('never exceeds the configured concurrency bound', async () => {
      for (let i = 0; i < 12; i += 1) {
        await insertDelivery(testDb, { state: 'READY' });
      }

      let current = 0;
      let max = 0;
      const stack = track(
        createWorkerStack(
          async () => {
            current += 1;
            max = Math.max(max, current);
            await sleep(40);
            current -= 1;
            return OK;
          },
          { concurrency: 4, claimBatchSize: 4 },
        ),
      );

      stack.worker.start();
      const deadline = Date.now() + 4_000;
      while (Date.now() < deadline) {
        const rows = await q("SELECT id FROM deliveries WHERE state = 'DELIVERED'");
        if (rows.length === 12) break;
        await sleep(20);
      }
      await stack.worker.stop();

      expect(max).toBeLessThanOrEqual(4); // hard bound
      expect(max).toBeGreaterThanOrEqual(2); // proves real overlap happened
      expect(current).toBe(0);
    });

    it('stops claiming on shutdown and drains in-flight work', async () => {
      for (let i = 0; i < 4; i += 1) {
        await insertDelivery(testDb, { state: 'READY' });
      }

      let started = 0;
      const stack = track(
        createWorkerStack(
          async () => {
            started += 1;
            await sleep(50);
            return OK;
          },
          { concurrency: 4, claimBatchSize: 4 },
        ),
      );

      stack.worker.start();
      await sleep(30); // let it claim and begin dispatch
      await stack.worker.stop();

      // After stop, nothing is in flight and no new claims occur.
      expect(stack.worker.inFlightCount).toBe(0);
      const before = started;
      await sleep(80);
      expect(started).toBe(before); // no further dispatches after shutdown
    });
  });

  describe('completeAttempt fencing', () => {
    it('applies the state transition when the lease still matches', async () => {
      const { deliveryId } = await insertDelivery(testDb, { state: 'READY' });
      const clock = new FakeClock(BASE_MS);
      const queue = new DeliveryQueue(testDb, clock);
      const work = (await queue.claimNext('worker-a', 30_000))!;

      const res = await queue.completeAttempt({
        deliveryId,
        attemptRowId: work.attemptRowId,
        leaseOwner: work.leaseOwner,
        leaseGeneration: work.leaseGeneration,
        outcome: 'SUCCESS',
        httpStatus: 200,
        errorCode: null,
        responseSnippet: null,
        nextState: 'DELIVERED',
        nextAttemptAt: null,
      });
      expect(res.applied).toBe(true);

      const [d] = await q(
        'SELECT state, lease_owner, lease_expires_at, last_http_status FROM deliveries WHERE id = $1',
        [deliveryId],
      );
      expect(d.state).toBe('DELIVERED');
      expect(d.lease_owner).toBeNull(); // released on terminal
      expect(d.lease_expires_at).toBeNull();
      expect(d.last_http_status).toBe(200);
    });

    it('is fenced out when another worker has advanced the lease generation', async () => {
      const { deliveryId } = await insertDelivery(testDb, { state: 'READY' });
      const clock = new FakeClock(BASE_MS);
      const queue = new DeliveryQueue(testDb, clock);

      // Worker A claims (generation 1).
      const a = (await queue.claimNext('worker-a', 30_000))!;
      // Simulate A's lease expiring and worker B recovering (generation 2).
      clock.advance(60_000);
      const b = (await queue.claimNext('worker-b', 30_000))!;
      expect(b.leaseGeneration).toBe('2');

      // Stale worker A now tries to complete with its old fencing token.
      const staleRes = await queue.completeAttempt({
        deliveryId,
        attemptRowId: a.attemptRowId,
        leaseOwner: a.leaseOwner,
        leaseGeneration: a.leaseGeneration, // '1'
        outcome: 'SUCCESS',
        httpStatus: 200,
        errorCode: null,
        responseSnippet: null,
        nextState: 'DELIVERED',
        nextAttemptAt: null,
      });
      expect(staleRes.applied).toBe(false);

      // Delivery is still owned by B and IN_FLIGHT - A could not overwrite it.
      const [d] = await q('SELECT state, lease_owner, lease_generation FROM deliveries WHERE id = $1', [
        deliveryId,
      ]);
      expect(d.state).toBe('IN_FLIGHT');
      expect(d.lease_owner).toBe('worker-b');
      expect(Number(d.lease_generation)).toBe(2);

      // But A's own attempt row still records its true outcome (accurate history).
      const [attemptA] = await q(
        'SELECT outcome, http_status FROM delivery_attempts WHERE id = $1',
        [a.attemptRowId],
      );
      expect(attemptA.outcome).toBe('SUCCESS');
    });
  });
});
