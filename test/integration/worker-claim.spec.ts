import { PgDeliveryQueue } from '../../src/db/pg-delivery-queue';
import { Database } from '../../src/db/pool';
import { FakeClock } from '../../src/common/clock';
import { q, resetDatabase, testDb } from '../helpers/db';
import {
  BASE_MS,
  createWorkerStack,
  insertDelivery,
  type WorkerStack,
} from '../helpers/worker';
import type { DeliveryAttemptResult } from '../../src/domain/attempt';
import { createGate, waitUntil, yieldTurns } from '../helpers/wait';

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
    // Stop workers first: closing a pool under a still-running worker would wait
    // on its checked-out connections forever. (A no-op for a worker never started.)
    const done = stacks.splice(0);
    await Promise.all(done.map((s) => s.worker.stop()));
    await Promise.all(done.map((s) => s.db.close()));
    await Promise.all(extraDbs.splice(0).map((d) => d.close()));
    await resetDatabase();
  });

  function track(stack: WorkerStack): WorkerStack {
    stacks.push(stack);
    return stack;
  }

  describe('PgDeliveryQueue.claimNext', () => {
    it('claims a due delivery, takes a lease, bumps generation, and pre-allocates the attempt', async () => {
      const { deliveryId } = await insertDelivery(testDb, { state: 'READY' });
      const clock = new FakeClock(BASE_MS);
      const queue = new PgDeliveryQueue(testDb, clock);

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
      const queue = new PgDeliveryQueue(testDb, new FakeClock(BASE_MS));
      expect(await queue.claimNext('worker-a', 30_000)).toBeNull();
    });

    it('does not double-claim: two concurrent claims on one delivery yield one winner', async () => {
      await insertDelivery(testDb, { state: 'READY' });
      const queue = new PgDeliveryQueue(testDb, new FakeClock(BASE_MS));

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
      const queue = new PgDeliveryQueue(testDb, new FakeClock(BASE_MS));

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
      const queue = new PgDeliveryQueue(testDb, new FakeClock(BASE_MS));
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

      // Wait until all six are terminal.
      const delivered = await waitUntil(
        () => q("SELECT id FROM deliveries WHERE state = 'DELIVERED'"),
        (rows) => rows.length === ids.length,
        'all six deliveries to be DELIVERED',
      );
      await stack.worker.stop();

      expect(delivered).toHaveLength(ids.length);
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
      // Dispatches stay open until the test releases them, so the overlap is
      // forced rather than hoped for by sleeping.
      const release = createGate();
      const stack = track(
        createWorkerStack(
          async () => {
            current += 1;
            max = Math.max(max, current);
            await release.promise;
            current -= 1;
            return OK;
          },
          { concurrency: 4, claimBatchSize: 4 },
        ),
      );

      stack.worker.start();
      await waitUntil(() => current, (n) => n >= 4, 'four dispatches in flight');
      // Eight deliveries are still due and the worker keeps polling while full;
      // give it several passes in which an unbounded worker would over-claim.
      const polls = stack.worker.pollCount;
      await waitUntil(() => stack.worker.pollCount, (n) => n >= polls + 5, 'five saturated polls');
      expect(current).toBe(4);
      expect(max).toBe(4);

      release.release();
      await waitUntil(
        () => q("SELECT id FROM deliveries WHERE state = 'DELIVERED'"),
        (rows) => rows.length === 12,
        'all twelve deliveries to be DELIVERED',
        4_000,
      );
      await stack.worker.stop();

      expect(max).toBe(4); // hard bound, and genuinely reached
      expect(current).toBe(0);
    });

    it('stops claiming on shutdown and drains in-flight work', async () => {
      for (let i = 0; i < 4; i += 1) {
        await insertDelivery(testDb, { state: 'READY' });
      }

      let started = 0;
      const finishDispatch = createGate();
      const stack = track(
        createWorkerStack(
          async () => {
            started += 1;
            await finishDispatch.promise;
            return OK;
          },
          { concurrency: 2, claimBatchSize: 2 },
        ),
      );

      stack.worker.start();
      await waitUntil(() => started, (n) => n >= 2, 'two dispatches to begin');

      // Stop while both dispatches are still open, then let them finish: stop()
      // must wait for them (drain) rather than abandon them.
      const stopping = stack.worker.stop();
      finishDispatch.release();
      await stopping;

      // Nothing is in flight, and the two undone deliveries were never claimed.
      expect(stack.worker.inFlightCount).toBe(0);
      expect(started).toBe(2);
      const afterStop = stack.worker.pollCount;
      await yieldTurns();
      expect(stack.worker.pollCount).toBe(afterStop); // the poll loop is really gone
      expect(await q("SELECT id FROM deliveries WHERE state = 'READY'")).toHaveLength(2);
    });
  });

  describe('completeAttempt fencing', () => {
    it('applies the state transition when the lease still matches', async () => {
      const { deliveryId } = await insertDelivery(testDb, { state: 'READY' });
      const clock = new FakeClock(BASE_MS);
      const queue = new PgDeliveryQueue(testDb, clock);
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
      const queue = new PgDeliveryQueue(testDb, clock);

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
