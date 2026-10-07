import { createQueue, createWorkerStack, insertDelivery, sleep, type WorkerStack } from '../helpers/worker';
import { q, resetDatabase } from '../helpers/db';
import type { DeliveryAttemptResult } from '../../src/worker/types';

const SUCCESS: DeliveryAttemptResult = {
  outcome: 'SUCCESS',
  httpStatus: 200,
  errorCode: null,
  responseSnippet: null,
  retryAfterMs: null,
};

/**
 * PDF acceptance TEST 7: stale worker fencing.
 *
 * A deterministic simulation of "Worker A claims; A pauses beyond its lease;
 * Worker B recovers and claims; B progresses; A resumes": the fake clock is the
 * pause, and A's late completion must be fenced out by
 * (lease_owner, lease_generation).
 *
 * test/acceptance/t7-stale-worker.spec.ts is the end-to-end counterpart: same
 * interleaving, but with real HTTP dispatch and a real receiver effect count.
 */
describe('M8 stale worker fencing (PDF test 7)', () => {
  let stack: WorkerStack;

  beforeAll(() => {
    stack = createWorkerStack(async () => SUCCESS);
  });
  afterAll(async () => {
    await stack.db.close();
  });
  beforeEach(async () => {
    await resetDatabase();
  });

  it('a paused worker cannot overwrite the state the recovering worker committed', async () => {
    const { deliveryId } = await insertDelivery(stack.db, { state: 'READY' });
    const queueA = createQueue(stack);
    const queueB = createQueue(stack);

    // Worker A claims under lease generation 1.
    const a = (await queueA.claimNext('worker-a', 30_000))!;
    expect(a.leaseGeneration).toBe('1');

    // Worker A pauses beyond its lease: the clock advances, A does nothing.
    stack.clock.advance(45_000);

    // Worker B recovers the expired lease; generation bumps to 2.
    const b = (await queueB.claimNext('worker-b', 30_000))!;
    expect(b.leaseGeneration).toBe('2');
    expect(b.attemptNumber).toBe(2);

    // B progresses the delivery to a terminal DELIVERED state.
    const bDone = await queueB.completeAttempt({
      deliveryId,
      attemptRowId: b.attemptRowId,
      leaseOwner: b.leaseOwner,
      leaseGeneration: b.leaseGeneration,
      outcome: 'SUCCESS',
      httpStatus: 200,
      errorCode: null,
      responseSnippet: null,
      nextState: 'DELIVERED',
      nextAttemptAt: null,
    });
    expect(bDone.applied).toBe(true);

    // A finally resumes and tries to write RETRY_WAIT with its stale fencing token.
    const aDone = await queueA.completeAttempt({
      deliveryId,
      attemptRowId: a.attemptRowId,
      leaseOwner: a.leaseOwner,
      leaseGeneration: a.leaseGeneration,
      outcome: 'RETRYABLE',
      httpStatus: 503,
      errorCode: 'http_5xx',
      responseSnippet: null,
      nextState: 'RETRY_WAIT',
      nextAttemptAt: new Date(stack.clock.nowMs() + 1000),
    });
    expect(aDone.applied).toBe(false);

    // B's newer state survived: DELIVERED, no retry scheduled, lease released.
    const [d] = await q(
      'SELECT state, next_attempt_at, lease_owner, lease_expires_at, last_http_status, updated_at FROM deliveries WHERE id = $1',
      [deliveryId],
    );
    expect(d.state).toBe('DELIVERED');
    expect(d.next_attempt_at).toBeNull();
    expect(d.lease_owner).toBeNull();
    expect(d.lease_expires_at).toBeNull();
    expect(d.last_http_status).toBe(200);
  });

  it('the stale worker still records its own attempt outcome accurately', async () => {
    const { deliveryId } = await insertDelivery(stack.db, { state: 'READY' });
    const queueA = createQueue(stack);
    const queueB = createQueue(stack);

    const a = (await queueA.claimNext('worker-a', 30_000))!;
    stack.clock.advance(45_000);
    await queueB.claimNext('worker-b', 30_000);

    // A's HTTP call did actually happen and failed with 503.
    await queueA.completeAttempt({
      deliveryId,
      attemptRowId: a.attemptRowId,
      leaseOwner: a.leaseOwner,
      leaseGeneration: a.leaseGeneration,
      outcome: 'RETRYABLE',
      httpStatus: 503,
      errorCode: 'http_5xx',
      responseSnippet: 'Service Unavailable',
      nextState: 'RETRY_WAIT',
      nextAttemptAt: new Date(stack.clock.nowMs() + 1000),
    });

    // Attempt history shows BOTH dispatches with truthful outcomes, even though
    // A's delivery-state write was fenced out.
    const attempts = await q(
      'SELECT lease_owner, outcome, http_status, finished_at FROM delivery_attempts WHERE delivery_id = $1 ORDER BY attempt_number',
      [deliveryId],
    );
    expect(attempts).toHaveLength(2);
    expect(attempts[0].lease_owner).toBe('worker-a');
    expect(attempts[0].outcome).toBe('RETRYABLE');
    expect(attempts[0].http_status).toBe(503);
    expect(attempts[0].finished_at).not.toBeNull();
    // B's attempt is still open (allocated, not completed): uncertainty preserved.
    expect(attempts[1].lease_owner).toBe('worker-b');
    expect(attempts[1].outcome).toBe('UNKNOWN');
    expect(attempts[1].finished_at).toBeNull();
  });

  it('two workers can interleave claims without corrupting state', async () => {
    // 4 deliveries, 2 workers racing over the same due window.
    for (let i = 0; i < 4; i += 1) await insertDelivery(stack.db, { state: 'READY' });
    const queueA = createQueue(stack);
    const queueB = createQueue(stack);

    const claims = await Promise.all([
      queueA.claimNext('worker-a', 30_000),
      queueB.claimNext('worker-b', 30_000),
      queueA.claimNext('worker-a', 30_000),
      queueB.claimNext('worker-b', 30_000),
    ]);

    // SKIP LOCKED guarantees distinct deliveries: no double claims.
    const ids = claims.map((c) => c?.deliveryId);
    expect(ids.every((id) => id !== null)).toBe(true);
    expect(new Set(ids).size).toBe(4);

    // Each claimed row is IN_FLIGHT under exactly one owner.
    const rows = await q(
      "SELECT id, state, lease_owner, lease_generation FROM deliveries WHERE id = ANY($1::text[])",
      [ids as string[]],
    );
    expect(rows).toHaveLength(4);
    for (const r of rows) {
      expect(r.state).toBe('IN_FLIGHT');
      expect(Number(r.lease_generation)).toBe(1);
    }
    await sleep(10);
  });
});
