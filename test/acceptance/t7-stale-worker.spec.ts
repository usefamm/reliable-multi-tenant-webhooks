/**
 * PDF acceptance test 7 - stale worker.
 *
 * Verbatim requirement: "Stale worker: Pause worker A beyond its lease, let B
 * recover, then resume A. Assert fenced local updates and a single receiver
 * effect, even if duplicate HTTP requests occur."
 *
 * The fencing MECHANISM - a completion UPDATE whose lease predicate matches zero
 * rows - is proven in test/integration/worker-fencing.spec.ts, and the
 * stalled-past-lease schedule behaviour in delivery-loop.spec.ts. This file adds
 * the shape those two do not cover: A resumes AFTER B has already committed a
 * terminal DELIVERED. A's late write must not resurrect, re-schedule or re-queue
 * the delivery, its own attempt row must still be truthful, and the receiver must
 * hold one business effect from two genuine HTTP requests.
 */
import { DeliveryState } from '../../src/domain/types';
import { ReceiverMode } from '../../src/receiver/repository';
import { q, resetDatabase } from '../helpers/db';
import { startReceiver, type ReceiverApp } from '../helpers/receiver';
import { startLoop, type DeliveryLoop, type LoopOptions } from '../helpers/delivery-loop';
import { BASE_MS } from '../helpers/worker';
import { TEST_DATABASE_URL } from '../helpers/test-env';

describe('PDF test 7: stale worker resumes after the lease was recovered', () => {
  let receiver: ReceiverApp;
  let loops: DeliveryLoop[];

  beforeEach(async () => {
    await resetDatabase();
    receiver = await startReceiver({ databaseUrl: TEST_DATABASE_URL });
    receiver.clock.set(BASE_MS);
    loops = [];
  });

  afterEach(async () => {
    for (const loop of loops) await loop.close();
    await receiver.close();
  });

  async function newLoop(opts: LoopOptions = {}): Promise<DeliveryLoop> {
    const loop = await startLoop(receiver, opts);
    loops.push(loop);
    return loop;
  }

  it('fences the late completion, keeps both attempt records and applies one effect', async () => {
    // A's lease is far shorter than the receiver's deliberate delay: from the
    // queue's point of view A is a paused worker, and its HTTP call is still in
    // flight when B takes the row over.
    const a = await newLoop({ owner: 'worker-a', leaseTtlMs: 300 });
    await receiver.repo.setMode({
      endpointId: a.endpointId,
      mode: ReceiverMode.SLOW,
      delayMs: 1_200,
    });
    const { deliveryId, eventId } = await a.publish();
    a.start();

    const claimed = await a.waitForClaim(deliveryId, 1);
    expect(claimed[0]).toMatchObject({ lease_owner: 'worker-a', outcome: 'UNKNOWN' });
    expect(claimed[0].finished_at).toBeNull();
    expect((await a.delivery(deliveryId)).state).toBe(DeliveryState.IN_FLIGHT);

    // A pauses beyond its lease: the clock moves, A does nothing.
    receiver.clock.advance(30_000);

    // B recovers the expired lease under a NEW fencing token and dispatches too.
    const b = await newLoop({ owner: 'worker-b' });
    b.start();
    await b.waitForClaim(deliveryId, 2);

    // Both dispatches eventually answer. Whichever finishes second is the one
    // whose state write matches the current lease; the other is fenced out.
    const done = await b.waitForAttempts(deliveryId, 2);
    const state = await b.waitForState(deliveryId, [DeliveryState.DELIVERED]);

    expect(done.map((r) => r.outcome)).toEqual(['SUCCESS', 'SUCCESS']);
    expect(done.map((r) => r.lease_owner)).toEqual(['worker-a', 'worker-b']);
    expect(Number(done[0].lease_generation)).toBe(1);
    expect(Number(done[1].lease_generation)).toBe(2);

    // Fenced local update: the delivery is terminal, holds no lease and has no
    // scheduled retry - the stale worker could not re-queue it.
    expect(state).toMatchObject({
      state: DeliveryState.DELIVERED,
      attempt_count: 2,
      cycle: 1,
      lease_owner: null,
      next_attempt_at: null,
    });

    // A's own attempt row stayed truthful even though its state write was refused.
    const rows = await q<{ lease_owner: string; outcome: string; finished_at: Date | null }>(
      `SELECT lease_owner, outcome, finished_at FROM delivery_attempts
        WHERE delivery_id = $1 ORDER BY attempt_number`,
      [deliveryId],
    );
    expect(rows.map((r) => r.lease_owner)).toEqual(['worker-a', 'worker-b']);
    expect(rows.every((r) => r.outcome === 'SUCCESS' && r.finished_at !== null)).toBe(true);

    // Two HTTP requests reached the receiver, one business effect came out of it.
    const requests = await receiver.repo.listRequests(a.endpointId);
    expect(requests).toHaveLength(2);
    expect(new Set(requests.map((r) => r.attempt_id)).size).toBe(2);
    expect(requests.every((r) => r.event_id === eventId)).toBe(true);
    const effects = await receiver.repo.listEffects(a.endpointId);
    expect(effects).toHaveLength(1);
    expect(effects[0].event_id).toBe(eventId);
  });

  it('does not let the stale worker produce a third dispatch after the recovery', async () => {
    // Same pause, but checked from the other side: after B delivered, the queue
    // must stay empty for this delivery even while A is still winding down.
    const a = await newLoop({ owner: 'worker-a2', leaseTtlMs: 250 });
    await receiver.repo.setMode({
      endpointId: a.endpointId,
      mode: ReceiverMode.SLOW,
      delayMs: 900,
    });
    const { deliveryId } = await a.publish();
    a.start();
    await a.waitForClaim(deliveryId, 1);
    receiver.clock.advance(30_000);

    const b = await newLoop({ owner: 'worker-b2' });
    b.start();
    await b.waitForState(deliveryId, [DeliveryState.DELIVERED]);

    // Real time, not the fake clock: A's in-flight HTTP call returns inside this
    // window, so its late completion genuinely happens after DELIVERED.
    await new Promise((r) => setTimeout(r, 400));
    const final = await b.delivery(deliveryId);
    expect(final).toMatchObject({ state: DeliveryState.DELIVERED, attempt_count: 2, next_attempt_at: null });
    expect(await b.attempts(deliveryId)).toHaveLength(2);
    expect(await receiver.repo.listEffects(a.endpointId)).toHaveLength(1);
  });
});
