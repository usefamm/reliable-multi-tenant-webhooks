/**
 * PDF acceptance test 8 - redrive concurrency.
 *
 * Verbatim requirement: "Redrive concurrency: Concurrent redrives of DEAD
 * produce one retry cycle. Replay does not reset its budget; changed input under
 * the same key conflicts. DELIVERED cannot be redriven."
 *
 * test/integration/redrive.spec.ts proves the API rules in isolation (one cycle
 * from eight concurrent redrives, byte-equal replay, 409 on changed input, and
 * refusal for every state that is not DEAD). What only a RUNNING queue can show
 * is the part a reviewer will ask about: that a concurrent burst really buys one
 * dispatch cycle, that replaying a redrive does not hand the delivery a fresh
 * attempt budget it has already started spending, and that a delivered event is
 * never dispatched again.
 */
import type { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { DeliveryState } from '../../src/domain/types';
import { ReceiverMode } from '../../src/receiver/repository';
import { createTestApp } from '../helpers/app';
import { q, resetDatabase } from '../helpers/db';
import { startReceiver, type ReceiverApp } from '../helpers/receiver';
import { startLoop, type DeliveryLoop, type LoopOptions } from '../helpers/delivery-loop';
import { BASE_MS } from '../helpers/worker';
import { canonicalJson } from '../../src/common/canonical-json';
import { TEST_DATABASE_URL, TEST_TOKENS } from '../helpers/test-env';

const operator = { Authorization: `Bearer ${TEST_TOKENS.operator}` };

type Row = { state: string; cycle: number; attempts_in_cycle: number; attempt_count: number; next_attempt_at: Date | null };

async function row(deliveryId: string): Promise<Row> {
  const rows = await q<Row>(
    `SELECT state, cycle, attempts_in_cycle, attempt_count, next_attempt_at
       FROM deliveries WHERE id = $1`,
    [deliveryId],
  );
  return rows[0];
}

describe('PDF test 8: redrive concurrency', () => {
  let receiver: ReceiverApp;
  let app: INestApplication;
  let loops: DeliveryLoop[];

  beforeEach(async () => {
    await resetDatabase();
    receiver = await startReceiver({ databaseUrl: TEST_DATABASE_URL });
    receiver.clock.set(BASE_MS);
    app = await createTestApp({ clock: receiver.clock });
    loops = [];
  });

  afterEach(async () => {
    for (const loop of loops) await loop.close();
    await app.close();
    await receiver.close();
  });

  async function newLoop(opts: LoopOptions = {}): Promise<DeliveryLoop> {
    const loop = await startLoop(receiver, opts);
    loops.push(loop);
    return loop;
  }

  function redrive(deliveryId: string, key: string, reason: string) {
    return request(app.getHttpServer())
      .post(`/ops/deliveries/${deliveryId}/redrive`)
      .set(operator)
      .set('Idempotency-Key', key)
      .send({ reason });
  }

  /** Burn one automatic cycle to DEAD against a receiver that refuses everything retryable. */
  async function driveToDead(loop: DeliveryLoop, deliveryId: string): Promise<void> {
    for (let n = 1; n < 5; n += 1) {
      await loop.waitForAttempts(deliveryId, n);
      await loop.advanceToDue(deliveryId);
    }
    await loop.waitForState(deliveryId, [DeliveryState.DEAD]);
  }

  it('turns six concurrent same-key redrives into one cycle and one delivery', async () => {
    const loop = await newLoop({ owner: 'worker-t8' });
    const { deliveryId, eventId } = await loop.publish();
    await receiver.repo.setMode({ endpointId: loop.endpointId, mode: ReceiverMode.PERM_FAILURE });
    loop.start();
    await driveToDead(loop, deliveryId);
    expect(await row(deliveryId)).toMatchObject({ state: DeliveryState.DEAD, cycle: 1, attempt_count: 5 });

    // Quiesce the queue so the state the burst leaves behind is observable rather
    // than already consumed: production redrives are followed by a worker taking
    // the work, and the next block proves exactly that part.
    await loop.worker.stop();

    const results = await Promise.all(
      Array.from({ length: 6 }, () => redrive(deliveryId, 'burst-redrive', 'receiver patched')),
    );
    expect(results.every((r) => r.status === 202)).toBe(true);
    // One action, reported identically to every caller except the request id.
    expect(new Set(results.map((r) => canonicalJson(r.body))).size).toBe(1);
    expect(new Set(results.map((r) => r.headers['x-request-id'] as string)).size).toBe(6);

    expect(await row(deliveryId)).toMatchObject({ state: DeliveryState.READY, cycle: 2, attempts_in_cycle: 0 });
    const audits = await q<{ n: number }>(
      'SELECT count(*)::int AS n FROM redrive_audit WHERE delivery_id = $1',
      [deliveryId],
    );
    expect(audits[0].n).toBe(1);

    // The requeued work is delivered exactly once - and by a worker that only ever
    // sees the new cycle, so six callers bought one dispatch cycle, not six.
    await receiver.repo.clearModes();
    const redeemer = await newLoop({ owner: 'worker-t8-after-redrive' });
    redeemer.start();
    const done = await redeemer.waitForState(deliveryId, [DeliveryState.DELIVERED]);
    expect(done).toMatchObject({ state: DeliveryState.DELIVERED, cycle: 2, attempt_count: 6 });

    const attempts = await loop.attempts(deliveryId);
    expect(attempts.map((a) => a.cycle)).toEqual([1, 1, 1, 1, 1, 2]);
    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
    const effects = await q<{ event_id: string }>('SELECT event_id FROM receiver_effects');
    expect(effects.map((e) => e.event_id)).toEqual([eventId]);
  });

  it('replays a redrive without resetting the budget the cycle is already spending', async () => {
    const loop = await newLoop({ owner: 'worker-t8' });
    const { deliveryId } = await loop.publish();
    await receiver.repo.setMode({ endpointId: loop.endpointId, mode: ReceiverMode.PERM_FAILURE });
    loop.start();
    await driveToDead(loop, deliveryId);

    const first = await redrive(deliveryId, 'budget-key', 'first redrive');
    expect(first.status).toBe(202);
    expect(first.body).toMatchObject({ cycle: 2, attemptsInCycle: 0, state: DeliveryState.READY });

    // Let cycle 2 burn three attempts (lifetime 6, 7, 8): the clock has to reach
    // each scheduled retry, because time is a shared fake that only moves here.
    for (let n = 6; n < 8; n += 1) {
      await loop.waitForAttempts(deliveryId, n);
      await loop.advanceToDue(deliveryId);
    }
    await loop.waitForAttempts(deliveryId, 8);
    await loop.worker.stop();
    const burning = await row(deliveryId);
    expect(burning).toMatchObject({ state: DeliveryState.RETRY_WAIT, cycle: 2, attempts_in_cycle: 3, attempt_count: 8 });

    // The same key and reason replay the ORIGINAL response snapshot...
    const replay = await redrive(deliveryId, 'budget-key', 'first redrive');
    expect(replay.status).toBe(202);
    expect(canonicalJson(replay.body)).toBe(canonicalJson(first.body));
    // ...and change nothing about the live cycle: no fresh budget, no new cycle,
    // no extra audit row. A lost redrive response must not buy a second one.
    expect(await row(deliveryId)).toEqual(burning);
    const audits = await q<{ n: number }>('SELECT count(*)::int AS n FROM redrive_audit WHERE delivery_id = $1', [
      deliveryId,
    ]);
    expect(audits[0].n).toBe(1);

    // Changed input under the same key is a conflict, and still changes nothing.
    const conflict = await redrive(deliveryId, 'budget-key', 'a different reason');
    expect(conflict.status).toBe(409);
    expect(conflict.body).toMatchObject({ code: 'conflict' });
    expect(await row(deliveryId)).toEqual(burning);
  });

  it('refuses to redrive a DELIVERED delivery and dispatches nothing extra', async () => {
    const loop = await newLoop({ owner: 'worker-t8' });
    const { deliveryId } = await loop.publish();
    loop.start();
    const delivered = await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);
    expect(delivered.state).toBe(DeliveryState.DELIVERED);

    const before = await row(deliveryId);
    const res = await redrive(deliveryId, 'already-done', 'retry for good measure');
    expect(res.status).toBe(409);
    expect(res.body.message).toMatch(/DELIVERED/);
    expect(await row(deliveryId)).toEqual(before);
    expect(await q('SELECT 1 FROM redrive_audit')).toEqual([]);

    // Nothing was re-queued, so nothing was re-sent: still one request, one effect.
    await loop.waitForIdlePolls();
    expect(await receiver.repo.listRequests(loop.endpointId)).toHaveLength(1);
    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
    expect(await loop.attempts(deliveryId)).toHaveLength(1);
  });
});
