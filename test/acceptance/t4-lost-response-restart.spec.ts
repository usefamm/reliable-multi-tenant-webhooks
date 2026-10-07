/**
 * PDF acceptance test 4 - lost response and restart.
 *
 * Verbatim requirement: "Lost response and restart: Commit the receiver effect
 * and drop the response. Restart the receiver and retry. Assert two or more HTTP
 * attempts but exactly one durable effect."
 *
 * "Restart" is real here: the receiver is closed and re-opened on the SAME port
 * with a new process object and a new connection pool. Anything it remembered in
 * memory is gone; only PostgreSQL still knows the event was applied. The effect
 * row is re-read byte-for-byte (applied_at included) to prove the retry
 * deduplicated rather than re-applied.
 */
import { FakeClock } from '../../src/common/clock';
import { DeliveryState } from '../../src/domain/types';
import { ReceiverMode } from '../../src/receiver/repository';
import { q, resetDatabase } from '../helpers/db';
import { startReceiver, type ReceiverApp } from '../helpers/receiver';
import { startLoop, type DeliveryLoop, type LoopOptions } from '../helpers/delivery-loop';
import { BASE_MS } from '../helpers/worker';
import { TEST_DATABASE_URL } from '../helpers/test-env';

type EffectRow = { applied_at: Date; content_hash: string; event_id: string };

describe('PDF test 4: lost response and receiver restart', () => {
  let receiver: ReceiverApp;
  let clock: FakeClock;
  let loops: DeliveryLoop[];

  beforeEach(async () => {
    await resetDatabase();
    clock = new FakeClock(BASE_MS);
    receiver = await startReceiver({ databaseUrl: TEST_DATABASE_URL, clock });
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

  /** Close and re-open the receiver on the same port: a restart, not a new service. */
  async function restartReceiver(): Promise<ReceiverApp> {
    const port = receiver.port;
    await receiver.close();
    receiver = await startReceiver({ databaseUrl: TEST_DATABASE_URL, clock, port });
    return receiver;
  }

  async function effectRow(endpointId: string, eventId: string): Promise<EffectRow | undefined> {
    const rows = await q<EffectRow>(
      'SELECT applied_at, content_hash, event_id FROM receiver_effects WHERE endpoint_id = $1 AND event_id = $2',
      [endpointId, eventId],
    );
    return rows[0];
  }

  it('delivers over a lost response and a restarted receiver: two attempts, one durable effect', async () => {
    const loop = await newLoop({ owner: 'worker-t4' });
    await receiver.repo.setMode({
      endpointId: loop.endpointId,
      mode: ReceiverMode.LOST_RESPONSE,
      remaining: 1,
    });
    loop.start();
    const { deliveryId, eventId } = await loop.publish();

    // Attempt 1: the receiver applied the effect, committed it, then the response
    // disappeared. The sender cannot know the outcome and says so.
    const first = await loop.waitForAttempts(deliveryId, 1);
    expect(first[0]).toMatchObject({
      outcome: 'UNKNOWN',
      http_status: null,
      error_code: 'transport_error',
    });
    const before = await effectRow(loop.endpointId, eventId);
    expect(before?.event_id).toBe(eventId);

    // RESTART between the attempts.
    const restarted = await restartReceiver();

    await loop.advanceToDue(deliveryId);
    const done = await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);
    const attempts = await loop.attempts(deliveryId);

    expect(done.state).toBe(DeliveryState.DELIVERED);
    expect(done.attempt_count).toBe(2);
    expect(attempts.map((a) => a.outcome)).toEqual(['UNKNOWN', 'SUCCESS']);
    expect(attempts.map((a) => a.attempt_number)).toEqual([1, 2]);

    // Two HTTP attempts, recorded by two different receiver processes.
    const requests = await restarted.repo.listRequests(loop.endpointId);
    expect(requests).toHaveLength(2);
    expect(requests.every((r) => r.signature_ok)).toBe(true);
    expect(new Set(requests.map((r) => r.attempt_id)).size).toBe(2);

    // Exactly one durable effect, still the row the FIRST process wrote: the
    // restarted one deduplicated against the database, not against memory.
    const effects = await restarted.repo.listEffects(loop.endpointId);
    expect(effects).toHaveLength(1);
    const after = await effectRow(loop.endpointId, eventId);
    expect(after?.applied_at.getTime()).toBe(before?.applied_at.getTime());
    expect(after?.content_hash).toBe(before?.content_hash);
  });

  it('converges on one effect when the receiver dies mid-response, before its reply arrives', async () => {
    // The outbound timeout is far shorter than the receiver's deliberate delay,
    // so the sender walks away while the effect is already committed. Then the
    // receiver is killed while its reply is still pending.
    const loop = await newLoop({ owner: 'worker-t4-kill', timeoutMs: 100 });
    await receiver.repo.setMode({
      endpointId: loop.endpointId,
      mode: ReceiverMode.SLOW,
      delayMs: 400,
      remaining: 1,
    });
    loop.start();
    const { deliveryId, eventId } = await loop.publish();

    const first = await loop.waitForAttempts(deliveryId, 1);
    expect(first[0]).toMatchObject({ outcome: 'RETRYABLE', error_code: 'timeout', http_status: null });
    const before = await effectRow(loop.endpointId, eventId);
    expect(before?.event_id).toBe(eventId);

    await restartReceiver();

    await loop.advanceToDue(deliveryId);
    await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);

    const attempts = await loop.attempts(deliveryId);
    expect(attempts.map((a) => a.outcome)).toEqual(['RETRYABLE', 'SUCCESS']);
    expect(await receiver.repo.listRequests(loop.endpointId)).toHaveLength(2);
    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);

    // The retry did not touch the committed effect: same row, same applied_at,
    // same content hash - a duplicate, acknowledged as such.
    const after = await effectRow(loop.endpointId, eventId);
    expect(after?.applied_at.getTime()).toBe(before?.applied_at.getTime());
    expect(after?.content_hash).toBe(before?.content_hash);
  });

  it('restarts the worker too: neither side remembers anything, the queue does', async () => {
    // Both processes die between the attempts - the harshest version of the same
    // requirement. The delivery's durable state is the only continuity.
    const loop = await newLoop({ owner: 'worker-t4-crash' });
    await receiver.repo.setMode({
      endpointId: loop.endpointId,
      mode: ReceiverMode.LOST_RESPONSE,
      remaining: 1,
    });
    loop.start();
    const { deliveryId, eventId } = await loop.publish();
    await loop.waitForAttempts(deliveryId, 1);

    // Kill the worker's poll loop, then restart both sides.
    await loop.worker.stop();
    await restartReceiver();
    const recovered = await newLoop({ owner: 'worker-t4-after' });
    recovered.start();

    await recovered.advanceToDue(deliveryId);
    await recovered.waitForState(deliveryId, [DeliveryState.DELIVERED]);

    const attempts = await recovered.attempts(deliveryId);
    expect(attempts.map((a) => a.lease_owner)).toEqual(['worker-t4-crash', 'worker-t4-after']);
    expect(attempts.map((a) => a.cycle)).toEqual([1, 1]);
    expect(await recovered.delivery(deliveryId)).toMatchObject({ attempt_count: 2, attempts_in_cycle: 2 });
    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
    expect((await effectRow(loop.endpointId, eventId))?.event_id).toBe(eventId);
  });
});
