/**
 * PDF acceptance TEST 8 (redrive) plus the operator-state rules:
 * a DEAD delivery can be bought a fresh automatic cycle by an operator, and
 * nothing about the event's identity, its history or its receiver-side effect
 * changes. Redrive is idempotent (key + input), audited, and cannot start two
 * cycles for one delivery.
 */
import type { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { createTestApp } from '../helpers/app';
import { q, resetDatabase, testDb } from '../helpers/db';
import { SEED, TEST_DATABASE_URL, TEST_TOKENS } from '../helpers/test-env';
import { insertDelivery } from '../helpers/worker';
import { newUuid } from '../../src/common/ids';
import { DeliveryState } from '../../src/domain/types';
import { ReceiverMode } from '../../src/receiver/repository';
import { startReceiver } from '../helpers/receiver';
import { startLoop } from '../helpers/delivery-loop';

const authOperator = { Authorization: `Bearer ${TEST_TOKENS.operator}` };
const authTenantA = { Authorization: `Bearer ${TEST_TOKENS.tenantA}` };

async function deadDelivery(): Promise<{ eventId: string; deliveryId: string }> {
  return insertDelivery(testDb, {
    state: DeliveryState.DEAD,
    attemptCount: 5,
    attemptsInCycle: 5,
    cycle: 1,
    nextAttemptAt: null,
  });
}

/** Give a DEAD delivery a realistic five-attempt history to preserve. */
async function seedAttemptHistory(deliveryId: string, count: number): Promise<void> {
  for (let n = 1; n <= count; n += 1) {
    await q(
      `INSERT INTO delivery_attempts
         (id, delivery_id, attempt_number, cycle, attempt_id, lease_owner, lease_generation,
          started_at, finished_at, outcome, http_status, error_code)
       VALUES ($1,$2,$3,1,$4,'worker-a',$5,now(),now(),'RETRYABLE',500,'http_500')`,
      [newUuid(), deliveryId, n, newUuid(), n],
    );
  }
}

describe('M12 operator redrive', () => {
  let app: INestApplication;

  beforeAll(async () => {
    app = await createTestApp();
  });
  afterAll(async () => {
    await app.close();
  });
  beforeEach(async () => {
    await resetDatabase();
  });

  function redriveOn(
    target: INestApplication,
    id: string,
    key: string,
    body: Record<string, unknown> | string,
    auth = authOperator,
  ) {
    return request(target.getHttpServer())
      .post(`/ops/deliveries/${id}/redrive`)
      .set(auth)
      .set('Idempotency-Key', key)
      .send(body);
  }

  const redrive = (
    id: string,
    key: string,
    body: Record<string, unknown> | string,
    auth = authOperator,
  ) => redriveOn(app, id, key, body, auth);

  describe('state transition', () => {
    it('moves a DEAD delivery into a fresh cycle while preserving identity, bytes and history', async () => {
      const { deliveryId, eventId } = await deadDelivery();
      await seedAttemptHistory(deliveryId, 5);
      const before = await q<{ envelope_hash: string; event_id: string; lease_generation: string }>(
        'SELECT envelope_hash, event_id, lease_generation FROM deliveries WHERE id = $1',
        [deliveryId],
      );

      const res = await redrive(deliveryId, 'rd-1', { reason: 'receiver fixed' }).expect(202);

      expect(res.body).toMatchObject({
        deliveryId,
        eventId,
        state: DeliveryState.READY,
        cycle: 2,
        attemptCount: 5,
        attemptsInCycle: 0,
      });
      expect(typeof res.body.nextAttemptAt).toBe('string');

      const after = await q<{
        state: string;
        cycle: number;
        attempts_in_cycle: number;
        attempt_count: number;
        next_attempt_at: Date | null;
        lease_owner: string | null;
        envelope_hash: string;
        event_id: string;
        lease_generation: string;
      }>(
        `SELECT state, cycle, attempts_in_cycle, attempt_count, next_attempt_at, lease_owner,
                envelope_hash, event_id, lease_generation
           FROM deliveries WHERE id = $1`,
        [deliveryId],
      );
      expect(after[0]).toMatchObject({
        state: DeliveryState.READY,
        cycle: 2,
        attempts_in_cycle: 0,
        attempt_count: 5,
        lease_owner: null,
      });
      expect(after[0].next_attempt_at).not.toBeNull();
      // Same event, same bytes: the receiver's dedup identity is untouched.
      expect(after[0].event_id).toBe(before[0].event_id);
      expect(after[0].envelope_hash).toBe(before[0].envelope_hash);
      // Redrive does not mint a new fencing token - it only makes the row due.
      expect(after[0].lease_generation).toBe(before[0].lease_generation);

      // The previous cycle's attempts are still on the record.
      const attempts = await q<{ attempt_number: number; cycle: number }>(
        'SELECT attempt_number, cycle FROM delivery_attempts WHERE delivery_id = $1 ORDER BY attempt_number',
        [deliveryId],
      );
      expect(attempts.map((a) => a.attempt_number)).toEqual([1, 2, 3, 4, 5]);
      expect(attempts.every((a) => a.cycle === 1)).toBe(true);
    });

    it('refuses every state that is not DEAD, and changes nothing', async () => {
      const states = [
        DeliveryState.READY,
        DeliveryState.RETRY_WAIT,
        DeliveryState.IN_FLIGHT,
        DeliveryState.DELIVERED,
      ] as const;

      for (const state of states) {
        const { deliveryId } = await insertDelivery(testDb, {
          state,
          nextAttemptAt: new Date(),
          attemptCount: 1,
          attemptsInCycle: 1,
        });
        const res = await redrive(deliveryId, `rd-${state}`, { reason: 'try again' });
        expect(res.status).toBe(409);
        expect(res.body).toMatchObject({ code: 'conflict' });
        expect(res.body.message).toContain(state);

        const after = await q<{ cycle: number; state: string }>(
          'SELECT cycle, state FROM deliveries WHERE id = $1',
          [deliveryId],
        );
        expect(after[0]).toMatchObject({ cycle: 1, state });
      }
      // A refused redrive is not an audit-worthy action.
      expect(await q('SELECT id FROM redrive_audit')).toHaveLength(0);
    });

    it('cannot run two cycles for one delivery: concurrent redrives yield one READY', async () => {
      const { deliveryId } = await deadDelivery();
      const responses = await Promise.all(
        Array.from({ length: 8 }, (_, i) => redrive(deliveryId, `rd-race-${i}`, { reason: 'race' })),
      );

      const accepted = responses.filter((r) => r.status === 202);
      const conflicts = responses.filter((r) => r.status === 409);
      expect(accepted).toHaveLength(1);
      expect(conflicts).toHaveLength(7);

      const after = await q<{ cycle: number; state: string }>(
        'SELECT cycle, state FROM deliveries WHERE id = $1',
        [deliveryId],
      );
      expect(after[0]).toMatchObject({ cycle: 2, state: DeliveryState.READY });
      // Exactly one operator action is recorded.
      expect(await q('SELECT id FROM redrive_audit WHERE delivery_id = $1', [deliveryId])).toHaveLength(1);
    });
  });

  describe('idempotency', () => {
    it('replays the original response for the same key and reason', async () => {
      const { deliveryId } = await deadDelivery();
      const first = await redrive(deliveryId, 'rd-same', { reason: 'fixed upstream' }).expect(202);

      // Put it back to DEAD: the replay must come from the record, not the state.
      await q("UPDATE deliveries SET state = 'DEAD', next_attempt_at = NULL WHERE id = $1", [deliveryId]);

      const replay = await redrive(deliveryId, 'rd-same', { reason: 'fixed upstream' }).expect(202);
      expect(replay.body).toEqual(first.body);

      const cycle = await q<{ cycle: number }>('SELECT cycle FROM deliveries WHERE id = $1', [deliveryId]);
      expect(cycle[0].cycle).toBe(2);
      expect(await q('SELECT id FROM redrive_audit WHERE delivery_id = $1', [deliveryId])).toHaveLength(1);
      expect(await q("SELECT id FROM idempotency_records WHERE operation = 'redrive'")).toHaveLength(1);
    });

    it('returns 409 when a redrive key is reused with a different reason', async () => {
      const { deliveryId } = await deadDelivery();
      await redrive(deliveryId, 'rd-key', { reason: 'first reason' }).expect(202);

      const res = await redrive(deliveryId, 'rd-key', { reason: 'different reason' });
      expect(res.status).toBe(409);
      expect(res.body).toMatchObject({ code: 'conflict' });
      expect(await q('SELECT id FROM redrive_audit WHERE delivery_id = $1', [deliveryId])).toHaveLength(1);
    });

    it('does not consume the key when the request is rejected up front', async () => {
      const { deliveryId } = await deadDelivery();

      const bad = await redrive(deliveryId, 'rd-retry', {});
      expect(bad.status).toBe(400);
      expect(await q('SELECT id FROM idempotency_records')).toHaveLength(0);

      // The same key is still usable by the corrected request.
      await redrive(deliveryId, 'rd-retry', { reason: 'now with reason' }).expect(202);
      const after = await q<{ cycle: number }>(
        'SELECT cycle FROM deliveries WHERE id = $1',
        [deliveryId],
      );
      expect(after[0].cycle).toBe(2);
      expect(await q('SELECT id FROM redrive_audit WHERE delivery_id = $1', [deliveryId])).toHaveLength(1);
    });

    it('replays concurrently identical redrives as one action', async () => {
      const { deliveryId } = await deadDelivery();
      const responses = await Promise.all(
        Array.from({ length: 6 }, () => redrive(deliveryId, 'rd-one', { reason: 'same' })),
      );
      for (const res of responses) {
        expect(res.status).toBe(202);
        expect(res.body).toEqual(responses[0].body);
      }
      const after = await q<{ cycle: number }>('SELECT cycle FROM deliveries WHERE id = $1', [deliveryId]);
      expect(after[0].cycle).toBe(2);
      expect(await q('SELECT id FROM redrive_audit WHERE delivery_id = $1', [deliveryId])).toHaveLength(1);
    });

    it('scopes redrive keys per tenant', async () => {
      const a = await deadDelivery();
      const b = await insertDelivery(testDb, {
        state: DeliveryState.DEAD,
        tenantId: SEED.tenantBId,
        endpointId: SEED.endpointB1,
        attemptCount: 5,
        attemptsInCycle: 5,
        cycle: 1,
        nextAttemptAt: null,
      });

      await redrive(a.deliveryId, 'shared-key', { reason: 'tenant a' }).expect(202);
      // Same key, different tenant's delivery: a distinct operation, not a replay.
      await redrive(b.deliveryId, 'shared-key', { reason: 'tenant b' }).expect(202);

      const cycles = await q<{ cycle: number }>(
        'SELECT cycle FROM deliveries WHERE id = ANY($1) ORDER BY id',
        [[a.deliveryId, b.deliveryId]],
      );
      expect(cycles.map((c) => c.cycle)).toEqual([2, 2]);
    });
  });

  describe('authorization and input validation', () => {
    it('requires an operator token', async () => {
      const { deliveryId } = await deadDelivery();
      const res = await redrive(deliveryId, 'rd-auth', { reason: 'no privilege' }, authTenantA);
      expect(res.status).toBe(403);
      expect(res.body).toMatchObject({ code: 'forbidden' });
      const rows = await q<{ cycle: number }>('SELECT cycle FROM deliveries WHERE id = $1', [deliveryId]);
      expect(rows[0].cycle).toBe(1);
      expect(await q('SELECT id FROM redrive_audit WHERE delivery_id = $1', [deliveryId])).toHaveLength(0);
    });

    it('requires an Idempotency-Key header', async () => {
      const { deliveryId } = await deadDelivery();
      const missing = await request(app.getHttpServer())
        .post(`/ops/deliveries/${deliveryId}/redrive`)
        .set(authOperator)
        .send({ reason: 'no key' });
      expect(missing.status).toBe(400);
      expect(missing.body).toMatchObject({ code: 'bad_request' });

      const tooLong = await redrive(deliveryId, 'k'.repeat(201), { reason: 'long key' });
      expect(tooLong.status).toBe(400);
    });

    it('requires a reason and bounds its length', async () => {
      const { deliveryId } = await deadDelivery();
      expect((await redrive(deliveryId, 'rd-r1', {})).status).toBe(400);
      expect((await redrive(deliveryId, 'rd-r2', { reason: '   ' })).status).toBe(400);
      expect((await redrive(deliveryId, 'rd-r3', { reason: 'x'.repeat(501) })).status).toBe(400);
      expect((await redrive(deliveryId, 'rd-r4', { reason: 42 })).status).toBe(400);
      expect((await redrive(deliveryId, 'rd-r5', 'not-an-object')).status).toBe(400);
      // Unknown fields are rejected: a typo'd operator request should not
      // silently succeed while ignoring part of what the operator meant.
      expect((await redrive(deliveryId, 'rd-r6', { reason: 'ok', extra: 1 })).status).toBe(400);
      expect((await redrive(deliveryId, 'rd-r7', { reason: '  padded  ' })).status).toBe(202);
      const stored = await q<{ reason: string }>('SELECT reason FROM redrive_audit LIMIT 1');
      expect(stored[0].reason).toBe('padded');
    });

    it('answers 404 for an unknown or malformed delivery id without leaking existence', async () => {
      const unknown = await redrive(newUuid(), 'rd-404', { reason: 'nothing here' });
      expect(unknown.status).toBe(404);
      expect(unknown.body).toMatchObject({ code: 'not_found' });

      const malformed = await redrive('not-a-uuid', 'rd-404b', { reason: 'nothing here' });
      expect(malformed.status).toBe(404);
    });
  });

  describe('audit trail', () => {
    it('records the operator label, the reason and the key - never the token', async () => {
      const { deliveryId } = await deadDelivery();
      await redrive(deliveryId, 'rd-audit', { reason: 'customer confirmed receiver is back' }).expect(202);

      const rows = await q<{ operator: string; reason: string; idempotency_key: string }>(
        'SELECT operator, reason, idempotency_key FROM redrive_audit WHERE delivery_id = $1',
        [deliveryId],
      );
      expect(rows).toHaveLength(1);
      expect(rows[0].reason).toBe('customer confirmed receiver is back');
      expect(rows[0].idempotency_key).toBe('rd-audit');
      expect(rows[0].operator.length).toBeGreaterThan(0);
      // The credential itself must never be persisted.
      expect(JSON.stringify(rows)).not.toContain(TEST_TOKENS.operator);
    });
  });

  describe('redrive through the delivery loop', () => {
    it('buys exactly one more automatic cycle, and the worker delivers it', async () => {
      const receiver = await startReceiver({ databaseUrl: TEST_DATABASE_URL });
      await receiver.repo.resetAll();
      const loop = await startLoop(receiver);
      // Same clock for API and worker: the redrive schedules from this instant.
      const api = await createTestApp({ clock: receiver.clock });
      try {
        // Drive the delivery to DEAD for real: five 500s, nothing applied.
        await receiver.repo.setMode({ endpointId: loop.endpointId, mode: ReceiverMode.PERM_FAILURE });
        const { deliveryId } = await loop.publish();
        loop.start();
        for (let n = 1; n < 5; n += 1) {
          await loop.waitForAttempts(deliveryId, n);
          await loop.advanceToDue(deliveryId);
        }
        const dead = await loop.waitForState(deliveryId, [DeliveryState.DEAD]);
        expect(dead.attempt_count).toBe(5);

        // The failure is over; an operator buys a fresh cycle.
        await receiver.repo.clearModes();
        await redriveOn(api, deliveryId, 'rd-loop', { reason: 'receiver recovered' }).expect(202);

        // The row is due at the redrive instant, so the polling worker resumes.
        const delivered = await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);
        const attempts = await loop.attempts(deliveryId);

        expect(delivered).toMatchObject({ cycle: 2, attempt_count: 6, attempts_in_cycle: 1 });
        expect(attempts).toHaveLength(6);
        // Lifetime numbering continues across cycles; the new attempts belong to cycle 2.
        expect(attempts.map((a) => a.attempt_number)).toEqual([1, 2, 3, 4, 5, 6]);
        expect(attempts.slice(0, 5).every((a) => a.cycle === 1)).toBe(true);
        expect(attempts[5]).toMatchObject({ cycle: 2, outcome: 'SUCCESS', http_status: 200 });
        expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
      } finally {
        await api.close();
        await loop.close();
        await receiver.close();
      }
    });

    it('cannot apply the receiver-side effect twice after a cycle that exhausted on timeouts', async () => {
      const receiver = await startReceiver({ databaseUrl: TEST_DATABASE_URL });
      await receiver.repo.resetAll();
      // The receiver applies the effect but answers slower than the sender waits.
      const loop = await startLoop(receiver, { timeoutMs: 100 });
      const api = await createTestApp({ clock: receiver.clock });
      try {
        await receiver.repo.setMode({ endpointId: loop.endpointId, mode: ReceiverMode.SLOW, delayMs: 400 });
        const { deliveryId } = await loop.publish();
        loop.start();

        for (let n = 1; n < 5; n += 1) {
          await loop.waitForAttempts(deliveryId, n);
          await loop.advanceToDue(deliveryId);
        }
        const dead = await loop.waitForState(deliveryId, [DeliveryState.DEAD]);
        expect(dead.attempt_count).toBe(5);
        // Five dispatches, one business effect: dedup already held.
        expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);

        await receiver.repo.clearModes();
        await redriveOn(api, deliveryId, 'rd-dedup', { reason: 'operator redrive' }).expect(202);
        await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);

        expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
        expect(await receiver.repo.listRequests(loop.endpointId)).toHaveLength(6);
        expect((await loop.attempts(deliveryId)).at(-1)?.outcome).toBe('SUCCESS');
      } finally {
        await api.close();
        await loop.close();
        await receiver.close();
      }
    });
  });
});
