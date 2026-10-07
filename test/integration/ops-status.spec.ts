/**
 * M13 operational counters (PDF: "ready / in-flight / retry-wait / dead /
 * oldest pending age"). The endpoint reads only durable state, so these tests
 * assert that the numbers describe the queue as it actually is - including the
 * work a dead worker parked behind an expired lease.
 */
import type { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { createTestApp } from '../helpers/app';
import { q, resetDatabase, testDb } from '../helpers/db';
import { TEST_TOKENS } from '../helpers/test-env';
import { insertDelivery } from '../helpers/worker';
import { DeliveryState } from '../../src/domain/types';
import type { OpsStatus } from '../../src/modules/operations/status.service';

const authOperator = { Authorization: `Bearer ${TEST_TOKENS.operator}` };
const authTenantA = { Authorization: `Bearer ${TEST_TOKENS.tenantA}` };

const ago = (ms: number): Date => new Date(Date.now() - ms);
const hence = (ms: number): Date => new Date(Date.now() + ms);

async function status(): Promise<OpsStatus> {
  const res = await request(app.getHttpServer()).get('/ops/status').set(authOperator).expect(200);
  return res.body as OpsStatus;
}

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

describe('M13 operator status', () => {
  describe('counters', () => {
    it('reports every state on an empty queue instead of omitting it', async () => {
      const body = await status();
      expect(body.counts).toEqual({ ready: 0, inFlight: 0, retryWait: 0, delivered: 0, dead: 0 });
      expect(body.oldestPending).toBeNull();
      expect(body.expiredLeases).toBe(0);
      expect(body.workers).toEqual([]);
      expect(typeof body.snapshotAt).toBe('string');
    });

    it('counts each state exactly', async () => {
      const rows: Array<[DeliveryState, number]> = [
        [DeliveryState.READY, 2],
        [DeliveryState.IN_FLIGHT, 1],
        [DeliveryState.RETRY_WAIT, 3],
        [DeliveryState.DELIVERED, 4],
        [DeliveryState.DEAD, 5],
      ];
      for (const [state, count] of rows) {
        for (let i = 0; i < count; i += 1) {
          await insertDelivery(testDb, {
            state,
            nextAttemptAt: state === DeliveryState.DEAD ? null : ago(i),
            leaseOwner: state === DeliveryState.IN_FLIGHT ? 'worker-a' : null,
            leaseExpiresAt: state === DeliveryState.IN_FLIGHT ? hence(30_000) : null,
          });
        }
      }

      expect((await status()).counts).toEqual({
        ready: 2,
        inFlight: 1,
        retryWait: 3,
        delivered: 4,
        dead: 5,
      });
    });

    it('never counts terminal work as pending', async () => {
      await insertDelivery(testDb, {
        state: DeliveryState.DELIVERED,
        nextAttemptAt: null,
      });
      await insertDelivery(testDb, { state: DeliveryState.DEAD, nextAttemptAt: null });
      expect((await status()).oldestPending).toBeNull();
    });

    it('splits in-flight work by lease owner, which is how the two workers stay visible', async () => {
      for (const owner of ['worker-a', 'worker-b', 'worker-b']) {
        await insertDelivery(testDb, {
          state: DeliveryState.IN_FLIGHT,
          leaseOwner: owner,
          leaseExpiresAt: hence(30_000),
        });
      }
      expect((await status()).workers).toEqual([
        { owner: 'worker-a', inFlight: 1 },
        { owner: 'worker-b', inFlight: 2 },
      ]);
    });

    it('reports an expired lease as parked work that still exists durably', async () => {
      // A worker died mid-attempt: the row is IN_FLIGHT with a lapsed lease, and
      // its attempt row is already written (persisted before dispatch).
      const crashed = await insertDelivery(testDb, {
        state: DeliveryState.IN_FLIGHT,
        leaseOwner: 'worker-ghost',
        leaseExpiresAt: ago(5_000),
      });
      const attempt = await q<{ id: string }>(
        `INSERT INTO delivery_attempts
           (id, delivery_id, attempt_number, cycle, attempt_id, lease_owner, lease_generation,
            started_at, outcome)
         VALUES (gen_random_uuid()::text, $1, 1, 1, $2, 'worker-ghost', 1, now(), 'UNKNOWN')
         RETURNING id`,
        [crashed.deliveryId, 'aaaaaaaa-0000-4000-8000-000000000001'],
      );
      await insertDelivery(testDb, {
        state: DeliveryState.IN_FLIGHT,
        leaseOwner: 'worker-alive',
        leaseExpiresAt: hence(20_000),
      });

      const body = await status();
      expect(body.expiredLeases).toBe(1);
      expect(body.counts.inFlight).toBe(2);
      expect(attempt).toHaveLength(1);
    });
  });

  describe('oldest pending age', () => {
    it('names the delivery the queue has been failing to attempt the longest', async () => {
      const olderAt = ago(90_000);
      const newerAt = ago(10_000);
      await insertDelivery(testDb, { state: DeliveryState.RETRY_WAIT, nextAttemptAt: newerAt });
      const oldest = await insertDelivery(testDb, {
        state: DeliveryState.READY,
        nextAttemptAt: olderAt,
      });
      await insertDelivery(testDb, { state: DeliveryState.RETRY_WAIT, nextAttemptAt: hence(60_000) });

      const body = await status();
      expect(body.oldestPending).toMatchObject({
        deliveryId: oldest.deliveryId,
        state: DeliveryState.READY,
      });
      // The instant is reported exactly as scheduled: it is the queue's promise,
      // not something derived from a second clock.
      expect(body.oldestPending?.scheduledFor).toBe(olderAt.toISOString());
    });

    it('measures age against the scheduled instant', async () => {
      const due = ago(90_000);
      const { deliveryId } = await insertDelivery(testDb, {
        state: DeliveryState.RETRY_WAIT,
        nextAttemptAt: due,
      });

      const pending = (await status()).oldestPending;
      expect(pending?.deliveryId).toBe(deliveryId);
      // Tolerance, not equality: real time passes while the request is served.
      expect(pending?.overdueMs).toBeGreaterThanOrEqual(89_000);
      expect(pending?.overdueMs).toBeLessThan(100_000);
    });

    it('clamps work that is not due yet at zero and does not call it stuck', async () => {
      const overdue = await insertDelivery(testDb, {
        state: DeliveryState.RETRY_WAIT,
        nextAttemptAt: ago(30_000),
      });
      const early = await insertDelivery(testDb, {
        state: DeliveryState.READY,
        nextAttemptAt: hence(600_000),
      });

      // The earliest scheduled row wins even though another row is not due for 10 minutes.
      expect((await status()).oldestPending?.deliveryId).toBe(overdue.deliveryId);

      await q('DELETE FROM deliveries WHERE id = $1', [overdue.deliveryId]);
      const only = (await status()).oldestPending;
      expect(only?.deliveryId).toBe(early.deliveryId);
      expect(only?.overdueMs).toBe(0);
    });
  });

  describe('counters follow the state machine', () => {
    it('turns one DEAD into one READY when an operator redrives', async () => {
      const { deliveryId } = await insertDelivery(testDb, {
        state: DeliveryState.DEAD,
        attemptCount: 5,
        attemptsInCycle: 5,
        cycle: 1,
        nextAttemptAt: null,
      });

      expect((await status()).counts).toMatchObject({ dead: 1, ready: 0 });

      await request(app.getHttpServer())
        .post(`/ops/deliveries/${deliveryId}/redrive`)
        .set(authOperator)
        .set('Idempotency-Key', 'status-redrive')
        .send({ reason: 'receiver fixed' })
        .expect(202);

      const after = await status();
      expect(after.counts).toMatchObject({ dead: 0, ready: 1 });
      expect(after.oldestPending?.deliveryId).toBe(deliveryId);
    });
  });

  describe('authorization', () => {
    it('is operator-only', async () => {
      const forbidden = await request(app.getHttpServer()).get('/ops/status').set(authTenantA);
      expect(forbidden.status).toBe(403);
      expect(forbidden.body).toMatchObject({ code: 'forbidden' });

      const anonymous = await request(app.getHttpServer()).get('/ops/status');
      expect(anonymous.status).toBe(401);
    });
  });
});
