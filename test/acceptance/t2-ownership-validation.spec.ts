/**
 * PDF acceptance test 2 - ownership and validation.
 *
 * Verbatim requirement: "Ownership and validation: Reject cross-tenant access,
 * invalid input, oversized bodies and unauthorised redrives. A caller cannot
 * replace the configured destination."
 *
 * Every rejection here is asserted twice: on the HTTP answer AND on the durable
 * state, because a validation bug that leaves a half-written event or a consumed
 * idempotency key is invisible in the response.
 */
import type { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { DeliveryState } from '../../src/domain/types';
import { createTestApp } from '../helpers/app';
import { q, resetDatabase, testDb } from '../helpers/db';
import { startReceiver, type ReceiverApp } from '../helpers/receiver';
import { startLoop, type DeliveryLoop } from '../helpers/delivery-loop';
import { insertDelivery, BASE_MS } from '../helpers/worker';
import { startCaptureEndpoint } from '../helpers/capture-endpoint';
import { SEED, TEST_DATABASE_URL, TEST_TOKENS } from '../helpers/test-env';

const authA = { Authorization: `Bearer ${TEST_TOKENS.tenantA}` };
const authB = { Authorization: `Bearer ${TEST_TOKENS.tenantB}` };

type Counts = { events: number; deliveries: number; claims: number };

async function counts(): Promise<Counts> {
  const [rows] = await q<Counts>(
    `SELECT (SELECT count(*) FROM events)::int              AS events,
            (SELECT count(*) FROM deliveries)::int          AS deliveries,
            (SELECT count(*) FROM idempotency_records)::int AS claims`,
  );
  return rows;
}

describe('PDF test 2: ownership and validation', () => {
  let receiver: ReceiverApp;
  let app: INestApplication;
  let loop: DeliveryLoop;

  beforeEach(async () => {
    await resetDatabase();
    receiver = await startReceiver({ databaseUrl: TEST_DATABASE_URL });
    receiver.clock.set(BASE_MS);
    app = await createTestApp({ clock: receiver.clock });
  });

  afterEach(async () => {
    if (loop) await loop.close();
    await app.close();
    await receiver.close();
  });

  function publish(key: string, json: unknown, auth = authA) {
    return request(app.getHttpServer())
      .post('/events')
      .set(auth)
      .set('Idempotency-Key', key)
      .send(json as Record<string, unknown>);
  }

  describe('invalid input', () => {
    it.each([
      ['missing endpointId', { eventType: 'a.b', payload: {} }],
      ['malformed endpointId', { endpointId: 'not-a-uuid', eventType: 'a.b', payload: {} }],
      ['unknown endpointId shape', { endpointId: 42, eventType: 'a.b', payload: {} }],
      ['empty eventType', { endpointId: SEED.endpointA1, eventType: '', payload: {} }],
      ['eventType over 100 chars', { endpointId: SEED.endpointA1, eventType: 'x'.repeat(101), payload: {} }],
      ['array payload', { endpointId: SEED.endpointA1, eventType: 'a.b', payload: [1, 2] }],
      ['scalar payload', { endpointId: SEED.endpointA1, eventType: 'a.b', payload: 'nope' }],
      ['missing payload', { endpointId: SEED.endpointA1, eventType: 'a.b' }],
    ])('rejects %s with 400 and writes nothing', async (_label, json) => {
      const res = await publish('invalid', json);
      expect(res.status).toBe(400);
      expect(res.body).toMatchObject({ code: 'bad_request' });
      expect(typeof res.body.message).toBe('string');
      expect(res.body.stack).toBeUndefined();
      expect(await counts()).toEqual({ events: 0, deliveries: 0, claims: 0 });
    });

    it('rejects a caller-supplied destination field instead of ignoring it', async () => {
      // A caller trying to steer the webhook somewhere: the contract has no
      // place for a URL, so this is a 400, not a silently dropped field.
      const attempts = [
        { endpointId: SEED.endpointA1, eventType: 'a.b', payload: {}, url: 'http://169.254.169.254/' },
        { endpointId: SEED.endpointA1, eventType: 'a.b', payload: {}, destination: 'http://evil.test' },
        { endpointId: SEED.endpointA1, eventType: 'a.b', payload: {}, secret: 'rotate-me' },
      ];
      for (const [index, json] of attempts.entries()) {
        const res = await publish(`dest-${index}`, json);
        expect(res.status).toBe(400);
        expect(res.body.code).toBe('bad_request');
      }
      expect(await counts()).toEqual({ events: 0, deliveries: 0, claims: 0 });

      // A rejected request never consumes its key: the corrected body succeeds.
      const ok = await publish('dest-0', {
        endpointId: SEED.endpointA1,
        eventType: 'a.b',
        payload: { orderId: 'ord_ok' },
      });
      expect(ok.status).toBe(202);
      expect(await counts()).toEqual({ events: 1, deliveries: 1, claims: 1 });
    });

    it('rejects an oversized body with 413 before any row exists', async () => {
      const res = await publish('big', {
        endpointId: SEED.endpointA1,
        eventType: 'order.created',
        payload: { blob: 'x'.repeat(70 * 1024) },
      });
      expect(res.status).toBe(413);
      expect(res.body).toMatchObject({ code: 'payload_too_large' });
      expect(await counts()).toEqual({ events: 0, deliveries: 0, claims: 0 });
    });
  });

  describe('cross-tenant access', () => {
    it('answers 404 for another tenant endpoint without creating work', async () => {
      const res = await publish('cross', {
        endpointId: SEED.endpointB1,
        eventType: 'order.created',
        payload: { orderId: 'ord_x' },
      });
      expect(res.status).toBe(404);
      expect(res.body).toMatchObject({ code: 'not_found' });
      // Identical shape to "unknown id": existence is never leaked.
      const unknown = await publish('unknown', {
        endpointId: 'ffffffff-0000-4000-8000-0000000000ff',
        eventType: 'order.created',
        payload: { orderId: 'ord_x' },
      });
      expect(unknown.body.code).toBe(res.body.code);
      expect(await counts()).toEqual({ events: 0, deliveries: 0, claims: 0 });
    });

    it('hides another tenant event, and its delivery, behind the same 404', async () => {
      const published = await publish('owner', {
        endpointId: SEED.endpointA1,
        eventType: 'order.created',
        payload: { orderId: 'ord_owner' },
      });
      const { eventId, deliveryId } = published.body;

      await request(app.getHttpServer()).get(`/events/${eventId}`).set(authB).expect(404);
      const listedB = await request(app.getHttpServer()).get('/deliveries').set(authB).expect(200);
      expect(listedB.body.data.map((i: { deliveryId: string }) => i.deliveryId)).not.toContain(
        deliveryId,
      );
      const listedA = await request(app.getHttpServer()).get('/deliveries').set(authA).expect(200);
      expect(listedA.body.data).toHaveLength(1);
    });
  });

  describe('unauthorised redrive', () => {
    it('requires the operator role and leaves the queue unchanged', async () => {
      const { deliveryId } = await insertDelivery(testDb, {
        state: 'DEAD',
        endpointId: SEED.endpointA1,
        attemptCount: 5,
        attemptsInCycle: 5,
        nextAttemptAt: null,
      });

      const asTenant = await request(app.getHttpServer())
        .post(`/ops/deliveries/${deliveryId}/redrive`)
        .set(authA)
        .set('Idempotency-Key', 'tenant-tries')
        .send({ reason: 'escalate' });
      expect(asTenant.status).toBe(403);
      expect(asTenant.body).toMatchObject({ code: 'forbidden' });

      const anonymous = await request(app.getHttpServer())
        .post(`/ops/deliveries/${deliveryId}/redrive`)
        .set('Idempotency-Key', 'anon-tries')
        .send({ reason: 'no token' });
      expect(anonymous.status).toBe(401);

      const rows = await q<{ state: string; cycle: number; attempts_in_cycle: number }>(
        'SELECT state, cycle, attempts_in_cycle FROM deliveries WHERE id = $1',
        [deliveryId],
      );
      expect(rows[0]).toMatchObject({ state: DeliveryState.DEAD, cycle: 1, attempts_in_cycle: 5 });
      expect(await q('SELECT 1 FROM redrive_audit')).toEqual([]);
      // Rejected attempts consumed no idempotency keys.
      expect(await q('SELECT 1 FROM idempotency_records')).toEqual([]);

      // The operator can, and that is the only path that requeues work.
      const asOperator = await request(app.getHttpServer())
        .post(`/ops/deliveries/${deliveryId}/redrive`)
        .set({ Authorization: `Bearer ${TEST_TOKENS.operator}` })
        .set('Idempotency-Key', 'operator-legit')
        .send({ reason: 'receiver fixed' });
      expect(asOperator.status).toBe(202);
      expect((await q<{ state: string }>('SELECT state FROM deliveries WHERE id = $1', [deliveryId]))[0]
        .state).toBe(DeliveryState.READY);
    });

    it('refuses the operator token on tenant-scoped reads', async () => {
      await request(app.getHttpServer()).get('/deliveries').set(authA).expect(200);
      await request(app.getHttpServer())
        .get('/deliveries')
        .set({ Authorization: `Bearer ${TEST_TOKENS.operator}` })
        .expect(403);
    });
  });

  describe('the destination cannot be replaced by the caller', () => {
    it('dispatches to the configured endpoint even when the payload carries a URL', async () => {
      const capture = await startCaptureEndpoint(undefined, SEED.endpointSecretA1);
      const diverted = await startCaptureEndpoint();
      try {
        loop = await startLoop(receiver, {
          owner: 'worker-t2',
          destinationUrl: capture.url('/configured'),
        });
        const res = await publish('payload-url', {
          endpointId: loop.endpointId,
          eventType: 'order.created',
          // Arbitrary business data: the payload is never interpreted as config.
          payload: { url: diverted.url('/injected'), callbackUrl: diverted.url('/cb'), webhook: diverted.url() },
        });
        expect(res.status).toBe(202);
        const { deliveryId } = res.body;

        loop.start();
        await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);

        const stored = await q<{ endpoint_id: string }>('SELECT endpoint_id FROM deliveries WHERE id = $1', [
          deliveryId,
        ]);
        expect(stored[0].endpoint_id).toBe(loop.endpointId);

        expect(capture.requests).toHaveLength(1);
        expect(capture.requests[0].path).toBe('/configured');
        // The URL the caller put in the payload was never contacted.
        expect(diverted.requests).toHaveLength(0);
      } finally {
        await capture.close();
        await diverted.close();
      }
    });
  });
});
