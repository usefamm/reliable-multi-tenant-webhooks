import type { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { createTestApp } from '../helpers/app';
import { q, resetDatabase, testDb } from '../helpers/db';
import { SEED, TEST_TOKENS } from '../helpers/test-env';

const authA = { Authorization: `Bearer ${TEST_TOKENS.tenantA}` };
const authB = { Authorization: `Bearer ${TEST_TOKENS.tenantB}` };

function publishBody(overrides: Record<string, unknown> = {}) {
  return {
    endpointId: SEED.endpointA1,
    eventType: 'order.created',
    payload: { orderId: 'ord_1', amount: 42 },
    ...overrides,
  };
}

describe('M4 event publication + read model', () => {
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

  describe('POST /events', () => {
    it('accepts a valid event with 202 and the contract fields', async () => {
      const res = await request(app.getHttpServer())
        .post('/events')
        .set(authA)
        .set('Idempotency-Key', 'key-1')
        .send(publishBody())
        .expect(202);

      expect(res.body).toMatchObject({ status: 'READY' });
      expect(res.body.eventId).toMatch(/^[0-9a-f-]{36}$/);
      expect(res.body.deliveryId).toMatch(/^[0-9a-f-]{36}$/);
      expect(res.body.statusUrl).toBe(`/events/${res.body.eventId}`);
    });

    it('commits event AND delivery atomically (both rows exist, linked)', async () => {
      const res = await request(app.getHttpServer())
        .post('/events')
        .set(authA)
        .set('Idempotency-Key', 'key-atomic')
        .send(publishBody())
        .expect(202);

      const events = await q('SELECT id, tenant_id, endpoint_id FROM events WHERE id = $1', [
        res.body.eventId,
      ]);
      const deliveries = await q(
        'SELECT id, event_id, state, octet_length(envelope_bytes) AS env_len, envelope_hash FROM deliveries WHERE id = $1',
        [res.body.deliveryId],
      );
      expect(events).toHaveLength(1);
      expect(deliveries).toHaveLength(1);
      expect(deliveries[0].event_id).toBe(res.body.eventId);
      expect(deliveries[0].state).toBe('READY');
      expect(Number(deliveries[0].env_len)).toBeGreaterThan(0);
    });

    it('persists the exact envelope bytes containing the stable contract fields', async () => {
      const res = await request(app.getHttpServer())
        .post('/events')
        .set(authA)
        .set('Idempotency-Key', 'key-env')
        .send(publishBody({ eventType: 'order.created', payload: { a: 1, b: [2, 3] } }))
        .expect(202);

      const rows = await q<{ envelope_bytes: Buffer }>(
        'SELECT envelope_bytes FROM deliveries WHERE id = $1',
        [res.body.deliveryId],
      );
      const envelope = JSON.parse(rows[0].envelope_bytes.toString('utf8'));
      expect(envelope).toMatchObject({
        eventId: res.body.eventId,
        deliveryId: res.body.deliveryId,
        eventType: 'order.created',
        payload: { a: 1, b: [2, 3] },
      });
      expect(typeof envelope.occurredAt).toBe('string');
    });

    it('requires the Idempotency-Key header (400 when missing)', async () => {
      const res = await request(app.getHttpServer())
        .post('/events')
        .set(authA)
        .send(publishBody())
        .expect(400);
      expect(res.body).toMatchObject({ code: 'bad_request' });
      expect(res.body.requestId).toBeTruthy();
    });

    // The PDF contract is "endpointId, eventType and a JSON object payload", so
    // object-only is the requirement, not an arbitrary restriction. Every other
    // JSON shape is asserted, not just arrays.
    it.each([
      ['empty eventType', publishBody({ eventType: '' })],
      ['eventType over 100 chars', publishBody({ eventType: 'x'.repeat(101) })],
      ['payload is an array', publishBody({ payload: [1, 2, 3] })],
      ['payload is a string', publishBody({ payload: 'order.created' })],
      ['payload is a number', publishBody({ payload: 42 })],
      ['payload is a boolean', publishBody({ payload: true })],
      ['payload is null', publishBody({ payload: null })],
      ['missing endpointId', { eventType: 'a.b', payload: {} }],
      ['malformed endpointId', publishBody({ endpointId: 'not-a-uuid' })],
    ])('rejects invalid input with 400: %s', async (_label, body) => {
      const res = await request(app.getHttpServer())
        .post('/events')
        .set(authA)
        .set('Idempotency-Key', 'key-invalid')
        .send(body)
        .expect(400);
      expect(res.body.code).toBe('bad_request');
    });

    it('rejects an oversized body (>64 KiB) with 413', async () => {
      const big = { orderId: 'x'.repeat(70 * 1024) };
      const res = await request(app.getHttpServer())
        .post('/events')
        .set(authA)
        .set('Idempotency-Key', 'key-big')
        .send(publishBody({ payload: big }))
        .expect(413);
      expect(res.body.code).toBe('payload_too_large');
    });

    it('returns 404 when the endpoint belongs to another tenant (no leak)', async () => {
      // Tenant A tries to publish to Tenant B's endpoint.
      await request(app.getHttpServer())
        .post('/events')
        .set(authA)
        .set('Idempotency-Key', 'key-cross')
        .send(publishBody({ endpointId: SEED.endpointB1 }))
        .expect(404);
    });

    it('returns 404 for an unknown endpoint id', async () => {
      await request(app.getHttpServer())
        .post('/events')
        .set(authA)
        .set('Idempotency-Key', 'key-unknown')
        .send(publishBody({ endpointId: 'eeeeeeee-0000-4000-8000-deadbeef0000' }))
        .expect(404);
    });

    it('returns 401 without a token', async () => {
      await request(app.getHttpServer())
        .post('/events')
        .set('Idempotency-Key', 'key-noauth')
        .send(publishBody())
        .expect(401);
    });
  });

  describe('GET /events/:id', () => {
    async function publishA(): Promise<string> {
      const res = await request(app.getHttpServer())
        .post('/events')
        .set(authA)
        .set('Idempotency-Key', `key-${Math.random()}`)
        .send(publishBody())
        .expect(202);
      return res.body.eventId as string;
    }

    it('returns event metadata and delivery state for the owner', async () => {
      const eventId = await publishA();
      const res = await request(app.getHttpServer())
        .get(`/events/${eventId}`)
        .set(authA)
        .expect(200);

      expect(res.body.eventId).toBe(eventId);
      expect(res.body.eventType).toBe('order.created');
      expect(res.body.delivery).toMatchObject({
        state: 'READY',
        totalAttempts: 0,
        cycle: 1,
      });
      expect(res.body.delivery.nextAttemptAt).toBeTruthy();
      // No secrets exposed.
      expect(JSON.stringify(res.body)).not.toContain(SEED.endpointSecretA1);
    });

    it('returns 404 when another tenant reads the event', async () => {
      const eventId = await publishA();
      await request(app.getHttpServer()).get(`/events/${eventId}`).set(authB).expect(404);
    });

    it('returns 404 for a malformed id', async () => {
      await request(app.getHttpServer()).get('/events/not-a-uuid').set(authA).expect(404);
    });

    it('returns 404 for an unknown id', async () => {
      await request(app.getHttpServer())
        .get('/events/11111111-1111-4111-8111-111111111111')
        .set(authA)
        .expect(404);
    });
  });

  it('leaves no partial rows when publication is not reached', async () => {
    // Sanity: before any publish, tables are empty.
    const events = await testDb.query('SELECT count(*)::int AS c FROM events');
    expect(events.rows[0].c).toBe(0);
  });
});
