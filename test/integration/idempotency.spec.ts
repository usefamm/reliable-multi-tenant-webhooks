import type { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { createTestApp } from '../helpers/app';
import { q, resetDatabase } from '../helpers/db';
import { SEED, TEST_TOKENS } from '../helpers/test-env';

const authA = { Authorization: `Bearer ${TEST_TOKENS.tenantA}` };
const authB = { Authorization: `Bearer ${TEST_TOKENS.tenantB}` };

/**
 * PDF acceptance TEST 1: publication idempotency.
 *
 * Covers:
 *   - 20 concurrent identical publications -> exactly 1 event, 1 delivery,
 *     and every caller receives the same response body.
 *   - Same key + reordered JSON object keys -> replays the original response
 *     (object key order is irrelevant).
 *   - Same key + changed payload -> 409 conflict.
 *   - Different tenants may reuse the same key independently.
 *   - Array order remains significant (changed array -> 409).
 */
describe('M5 publication idempotency', () => {
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

  function post(key: string, body: Record<string, unknown>, auth = authA) {
    return request(app.getHttpServer()).post('/events').set(auth).set('Idempotency-Key', key).send(body);
  }

  it('collapses 20 concurrent identical publications into one event and one delivery', async () => {
    const body = {
      endpointId: SEED.endpointA1,
      eventType: 'order.created',
      payload: { orderId: 'ord_concurrent', amount: 100 },
    };

    const responses = await Promise.all(
      Array.from({ length: 20 }, () => post('key-concurrent', body)),
    );

    // Every caller sees 202 and the SAME eventId/deliveryId.
    for (const res of responses) {
      expect(res.status).toBe(202);
      expect(res.body.eventId).toBe(responses[0].body.eventId);
      expect(res.body.deliveryId).toBe(responses[0].body.deliveryId);
    }

    const events = await q('SELECT id FROM events');
    const deliveries = await q('SELECT id FROM deliveries');
    const records = await q('SELECT id FROM idempotency_records');
    expect(events).toHaveLength(1);
    expect(deliveries).toHaveLength(1);
    expect(records).toHaveLength(1);
  });

  it('replays the original response for the same key with reordered object keys', async () => {
    const first = await post('key-reorder', {
      endpointId: SEED.endpointA1,
      eventType: 'order.created',
      payload: { a: 1, b: 2, nested: { x: 1, y: 2 } },
    }).expect(202);

    const replay = await post('key-reorder', {
      endpointId: SEED.endpointA1,
      eventType: 'order.created',
      payload: { b: 2, nested: { y: 2, x: 1 }, a: 1 },
    }).expect(202);

    expect(replay.body).toEqual(first.body);

    const events = await q('SELECT id FROM events');
    expect(events).toHaveLength(1);
  });

  it('returns 409 when the same key is reused with a changed payload', async () => {
    await post('key-changed', {
      endpointId: SEED.endpointA1,
      eventType: 'order.created',
      payload: { orderId: 'ord_1' },
    }).expect(202);

    const res = await post('key-changed', {
      endpointId: SEED.endpointA1,
      eventType: 'order.created',
      payload: { orderId: 'ord_DIFFERENT' },
    });

    expect(res.status).toBe(409);
    expect(res.body).toMatchObject({ code: 'conflict' });
    expect(res.body.requestId).toBeDefined();

    // The conflict did not create a second event.
    const events = await q('SELECT id FROM events');
    expect(events).toHaveLength(1);
  });

  it('returns 409 when the same key is reused with a changed eventType', async () => {
    await post('key-type', {
      endpointId: SEED.endpointA1,
      eventType: 'order.created',
      payload: { orderId: 'ord_1' },
    }).expect(202);

    await post('key-type', {
      endpointId: SEED.endpointA1,
      eventType: 'order.updated',
      payload: { orderId: 'ord_1' },
    }).expect(409);
  });

  it('returns 409 when the same key is reused with a changed endpointId', async () => {
    await post('key-endpoint', {
      endpointId: SEED.endpointA1,
      eventType: 'order.created',
      payload: { orderId: 'ord_1' },
    }).expect(202);

    await post('key-endpoint', {
      endpointId: SEED.endpointA2,
      eventType: 'order.created',
      payload: { orderId: 'ord_1' },
    }).expect(409);
  });

  it('treats array order as significant (reordered array -> 409)', async () => {
    await post('key-array', {
      endpointId: SEED.endpointA1,
      eventType: 'order.created',
      payload: { items: [1, 2, 3] },
    }).expect(202);

    await post('key-array', {
      endpointId: SEED.endpointA1,
      eventType: 'order.created',
      payload: { items: [3, 2, 1] },
    }).expect(409);
  });

  it('allows different tenants to reuse the same idempotency key independently', async () => {
    const a = await post('shared-key', {
      endpointId: SEED.endpointA1,
      eventType: 'order.created',
      payload: { orderId: 'ord_a' },
    }, authA).expect(202);

    const b = await post('shared-key', {
      endpointId: SEED.endpointB1,
      eventType: 'order.created',
      payload: { orderId: 'ord_b' },
    }, authB).expect(202);

    // Distinct events for distinct tenants despite the identical key.
    expect(a.body.eventId).not.toBe(b.body.eventId);

    const events = await q('SELECT id, tenant_id FROM events');
    expect(events).toHaveLength(2);
    const records = await q('SELECT id FROM idempotency_records');
    expect(records).toHaveLength(2);
  });

  it('does not consume the key when validation fails (400 then success replays nothing)', async () => {
    // Invalid body first: must NOT write an idempotency record.
    await post('key-validation', {
      endpointId: SEED.endpointA1,
      eventType: '', // invalid: empty
      payload: { orderId: 'ord_1' },
    }).expect(400);

    const recordsAfterFailure = await q(
      "SELECT id FROM idempotency_records WHERE idempotency_key = 'key-validation'",
    );
    expect(recordsAfterFailure).toHaveLength(0);

    // The same key now succeeds because it was never consumed.
    await post('key-validation', {
      endpointId: SEED.endpointA1,
      eventType: 'order.created',
      payload: { orderId: 'ord_1' },
    }).expect(202);
  });
});
