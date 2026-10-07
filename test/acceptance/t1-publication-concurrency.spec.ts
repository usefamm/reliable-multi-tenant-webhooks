/**
 * PDF acceptance test 1 - publication concurrency.
 *
 * Verbatim requirement: "Twenty identical concurrent publications produce one
 * event and delivery. Reordered object keys replay correctly; changed payload
 * returns 409. Separate tenants may reuse the same key."
 *
 * The assertion that matters is not the HTTP response - it is that after 20
 * racing callers the durable queue holds ONE unit of work, and the real delivery
 * loop dispatches it exactly once to a real receiver that applies exactly one
 * business effect. Everything below inspects tables and receiver state.
 */
import type { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { DeliveryState } from '../../src/domain/types';
import { canonicalJson } from '../../src/common/canonical-json';
import { createTestApp } from '../helpers/app';
import { q, resetDatabase } from '../helpers/db';
import { startReceiver, type ReceiverApp } from '../helpers/receiver';
import { startLoop, type DeliveryLoop } from '../helpers/delivery-loop';
import { BASE_MS } from '../helpers/worker';
import { SEED, TEST_DATABASE_URL, TEST_TOKENS } from '../helpers/test-env';

const authA = { Authorization: `Bearer ${TEST_TOKENS.tenantA}` };
const authB = { Authorization: `Bearer ${TEST_TOKENS.tenantB}` };

const BURST_PAYLOAD = {
  orderId: 'ord_burst',
  amount: 4200,
  lines: [{ sku: 'a', qty: 1 }, { sku: 'b', qty: 2 }],
};

function body(endpointId: string, payload: Record<string, unknown> = BURST_PAYLOAD) {
  return JSON.parse(
    JSON.stringify({ endpointId, eventType: 'order.created', payload }),
  ) as Record<string, unknown>;
}

type Counts = { events: number; deliveries: number; claims: number };

async function counts(): Promise<Counts> {
  const [rows] = await q<Counts>(
    `SELECT (SELECT count(*) FROM events)::int              AS events,
            (SELECT count(*) FROM deliveries)::int          AS deliveries,
            (SELECT count(*) FROM idempotency_records)::int AS claims`,
  );
  return rows;
}

describe('PDF test 1: publication concurrency', () => {
  let receiver: ReceiverApp;
  let app: INestApplication;
  let loop: DeliveryLoop;

  beforeEach(async () => {
    await resetDatabase();
    receiver = await startReceiver({ databaseUrl: TEST_DATABASE_URL });
    receiver.clock.set(BASE_MS);
    // ONE clock for the API and the worker: the API schedules next_attempt_at,
    // the worker only claims work that is due on ITS clock.
    app = await createTestApp({ clock: receiver.clock });
    loop = await startLoop(receiver, { owner: 'worker-t1' });
  });

  afterEach(async () => {
    await loop.close();
    await app.close();
    await receiver.close();
  });

  async function publish(key: string, json: unknown, auth = authA) {
    return request(app.getHttpServer())
      .post('/events')
      .set(auth)
      .set('Idempotency-Key', key)
      .send(json as Record<string, unknown>);
  }

  it('collapses 20 concurrent identical publications into one event, one delivery and one receiver effect', async () => {
    const results = await Promise.all(
      Array.from({ length: 20 }, () => publish('burst-20', body(loop.endpointId))),
    );

    expect(results.every((r) => r.status === 202)).toBe(true);
    const eventIds = new Set(results.map((r) => r.body.eventId as string));
    const deliveryIds = new Set(results.map((r) => r.body.deliveryId as string));
    expect(eventIds.size).toBe(1);
    expect(deliveryIds.size).toBe(1);
    // Same resource identity for all 20; only the request id differs per call.
    // Compared canonically: a replay is re-read from JSONB, where key order is
    // Postgres', not ours.
    const payloads = new Set(results.map((r) => canonicalJson(r.body)));
    expect(payloads.size).toBe(1);
    // Each call is still individually traceable: the request id lives in the
    // X-Request-Id header (errors repeat it in the body), so 20 replays of one
    // resource produce 20 distinct ids, not one shared one.
    expect(new Set(results.map((r) => r.headers['x-request-id'] as string)).size).toBe(20);

    expect(await counts()).toEqual({ events: 1, deliveries: 1, claims: 1 });

    const deliveryId = [...deliveryIds][0];
    loop.start();
    const done = await loop.waitForState(deliveryId, [DeliveryState.DELIVERED]);
    const attempts = await loop.attempts(deliveryId);

    expect(done.attempt_count).toBe(1);
    expect(attempts).toHaveLength(1);
    expect(await receiver.repo.listRequests(loop.endpointId)).toHaveLength(1);
    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
  });

  it('replays the original event when the same key arrives with reordered object keys', async () => {
    const first = await publish('reorder', body(loop.endpointId, {
      amount: 4200,
      lines: [{ qty: 1, sku: 'a' }],
      orderId: 'ord_reorder',
    }));
    expect(first.status).toBe(202);

    // Same document, keys written in a different order by a different SDK.
    const replay = await publish('reorder', body(loop.endpointId, {
      orderId: 'ord_reorder',
      lines: [{ sku: 'a', qty: 1 }],
      amount: 4200,
    }));

    expect(replay.status).toBe(202);
    expect(replay.body.eventId).toBe(first.body.eventId);
    expect(canonicalJson(replay.body)).toBe(canonicalJson(first.body));
    expect(await counts()).toEqual({ events: 1, deliveries: 1, claims: 1 });
  });

  it('rejects a changed payload under the same key with 409 and creates nothing', async () => {
    const first = await publish('changed', body(loop.endpointId));
    expect(first.status).toBe(202);

    const conflictRes = await publish('changed', body(loop.endpointId, {
      ...BURST_PAYLOAD,
      amount: 9999,
    }));
    expect(conflictRes.status).toBe(409);
    expect(conflictRes.body).toMatchObject({ code: 'conflict' });
    expect(conflictRes.body.requestId).toBeTruthy();

    // A rejected call is not a second event, and the original claim survives:
    // the same key still replays the first response instead of re-executing.
    expect(await counts()).toEqual({ events: 1, deliveries: 1, claims: 1 });
    const replay = await publish('changed', body(loop.endpointId));
    expect(replay.status).toBe(202);
    expect(replay.body.eventId).toBe(first.body.eventId);
  });

  it('lets two tenants reuse the same idempotency key independently', async () => {
    const a = await publish('shared-key', body(loop.endpointId, { orderId: 'ord_a' }), authA);
    expect(a.status).toBe(202);

    // Tenant B naming tenant A's endpoint is a 404, and it neither consumes nor
    // collides with A's key - keys are scoped per tenant.
    const hijack = await publish('shared-key', body(loop.endpointId, { orderId: 'ord_b' }), authB);
    expect(hijack.status).toBe(404);
    expect(hijack.body).toMatchObject({ code: 'not_found' });

    const b = await publish('shared-key', body(SEED.endpointB1, { orderId: 'ord_b' }), authB);
    expect(b.status).toBe(202);
    expect(b.body.eventId).not.toBe(a.body.eventId);

    expect(await counts()).toEqual({ events: 2, deliveries: 2, claims: 2 });
    const tenants = await q<{ tenant_id: string }>('SELECT tenant_id FROM events ORDER BY tenant_id');
    expect(tenants.map((t) => t.tenant_id)).toEqual([SEED.tenantAId, SEED.tenantBId]);

    // Each tenant reads only its own event; the other gets a 404 that leaks nothing.
    await request(app.getHttpServer()).get(`/events/${a.body.eventId}`).set(authA).expect(200);
    await request(app.getHttpServer()).get(`/events/${a.body.eventId}`).set(authB).expect(404);
  });
});
