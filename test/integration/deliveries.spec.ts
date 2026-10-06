import type { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { createTestApp } from '../helpers/app';
import { resetDatabase } from '../helpers/db';
import { SEED, TEST_TOKENS } from '../helpers/test-env';
import { MAX_PAGE_SIZE } from '../../src/modules/deliveries/dto';

const authA = { Authorization: `Bearer ${TEST_TOKENS.tenantA}` };
const authB = { Authorization: `Bearer ${TEST_TOKENS.tenantB}` };
const authOp = { Authorization: `Bearer ${TEST_TOKENS.operator}` };

/**
 * M6: GET /deliveries - tenant-scoped, paginated, state-filterable listing.
 */
describe('M6 delivery listing', () => {
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

  async function publish(key: string, auth = authA, endpointId = SEED.endpointA1) {
    return request(app.getHttpServer())
      .post('/events')
      .set(auth)
      .set('Idempotency-Key', key)
      .send({ endpointId, eventType: 'order.created', payload: { key } })
      .expect(202);
  }

  function list(query: string, auth = authA) {
    return request(app.getHttpServer()).get(`/deliveries${query}`).set(auth);
  }

  it('returns only the authenticated tenant deliveries (isolation)', async () => {
    await publish('a1', authA);
    await publish('a2', authA);
    await publish('b1', authB, SEED.endpointB1);

    const resA = await list('', authA).expect(200);
    expect(resA.body.data).toHaveLength(2);
    for (const item of resA.body.data) {
      expect(item.endpointId).toMatch(/-0000000000a[12]$/);
    }

    const resB = await list('', authB).expect(200);
    expect(resB.body.data).toHaveLength(1);
    expect(resB.body.data[0].endpointId).toBe(SEED.endpointB1);
  });

  it('rejects an operator token on this tenant-scoped route (403)', async () => {
    await list('', authOp).expect(403);
  });

  it('paginates with a stable keyset cursor and no duplicates or gaps', async () => {
    for (let i = 0; i < 5; i += 1) {
      await publish(`p${i}`, authA);
    }

    const seen: string[] = [];
    let cursor: string | undefined;
    let pages = 0;
    do {
      const qs = cursor ? `?limit=2&cursor=${encodeURIComponent(cursor)}` : '?limit=2';
      const res = await list(qs, authA).expect(200);
      expect(res.body.data.length).toBeLessThanOrEqual(2);
      for (const item of res.body.data) seen.push(item.deliveryId);
      cursor = res.body.pagination.nextCursor ?? undefined;
      pages += 1;
      expect(pages).toBeLessThanOrEqual(4); // safety against infinite loop
    } while (cursor);

    expect(seen).toHaveLength(5);
    expect(new Set(seen).size).toBe(5); // no duplicates
  });

  it('returns null nextCursor on the final page', async () => {
    await publish('only', authA);
    const res = await list('?limit=10', authA).expect(200);
    expect(res.body.data).toHaveLength(1);
    expect(res.body.pagination.nextCursor).toBeNull();
  });

  it('orders newest-first by creation', async () => {
    const first = await publish('older', authA);
    const second = await publish('newer', authA);
    const res = await list('', authA).expect(200);
    expect(res.body.data[0].deliveryId).toBe(second.body.deliveryId);
    expect(res.body.data[1].deliveryId).toBe(first.body.deliveryId);
  });

  it('filters by state', async () => {
    await publish('s1', authA);
    const ready = await list('?state=READY', authA).expect(200);
    expect(ready.body.data).toHaveLength(1);
    expect(ready.body.data[0].state).toBe('READY');

    const dead = await list('?state=DEAD', authA).expect(200);
    expect(dead.body.data).toHaveLength(0);
  });

  it('never exposes secrets, envelope bytes, or endpoint URLs', async () => {
    await publish('nosecrets', authA);
    const res = await list('', authA).expect(200);
    const serialized = JSON.stringify(res.body.data[0]);
    expect(serialized).not.toMatch(/secret/i);
    expect(serialized).not.toMatch(/envelope/i);
    expect(serialized).not.toMatch(/http:\/\//);
    expect(res.body.data[0]).not.toHaveProperty('url');
  });

  it('rejects an invalid state filter (400)', async () => {
    const res = await list('?state=BOGUS', authA).expect(400);
    expect(res.body.code).toBe('bad_request');
  });

  it.each([
    ['zero', '?limit=0'],
    ['over max', `?limit=${MAX_PAGE_SIZE + 1}`],
    ['non-integer', '?limit=abc'],
    ['negative', '?limit=-1'],
  ])('rejects an out-of-range limit: %s (400)', async (_label, qs) => {
    await list(qs, authA).expect(400);
  });

  it('rejects a malformed cursor (400)', async () => {
    await list('?cursor=not-valid-base64-json', authA).expect(400);
  });

  it('accepts limit at the documented maximum', async () => {
    await publish('max', authA);
    const res = await list(`?limit=${MAX_PAGE_SIZE}`, authA).expect(200);
    expect(res.body.pagination.limit).toBe(MAX_PAGE_SIZE);
  });
});
