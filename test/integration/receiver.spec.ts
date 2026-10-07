/**
 * Mock receiver acceptance tests (PDF sections 14, 15, 22).
 *
 * Everything here runs against a real receiver over a real socket and the real
 * PostgreSQL instance: verification order, durable deduplication, restart
 * survival and each test-only failure mode are observed, not simulated.
 */
import { newUuid } from '../../src/common/ids';
import { canonicalJson } from '../../src/common/canonical-json';
import { sha256Hex } from '../../src/common/hash';
import { ReceiverMode } from '../../src/receiver/repository';
import { SEED, TEST_DATABASE_URL } from '../helpers/test-env';
import { startReceiver, type ReceiverApp } from '../helpers/receiver';

const { endpointA1, endpointA2, endpointSecretA1: SECRET } = SEED;

/** Envelope bytes are built once per test so retries reuse them exactly,
 *  mirroring how the delivery service persists envelope_bytes. */
function envelope(eventId: string, deliveryId: string, payload: Record<string, unknown> = {}): string {
  return JSON.stringify({
    eventId,
    deliveryId,
    eventType: 'order.created',
    occurredAt: '2026-01-01T00:00:00.000Z',
    payload: { orderId: 'order-1', ...payload },
  });
}

describe('mock receiver', () => {
  let app: ReceiverApp;

  beforeEach(async () => {
    app = await startReceiver({ databaseUrl: TEST_DATABASE_URL });
    await app.repo.resetAll();
  });
  afterEach(async () => {
    await app.close();
  });

  const endpointId = endpointA1;

  async function effectsFor(eventId: string, endpoint = endpointId): Promise<number> {
    const rows = await app.repo.listEffects(endpoint);
    return rows.filter((r) => r.event_id === eventId).length;
  }

  describe('signature verification', () => {
    it('accepts a correctly signed webhook and applies the effect', async () => {
      const eventId = newUuid();
      const deliveryId = newUuid();
      const res = await app.post({
        endpointId,
        eventId,
        deliveryId,
        attemptId: newUuid(),
        secret: SECRET,
        body: envelope(eventId, deliveryId),
      });

      expect(res.status).toBe(200);
      expect(res.json).toMatchObject({ ok: true, applied: true, deduplicated: false });
      expect(await effectsFor(eventId)).toBe(1);
    });

    it('rejects a body modified after signing', async () => {
      const eventId = newUuid();
      const res = await app.post({
        endpointId,
        eventId,
        deliveryId: newUuid(),
        attemptId: newUuid(),
        secret: SECRET,
        body: envelope(eventId, newUuid()),
        tamper: true,
      });

      expect(res.status).toBe(401);
      expect(res.json?.code).toBe('invalid_signature');
      expect(await effectsFor(eventId)).toBe(0);
      const requests = await app.repo.listRequests(endpointId);
      expect(requests.at(-1)?.signature_ok).toBe(false);
    });

    it('rejects a signature produced with a different secret', async () => {
      const eventId = newUuid();
      const res = await app.post({
        endpointId,
        eventId,
        deliveryId: newUuid(),
        attemptId: newUuid(),
        secret: 'not-the-endpoint-secret',
        body: envelope(eventId, newUuid()),
      });
      expect(res.status).toBe(401);
      expect(res.json?.code).toBe('invalid_signature');
      expect(await effectsFor(eventId)).toBe(0);
    });

    it('rejects a malformed signature instead of throwing', async () => {
      const eventId = newUuid();
      const res = await app.post({
        endpointId,
        eventId,
        deliveryId: newUuid(),
        attemptId: newUuid(),
        secret: SECRET,
        body: envelope(eventId, newUuid()),
        signature: 'deadbeef',
      });
      expect(res.status).toBe(401);
      expect(await effectsFor(eventId)).toBe(0);
    });

    it('rejects a request with no signature header', async () => {
      const eventId = newUuid();
      const res = await app.post({
        endpointId,
        eventId,
        deliveryId: newUuid(),
        attemptId: newUuid(),
        secret: SECRET,
        body: envelope(eventId, newUuid()),
        omit: ['signature'],
      });
      expect(res.status).toBe(401);
    });

    it('rejects a missing attempt identity', async () => {
      const eventId = newUuid();
      const res = await app.post({
        endpointId,
        eventId,
        deliveryId: newUuid(),
        attemptId: newUuid(),
        secret: SECRET,
        body: envelope(eventId, newUuid()),
        omit: ['attempt'],
      });
      expect(res.status).toBe(401);
    });
  });

  describe('timestamp freshness', () => {
    it('rejects a timestamp older than the tolerance', async () => {
      const eventId = newUuid();
      const res = await app.post({
        endpointId,
        eventId,
        deliveryId: newUuid(),
        attemptId: newUuid(),
        secret: SECRET,
        body: envelope(eventId, newUuid()),
        timestampSec: app.clock.nowUnixSeconds() - 301,
      });
      expect(res.status).toBe(401);
      expect(res.json?.code).toBe('stale_timestamp');
      expect(await effectsFor(eventId)).toBe(0);
    });

    it('rejects a future timestamp beyond the tolerance (replay both directions)', async () => {
      const eventId = newUuid();
      const res = await app.post({
        endpointId,
        eventId,
        deliveryId: newUuid(),
        attemptId: newUuid(),
        secret: SECRET,
        body: envelope(eventId, newUuid()),
        timestampSec: app.clock.nowUnixSeconds() + 301,
      });
      expect(res.json?.code).toBe('stale_timestamp');
    });

    it('accepts a timestamp just inside the tolerance', async () => {
      const eventId = newUuid();
      const deliveryId = newUuid();
      const body = envelope(eventId, deliveryId);
      const res = await app.post({
        endpointId,
        eventId,
        deliveryId,
        attemptId: newUuid(),
        secret: SECRET,
        body,
        timestampSec: app.clock.nowUnixSeconds() - 299,
      });
      expect(res.status).toBe(200);
    });

    it('rejects a request with no timestamp header', async () => {
      const eventId = newUuid();
      const res = await app.post({
        endpointId,
        eventId,
        deliveryId: newUuid(),
        attemptId: newUuid(),
        secret: SECRET,
        body: envelope(eventId, newUuid()),
        omit: ['timestamp'],
      });
      expect(res.json?.code).toBe('stale_timestamp');
    });
  });

  describe('durable deduplication', () => {
    it('acknowledges a replay without applying a second effect', async () => {
      const eventId = newUuid();
      const deliveryId = newUuid();
      const body = envelope(eventId, deliveryId);

      const first = await app.post({ endpointId, eventId, deliveryId, attemptId: newUuid(), secret: SECRET, body });
      app.clock.advance(1_000);
      const second = await app.post({ endpointId, eventId, deliveryId, attemptId: newUuid(), secret: SECRET, body });

      expect(first.json).toMatchObject({ applied: true });
      expect(second.status).toBe(200);
      expect(second.json).toMatchObject({ applied: false, deduplicated: true });
      expect(await effectsFor(eventId)).toBe(1);
    });

    it('treats the same identity with different content as a conflict', async () => {
      const eventId = newUuid();
      const deliveryId = newUuid();
      const body = envelope(eventId, deliveryId);
      await app.post({
        endpointId,
        eventId,
        deliveryId,
        attemptId: newUuid(),
        secret: SECRET,
        body,
      });
      app.clock.advance(1_000);

      const conflict = await app.post({
        endpointId,
        eventId,
        deliveryId,
        attemptId: newUuid(),
        secret: SECRET,
        // Same ids, different business content - never silently accepted.
        body: envelope(eventId, deliveryId, { orderId: 'order-2' }),
      });

      expect(conflict.status).toBe(409);
      expect(conflict.json?.code).toBe('content_conflict');
      const effects = await app.repo.listEffects(endpointId);
      expect(effects).toHaveLength(1);
      // The stored effect still describes the ORIGINAL content.
      expect(effects[0].content_hash).toBe(
        sha256Hex(canonicalJson(JSON.parse(body) as unknown)),
      );
    });

    it('survives a receiver restart: a fresh process deduplicates against the database', async () => {
      const eventId = newUuid();
      const deliveryId = newUuid();
      const body = envelope(eventId, deliveryId);
      await app.post({ endpointId, eventId, deliveryId, attemptId: newUuid(), secret: SECRET, body });

      // Simulate a restart: new process state, new pool, new handler.
      await app.close();
      const restarted = await startReceiver({ databaseUrl: TEST_DATABASE_URL });
      app = restarted;

      const res = await restarted.post({
        endpointId,
        eventId,
        deliveryId,
        attemptId: newUuid(),
        secret: SECRET,
        body,
      });
      expect(res.status).toBe(200);
      expect(res.json).toMatchObject({ applied: false, deduplicated: true });
      expect(await restarted.repo.listEffects(endpointId)).toHaveLength(1);
    });

    it('refuses to apply anything when header and body identities disagree', async () => {
      const eventId = newUuid();
      const otherEvent = newUuid();
      const deliveryId = newUuid();
      const res = await app.post({
        endpointId,
        eventId,
        deliveryId,
        attemptId: newUuid(),
        secret: SECRET,
        // Correctly signed, but the body claims a different event.
        body: envelope(otherEvent, deliveryId),
      });
      expect(res.status).toBe(400);
      expect(res.json?.code).toBe('identity_mismatch');
      expect(await effectsFor(eventId)).toBe(0);
      expect(await effectsFor(otherEvent)).toBe(0);
    });

    it('rejects an endpoint the receiver has no identity for', async () => {
      const eventId = newUuid();
      const res = await app.post({
        endpointId: newUuid(),
        eventId,
        deliveryId: newUuid(),
        attemptId: newUuid(),
        secret: SECRET,
        body: envelope(eventId, newUuid()),
      });
      expect(res.status).toBe(401);
      expect(res.json?.code).toBe('unknown_endpoint');
      expect(await app.repo.listEffects()).toHaveLength(0);
    });

    it('isolates deduplication per endpoint', async () => {
      const eventId = newUuid();
      const deliveryId = newUuid();
      const body = envelope(eventId, deliveryId);
      const at = (endpoint: string, attemptId: string) =>
        app.post({ endpointId: endpoint, eventId, deliveryId, attemptId, secret: SECRET, body });

      const original = await app.db.query<{ secret: string }>(
        'SELECT secret FROM endpoints WHERE id = $1',
        [endpointA2],
      );
      // endpointA2 has a different secret, so share this receiver identity with it.
      await app.db.query('UPDATE endpoints SET secret = $2 WHERE id = $1', [endpointA2, SECRET]);
      try {
        expect((await at(endpointA1, newUuid())).status).toBe(200);
        expect((await at(endpointA2, newUuid())).status).toBe(200);
        expect((await at(endpointA1, newUuid())).json).toMatchObject({ deduplicated: true });
        expect(await effectsFor(eventId, endpointA1)).toBe(1);
        expect(await effectsFor(eventId, endpointA2)).toBe(1);
      } finally {
        await app.db.query('UPDATE endpoints SET secret = $2 WHERE id = $1', [
          endpointA2,
          original.rows[0].secret,
        ]);
      }
    });

    it('rejects an oversized body with 413 instead of buffering it', async () => {
      await app.close();
      app = await startReceiver({ databaseUrl: TEST_DATABASE_URL, maxBodyBytes: 128 });
      const eventId = newUuid();
      const res = await app.post({
        endpointId,
        eventId,
        deliveryId: newUuid(),
        attemptId: newUuid(),
        secret: SECRET,
        body: envelope(eventId, newUuid(), { blob: 'x'.repeat(400) }),
      });
      expect(res.status).toBe(413);
      expect(res.json?.code).toBe('payload_too_large');
      expect(await effectsFor(eventId)).toBe(0);
    });
  });

  describe('failure modes', () => {
    const ids = () => {
      const eventId = newUuid();
      const deliveryId = newUuid();
      return { eventId, deliveryId, body: envelope(eventId, deliveryId) };
    };

    it('lost_response commits the effect durably, then drops the connection', async () => {
      const { eventId, deliveryId, body } = ids();
      await app.repo.setMode({ endpointId, mode: ReceiverMode.LOST_RESPONSE, remaining: 1 });

      const first = await app.post({ endpointId, eventId, deliveryId, attemptId: newUuid(), secret: SECRET, body });
      // The sender sees a broken transport, not a status - so it must retry.
      expect(first.status).toBe(0);
      expect(first.transportError).toBeTruthy();
      // ...and the effect is already durable.
      expect(await effectsFor(eventId)).toBe(1);

      const retry = await app.post({ endpointId, eventId, deliveryId, attemptId: newUuid(), secret: SECRET, body });
      expect(retry.status).toBe(200);
      expect(retry.json).toMatchObject({ applied: false, deduplicated: true });
      expect(await effectsFor(eventId)).toBe(1);
    });

    it('temp_failure fails a bounded number of times then returns to success', async () => {
      const { eventId, deliveryId, body } = ids();
      await app.repo.setMode({ endpointId, mode: ReceiverMode.TEMP_FAILURE, remaining: 2 });

      const first = await app.post({ endpointId, eventId, deliveryId, attemptId: newUuid(), secret: SECRET, body });
      expect(first.status).toBe(503);
      expect(await effectsFor(eventId)).toBe(0);

      const second = await app.post({ endpointId, eventId, deliveryId, attemptId: 'a', secret: SECRET, body });
      expect(second.status).toBe(503);

      // Budget exhausted: the counted mode stops applying.
      const third = await app.post({ endpointId, eventId, deliveryId, attemptId: newUuid(), secret: SECRET, body });
      expect(third.status).toBe(200);
      expect(third.json).toMatchObject({ applied: true });
      expect(await effectsFor(eventId)).toBe(1);
    });

    it('perm_failure returns 500 and applies nothing', async () => {
      const { eventId, deliveryId, body } = ids();
      await app.repo.setMode({ endpointId, mode: ReceiverMode.PERM_FAILURE });
      const res = await app.post({ endpointId, eventId, deliveryId, attemptId: newUuid(), secret: SECRET, body });
      expect(res.status).toBe(500);
      expect(await effectsFor(eventId)).toBe(0);
    });

    it('rate_limited returns 429 with a Retry-After the sender can honour', async () => {
      const { eventId, deliveryId, body } = ids();
      await app.repo.setMode({ endpointId, mode: ReceiverMode.RATE_LIMITED, retryAfter: 7 });
      const res = await app.post({ endpointId, eventId, deliveryId, attemptId: newUuid(), secret: SECRET, body });
      expect(res.status).toBe(429);
      expect(res.headers['retry-after']).toBe('7');
      expect(await effectsFor(eventId)).toBe(0);
    });

    it('reject_400 returns a non-retryable rejection and applies nothing', async () => {
      const { eventId, deliveryId, body } = ids();
      await app.repo.setMode({ endpointId, mode: ReceiverMode.REJECT_400 });
      const res = await app.post({ endpointId, eventId, deliveryId, attemptId: newUuid(), secret: SECRET, body });
      expect(res.status).toBe(400);
      expect(await effectsFor(eventId)).toBe(0);
    });

    it('redirect answers 3xx with a Location, which the sender must never follow', async () => {
      const { eventId, deliveryId, body } = ids();
      await app.repo.setMode({ endpointId, mode: ReceiverMode.REDIRECT });
      const res = await app.post({ endpointId, eventId, deliveryId, attemptId: newUuid(), secret: SECRET, body });
      expect(res.status).toBe(302);
      expect(res.headers.location).toBeTruthy();
      expect(await effectsFor(eventId)).toBe(0);
    });

    it('slow applies the effect durably before delaying the response', async () => {
      const { eventId, deliveryId, body } = ids();
      await app.repo.setMode({ endpointId, mode: ReceiverMode.SLOW, delayMs: 40 });
      const started = Date.now();
      const res = await app.post({ endpointId, eventId, deliveryId, attemptId: newUuid(), secret: SECRET, body });
      expect(res.status).toBe(200);
      expect(res.json).toMatchObject({ applied: true });
      expect(Date.now() - started).toBeGreaterThanOrEqual(30);
      // A sender that timed out and retried would find the effect already applied.
      expect(await effectsFor(eventId)).toBe(1);
    });

    it('prefers an event-scoped mode over an endpoint-wide one', async () => {
      const targeted = newUuid();
      const other = newUuid();
      const deliveryId = newUuid();
      await app.repo.setMode({ endpointId, mode: ReceiverMode.TEMP_FAILURE });
      await app.repo.setMode({ endpointId, eventId: targeted, mode: ReceiverMode.SUCCESS });

      const hit = await app.post({
        endpointId,
        eventId: targeted,
        deliveryId,
        attemptId: newUuid(),
        secret: SECRET,
        body: envelope(targeted, deliveryId),
      });
      expect(hit.status).toBe(200);

      const swept = await app.post({
        endpointId,
        eventId: other,
        deliveryId,
        attemptId: newUuid(),
        secret: SECRET,
        body: envelope(other, deliveryId),
      });
      expect(swept.status).toBe(503);
    });

    it('records every inbound request, including rejected ones', async () => {
      const { eventId, deliveryId, body } = ids();
      await app.post({ endpointId, eventId, deliveryId, attemptId: newUuid(), secret: 'wrong', body });
      await app.post({ endpointId, eventId, deliveryId, attemptId: newUuid(), secret: SECRET, body });

      const requests = await app.repo.listRequests(endpointId);
      expect(requests).toHaveLength(2);
      expect(requests.filter((r) => r.signature_ok)).toHaveLength(1);
      expect(requests.filter((r) => !r.signature_ok)).toHaveLength(1);
      expect(new Set(requests.map((r) => r.mode))).toEqual(new Set(['rejected', 'success']));
    });
  });

  describe('test control surface', () => {
    it('sets and clears modes over HTTP and exposes inspection endpoints', async () => {
      const eventId = newUuid();
      const deliveryId = newUuid();

      const put = await app.call('PUT', '/__control/modes', {
        endpointId,
        eventId,
        mode: ReceiverMode.TEMP_FAILURE,
        remaining: 1,
      });
      expect(put.status).toBe(200);

      const declined = await app.post({
        endpointId,
        eventId,
        deliveryId,
        attemptId: newUuid(),
        secret: SECRET,
        body: envelope(eventId, deliveryId),
      });
      expect(declined.status).toBe(503);

      const requests = await app.call('GET', `/__control/requests?endpointId=${endpointId}`);
      expect(requests.status).toBe(200);
      expect(requests.json?.requests).toBeInstanceOf(Array);

      const reset = await app.call('POST', '/__control/reset');
      expect(reset.status).toBe(200);
      const after = await app.call('GET', '/__control/requests');
      expect(after.json?.requests).toEqual([]);
    });

    it('validates mode names rather than accepting arbitrary strings', async () => {
      const bad = await app.call('PUT', '/__control/modes', { endpointId, mode: 'explode' });
      expect(bad.status).toBe(400);
      expect(bad.json?.code).toBe('bad_mode_request');
    });

    it('refuses the control surface when test controls are disabled', async () => {
      await app.close();
      app = await startReceiver({ databaseUrl: TEST_DATABASE_URL, testControls: false });
      const res = await app.call('PUT', '/__control/modes', { endpointId, mode: ReceiverMode.PERM_FAILURE });
      expect(res.status).toBe(403);
      expect(res.json?.code).toBe('test_controls_disabled');
      const health = await app.call('GET', '/health');
      expect(health.status).toBe(200);
    });

    it('answers 404 for unknown paths and non-POST webhooks', async () => {
      expect((await app.call('GET', '/nope')).status).toBe(404);
      expect((await app.call('GET', `/hook/${endpointId}`)).status).toBe(404);
    });
  });
});
