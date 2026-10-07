/**
 * PDF acceptance test 3 - signature verification.
 *
 * Verbatim requirement: "Signature verification: Accept valid raw bytes; reject
 * body tampering, a wrong secret and stale timestamps. A valid retry has a fresh
 * timestamp and stable event identity."
 *
 * Split across the two sides of the wire on purpose:
 *  - a raw capture endpoint records exactly what OUR service signed, so "fresh
 *    timestamp, stable event identity" is measured, not asserted from the code;
 *  - the real mock receiver decides what is acceptable, so replaying a captured
 *    request after its timestamp ages out is proved useless.
 */
import { DeliveryState } from '../../src/domain/types';
import { signWebhook } from '../../src/modules/webhooks/signing';
import { q, resetDatabase } from '../helpers/db';
import { startReceiver, type ReceiverApp } from '../helpers/receiver';
import { startLoop, type DeliveryLoop } from '../helpers/delivery-loop';
import { BASE_MS } from '../helpers/worker';
import { startCaptureEndpoint, type CaptureEndpoint } from '../helpers/capture-endpoint';
import { SEED, TEST_DATABASE_URL } from '../helpers/test-env';

describe('PDF test 3: signature verification', () => {
  let receiver: ReceiverApp;
  let capture: CaptureEndpoint;
  let loop: DeliveryLoop;

  const epochSec = Math.floor(BASE_MS / 1000);

  beforeEach(async () => {
    await resetDatabase();
    receiver = await startReceiver({ databaseUrl: TEST_DATABASE_URL });
    receiver.clock.set(BASE_MS);
    // The capture endpoint shares the endpoint row's secret, exactly as a real
    // customer's receiver would.
    capture = await startCaptureEndpoint(
      (_req, index) =>
        index === 0 ? { status: 503, body: '{"code":"temporary"}' } : { status: 200, body: '{"ok":true}' },
      SEED.endpointSecretA1,
    );
    loop = await startLoop(receiver, {
      owner: 'worker-t3',
      destinationUrl: capture.url('/hook/signature'),
    });
    loop.start();
  });

  afterEach(async () => {
    await loop.close();
    await capture.close();
    await receiver.close();
  });

  /**
   * Publish and drive exactly one retry. Time is injected, so the failed first
   * attempt sits in RETRY_WAIT until the shared clock reaches its scheduled
   * instant - which is also what makes the two signatures comparable: one second
   * apart by construction, not by luck.
   */
  async function publishAndRetry(): Promise<{ eventId: string; deliveryId: string }> {
    const published = await loop.publish();
    await loop.waitForAttempts(published.deliveryId, 1);
    await loop.advanceToDue(published.deliveryId);
    await loop.waitForState(published.deliveryId, [DeliveryState.DELIVERED]);
    return published;
  }

  it('signs the exact persisted bytes with a fresh timestamp and stable identity on every attempt', async () => {
    const { deliveryId, eventId } = await publishAndRetry();

    expect(capture.requests).toHaveLength(2);
    const [first, retry] = capture.requests;

    // Stable identity: the event and the delivery are the same resource.
    expect(first.eventId).toBe(eventId);
    expect(retry.eventId).toBe(eventId);
    expect(first.deliveryId).toBe(retry.deliveryId);

    // Fresh per attempt: a new attempt id, a new timestamp, a new signature.
    expect(retry.attemptId).not.toBe(first.attemptId);
    expect(first.timestampSec).toBe(epochSec);
    expect(retry.timestampSec).toBe(epochSec + 1);
    expect(retry.signature).not.toBe(first.signature);

    // Both signatures verify over the bytes as received, with the configured secret.
    expect(first.signatureValid).toBe(true);
    expect(retry.signatureValid).toBe(true);

    // Same bytes, not a re-serialisation: a retry must not change what was signed.
    expect(retry.body.equals(first.body)).toBe(true);
    const [stored] = await q<{ envelope_bytes: Buffer }>(
      'SELECT envelope_bytes FROM deliveries WHERE id = $1',
      [deliveryId],
    );
    expect(first.body.equals(stored.envelope_bytes)).toBe(true);
    expect(JSON.parse(first.body.toString('utf8'))).toMatchObject({ eventId, deliveryId });

    // Outbound hygiene: the API token is never forwarded and the secret is never sent.
    for (const req of capture.requests) {
      expect(req.headers.authorization).toBeUndefined();
      expect(JSON.stringify(req.headers)).not.toMatch(SEED.endpointSecretA1);
      expect(req.body.toString('utf8')).not.toMatch(SEED.endpointSecretA1);
    }

    const attempts = await loop.attempts(deliveryId);
    expect(attempts.map((a) => a.outcome)).toEqual(['RETRYABLE', 'SUCCESS']);
    expect(new Set(attempts.map((a) => a.attempt_id)).size).toBe(2);
  });

  it('makes a captured request useless once its timestamp ages out, and accepts the same bytes re-signed', async () => {
    const { deliveryId, eventId } = await publishAndRetry();
    const captured = capture.requests[0];
    const body = captured.body;

    // Replay of the ORIGINAL, perfectly signed request 301s later: identity and
    // HMAC are intact, the timestamp is not - so the receiver refuses it.
    receiver.clock.set(BASE_MS + 301_000);
    const replay = await receiver.post({
      endpointId: loop.endpointId,
      eventId,
      deliveryId,
      attemptId: captured.attemptId as string,
      secret: SEED.endpointSecretA1,
      body,
      timestampSec: captured.timestampSec as number,
      signature: captured.signature as string,
    });
    expect(replay.status).toBe(401);
    expect(replay.json).toMatchObject({ code: 'stale_timestamp' });
    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(0);

    // A valid retry: identical bytes and identical event identity, freshly
    // timestamped and re-signed. The receiver accepts it and applies once.
    const freshSec = receiver.clock.nowUnixSeconds();
    const retried = await receiver.post({
      endpointId: loop.endpointId,
      eventId,
      deliveryId,
      attemptId: 'attempt-from-doc',
      secret: SEED.endpointSecretA1,
      body,
      timestampSec: freshSec,
      signature: signWebhook(SEED.endpointSecretA1, freshSec, body),
    });
    expect(retried.status).toBe(200);
    expect(retried.json).toMatchObject({ applied: true });
    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);

    // The replay attempt left no accepted request behind - only the fresh one.
    const requests = await receiver.repo.listRequests(loop.endpointId);
    expect(requests).toHaveLength(2);
    expect(requests.map((r) => r.signature_ok)).toEqual([false, true]);
  });

  it('rejects tampering, a wrong secret and a stale timestamp without applying anything', async () => {
    const { eventId, deliveryId } = await publishAndRetry();
    const body = capture.requests[0].body;
    const base = {
      endpointId: loop.endpointId,
      eventId,
      deliveryId,
      secret: SEED.endpointSecretA1,
      body,
    };

    // Bytes changed after signing (one trailing space) - signature no longer matches.
    const tampered = await receiver.post({ ...base, attemptId: 'a-tamper', tamper: true });
    expect(tampered.status).toBe(401);
    expect(tampered.json).toMatchObject({ code: 'invalid_signature' });

    // Right bytes, wrong key.
    const wrongSecret = await receiver.post({
      ...base,
      attemptId: 'a-wrong',
      secret: 'not-the-endpoint-secret',
    });
    expect(wrongSecret.status).toBe(401);
    expect(wrongSecret.json).toMatchObject({ code: 'invalid_signature' });

    // Right bytes, right key, timestamp outside the tolerance window.
    const stale = await receiver.post({
      ...base,
      attemptId: 'a-stale',
      timestampSec: receiver.clock.nowUnixSeconds() - 301,
    });
    expect(stale.status).toBe(401);
    expect(stale.json).toMatchObject({ code: 'stale_timestamp' });

    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(0);
    const rejected = await receiver.repo.listRequests(loop.endpointId);
    expect(rejected.every((r) => !r.signature_ok)).toBe(true);

    // The correctly signed original is still accepted afterwards.
    const accepted = await receiver.post({ ...base, attemptId: 'a-valid' });
    expect(accepted.status).toBe(200);
    expect(accepted.json).toMatchObject({ applied: true });
    expect(await receiver.repo.listEffects(loop.endpointId)).toHaveLength(1);
  });
});
