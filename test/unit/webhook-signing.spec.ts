import { createHmac } from 'node:crypto';
import { signWebhook, verifyWebhookSignature } from '../../src/modules/webhooks/signing';
import {
  DestinationNotAllowedError,
  WebhookClient,
  isAllowedDestination,
  parseRetryAfterMs,
} from '../../src/modules/webhooks/webhook.client';

const BODY = Buffer.from('{"a":1,"b":[2,3]}', 'utf8');
const TS = 1_767_225_600;

describe('webhook signing (PDF section 14)', () => {
  it('signs UTF8(timestamp + "." + raw bytes) as lowercase hex HMAC-SHA256', () => {
    const sig = signWebhook('s3cr3t', TS, BODY);
    const expected = createHmac('sha256', 's3cr3t')
      .update(`${TS}.`)
      .update(BODY)
      .digest('hex');
    expect(sig).toBe(expected);
    expect(sig).toBe(sig.toLowerCase());
    expect(sig).toMatch(/^[0-9a-f]{64}$/);
  });

  it('is sensitive to byte-level body changes (tampering fails verification)', () => {
    const sig = signWebhook('key', TS, BODY);
    expect(verifyWebhookSignature('key', TS, BODY, sig)).toBe(true);
    const tampered = Buffer.from('{"a":1,"b":[2,4]}', 'utf8'); // one byte changed
    expect(verifyWebhookSignature('key', TS, tampered, sig)).toBe(false);
  });

  it('rejects a wrong secret', () => {
    const sig = signWebhook('key', TS, BODY);
    expect(verifyWebhookSignature('other-key', TS, BODY, sig)).toBe(false);
  });

  it('a fresh timestamp yields a fresh signature over the same body', () => {
    const s1 = signWebhook('key', TS, BODY);
    const s2 = signWebhook('key', TS + 1, BODY);
    expect(s1).not.toBe(s2);
  });

  it('verify fails closed on malformed input', () => {
    expect(verifyWebhookSignature('key', TS, BODY, '')).toBe(false);
    expect(verifyWebhookSignature('key', TS, BODY, 'zz'.repeat(32))).toBe(false);
  });
});

describe('Retry-After parsing (PDF section 16)', () => {
  it.each([
    ['5', 5000],
    ['  12 ', 12000],
    ['0', 0],
  ])('accepts delta-seconds %s', (raw, expected) => {
    expect(parseRetryAfterMs(raw)).toBe(expected);
  });

  it.each([
    ['Wed, 21 Oct 2026 07:28:00 GMT'], // HTTP-date: unsupported -> normal backoff
    ['not-a-number'],
    ['-5'],
    ['1e9'],
    ['86401'],
    [''],
    [null],
  ])('rejects %s and falls back to normal backoff', (raw) => {
    expect(parseRetryAfterMs(raw as string | null)).toBeNull();
  });
});

describe('destination allowlist / SSRF guard', () => {
  it('allows everything when no allowlist is configured (endpoints table is the only source)', () => {
    expect(isAllowedDestination('http://127.0.0.1:4000/hook/x', '')).toBe(true);
  });

  it('matches host and host:port entries', () => {
    expect(isAllowedDestination('http://receiver.internal:4000/hook/1', 'receiver.internal')).toBe(true);
    expect(isAllowedDestination('http://receiver.internal:4000/hook/1', 'receiver.internal:4000')).toBe(true);
    expect(isAllowedDestination('http://receiver.internal:4000/hook/1', 'other.host')).toBe(false);
  });

  it('rejects unparseable URLs', () => {
    expect(isAllowedDestination('not a url', 'allowed.host')).toBe(false);
  });

  it('client refuses dispatch to a non-allowlisted destination', () => {
    const client = new WebhookClient({
      WEBHOOK_TIMEOUT_MS: 2000,
      WEBHOOK_MAX_RESPONSE_BYTES: 4096,
      WEBHOOK_ALLOWED_HOSTS: '127.0.0.1:9',
    });
    // Rejection is synchronous: a disallowed destination must never reach the
    // network at all, let alone consume a retry attempt.
    expect(() =>
      client.dispatch({
        url: 'http://169.254.169.254/latest/meta-data',
        secret: 'k',
        eventId: 'e',
        deliveryId: 'd',
        attemptId: 'a',
        body: BODY,
        timestampUnixSec: TS,
      }),
    ).toThrow(DestinationNotAllowedError);
  });
});
