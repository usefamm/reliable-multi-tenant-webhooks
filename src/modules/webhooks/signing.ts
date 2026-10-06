import { createHmac, timingSafeEqual } from 'node:crypto';

/**
 * HMAC-SHA256 webhook signing (PDF sections 14).
 *
 * Signed input is UTF8( `${timestamp}.${exact raw body bytes}` ) where
 * `timestamp` is Unix seconds as a decimal string, and the body is the exact
 * persisted envelope bytes - never a re-serialization. The signature is
 * lowercase hex, so a receiver can recompute it byte-for-byte.
 *
 * A fresh attempt always gets a fresh timestamp, attempt id, and signature;
 * eventId/deliveryId stay stable.
 */
export function signWebhook(secret: string, timestampUnixSec: number, body: Buffer): string {
  return createHmac('sha256', secret).update(`${timestampUnixSec}.`).update(body).digest('hex');
}

/**
 * Recompute and compare with a constant-time comparison over equal-length
 * buffers. Any structural mismatch (bad hex, wrong length) fails closed without
 * leaking comparison timing about where it diverged.
 */
export function verifyWebhookSignature(
  secret: string,
  timestampUnixSec: number,
  body: Buffer,
  presented: string,
): boolean {
  const expected = Buffer.from(signWebhook(secret, timestampUnixSec, body), 'utf8');
  const actual = Buffer.from(presented ?? '', 'utf8');
  if (actual.length !== expected.length) return false;
  return timingSafeEqual(expected, actual);
}
