import type { IncomingMessage } from 'node:http';
import type { AttemptOutcomeKind } from '../../domain/attempt';

/**
 * Pure mapping from an observed HTTP response to a delivery-attempt
 * classification. No sockets, no timers: every rule of the retry contract that is
 * about a status code or a Retry-After header lives here.
 */

export function describeStatus(status: number, incomplete?: 'timeout' | 'stream_error'): string {
  if (status >= 300 && status < 400) return 'redirect'; // never followed
  return incomplete ? `${incomplete}_http_${status}` : `http_${status}`;
}

export function headerOf(res: IncomingMessage, name: string): string | null {
  const value = res.headers[name];
  return Array.isArray(value) ? (value[0] ?? null) : (value ?? null);
}

export function isTimeoutLike(err: Error): boolean {
  return (err as NodeJS.ErrnoException).code === 'ETIMEDOUT';
}

export function classifyStatus(status: number): AttemptOutcomeKind {
  if (status >= 200 && status < 300) return 'SUCCESS';
  if (status === 408 || status === 429 || status >= 500) return 'RETRYABLE';
  return 'NON_RETRYABLE'; // includes all 3xx (never followed) and other 4xx
}

/**
 * Parse Retry-After (PDF section 16): only delta-seconds is supported.
 * Invalid, negative, unparseable, or HTTP-date values return null so the retry
 * policy falls back to normal backoff.
 */
export function parseRetryAfterMs(raw: string | null): number | null {
  if (raw === null) return null;
  const trimmed = raw.trim();
  if (!/^\d+$/.test(trimmed)) return null; // not a plain delta-seconds value
  const seconds = Number(trimmed);
  if (!Number.isFinite(seconds) || seconds > 86_400) return null;
  return seconds * 1000;
}
