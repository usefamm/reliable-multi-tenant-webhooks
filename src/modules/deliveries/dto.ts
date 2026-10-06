import { DeliveryState } from '../../domain/types';
import { badRequest } from '../../common/errors';

/** Documented maximum page size for GET /deliveries. */
export const MAX_PAGE_SIZE = 100;
export const DEFAULT_PAGE_SIZE = 50;

const VALID_STATES = new Set<string>(Object.values(DeliveryState));

export interface DeliveryQuery {
  limit: number;
  state?: DeliveryState;
  cursor?: { createdAt: string; id: string };
}

/**
 * Parse and validate the GET /deliveries query string.
 *
 * - limit: 1..MAX_PAGE_SIZE (default DEFAULT_PAGE_SIZE); out-of-range -> 400.
 * - state: must be one of the delivery states when present; else 400.
 * - cursor: opaque keyset token (see encodeCursor); malformed -> 400.
 */
export function parseDeliveryQuery(query: Record<string, unknown>): DeliveryQuery {
  const result: DeliveryQuery = { limit: DEFAULT_PAGE_SIZE };

  const rawLimit = query.limit;
  if (rawLimit !== undefined && rawLimit !== '') {
    const limit = Number(rawLimit);
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_PAGE_SIZE) {
      throw badRequest(`limit must be an integer between 1 and ${MAX_PAGE_SIZE}`);
    }
    result.limit = limit;
  }

  const rawState = query.state;
  if (rawState !== undefined && rawState !== '') {
    if (typeof rawState !== 'string' || !VALID_STATES.has(rawState)) {
      throw badRequest('state must be one of READY, IN_FLIGHT, RETRY_WAIT, DELIVERED, DEAD');
    }
    result.state = rawState as DeliveryState;
  }

  const rawCursor = query.cursor;
  if (rawCursor !== undefined && rawCursor !== '') {
    if (typeof rawCursor !== 'string') {
      throw badRequest('cursor must be a string');
    }
    result.cursor = decodeCursor(rawCursor);
  }

  return result;
}

/**
 * Encode a keyset cursor from the last row of a page. Ordering is
 * (created_at DESC, id DESC), so the cursor carries both to break ties when many
 * rows share a timestamp. Opaque base64url JSON; not a security boundary.
 */
export function encodeCursor(createdAt: Date, id: string): string {
  const json = JSON.stringify({ c: createdAt.toISOString(), i: id });
  return Buffer.from(json, 'utf8').toString('base64url');
}

export function decodeCursor(cursor: string): { createdAt: string; id: string } {
  try {
    const json = Buffer.from(cursor, 'base64url').toString('utf8');
    const parsed = JSON.parse(json) as { c?: unknown; i?: unknown };
    if (typeof parsed.c !== 'string' || typeof parsed.i !== 'string') {
      throw new Error('bad cursor shape');
    }
    // Validate the timestamp parses; reject NaN dates.
    if (Number.isNaN(Date.parse(parsed.c))) {
      throw new Error('bad cursor timestamp');
    }
    return { createdAt: parsed.c, id: parsed.i };
  } catch {
    throw badRequest('cursor is malformed');
  }
}
