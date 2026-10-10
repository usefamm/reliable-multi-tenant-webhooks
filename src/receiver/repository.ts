import type { Database } from '../db/pool';

/** Test-only failure modes the mock receiver can exhibit (PDF section 22). */
export const ReceiverMode = {
  SUCCESS: 'success',
  TEMP_FAILURE: 'temp_failure', // 503 for a bounded number of calls, then success
  PERM_FAILURE: 'perm_failure', // 500 forever
  RATE_LIMITED: 'rate_limited', // 429 with Retry-After
  LOST_RESPONSE: 'lost_response', // commit the effect, then destroy the connection
  SLOW: 'slow', // respond after delay_ms (longer than the sender timeout => timeout)
  REJECT_400: 'reject_400', // non-retryable rejection
  REDIRECT: 'redirect', // 3xx, which the sender must never follow
} as const;
export type ReceiverMode = (typeof ReceiverMode)[keyof typeof ReceiverMode];

export const RECEIVER_MODES: readonly ReceiverMode[] = Object.values(ReceiverMode);

export interface ModeRow {
  endpoint_id: string;
  event_id: string;
  mode: ReceiverMode;
  remaining: number | null;
  retry_after: number | null;
  delay_ms: number | null;
}

/** Endpoint-wide modes use the '' event_id sentinel (see migration 002). */
export const ENDPOINT_WIDE = '';

export interface EffectResultRow {
  endpoint_id: string;
  event_id: string;
  content_hash: string;
}

export interface RequestRow {
  endpoint_id: string;
  event_id: string | null;
  attempt_id: string | null;
  signature_ok: boolean;
  mode: string;
  received_at: Date;
}

/**
 * Durable state of the mock receiver.
 *
 * Everything the receiver "is" lives in PostgreSQL, so a receiver restart keeps
 * both the applied business effects and the deduplication records (PDF section
 * 15: "Deduplication must survive receiver restart").
 */
export class ReceiverRepository {
  constructor(private readonly db: Database) {}

  /**
   * In this exercise the receiver and the delivery service share one database,
   * so the receiver reads the endpoint's signing secret from the endpoints row.
   * A real receiver would hold its own copy of the shared secret.
   */
  async endpointSecret(endpointId: string): Promise<string | null> {
    const { rows } = await this.db.query<{ secret: string }>(
      'SELECT secret FROM endpoints WHERE id = $1',
      [endpointId],
    );
    return rows[0]?.secret ?? null;
  }

  /**
   * Resolve the mode in effect: an event-specific mode wins over an
   * endpoint-wide one. Counted modes decrement `remaining` atomically; a row
   * whose remaining has reached zero is no longer selected, so behaviour
   * automatically returns to success once the counted failures are consumed.
   */
  async takeMode(endpointId: string, eventId: string): Promise<ModeRow | null> {
    const { rows } = await this.db.query<ModeRow>(
      `SELECT endpoint_id, event_id, mode, remaining, retry_after, delay_ms
         FROM receiver_modes
        WHERE endpoint_id = $1 AND event_id = ANY($2::text[])
          AND (remaining IS NULL OR remaining > 0)
        ORDER BY (event_id = $3) DESC
        LIMIT 1`,
      [endpointId, [eventId, ENDPOINT_WIDE], eventId],
    );
    const row = rows[0];
    if (!row) return null;

    if (row.remaining !== null && row.remaining > 0) {
      await this.db.query(
        `UPDATE receiver_modes
            SET remaining = remaining - 1
          WHERE endpoint_id = $1 AND event_id = $2 AND remaining > 0`,
        [row.endpoint_id, row.event_id],
      );
    }
    return row;
  }

  setMode(mode: {
    endpointId: string;
    eventId?: string;
    mode: ReceiverMode;
    remaining?: number | null;
    retryAfter?: number | null;
    delayMs?: number | null;
  }): Promise<void> {
    return this.db
      .query(
        `INSERT INTO receiver_modes (endpoint_id, event_id, mode, remaining, retry_after, delay_ms)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (endpoint_id, event_id) DO UPDATE
            SET mode = EXCLUDED.mode,
                remaining = EXCLUDED.remaining,
                retry_after = EXCLUDED.retry_after,
                delay_ms = EXCLUDED.delay_ms,
                updated_at = now()`,
        [mode.endpointId, mode.eventId ?? ENDPOINT_WIDE, mode.mode, mode.remaining ?? null, mode.retryAfter ?? null, mode.delayMs ?? null],
      )
      .then(() => undefined);
  }

  clearModes(): Promise<void> {
    return this.db.query('DELETE FROM receiver_modes').then(() => undefined);
  }

  /**
   * Apply the business effect exactly once, atomically with the dedup record.
   *
   * The UNIQUE(endpoint_id, event_id) constraint IS the deduplication: the
   * insert and the effect are the same statement, so there is no window where a
   * duplicate could slip past a check-then-write.
   */
  async applyEffect(
    endpointId: string,
    eventId: string,
    contentHash: string,
  ): Promise<'applied' | 'duplicate' | 'conflict'> {
    const inserted = await this.db.query(
      `INSERT INTO receiver_effects (id, endpoint_id, event_id, content_hash)
       VALUES (gen_random_uuid()::text, $1, $2, $3)
       ON CONFLICT (endpoint_id, event_id) DO NOTHING
       RETURNING id`,
      [endpointId, eventId, contentHash],
    );
    if ((inserted.rowCount ?? 0) > 0) return 'applied';

    const existing = await this.db.query<{ content_hash: string }>(
      'SELECT content_hash FROM receiver_effects WHERE endpoint_id = $1 AND event_id = $2',
      [endpointId, eventId],
    );
    if (!existing.rows[0]) {
      // Vanished between the two statements (manual intervention): treat as
      // uncertain rather than claiming an effect we did not write.
      return 'conflict';
    }
    return existing.rows[0].content_hash === contentHash ? 'duplicate' : 'conflict';
  }

  recordRequest(entry: {
    endpointId: string;
    eventId: string | null;
    deliveryId: string | null;
    attemptId: string | null;
    signatureOk: boolean;
    mode: string;
  }): Promise<void> {
    return this.db
      .query(
        `INSERT INTO receiver_requests (id, endpoint_id, event_id, delivery_id, attempt_id, signature_ok, mode)
         VALUES (gen_random_uuid()::text, $1,$2,$3,$4,$5,$6)`,
        [
          entry.endpointId,
          entry.eventId,
          entry.deliveryId,
          entry.attemptId,
          entry.signatureOk,
          entry.mode,
        ],
      )
      .then(() => undefined);
  }

  listRequests(endpointId?: string): Promise<RequestRow[]> {
    const sql = endpointId
      ? 'SELECT endpoint_id, event_id, attempt_id, signature_ok, mode, received_at FROM receiver_requests WHERE endpoint_id = $1 ORDER BY received_at, id'
      : 'SELECT endpoint_id, event_id, attempt_id, signature_ok, mode, received_at FROM receiver_requests ORDER BY received_at, id';
    return this.db
      .query<RequestRow>(sql, endpointId ? [endpointId] : undefined)
      .then((r) => r.rows);
  }

  listEffects(endpointId?: string): Promise<EffectResultRow[]> {
    const sql = endpointId
      ? 'SELECT endpoint_id, event_id, content_hash FROM receiver_effects WHERE endpoint_id = $1 ORDER BY applied_at'
      : 'SELECT endpoint_id, event_id, content_hash FROM receiver_effects ORDER BY applied_at';
    return this.db
      .query<EffectResultRow>(sql, endpointId ? [endpointId] : undefined)
      .then((r) => r.rows);
  }

  /** Test helper: wipe observed state. Effects are durable business state, so
   *  this exists ONLY behind the test-controls switch. */
  resetAll(): Promise<void> {
    return this.db.query('TRUNCATE receiver_effects, receiver_requests, receiver_modes').then(
      () => undefined,
    );
  }
}
