import type { PoolClient } from 'pg';
import type { Database } from '../../db/pool';
import { newUuid } from '../../common/ids';
import { conflict } from '../../common/errors';
import { canonicalJson } from '../../common/canonical-json';
import { sha256Hex } from '../../common/hash';

/** Idempotency operations. Keys are scoped by (tenant, operation, key). */
export const IdempotencyOperation = {
  PUBLISH_EVENT: 'publish_event',
  REDRIVE: 'redrive',
} as const;
export type IdempotencyOperation =
  (typeof IdempotencyOperation)[keyof typeof IdempotencyOperation];

/** The result of the wrapped work, persisted for replay. */
export interface IdempotentWorkResult<T> {
  status: number;
  body: T;
  resourceId?: string | null;
}

export interface IdempotentOutcome<T> {
  status: number;
  body: T;
  /** True when the response was replayed from a previously committed record. */
  replayed: boolean;
}

interface IdempotencyRow {
  request_hash: string;
  response_status: number;
  response_body: unknown;
  resource_id: string | null;
}

/** Postgres unique_violation SQLSTATE. */
const UNIQUE_VIOLATION = '23505';
/** Name of the UNIQUE constraint on idempotency_records (see migration 001). */
const IDEMPOTENCY_CONSTRAINT = 'idempotency_unique';

interface PgError extends Error {
  code?: string;
  constraint?: string;
}

/**
 * Deterministic request fingerprint for idempotency comparison.
 * Canonical JSON makes object key order irrelevant while array order stays
 * significant; SHA-256 gives a fixed-size comparison token.
 */
export function requestFingerprint(input: unknown): string {
  return sha256Hex(canonicalJson(input));
}

/**
 * Publication/operation idempotency built directly on the database.
 *
 * Concurrency model: the UNIQUE(tenant_id, operation, idempotency_key) index
 * serializes concurrent requests that share a key. The first transaction to
 * commit wins; a concurrent transaction that did its own work but then hits the
 * unique violation on INSERT rolls back (discarding its duplicate event/delivery)
 * and replays the committed record. This is what makes "20 concurrent identical
 * publications produce exactly one event and one delivery" true.
 *
 * Failures BEFORE a record is written (validation, ownership, 413) never consume
 * the key: the record is only inserted in the same transaction as the work.
 */
export class IdempotencyService {
  constructor(private readonly db: Database) {}

  async execute<T>(
    params: {
      tenantId: string;
      operation: IdempotencyOperation;
      key: string;
      requestHash: string;
    },
    work: (client: PoolClient) => Promise<IdempotentWorkResult<T>>,
  ): Promise<IdempotentOutcome<T>> {
    try {
      return await this.db.withTransaction(async (client) => {
        const existing = await this.selectRecord(client, params);
        if (existing) {
          return this.replayOrConflict<T>(existing, params.requestHash);
        }
        const result = await work(client);
        await this.insertRecord(client, params, result);
        return { status: result.status, body: result.body, replayed: false };
      });
    } catch (err) {
      if (this.isIdempotencyConflict(err)) {
        // A concurrent request committed the record first. Re-read and replay.
        return this.replayAfterConflict<T>(params);
      }
      throw err;
    }
  }

  private async selectRecord(
    client: PoolClient,
    params: { tenantId: string; operation: string; key: string },
  ): Promise<IdempotencyRow | undefined> {
    const { rows } = await client.query<IdempotencyRow>(
      `SELECT request_hash, response_status, response_body, resource_id
         FROM idempotency_records
        WHERE tenant_id = $1 AND operation = $2 AND idempotency_key = $3`,
      [params.tenantId, params.operation, params.key],
    );
    return rows[0];
  }

  private async insertRecord<T>(
    client: PoolClient,
    params: { tenantId: string; operation: string; key: string; requestHash: string },
    result: IdempotentWorkResult<T>,
  ): Promise<void> {
    await client.query(
      `INSERT INTO idempotency_records
         (id, tenant_id, operation, idempotency_key, request_hash,
          response_status, response_body, resource_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [
        newUuid(),
        params.tenantId,
        params.operation,
        params.key,
        params.requestHash,
        result.status,
        JSON.stringify(result.body),
        result.resourceId ?? null,
      ],
    );
  }

  private replayOrConflict<T>(
    existing: IdempotencyRow,
    requestHash: string,
  ): IdempotentOutcome<T> {
    if (existing.request_hash !== requestHash) {
      throw conflict(
        'Idempotency-Key was reused with different input; the original response cannot be replayed',
      );
    }
    return {
      status: existing.response_status,
      body: existing.response_body as T,
      replayed: true,
    };
  }

  /**
   * Re-read the committed record after losing an insert race. The unique
   * violation is only raised once the winning transaction commits, so the record
   * is visible here; a short retry guards against rare visibility timing.
   */
  private async replayAfterConflict<T>(params: {
    tenantId: string;
    operation: string;
    key: string;
    requestHash: string;
  }): Promise<IdempotentOutcome<T>> {
    for (let attempt = 0; attempt < 5; attempt += 1) {
      const { rows } = await this.db.query<IdempotencyRow>(
        `SELECT request_hash, response_status, response_body, resource_id
           FROM idempotency_records
          WHERE tenant_id = $1 AND operation = $2 AND idempotency_key = $3`,
        [params.tenantId, params.operation, params.key],
      );
      const existing = rows[0];
      if (existing) {
        return this.replayOrConflict<T>(existing, params.requestHash);
      }
      await new Promise((r) => setTimeout(r, 10 * (attempt + 1)));
    }
    // The conflicting transaction must have rolled back; surface a conflict.
    throw conflict('Concurrent request with the same Idempotency-Key could not be reconciled');
  }

  private isIdempotencyConflict(err: unknown): boolean {
    const e = err as PgError;
    return e?.code === UNIQUE_VIOLATION && e?.constraint === IDEMPOTENCY_CONSTRAINT;
  }
}
