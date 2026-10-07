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
 * Idempotency built directly on the database: one row per
 * (tenant, operation, key) holding the original response so a retry replays it.
 *
 * Concurrency model: the key is CLAIMED (inserted) inside the same transaction
 * as the work, before the work runs. The UNIQUE(tenant_id, operation,
 * idempotency_key) index then serializes duplicates - a concurrent request with
 * the same key blocks on the claim, loses with SQLSTATE 23505 once the winner
 * commits, and replays the winner's stored response. Because the claim and the
 * work share one transaction, a request that fails validation (or finds the
 * resource missing) rolls the claim back too: a failed call never consumes the
 * key.
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

        // Claim the key BEFORE doing the work. A concurrent request with the
        // same key blocks on the unique index here and, once the winner commits,
        // takes the 23505 path below and replays - instead of racing to redo the
        // work and then failing a state precondition that the winner already
        // consumed (which would surface as a spurious 409).
        const recordId = newUuid();
        await this.claimRecord(client, recordId, params);

        const result = await work(client);

        await this.finalizeRecord(client, recordId, result);
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

  /**
   * Write the key claim. `response_status`/`response_body` are NOT NULL, so the
   * claim carries placeholders that `finalizeRecord` overwrites in the same
   * transaction: no other session can observe them, because the row is invisible
   * until this transaction commits.
   */
  private async claimRecord(
    client: PoolClient,
    recordId: string,
    params: { tenantId: string; operation: string; key: string; requestHash: string },
  ): Promise<void> {
    await client.query(
      `INSERT INTO idempotency_records
         (id, tenant_id, operation, idempotency_key, request_hash,
          response_status, response_body, resource_id)
       VALUES ($1, $2, $3, $4, $5, 0, 'null'::jsonb, NULL)`,
      [recordId, params.tenantId, params.operation, params.key, params.requestHash],
    );
  }

  private async finalizeRecord<T>(
    client: PoolClient,
    recordId: string,
    result: IdempotentWorkResult<T>,
  ): Promise<void> {
    await client.query(
      `UPDATE idempotency_records
          SET response_status = $2, response_body = $3, resource_id = $4
        WHERE id = $1`,
      [recordId, result.status, JSON.stringify(result.body), result.resourceId ?? null],
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
