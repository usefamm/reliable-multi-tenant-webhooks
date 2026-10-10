import type { PoolClient } from 'pg';
import type { Database } from '../../db/pool';
import { newUuid } from '../../common/ids';
import { conflict } from '../../common/errors';
import { canonicalJson } from '../../common/canonical-json';
import { sha256Hex } from '../../common/hash';
import {
  IdempotencyRepository,
  isIdempotencyKeyConflict,
  type IdempotencyRecord,
  type IdempotencyScope,
} from '../../db/repositories/idempotency.repository';

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
  constructor(
    private readonly db: Database,
    private readonly records: IdempotencyRepository = new IdempotencyRepository(),
  ) {}

  async execute<T>(
    params: {
      tenantId: string;
      operation: IdempotencyOperation;
      key: string;
      requestHash: string;
    },
    work: (client: PoolClient) => Promise<IdempotentWorkResult<T>>,
  ): Promise<IdempotentOutcome<T>> {
    const scope: IdempotencyScope = {
      tenantId: params.tenantId,
      operation: params.operation,
      key: params.key,
    };

    try {
      return await this.db.withTransaction(async (client) => {
        const existing = await this.records.find(client, scope);
        if (existing) {
          return this.replayOrConflict<T>(existing, params.requestHash);
        }

        // Claim the key BEFORE doing the work. A concurrent request with the
        // same key blocks on the unique index here and, once the winner commits,
        // takes the unique-violation path below and replays - instead of racing
        // to redo the work and then failing a state precondition that the winner
        // already consumed (which would surface as a spurious 409).
        const recordId = newUuid();
        await this.records.claim(client, recordId, scope, params.requestHash);

        const result = await work(client);

        await this.records.finalize(client, recordId, {
          status: result.status,
          body: result.body,
          resourceId: result.resourceId ?? null,
        });
        return { status: result.status, body: result.body, replayed: false };
      });
    } catch (err) {
      if (isIdempotencyKeyConflict(err)) {
        // A concurrent request committed the record first. Re-read and replay.
        return this.replayAfterConflict<T>(scope, params.requestHash);
      }
      throw err;
    }
  }

  private replayOrConflict<T>(
    existing: IdempotencyRecord,
    requestHash: string,
  ): IdempotentOutcome<T> {
    if (existing.requestHash !== requestHash) {
      throw conflict(
        'Idempotency-Key was reused with different input; the original response cannot be replayed',
      );
    }
    return {
      status: existing.responseStatus,
      body: existing.responseBody as T,
      replayed: true,
    };
  }

  /**
   * Re-read the committed record after losing an insert race. The unique
   * violation is only raised once the winning transaction commits, so the record
   * is visible here; a short retry guards against rare visibility timing.
   */
  private async replayAfterConflict<T>(
    scope: IdempotencyScope,
    requestHash: string,
  ): Promise<IdempotentOutcome<T>> {
    for (let attempt = 0; attempt < 5; attempt += 1) {
      const existing = await this.records.find(this.db, scope);
      if (existing) {
        return this.replayOrConflict<T>(existing, requestHash);
      }
      await new Promise((r) => setTimeout(r, 10 * (attempt + 1)));
    }
    // The conflicting transaction must have rolled back; surface a conflict.
    throw conflict('Concurrent request with the same Idempotency-Key could not be reconciled');
  }
}
