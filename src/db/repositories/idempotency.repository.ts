import type { Queryable } from '../pool';

export interface IdempotencyScope {
  tenantId: string;
  operation: string;
  key: string;
}

export interface IdempotencyRecord {
  requestHash: string;
  responseStatus: number;
  responseBody: unknown;
  resourceId: string | null;
}

/** Postgres unique_violation SQLSTATE. */
const UNIQUE_VIOLATION = '23505';
/** Name of the UNIQUE constraint on idempotency_records (see migration 001). */
const IDEMPOTENCY_CONSTRAINT = 'idempotency_unique';

interface PgError extends Error {
  code?: string;
  constraint?: string;
}

/** True when `err` is the unique violation raised by losing an idempotency-key race. */
export function isIdempotencyKeyConflict(err: unknown): boolean {
  const e = err as PgError;
  return e?.code === UNIQUE_VIOLATION && e?.constraint === IDEMPOTENCY_CONSTRAINT;
}

export class IdempotencyRepository {
  async find(q: Queryable, scope: IdempotencyScope): Promise<IdempotencyRecord | null> {
    const { rows } = await q.query<{
      request_hash: string;
      response_status: number;
      response_body: unknown;
      resource_id: string | null;
    }>(
      `SELECT request_hash, response_status, response_body, resource_id
         FROM idempotency_records
        WHERE tenant_id = $1 AND operation = $2 AND idempotency_key = $3`,
      [scope.tenantId, scope.operation, scope.key],
    );
    const r = rows[0];
    return r
      ? {
          requestHash: r.request_hash,
          responseStatus: r.response_status,
          responseBody: r.response_body,
          resourceId: r.resource_id,
        }
      : null;
  }

  /**
   * Write the key claim. `response_status`/`response_body` are NOT NULL, so the
   * claim carries placeholders that `finalize` overwrites in the same
   * transaction: no other session can observe them, because the row is invisible
   * until that transaction commits.
   */
  async claim(
    q: Queryable,
    recordId: string,
    scope: IdempotencyScope,
    requestHash: string,
  ): Promise<void> {
    await q.query(
      `INSERT INTO idempotency_records
         (id, tenant_id, operation, idempotency_key, request_hash,
          response_status, response_body, resource_id)
       VALUES ($1, $2, $3, $4, $5, 0, 'null'::jsonb, NULL)`,
      [recordId, scope.tenantId, scope.operation, scope.key, requestHash],
    );
  }

  async finalize(
    q: Queryable,
    recordId: string,
    response: { status: number; body: unknown; resourceId: string | null },
  ): Promise<void> {
    await q.query(
      `UPDATE idempotency_records
          SET response_status = $2, response_body = $3, resource_id = $4
        WHERE id = $1`,
      [recordId, response.status, JSON.stringify(response.body), response.resourceId],
    );
  }
}
