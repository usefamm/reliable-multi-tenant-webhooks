import { Pool, type PoolClient, type QueryResult, type QueryResultRow } from 'pg';
import type { AppConfig } from '../config/env';

/**
 * Thin wrapper around a pg Pool. We use node-postgres directly (no ORM) so we
 * keep full control over transaction boundaries, row locking
 * (FOR UPDATE SKIP LOCKED) and fencing predicates - the core of this challenge.
 */
export class Database {
  readonly pool: Pool;

  constructor(connectionString: string) {
    this.pool = new Pool({
      connectionString,
      // Bound the pool: workers hold at most WORKER_CONCURRENCY in-flight HTTP
      // calls and never keep a connection busy during HTTP (transactions are
      // short-lived), so a small pool is sufficient.
      max: 10,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
    });
  }

  static fromConfig(config: Pick<AppConfig, 'DATABASE_URL'>): Database {
    return new Database(config.DATABASE_URL);
  }

  query<T extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[],
  ): Promise<QueryResult<T>> {
    return this.pool.query<T>(text, params);
  }

  /**
   * Run `fn` inside a single transaction. Commits on success, rolls back on any
   * throw. The client is always released. Never perform HTTP inside `fn`.
   */
  async withTransaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (err) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // ROLLBACK can fail if the connection dropped; the pool discards it.
      }
      throw err;
    } finally {
      client.release();
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}

/** Convenience for running a query on either a Pool or a transaction client. */
export type QueryRunner = Pool | PoolClient;

export function runQuery<T extends QueryResultRow = QueryResultRow>(
  runner: QueryRunner,
  text: string,
  params?: unknown[],
): Promise<QueryResult<T>> {
  return runner.query<T>(text, params);
}
