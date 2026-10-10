import { Pool, type PoolClient, type QueryResult, type QueryResultRow } from 'pg';
import type { AppConfig } from '../config/env';

export interface TransactionOptions {
  /** Defaults to the server default (READ COMMITTED). */
  isolation?: 'REPEATABLE READ' | 'SERIALIZABLE';
}

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

    // An idle client can error asynchronously (e.g. the server terminates the
    // connection, or a network blip). Without a listener pg re-emits this as an
    // unhandled 'error' and crashes the process. The pool discards the bad client
    // and opens a fresh one on the next query, so swallowing here is safe.
    this.pool.on('error', (err) => {
      if (process.env.NODE_ENV !== 'test') {
        // eslint-disable-next-line no-console
        console.error('pg pool idle client error:', err.message);
      }
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
  async withTransaction<T>(
    fn: (client: PoolClient) => Promise<T>,
    options: TransactionOptions = {},
  ): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query(
        options.isolation ? `BEGIN ISOLATION LEVEL ${options.isolation}` : 'BEGIN',
      );
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

  /** Cheap liveness probe: resolves if the database answers, rejects otherwise. */
  async ping(): Promise<void> {
    await this.pool.query('SELECT 1');
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}

/**
 * Anything repositories can run SQL against: the `Database` (autocommit, one
 * statement per call) or the transaction client handed out by
 * `Database.withTransaction`. Repositories take one of these as their first
 * argument, so the CALLER decides the transaction boundary.
 */
export interface Queryable {
  query<T extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[],
  ): Promise<QueryResult<T>>;
}
