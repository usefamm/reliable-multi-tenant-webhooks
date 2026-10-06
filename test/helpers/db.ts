import { Database } from '../../src/db/pool';
import { TEST_DATABASE_URL } from './test-env';
import { seed } from '../../src/db/seed';

/** Shared Database handle for tests (one pool per jest worker). */
export const testDb = new Database(TEST_DATABASE_URL);

/** Transactional tables cleared between tests; seed tables (tenants/endpoints/tokens) kept. */
const TRANSACTIONAL_TABLES = [
  'receiver_requests',
  'receiver_effects',
  'receiver_modes',
  'redrive_audit',
  'idempotency_records',
  'delivery_attempts',
  'deliveries',
  'events',
];

/**
 * Reset per-test state: truncate transactional data and re-assert the seed.
 * Keeps tests independent without recreating the whole database each time.
 */
export async function resetDatabase(): Promise<void> {
  await testDb.query(`TRUNCATE TABLE ${TRANSACTIONAL_TABLES.join(', ')} RESTART IDENTITY CASCADE`);
  await seed(TEST_DATABASE_URL);
}

/** Raw query helper for assertions. */
export async function q<T extends { [k: string]: unknown } = { [k: string]: unknown }>(
  text: string,
  params?: unknown[],
): Promise<T[]> {
  const res = await testDb.query<T>(text, params);
  return res.rows;
}
