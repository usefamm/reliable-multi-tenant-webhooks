import { Pool } from 'pg';
import { TEST_ADMIN_URL, TEST_DATABASE_URL, TEST_DB_NAME } from './helpers/test-env';
import { migrateUp } from '../src/db/migrate';
import { seed } from '../src/db/seed';

/**
 * Runs once before all test files (in the jest parent process).
 * Drops and recreates the test database, then migrates from empty and seeds it.
 * This proves the schema is reproducible from scratch on every run.
 */
export default async function globalSetup(): Promise<void> {
  const admin = new Pool({ connectionString: TEST_ADMIN_URL });
  try {
    // Terminate lingering connections then drop/create for a clean slate.
    await admin.query(
      `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
       WHERE datname = $1 AND pid <> pg_backend_pid()`,
      [TEST_DB_NAME],
    );
    await admin.query(`DROP DATABASE IF EXISTS "${TEST_DB_NAME}" WITH (FORCE)`);
    await admin.query(`CREATE DATABASE "${TEST_DB_NAME}"`);
  } finally {
    await admin.end();
  }

  await migrateUp(TEST_DATABASE_URL);
  await seed(TEST_DATABASE_URL);
}
