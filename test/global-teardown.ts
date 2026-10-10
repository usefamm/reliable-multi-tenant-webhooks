import { Pool } from 'pg';
import { TEST_ADMIN_URL, TEST_DB_NAME } from './helpers/test-env';

/**
 * Runs once after all test files. Drops the test database so repeated runs start
 * clean. Failures here are non-fatal (the next run's global-setup drops anyway).
 */
export default async function globalTeardown(): Promise<void> {
  const admin = new Pool({ connectionString: TEST_ADMIN_URL });
  try {
    await admin.query(
      `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
       WHERE datname = $1 AND pid <> pg_backend_pid()`,
      [TEST_DB_NAME],
    );
    await admin.query(`DROP DATABASE IF EXISTS "${TEST_DB_NAME}" WITH (FORCE)`);
  } catch {
    // best-effort cleanup
  } finally {
    await admin.end();
  }
}
