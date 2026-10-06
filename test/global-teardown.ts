/**
 * Jest global teardown: runs once after all test files.
 * M2/M15 add: drop/clean the test database.
 */
export default async function globalTeardown(): Promise<void> {
  // Intentionally minimal in M1.
}
