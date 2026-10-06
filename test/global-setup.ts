/**
 * Jest global setup: runs once before all test files.
 * Ensures a dedicated test database exists and is migrated from empty.
 * Full implementation lands in M2/M15; this stub keeps the harness wiring valid.
 */
export default async function globalSetup(): Promise<void> {
  // Intentionally minimal in M1. M2 adds: create test DB, run migrations, seed.
}
