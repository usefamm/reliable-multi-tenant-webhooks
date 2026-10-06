/**
 * Shared test environment. Integration tests run against a REAL PostgreSQL
 * database (PDF requirement), never a mock. The database is dropped, recreated,
 * migrated and seeded once per jest run in global-setup.
 *
 * Override TEST_DATABASE_URL to point at a different server. The default targets
 * a local PostgreSQL with trust auth (the setup used in this environment).
 */
const DEFAULT_URL = 'postgres://yousef@127.0.0.1:5432/webhook_test';

export const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL ?? DEFAULT_URL;

/** Maintenance connection (to the `postgres` db) used to create/drop the test db. */
export const TEST_ADMIN_URL = TEST_DATABASE_URL.replace(/\/[^/]+$/, '/postgres');

export const TEST_DB_NAME = TEST_DATABASE_URL.split('/').pop() as string;

/** Dev tokens matching the deterministic seed defaults. */
export const TEST_TOKENS = {
  tenantA: process.env.TENANT_A_TOKEN ?? 'dev-token-tenant-a',
  tenantB: process.env.TENANT_B_TOKEN ?? 'dev-token-tenant-b',
  operator: process.env.OPERATOR_TOKEN ?? 'dev-token-operator',
};

/** Fixed IDs from the deterministic seed. */
export const SEED = {
  tenantAId: 'aaaaaaaa-0000-4000-8000-00000000000a',
  tenantBId: 'bbbbbbbb-0000-4000-8000-00000000000b',
  endpointA1: 'eeeeeeee-0000-4000-8000-0000000000a1',
  endpointA2: 'eeeeeeee-0000-4000-8000-0000000000a2',
  endpointB1: 'eeeeeeee-0000-4000-8000-0000000000b1',
  endpointB2: 'eeeeeeee-0000-4000-8000-0000000000b2',
  endpointSecretA1: 'dev-secret-a1',
};
