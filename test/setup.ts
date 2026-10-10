import { TEST_DATABASE_URL, TEST_TOKENS } from './helpers/test-env';
import { resetConfigCache } from '../src/config/env';

/**
 * Per-test-file setup (runs in each jest worker before the test file's imports of
 * config take effect). Points the app config at the test database and the seeded
 * dev tokens, then clears any cached config so loadConfig() re-reads these values.
 */
process.env.DATABASE_URL = TEST_DATABASE_URL;
process.env.TENANT_A_TOKEN = TEST_TOKENS.tenantA;
process.env.TENANT_B_TOKEN = TEST_TOKENS.tenantB;
process.env.OPERATOR_TOKEN = TEST_TOKENS.operator;
process.env.NODE_ENV = 'test';
process.env.LOG_LEVEL = process.env.LOG_LEVEL ?? 'silent';

resetConfigCache();

process.on('unhandledRejection', (reason) => {
  // eslint-disable-next-line no-console
  console.error('Unhandled rejection during tests:', reason);
});

export {};
