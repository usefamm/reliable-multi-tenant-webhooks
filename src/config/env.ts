import { z } from 'zod';

/**
 * Environment configuration, validated once at process start.
 * No magic constants live in business code - everything tunable is here.
 */
const EnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),

  DATABASE_URL: z.string().min(1),

  API_PORT: z.coerce.number().int().positive().default(3000),

  WORKER_NAME: z.string().min(1).default('worker-a'),
  WORKER_CONCURRENCY: z.coerce.number().int().positive().max(64).default(4),
  WORKER_LEASE_TTL_MS: z.coerce.number().int().positive().default(30_000),
  WORKER_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(250),
  WORKER_CLAIM_BATCH_SIZE: z.coerce.number().int().positive().default(4),
  WORKER_SHUTDOWN_GRACE_MS: z.coerce.number().int().nonnegative().default(5_000),

  WEBHOOK_TIMEOUT_MS: z.coerce.number().int().positive().default(2_000),
  WEBHOOK_MAX_RESPONSE_BYTES: z.coerce.number().int().positive().default(4_096),
  /**
   * SSRF defence (deployment-owned, never caller-owned): when non-empty the
   * worker only dispatches to endpoint URLs whose host:port appears in this
   * list. Empty means "allow the configured endpoints table only" - which is
   * already the only source of destinations; the list is the belt-and-braces
   * network boundary recommended for production.
   */
  WEBHOOK_ALLOWED_HOSTS: z.string().default(''),

  RETRY_MAX_ATTEMPTS_PER_CYCLE: z.coerce.number().int().positive().default(5),
  RETRY_BACKOFF_BASE_MS: z.coerce.number().int().positive().default(1_000),
  RETRY_JITTER_MAX_MS: z.coerce.number().int().nonnegative().default(250),
  RETRY_AFTER_CAP_MS: z.coerce.number().int().positive().default(60_000),

  RECEIVER_PORT: z.coerce.number().int().positive().default(4000),
  RECEIVER_TIMESTAMP_TOLERANCE_SEC: z.coerce.number().int().positive().default(300),
  /**
   * The receiver's failure-mode control surface is a TEST TOOL (PDF section 22):
   * modes must never be steerable through public event fields, and in a
   * production-like deployment the control endpoints should be switched off.
   */
  RECEIVER_TEST_CONTROLS: z
    .enum(['true', 'false', '1', '0'])
    .default('1')
    .transform((v) => v === '1' || v === 'true'),

  TENANT_A_TOKEN: z.string().min(1),
  TENANT_B_TOKEN: z.string().min(1),
  OPERATOR_TOKEN: z.string().min(1),
});

export type AppConfig = z.infer<typeof EnvSchema>;

/** Parse and validate the environment. Throws with an actionable message on invalid config. */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const result = EnvSchema.safeParse(env);
  if (!result.success) {
    const issues = result.error.issues
      .map((i) => `  - ${i.path.join('.') || '(root)'}: ${i.message}`)
      .join('\n');
    throw new Error(`Invalid environment configuration:\n${issues}`);
  }
  return result.data;
}

/**
 * A singleton is acceptable here because config is immutable process-wide state read
 * at startup. Tests can build isolated configs via loadConfig() with a custom env.
 */
let cached: AppConfig | undefined;
export function getConfig(): AppConfig {
  if (!cached) cached = loadConfig();
  return cached;
}

/** Reset the cached config (used by tests that inject a custom env). */
export function resetConfigCache(): void {
  cached = undefined;
}
