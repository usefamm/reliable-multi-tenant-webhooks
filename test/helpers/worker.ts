import { Database } from '../../src/db/pool';
import { FakeClock } from '../../src/common/clock';
import { FakeRandom } from '../../src/common/random';
import { newUuid } from '../../src/common/ids';
import { buildEnvelope } from '../../src/modules/webhooks/envelope';
import { DeliveryQueue } from '../../src/worker/delivery-queue';
import { RetryPolicy } from '../../src/worker/retry-policy';
import { DeliveryWorker } from '../../src/worker/delivery-worker';
import type { DeliveryProcessor } from '../../src/worker/types';
import type { Logger } from '../../src/common/logger';
import { SEED, TEST_DATABASE_URL } from './test-env';

/** Fixed base time for the fake clock used across worker tests. */
export const BASE_MS = Date.UTC(2026, 0, 1, 0, 0, 0);

/** Silent logger for tests (LOG_LEVEL is 'silent', but avoid pino entirely). */
export const silentLogger = {
  info: () => {},
  warn: () => {},
  error: () => {},
  debug: () => {},
  fatal: () => {},
  trace: () => {},
} as unknown as Logger;

const RETRY_CONFIG = {
  RETRY_MAX_ATTEMPTS_PER_CYCLE: 5,
  RETRY_BACKOFF_BASE_MS: 1000,
  RETRY_JITTER_MAX_MS: 250,
  RETRY_AFTER_CAP_MS: 60000,
} as const;

export interface InsertDeliveryOptions {
  tenantId?: string;
  endpointId?: string;
  state?: 'READY' | 'IN_FLIGHT' | 'RETRY_WAIT' | 'DELIVERED' | 'DEAD';
  /** Due time; defaults to BASE_MS (immediately due under the fake clock). */
  nextAttemptAt?: Date | null;
  leaseOwner?: string | null;
  leaseGeneration?: number;
  leaseExpiresAt?: Date | null;
  attemptCount?: number;
  cycle?: number;
  attemptsInCycle?: number;
  payload?: Record<string, unknown>;
  eventType?: string;
}

/**
 * Insert an event + delivery row directly, bypassing the API, so worker tests can
 * set up precise queue states (in-flight with an expired lease, retry-wait with a
 * future due time, exhausted budgets, etc.).
 */
export async function insertDelivery(
  db: Database,
  opts: InsertDeliveryOptions = {},
): Promise<{ eventId: string; deliveryId: string }> {
  const eventId = newUuid();
  const deliveryId = newUuid();
  const tenantId = opts.tenantId ?? SEED.tenantAId;
  const endpointId = opts.endpointId ?? SEED.endpointA1;
  const occurredAt = new Date(BASE_MS);
  const { bytes, hash } = buildEnvelope({
    eventId,
    deliveryId,
    eventType: opts.eventType ?? 'order.created',
    occurredAt,
    payload: opts.payload ?? { orderId: eventId },
  });

  await db.query(
    `INSERT INTO events (id, tenant_id, endpoint_id, event_type, payload, occurred_at)
     VALUES ($1,$2,$3,$4,$5,$6)`,
    [eventId, tenantId, endpointId, opts.eventType ?? 'order.created', JSON.stringify(opts.payload ?? { orderId: eventId }), occurredAt],
  );

  await db.query(
    `INSERT INTO deliveries
       (id, event_id, tenant_id, endpoint_id, state, envelope_bytes, envelope_hash,
        attempt_count, cycle, attempts_in_cycle, next_attempt_at,
        lease_owner, lease_generation, lease_expires_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
    [
      deliveryId,
      eventId,
      tenantId,
      endpointId,
      opts.state ?? 'READY',
      bytes,
      hash,
      opts.attemptCount ?? 0,
      opts.cycle ?? 1,
      opts.attemptsInCycle ?? 0,
      opts.nextAttemptAt === undefined ? occurredAt : opts.nextAttemptAt,
      opts.leaseOwner ?? null,
      opts.leaseGeneration ?? 0,
      opts.leaseExpiresAt ?? null,
    ],
  );

  return { eventId, deliveryId };
}

export interface WorkerStack {
  db: Database;
  clock: FakeClock;
  queue: DeliveryQueue;
  policy: RetryPolicy;
  worker: DeliveryWorker;
}

/**
 * Build a worker stack backed by the real test database and a fake clock/random
 * so lease timing and jitter are deterministic. The caller supplies the processor.
 */
export function createWorkerStack(
  processor: DeliveryProcessor,
  overrides: Partial<{
    owner: string;
    concurrency: number;
    leaseTtlMs: number;
    pollIntervalMs: number;
    claimBatchSize: number;
    shutdownGraceMs: number;
  }> = {},
): WorkerStack {
  const db = new Database(TEST_DATABASE_URL);
  const clock = new FakeClock(BASE_MS);
  const random = new FakeRandom([0]); // zero jitter by default for determinism
  const queue = new DeliveryQueue(db, clock);
  const policy = new RetryPolicy(clock, random, RETRY_CONFIG);
  const worker = new DeliveryWorker({
    queue,
    policy,
    processor,
    logger: silentLogger,
    owner: overrides.owner ?? 'worker-test',
    concurrency: overrides.concurrency ?? 4,
    leaseTtlMs: overrides.leaseTtlMs ?? 30_000,
    pollIntervalMs: overrides.pollIntervalMs ?? 10,
    claimBatchSize: overrides.claimBatchSize ?? 4,
    shutdownGraceMs: overrides.shutdownGraceMs ?? 1_000,
  });
  return { db, clock, queue, policy, worker };
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
