import 'reflect-metadata';
import { loadEnvFile } from '../config/load-env';

loadEnvFile();

import { Database } from '../db/pool';
import { SystemClock } from '../common/clock';
import { SystemRandom } from '../common/random';
import { createLogger } from '../common/logger';
import { loadConfig } from '../config/env';
import { WebhookClient } from '../modules/webhooks/webhook.client';
import { PgDeliveryQueue } from '../db/pg-delivery-queue';
import { RetryPolicy } from '../domain/retry-policy';
import { DeliveryWorker } from './delivery-worker';
import { createWebhookProcessor } from './processor';

/**
 * Worker process bootstrap. One process = one worker identity = one bounded set
 * of concurrency permits; the compose file runs two of these (worker-a, worker-b)
 * against the same database.
 *
 * All coordination is durable (leases + fencing tokens in Postgres), so this
 * process holds no scheduling state worth protecting: it can be killed at any
 * point and another worker recovers its leases when they expire.
 */
async function bootstrap(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config, config.WORKER_NAME);
  const db = Database.fromConfig(config);

  const queue = new PgDeliveryQueue(db, new SystemClock());
  const policy = new RetryPolicy(new SystemClock(), new SystemRandom(), config);
  const processor = createWebhookProcessor({
    db,
    clock: new SystemClock(),
    client: new WebhookClient(config),
  });

  const worker = new DeliveryWorker({
    queue,
    policy,
    processor,
    logger,
    owner: config.WORKER_NAME,
    concurrency: config.WORKER_CONCURRENCY,
    leaseTtlMs: config.WORKER_LEASE_TTL_MS,
    pollIntervalMs: config.WORKER_POLL_INTERVAL_MS,
    claimBatchSize: config.WORKER_CLAIM_BATCH_SIZE,
    shutdownGraceMs: config.WORKER_SHUTDOWN_GRACE_MS,
  });
  worker.start();

  let shuttingDown = false;
  const shutdown = (signal: string): void => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'worker stopping: draining in-flight dispatches');
    const hardExit = setTimeout(() => {
      logger.error('grace period exceeded; exiting with leases to expire naturally');
      process.exit(1);
    }, config.WORKER_SHUTDOWN_GRACE_MS + 1_000);
    hardExit.unref();
    worker
      .stop()
      .then(() => db.pool.end())
      .then(() => {
        logger.info('worker stopped cleanly');
        process.exit(0);
      })
      .catch((err: unknown) => {
        logger.error({ err }, 'error during shutdown');
        process.exit(1);
      });
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

if (require.main === module) {
  bootstrap().catch((err) => {
    // eslint-disable-next-line no-console
    console.error('failed to start worker', err);
    process.exit(1);
  });
}
