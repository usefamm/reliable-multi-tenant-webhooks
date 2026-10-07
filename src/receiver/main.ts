import 'reflect-metadata';
import { loadEnvFile } from '../config/load-env';

loadEnvFile();

import { createServer } from 'node:http';
import { Database } from '../db/pool';
import { SystemClock } from '../common/clock';
import { createLogger } from '../common/logger';
import { loadConfig } from '../config/env';
import { ReceiverRepository } from './repository';
import { createReceiverHandler } from './handler';

/**
 * Mock receiver bootstrap. It is deliberately a plain node:http server rather
 * than a Nest app: it is a stand-in for a third-party customer endpoint, so it
 * must be independent of the delivery service's framework (and cheap to run).
 *
 * Its only durable state is PostgreSQL, which is what makes the deduplication
 * survive a restart. Test-only failure modes are served from the same process
 * but gated behind RECEIVER_TEST_CONTROLS; in a real deployment that switch is
 * off, so a caller could never steer the receiver's behaviour.
 */
async function bootstrap(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config, 'receiver');
  const db = Database.fromConfig(config);
  const repo = new ReceiverRepository(db);
  const handler = createReceiverHandler(repo, new SystemClock(), {
    timestampToleranceSec: config.RECEIVER_TIMESTAMP_TOLERANCE_SEC,
    maxBodyBytes: 64 * 1024,
    testControls: config.RECEIVER_TEST_CONTROLS,
  });

  const server = createServer((req, res) => {
    void handler(req, res);
  });
  server.setMaxListeners(0);

  server.listen(config.RECEIVER_PORT, () => {
    logger.info({ port: config.RECEIVER_PORT, testControls: config.RECEIVER_TEST_CONTROLS }, 'receiver listening');
  });

  const shutdown = (): void => {
    server.close(() => {
      void db.pool.end().then(() => process.exit(0));
    });
    // Anything still in flight is answered from durable state, so a hard stop
    // after the grace period cannot duplicate or lose an effect.
    setTimeout(() => process.exit(0), config.WORKER_SHUTDOWN_GRACE_MS).unref();
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

if (require.main === module) {
  bootstrap().catch((err) => {
    // eslint-disable-next-line no-console
    console.error('failed to start receiver', err);
    process.exit(1);
  });
}
