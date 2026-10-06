import 'reflect-metadata';
import { loadEnvFile } from '../config/load-env';

loadEnvFile();

import { NestFactory } from '@nestjs/core';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { json } from 'express';
import { AppModule } from './app.module';
import { loadConfig } from '../config/env';
import { createLogger } from '../common/logger';

/** 64 KiB raw request body limit (PDF). Exceeding it yields 413 via express. */
const MAX_BODY_BYTES = 64 * 1024;

/**
 * API server bootstrap. Config is validated first so the server never boots with
 * an invalid environment. Body parsing is limited to 64 KiB and the JSON parser
 * is applied by hand so the limit is explicit and enforced before any handler.
 */
async function bootstrap(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config, 'api');

  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    logger: ['error', 'warn', 'log'],
    bodyParser: false,
  });

  app.use(json({ limit: MAX_BODY_BYTES }));
  app.disable('x-powered-by');

  await app.listen(config.API_PORT);
  logger.info({ port: config.API_PORT }, 'api listening');
}

if (require.main === module) {
  bootstrap().catch((err) => {
    // eslint-disable-next-line no-console
    console.error('failed to start api', err);
    process.exit(1);
  });
}

export { bootstrap, MAX_BODY_BYTES };
