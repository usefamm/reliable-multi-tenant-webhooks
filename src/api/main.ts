import 'reflect-metadata';
import { loadEnvFile } from '../config/load-env';

loadEnvFile();

import { NestFactory } from '@nestjs/core';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { AppModule } from './app.module';
import { loadConfig } from '../config/env';
import { createLogger } from '../common/logger';
import { configureHttp } from './http-setup';

/**
 * API server bootstrap. Config is validated first so the server never boots with
 * an invalid environment. HTTP middleware ordering matters:
 *   request-id -> body parser (64 KiB cap) -> Nest routes
 * Body-parser errors (413/400) propagate into Nest's exception zone and are
 * mapped to the standard error envelope by AllExceptionsFilter.
 */
async function bootstrap(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config, 'api');

  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    logger: ['error', 'warn', 'log'],
    bodyParser: false,
  });

  configureHttp(app);
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

export { bootstrap };
