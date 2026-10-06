import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { loadConfig } from '../config/env';
import { createLogger } from '../common/logger';

/**
 * API server bootstrap. Keeps the process lean: config is validated first so the
 * server never boots with invalid environment.
 */
async function bootstrap(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config, 'api');

  const app = await NestFactory.create(AppModule, {
    logger: ['error', 'warn', 'log'],
    bodyParser: true,
  });

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
