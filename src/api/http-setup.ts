import type { NestExpressApplication } from '@nestjs/platform-express';
import { json } from 'express';
import { requestIdHandler } from './request-id.middleware';

/** 64 KiB raw request body limit (PDF). Exceeding it yields 413. */
export const MAX_BODY_BYTES = 64 * 1024;

/**
 * Express middleware registered BEFORE Nest routes:
 *  - request-id correlation (so even a 413 carries an id)
 *  - JSON body parsing capped at 64 KiB
 *  - disable x-powered-by
 *
 * Errors raised here (e.g. the body parser's 413 entity.too.large) propagate into
 * Nest's exception zone and are mapped to the { code, message, requestId }
 * envelope by AllExceptionsFilter, so no separate express error handler is needed.
 */
export function configureHttp(app: NestExpressApplication): void {
  app.use(requestIdHandler);
  app.use(json({ limit: MAX_BODY_BYTES }));
  app.disable('x-powered-by');
}
