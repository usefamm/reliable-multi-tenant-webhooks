import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { AppModule } from '../../src/api/app.module';
import { CLOCK, DATABASE, LOGGER } from '../../src/common/tokens';
import { configureHttp } from '../../src/api/http-setup';
import type { Clock } from '../../src/common/clock';
import type { Logger } from '../../src/common/logger';
import type { Database } from '../../src/db/pool';

/**
 * Build the API app for supertest, mirroring production bootstrap exactly:
 * request-id -> 64 KiB body parser -> routes. Returns an initialized app the
 * caller must close.
 *
 * `clock` overrides the API's time source. Tests that combine the API with a
 * worker loop must share ONE clock: a redrive schedules `next_attempt_at` from
 * the API's clock, and the loop's worker only claims work that is due according
 * to its own. Two different clocks make the delivery permanently underived.
 *
 * `database` overrides the connection, which is how the readiness probe is
 * exercised against a database that is genuinely unreachable.
 *
 * `logger` captures what the API emits. A capture logger bypasses pino's
 * redaction on purpose: an assertion that the payload is absent then proves the
 * field was never handed to the logger, not merely censored on the way out.
 */
export async function createTestApp(
  opts: { clock?: Clock; database?: Database; logger?: Logger } = {},
): Promise<INestApplication> {
  const tester = Test.createTestingModule({ imports: [AppModule] });
  if (opts.clock) tester.overrideProvider(CLOCK).useValue(opts.clock);
  if (opts.database) tester.overrideProvider(DATABASE).useValue(opts.database);
  if (opts.logger) tester.overrideProvider(LOGGER).useValue(opts.logger);
  const moduleRef = await tester.compile();
  const app = moduleRef.createNestApplication<NestExpressApplication>({ bodyParser: false });
  configureHttp(app);
  await app.init();
  // supertest attaches listeners per request; many requests against one server
  // trip Node's default MaxListeners warning. Disable the cap for test servers.
  app.getHttpServer().setMaxListeners(0);
  return app;
}
