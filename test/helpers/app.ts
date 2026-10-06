import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { AppModule } from '../../src/api/app.module';
import { configureHttp } from '../../src/api/http-setup';

/**
 * Build the API app for supertest, mirroring production bootstrap exactly:
 * request-id -> 64 KiB body parser -> routes. Returns an initialized app the
 * caller must close.
 */
export async function createTestApp(): Promise<INestApplication> {
  const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
  const app = moduleRef.createNestApplication<NestExpressApplication>({ bodyParser: false });
  configureHttp(app);
  await app.init();
  // supertest attaches listeners per request; many requests against one server
  // trip Node's default MaxListeners warning. Disable the cap for test servers.
  app.getHttpServer().setMaxListeners(0);
  return app;
}
