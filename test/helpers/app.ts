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
  return app;
}
