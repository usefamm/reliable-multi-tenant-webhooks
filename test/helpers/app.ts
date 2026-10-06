import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { json } from 'express';
import { AppModule } from '../../src/api/app.module';
import { MAX_BODY_BYTES } from '../../src/api/main';

/**
 * Build the API app for supertest, mirroring production bootstrap:
 * body parsing limited to 64 KiB (so oversized bodies yield 413) and x-powered-by
 * disabled. Returns an initialized app the caller must close.
 */
export async function createTestApp(): Promise<INestApplication> {
  const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
  const app = moduleRef.createNestApplication<NestExpressApplication>({ bodyParser: false });
  app.use(json({ limit: MAX_BODY_BYTES }));
  app.disable('x-powered-by');
  await app.init();
  return app;
}
