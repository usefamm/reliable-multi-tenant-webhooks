import type { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { AuthService } from '../../src/modules/auth/auth.service';
import { Database } from '../../src/db/pool';
import { createTestApp } from '../helpers/app';
import { testDb } from '../helpers/db';
import { SEED, TEST_TOKENS } from '../helpers/test-env';

describe('M3 auth + tenant isolation foundations', () => {
  let app: INestApplication;
  let auth: AuthService;

  beforeAll(async () => {
    app = await createTestApp();
    auth = new AuthService(testDb);
  });

  afterAll(async () => {
    await app.close();
  });

  describe('token resolution (server-side identity)', () => {
    it('resolves tenant A token to tenant A principal', async () => {
      const principal = await auth.resolve(TEST_TOKENS.tenantA);
      expect(principal).toEqual({ kind: 'tenant', tenantId: SEED.tenantAId, label: 'tenant-a' });
    });

    it('resolves tenant B token to tenant B principal', async () => {
      const principal = await auth.resolve(TEST_TOKENS.tenantB);
      expect(principal).toEqual({ kind: 'tenant', tenantId: SEED.tenantBId, label: 'tenant-b' });
    });

    it('resolves operator token to operator principal with no tenantId', async () => {
      const principal = await auth.resolve(TEST_TOKENS.operator);
      expect(principal).toEqual({ kind: 'operator', label: 'operator' });
    });

    it('returns undefined for unknown token', async () => {
      expect(await auth.resolve('not-a-real-token')).toBeUndefined();
      expect(await auth.resolve(undefined)).toBeUndefined();
    });

    it('never stores the raw token (only sha256 hash)', async () => {
      const { rows } = await testDb.query(
        'SELECT token_hash FROM auth_tokens WHERE label = $1',
        ['tenant-a'],
      );
      expect(rows[0].token_hash).not.toContain(TEST_TOKENS.tenantA);
      expect(rows[0].token_hash).toMatch(/^[0-9a-f]{64}$/);
    });
  });

  describe('bearer header parsing', () => {
    it('parses a well-formed Bearer header', () => {
      expect(auth.extractBearer('Bearer abc123')).toBe('abc123');
      expect(auth.extractBearer('bearer   abc123 ')).toBe('abc123');
    });
    it('rejects malformed auth headers', () => {
      expect(auth.extractBearer('abc123')).toBeUndefined();
      expect(auth.extractBearer('Basic abc123')).toBeUndefined();
      expect(auth.extractBearer(undefined)).toBeUndefined();
    });
  });

  describe('HTTP guard behaviour', () => {
    it('GET /health is public, checks the database and sets a request id header', async () => {
      const res = await request(app.getHttpServer()).get('/health').expect(200);
      expect(res.body).toMatchObject({ status: 'ok', database: 'ok' });
      expect(res.headers['x-request-id']).toBeTruthy();
    });

    it('echoes a caller-provided request id', async () => {
      const res = await request(app.getHttpServer())
        .get('/health')
        .set('x-request-id', 'req-fixed-123')
        .expect(200);
      expect(res.headers['x-request-id']).toBe('req-fixed-123');
    });

    it('reports 503 when the database behind the readiness probe is unreachable', async () => {
      // Port 1 is not listening: the connection fails for real rather than for
      // the test being mocked.
      const unreachable = new Database('postgres://probe-user:probe-pass@127.0.0.1:1/probe-db');
      const probeApp = await createTestApp({ database: unreachable });
      try {
        const res = await request(probeApp.getHttpServer()).get('/health');
        expect(res.status).toBe(503);
        expect(res.body).toMatchObject({ code: 'service_unavailable', message: 'Service is not ready' });
        expect(res.body.requestId).toBeTruthy();
        // Driver errors name host, port and credentials; none of that is returned.
        expect(JSON.stringify(res.body)).not.toMatch(/127\.0\.0\.1|probe-user|probe-pass|probe-db/);
      } finally {
        // app.close() ends the pool that was injected into this app.
        await probeApp.close();
      }
    });
  });
});
