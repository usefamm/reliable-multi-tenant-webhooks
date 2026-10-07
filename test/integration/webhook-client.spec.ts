import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import { once } from 'node:events';
import { type AddressInfo } from 'node:net';
import { WebhookClient } from '../../src/modules/webhooks/webhook.client';
import { verifyWebhookSignature } from '../../src/modules/webhooks/signing';
import { createWebhookProcessor } from '../../src/worker/processor';
import { Database } from '../../src/db/pool';
import { FakeClock } from '../../src/common/clock';
import { FakeRandom } from '../../src/common/random';
import { newUuid } from '../../src/common/ids';
import { DeliveryQueue } from '../../src/worker/delivery-queue';
import { RetryPolicy } from '../../src/worker/retry-policy';
import { TEST_DATABASE_URL, SEED } from '../helpers/test-env';
import { insertDelivery } from '../helpers/worker';
import { q, resetDatabase } from '../helpers/db';
import type { ClaimedWork } from '../../src/worker/types';

const CLIENT_CONFIG = {
  WEBHOOK_TIMEOUT_MS: 300,
  WEBHOOK_MAX_RESPONSE_BYTES: 4096,
  WEBHOOK_ALLOWED_HOSTS: '',
};

const SECRET = 'test-secret-1';
const TS = 1_767_225_600;

interface Received {
  method: string;
  headers: Record<string, string | string[] | undefined>;
  body: Buffer;
}

/**
 * M9: outbound webhook client behaviour against a REAL local HTTP server:
 * exact bytes, five identity headers, fresh valid signature, no auth
 * forwarding, redirects never followed, a hard total timeout, bounded response
 * capture, and honest outcome classification (an uncertain outcome is recorded
 * as UNKNOWN - never a fabricated result).
 */
describe('M9 outbound webhook client', () => {
  let server: Server;
  let baseUrl: string;
  const received: Received[] = [];
  let handler: (req: IncomingMessage, res: ServerResponse) => void;

  const echoHandler = (req: IncomingMessage, res: ServerResponse) => {
    const chunks: Buffer[] = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      received.push({ method: req.method!, headers: req.headers, body: Buffer.concat(chunks) });
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{"ok":true}');
    });
  };

  beforeAll(async () => {
    handler = echoHandler;
    server = createServer((req, res) => handler(req, res));
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });

  afterAll(async () => {
    server.close();
    await once(server, 'close');
  });

  beforeEach(() => {
    handler = echoHandler;
    received.length = 0;
  });

  function dispatch(over: Partial<Parameters<WebhookClient['dispatch']>[0]> = {}) {
    const client = new WebhookClient(CLIENT_CONFIG);
    return client.dispatch({
      url: `${baseUrl}/hook/ep1`,
      secret: SECRET,
      eventId: 'evt-1',
      deliveryId: 'dlv-1',
      attemptId: 'att-1',
      body: Buffer.from(JSON.stringify({ hello: 'world' }), 'utf8'),
      timestampUnixSec: TS,
      ...over,
    });
  }

  it('delivers the exact bytes with the contract headers and a valid fresh signature', async () => {
    const body = Buffer.from('{"eventId":"evt-1","payload":{"x":1}}', 'utf8');
    const result = await dispatch({ body });

    expect(result.outcome).toBe('SUCCESS');
    expect(result.httpStatus).toBe(200);
    expect(result.errorCode).toBeNull();

    expect(received).toHaveLength(1);
    const req = received[0];
    expect(req.method).toBe('POST');
    expect(req.headers['content-type']).toBe('application/json');
    expect(req.headers['x-event-id']).toBe('evt-1');
    expect(req.headers['x-delivery-id']).toBe('dlv-1');
    expect(req.headers['x-attempt-id']).toBe('att-1');
    expect(req.headers['x-webhook-timestamp']).toBe(String(TS));
    expect(req.body.equals(body)).toBe(true); // byte-for-byte
    expect(
      verifyWebhookSignature(SECRET, TS, req.body, String(req.headers['x-webhook-signature'])),
    ).toBe(true);
  });

  it('never forwards an Authorization header and never sends the secret', async () => {
    await dispatch();
    const req = received[0];
    expect(req.headers['authorization']).toBeUndefined();
    expect(JSON.stringify(req.headers)).not.toMatch(/test-secret-1/i);
  });

  it('treats 5xx as RETRYABLE with the status recorded', async () => {
    handler = (_req, res) => {
      res.writeHead(503);
      res.end('temporarily unavailable');
    };
    const result = await dispatch();
    expect(result.outcome).toBe('RETRYABLE');
    expect(result.httpStatus).toBe(503);
    expect(result.errorCode).toBe('http_503');
    expect(result.responseSnippet).toContain('temporarily unavailable');
  });

  it('treats 408 as RETRYABLE and other 4xx as NON_RETRYABLE', async () => {
    handler = (_req, res) => {
      res.writeHead(408);
      res.end('req timeout');
    };
    expect((await dispatch()).outcome).toBe('RETRYABLE');

    handler = (_req, res) => {
      res.writeHead(400);
      res.end('bad request');
    };
    const rejected = await dispatch();
    expect(rejected.outcome).toBe('NON_RETRYABLE');
    expect(rejected.httpStatus).toBe(400);
  });

  it('parses Retry-After on 429 into delta milliseconds', async () => {
    handler = (_req, res) => {
      res.writeHead(429, { 'retry-after': '7' });
      res.end('slow down');
    };
    const result = await dispatch();
    expect(result.outcome).toBe('RETRYABLE');
    expect(result.retryAfterMs).toBe(7000);
  });

  it('does NOT follow redirects: a 3xx is NON_RETRYABLE', async () => {
    handler = (req, res) => {
      // Record the hit immediately (no body drain needed) so we can prove the
      // client never followed the redirect to a second request.
      received.push({ method: req.method!, headers: req.headers, body: Buffer.alloc(0) });
      res.writeHead(302, { location: `${baseUrl}/redirected` });
      res.end('go away');
    };
    const result = await dispatch();
    expect(result.outcome).toBe('NON_RETRYABLE');
    expect(result.errorCode).toBe('redirect');
    expect(result.httpStatus).toBe(302);
    // The client stopped at the redirect: exactly one request was made.
    expect(received).toHaveLength(1);
  });

  it('maps a stalled response (total timeout) to RETRYABLE with no fabricated status', async () => {
    handler = () => {
      /* accept the connection but never respond */
    };
    const result = await dispatch();
    expect(result.outcome).toBe('RETRYABLE');
    expect(result.httpStatus).toBeNull();
    expect(result.errorCode).toBe('timeout');
  });

  it('maps a refused connection to UNKNOWN - the outcome is genuinely uncertain', async () => {
    // Bind a server to learn a free port, close it, then call that dead port.
    const dead = createServer();
    dead.listen(0, '127.0.0.1');
    await once(dead, 'listening');
    const port = (dead.address() as AddressInfo).port;
    await new Promise<void>((r) => dead.close(() => r()));

    const result = await dispatch({ url: `http://127.0.0.1:${port}/hook` });
    expect(result.outcome).toBe('UNKNOWN');
    expect(result.httpStatus).toBeNull();
    expect(result.errorCode).toBe('transport_error');
  });

  it('captures the response bounded to the configured maximum', async () => {
    handler = (_req, res) => {
      res.writeHead(200);
      res.end('x'.repeat(100_000));
    };
    const result = await dispatch();
    expect(result.outcome).toBe('SUCCESS');
    expect(result.responseSnippet).not.toBeNull();
    expect(Buffer.byteLength(result.responseSnippet!, 'utf8')).toBeLessThanOrEqual(4096);
  });

  describe('worker processor wiring (claim -> sign -> dispatch -> complete)', () => {
    let db: Database;

    beforeAll(() => {
      db = new Database(TEST_DATABASE_URL);
    });
    afterAll(async () => {
      await db.close();
    });
    beforeEach(async () => {
      await resetDatabase();
    });

    /**
     * Seed a tenant endpoint pointing at the local test server, publish a
     * delivery through the queue, and claim it - exactly the production path.
     */
    async function seedAndClaim(): Promise<{ work: ClaimedWork; endpointId: string }> {
      const endpointId = newUuid();
      await db.query(
        'INSERT INTO endpoints (id, tenant_id, name, url, secret, created_at) VALUES ($1,$2,$3,$4,$5,now())',
        [endpointId, SEED.tenantAId, 'test-hook', `${baseUrl}/hook/${endpointId}`, SECRET],
      );
      await insertDelivery(db, { endpointId });
      const queue = new DeliveryQueue(db, new FakeClock(Date.UTC(2026, 0, 1)));
      return { work: (await queue.claimNext('worker-test', 30_000))!, endpointId };
    }

    it('signs the persisted envelope bytes and lands DELIVERED end-to-end', async () => {
      const clock = new FakeClock(Date.UTC(2026, 0, 1));
      const { work } = await seedAndClaim();
      const processor = createWebhookProcessor({
        db,
        clock,
        client: new WebhookClient({ ...CLIENT_CONFIG, WEBHOOK_TIMEOUT_MS: 2000 }),
      });

      const result = await processor(work);
      expect(result.outcome).toBe('SUCCESS');
      expect(received).toHaveLength(1);

      // The delivered body is byte-identical to the persisted envelope bytes.
      const [d] = await q<{ env: Buffer }>('SELECT envelope_bytes AS env FROM deliveries WHERE id = $1', [
        work.deliveryId,
      ]);
      expect(received[0].body.equals(d.env)).toBe(true);

      // Stable ids come from the envelope; the attempt id is fresh; signature
      // verifies over the fresh timestamp and exact bytes.
      const envelope = JSON.parse(d.env.toString('utf8')) as { eventId: string; deliveryId: string };
      expect(received[0].headers['x-event-id']).toBe(envelope.eventId);
      expect(received[0].headers['x-delivery-id']).toBe(envelope.deliveryId);
      expect(received[0].headers['x-attempt-id']).toBe(work.attemptId);
      expect(
        verifyWebhookSignature(
          SECRET,
          Number(received[0].headers['x-webhook-timestamp']),
          received[0].body,
          String(received[0].headers['x-webhook-signature']),
        ),
      ).toBe(true);

      // Completion through the real queue + policy transitions to DELIVERED.
      const policy = new RetryPolicy(clock, new FakeRandom([0]), {
        RETRY_MAX_ATTEMPTS_PER_CYCLE: 5,
        RETRY_BACKOFF_BASE_MS: 1000,
        RETRY_JITTER_MAX_MS: 250,
        RETRY_AFTER_CAP_MS: 60000,
      });
      const decision = policy.decide(work, result);
      const queue = new DeliveryQueue(db, clock);
      const done = await queue.completeAttempt({
        deliveryId: work.deliveryId,
        attemptRowId: work.attemptRowId,
        leaseOwner: work.leaseOwner,
        leaseGeneration: work.leaseGeneration,
        outcome: result.outcome,
        httpStatus: result.httpStatus,
        errorCode: result.errorCode,
        responseSnippet: result.responseSnippet,
        nextState: decision.nextState,
        nextAttemptAt: decision.nextAttemptAt,
      });
      expect(done.applied).toBe(true);
      const [after] = await q<{ state: string }>('SELECT state FROM deliveries WHERE id = $1', [
        work.deliveryId,
      ]);
      expect(after.state).toBe('DELIVERED');
    });

    it('never dispatches to a destination outside the allowlist (SSRF boundary)', async () => {
      const { work } = await seedAndClaim();
      const processor = createWebhookProcessor({
        db,
        clock: new FakeClock(Date.UTC(2026, 0, 1)),
        // Allowlist that does NOT contain the local test server.
        client: new WebhookClient({ ...CLIENT_CONFIG, WEBHOOK_ALLOWED_HOSTS: '10.255.255.1:1' }),
      });
      const result = await processor(work);
      expect(result.outcome).toBe('NON_RETRYABLE');
      expect(result.errorCode).toBe('destination_not_allowed');
      // Proof it never reached the network: the test server saw no request.
      expect(received).toHaveLength(0);
    });

    it('marks NON_RETRYABLE when the endpoint configuration is gone', async () => {
      const { work, endpointId } = await seedAndClaim();
      // Removing this destination requires clearing the rows that reference it
      // (deliveries cascade from events); the claimed work object already holds
      // everything the attempt needs, so only the endpoints lookup fails.
      // Scoped to this test's endpoint: the deterministic seed stays intact for
      // every other suite, which runs in a randomised file order.
      await db.query('DELETE FROM events WHERE endpoint_id = $1', [endpointId]);
      await db.query('DELETE FROM endpoints WHERE id = $1', [endpointId]);
      const processor = createWebhookProcessor({
        db,
        clock: new FakeClock(Date.UTC(2026, 0, 1)),
        client: new WebhookClient(CLIENT_CONFIG),
      });
      const result = await processor(work);
      expect(result.outcome).toBe('NON_RETRYABLE');
      expect(result.errorCode).toBe('endpoint_missing');
    });
  });
});
