import { createServer, request as nodeRequest, type Server } from 'node:http';
import { Database } from '../../src/db/pool';
import { FakeClock } from '../../src/common/clock';
import { signWebhook } from '../../src/modules/webhooks/signing';
import { MAX_ENVELOPE_BYTES } from '../../src/domain/types';
import { ReceiverRepository } from '../../src/receiver/repository';
import { createReceiverHandler } from '../../src/receiver/handler';
import { BASE_MS } from './worker';

/**
 * Test harness for the mock receiver.
 *
 * It boots the REAL receiver over a REAL network socket against the REAL
 * database, so signature verification, freshness, deduplication and the
 * failure modes are exercised end-to-end rather than simulated.
 *
 * `startReceiver` can be called twice with separate Database instances: a new
 * instance + new handler is how these tests prove a receiver restart keeps the
 * deduplication state (all of it lives in PostgreSQL).
 */
export interface ReceiverApp {
  port: number;
  db: Database;
  repo: ReceiverRepository;
  clock: FakeClock;
  /** POST a webhook with a valid signature unless overridden. Resolves even when the connection dies. */
  post(input: PostWebhookInput): Promise<PostWebhookResult>;
  /** Call the test-only control surface (or any path) with a raw body. */
  call(method: string, path: string, body?: unknown): Promise<PostWebhookResult>;
  close(): Promise<void>;
}

export interface PostWebhookInput {
  endpointId: string;
  eventId: string;
  deliveryId: string;
  attemptId: string;
  /** Signing secret used for the headers (pass a wrong one to test rejection). */
  secret: string;
  /** Body to send. Defaults to a well-formed envelope matching the headers. */
  body?: Buffer | string;
  /** Timestamp header; defaults to the receiver clock's current second. */
  timestampSec?: number;
  /** Signature header override (pass garbage to test rejection). */
  signature?: string;
  /** Drop a header entirely (e.g. no X-Attempt-Id). */
  omit?: Array<'event' | 'delivery' | 'attempt' | 'timestamp' | 'signature'>;
  /** Sign the pristine body but send this mutated one instead (tampering). */
  tamper?: boolean;
}

export interface PostWebhookResult {
  status: number;
  headers: Record<string, string | string[] | undefined>;
  text: string;
  json: Record<string, unknown> | null;
  /** Present when the transport failed (reset/hangup), e.g. lost_response mode. */
  transportError?: string;
}

export async function startReceiver(opts: {
  databaseUrl: string;
  clock?: FakeClock;
  timestampToleranceSec?: number;
  maxBodyBytes?: number;
  testControls?: boolean;
  /**
   * Bind this exact port. A restart then answers on the same address the sender
   * already has configured - which is how a container restart behaves behind
   * stable service DNS, and what lets a test prove the new process deduplicates
   * against the database rather than against memory.
   */
  port?: number;
}): Promise<ReceiverApp> {
  const db = new Database(opts.databaseUrl);
  const repo = new ReceiverRepository(db);
  const clock = opts.clock ?? new FakeClock(BASE_MS);
  const handler = createReceiverHandler(repo, clock, {
    timestampToleranceSec: opts.timestampToleranceSec ?? 300,
    maxBodyBytes: opts.maxBodyBytes ?? MAX_ENVELOPE_BYTES,
    testControls: opts.testControls ?? true,
  });

  const server: Server = createServer((req, res) => {
    void handler(req, res);
  });
  server.setMaxListeners(0);
  const port = await new Promise<number>((resolve, reject) => {
    server.once('error', reject);
    server.listen(opts.port ?? 0, '127.0.0.1', () => {
      server.removeListener('error', reject);
      const address = server.address();
      resolve(typeof address === 'object' && address ? address.port : 0);
    });
  });

  async function send(
    method: string,
    path: string,
    body: Buffer,
    headers: Record<string, string>,
  ): Promise<PostWebhookResult> {
    return new Promise((resolve) => {
      const req = nodeRequest({ host: '127.0.0.1', port, method, path, headers }, (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf8');
          let json: Record<string, unknown> | null = null;
          try {
            json = JSON.parse(text) as Record<string, unknown>;
          } catch {
            json = null;
          }
          resolve({ status: res.statusCode ?? 0, headers: res.headers, text, json });
        });
      });
      // A destroyed response (lost_response) is an expected outcome, not a test error.
      req.on('error', (err: Error & { code?: string }) => {
        resolve({
          status: 0,
          headers: {},
          text: '',
          json: null,
          transportError: err.code ?? err.message,
        });
      });
      req.end(body);
    });
  }

  return {
    port,
    db,
    repo,
    clock,
    async post(input) {
      const body = toBuffer(
        input.body ??
          JSON.stringify({
            eventId: input.eventId,
            deliveryId: input.deliveryId,
            eventType: 'order.created',
            occurredAt: clock.now().toISOString(),
            payload: { orderId: input.eventId },
          }),
      );
      // Tampering signs the pristine bytes but sends an extra trailing byte, so
      // the body is still parseable JSON while the HMAC no longer matches.
      const sent = input.tamper ? Buffer.concat([body, Buffer.from(' ', 'utf8')]) : body;
      const timestampSec = input.timestampSec ?? clock.nowUnixSeconds();
      const signature = input.signature ?? signWebhook(input.secret, timestampSec, body);

      const headers: Record<string, string> = {
        'content-type': 'application/json',
        'content-length': String(sent.byteLength),
        'x-event-id': input.omit?.includes('event') ? '' : input.eventId,
        'x-delivery-id': input.omit?.includes('delivery') ? '' : input.deliveryId,
        'x-attempt-id': input.omit?.includes('attempt') ? '' : input.attemptId,
        'x-webhook-timestamp': String(timestampSec),
        'x-webhook-signature': signature,
      };
      if (input.omit?.includes('timestamp')) delete headers['x-webhook-timestamp'];
      if (input.omit?.includes('signature')) delete headers['x-webhook-signature'];

      return send('POST', `/hook/${input.endpointId}`, sent, headers);
    },
    async call(method, path, body) {
      const payload = body === undefined ? Buffer.alloc(0) : Buffer.from(JSON.stringify(body), 'utf8');
      return send(method, path, payload, {
        'content-type': 'application/json',
        'content-length': String(payload.byteLength),
      });
    },
    close() {
      return new Promise<void>((resolve) => {
        server.close(() => {
          void db.pool.end().then(() => resolve());
        });
        // Don't let a keep-alive socket block teardown of a restarted instance.
        server.closeAllConnections?.();
      });
    },
  };
}

function toBuffer(value: Buffer | string): Buffer {
  return Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf8');
}
