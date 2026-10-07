import { Database } from '../../src/db/pool';
import { FakeClock } from '../../src/common/clock';
import { FakeRandom } from '../../src/common/random';
import { newUuid } from '../../src/common/ids';
import { WebhookClient } from '../../src/modules/webhooks/webhook.client';
import { createWebhookProcessor } from '../../src/worker/processor';
import { DeliveryQueue } from '../../src/worker/delivery-queue';
import { RetryPolicy } from '../../src/worker/retry-policy';
import { DeliveryWorker } from '../../src/worker/delivery-worker';
import { insertDelivery, RETRY_CONFIG, silentLogger, BASE_MS } from './worker';
import { SEED, TEST_DATABASE_URL } from './test-env';
import type { ReceiverApp } from './receiver';

/**
 * End-to-end delivery loop: real worker process logic, real outbound HTTP,
 * real mock receiver, real PostgreSQL.
 *
 * Time is a shared FakeClock (the receiver uses the same one), so retry
 * schedules, lease expiry and signature timestamps are asserted exactly rather
 * than waited on. The only real time involved is the socket I/O itself, which is
 * why every wait here polls the database for a durable fact instead of sleeping.
 */
export interface AttemptRow {
  attempt_number: number;
  cycle: number;
  attempt_id: string;
  outcome: string;
  http_status: number | null;
  error_code: string | null;
  finished_at: Date | null;
  lease_owner: string | null;
  lease_generation: string;
}

export interface DeliveryView {
  state: string;
  attempt_count: number;
  attempts_in_cycle: number;
  cycle: number;
  next_attempt_at: Date | null;
  last_http_status: number | null;
  last_error_code: string | null;
  lease_owner: string | null;
}

export interface LoopOptions {
  owner?: string;
  concurrency?: number;
  /** Outbound total timeout for this loop (short values let slow-mode tests be fast). */
  timeoutMs?: number;
  allowedHosts?: string;
  leaseTtlMs?: number;
  pollIntervalMs?: number;
  shutdownGraceMs?: number;
  /**
   * Register the loop's endpoint at this URL instead of the mock receiver's.
   * Used by the acceptance suites to dispatch at a raw capture endpoint that
   * records the exact bytes and headers, while time and queue state stay
   * observable through the same shared clock.
   */
  destinationUrl?: string;
}

export interface DeliveryLoop {
  db: Database;
  clock: FakeClock;
  queue: DeliveryQueue;
  policy: RetryPolicy;
  worker: DeliveryWorker;
  endpointId: string;
  start(): void;
  /** Tear everything down (worker stopped, endpoint + its events removed). */
  close(): Promise<void>;
  /** durable queue work for this loop's endpoint (READY, due immediately) */
  publish(opts?: { payload?: Record<string, unknown>; eventType?: string }): Promise<{
    eventId: string;
    deliveryId: string;
  }>;
  delivery(deliveryId: string): Promise<DeliveryView>;
  attempts(deliveryId: string): Promise<AttemptRow[]>;
  /** Wait for the nth attempt to be claimed (row exists, possibly still dispatching). */
  waitForClaim(deliveryId: string, count: number, timeoutMs?: number): Promise<AttemptRow[]>;
  /** Wait for the nth attempt to carry a recorded outcome (dispatch completed). */
  waitForAttempts(deliveryId: string, count: number, timeoutMs?: number): Promise<AttemptRow[]>;
  waitForState(deliveryId: string, states: string[], timeoutMs?: number): Promise<DeliveryView>;
  /** Move the shared clock to the delivery's scheduled retry instant. */
  advanceToDue(deliveryId: string): Promise<Date | null>;
}

export async function startLoop(receiver: ReceiverApp, opts: LoopOptions = {}): Promise<DeliveryLoop> {
  const db = new Database(TEST_DATABASE_URL);
  const endpointId = newUuid();
  const url = opts.destinationUrl ?? `http://127.0.0.1:${receiver.port}/hook/${endpointId}`;
  await db.query(
    `INSERT INTO endpoints (id, tenant_id, name, url, secret)
     VALUES ($1,$2,$3,$4,$5)`,
    [endpointId, SEED.tenantAId, 'loop-endpoint', url, SEED.endpointSecretA1],
  );

  const clock = receiver.clock;
  const client = new WebhookClient({
    WEBHOOK_TIMEOUT_MS: opts.timeoutMs ?? 2_000,
    WEBHOOK_MAX_RESPONSE_BYTES: 4_096,
    WEBHOOK_ALLOWED_HOSTS: opts.allowedHosts ?? '',
  });
  const queue = new DeliveryQueue(db, clock);
  const policy = new RetryPolicy(clock, new FakeRandom([0]), RETRY_CONFIG);
  const worker = new DeliveryWorker({
    queue,
    policy,
    processor: createWebhookProcessor({ db, clock, client }),
    logger: silentLogger,
    owner: opts.owner ?? 'loop-worker',
    concurrency: opts.concurrency ?? 1,
    leaseTtlMs: opts.leaseTtlMs ?? 30_000,
    pollIntervalMs: opts.pollIntervalMs ?? 10,
    claimBatchSize: opts.concurrency ?? 1,
    shutdownGraceMs: opts.shutdownGraceMs ?? 1_000,
  });

  async function delivery(deliveryId: string): Promise<DeliveryView> {
    const { rows } = await db.query<DeliveryView>(
      `SELECT state, attempt_count, attempts_in_cycle, cycle, next_attempt_at,
              last_http_status, last_error_code, lease_owner
         FROM deliveries WHERE id = $1`,
      [deliveryId],
    );
    return rows[0];
  }

  async function attempts(deliveryId: string): Promise<AttemptRow[]> {
    const { rows } = await db.query<AttemptRow>(
      `SELECT attempt_number, cycle, attempt_id, outcome, http_status, error_code, finished_at,
              lease_owner, lease_generation
         FROM delivery_attempts
        WHERE delivery_id = $1
        ORDER BY attempt_number`,
      [deliveryId],
    );
    return rows;
  }

  async function poll<T>(read: () => Promise<T>, done: (value: T) => boolean, what: string, timeoutMs: number): Promise<T> {
    const deadline = Date.now() + timeoutMs;
    let last: T | undefined;
    while (Date.now() < deadline) {
      last = await read();
      if (done(last)) return last;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    throw new Error(`timed out after ${timeoutMs}ms waiting for ${what}; last seen: ${JSON.stringify(last)}`);
  }

  let started = false;
  return {
    db,
    clock,
    queue,
    policy,
    worker,
    endpointId,
    start() {
      if (started) return;
      started = true;
      worker.start();
    },
    async close() {
      if (started) await worker.stop();
      await db.query('DELETE FROM events WHERE endpoint_id = $1', [endpointId]);
      await db.query('DELETE FROM endpoints WHERE id = $1', [endpointId]);
      await db.pool.end();
    },
    async publish(publishOpts = {}) {
      return insertDelivery(db, {
        endpointId,
        tenantId: SEED.tenantAId,
        state: 'READY',
        nextAttemptAt: new Date(BASE_MS),
        payload: publishOpts.payload ?? { orderId: newUuid() },
        eventType: publishOpts.eventType,
      });
    },
    delivery,
    attempts,
    async waitForAttempts(deliveryId, count, timeoutMs = 3_000) {
      // An attempt row is written BEFORE dispatch (outcome UNKNOWN, finished_at
      // NULL), so counting rows would race the HTTP call. Wait for the attempt
      // to carry a recorded outcome instead - that is written in the same
      // transaction as the delivery's state transition.
      const rows = await poll(
        () => attempts(deliveryId),
        (r) => r.length >= count && r[count - 1].finished_at !== null,
        `attempt ${count} to complete on ${deliveryId}`,
        timeoutMs,
      );
      return rows;
    },
    async waitForClaim(deliveryId, count, timeoutMs = 3_000) {
      // Exists but possibly unfinished: the attempt was allocated and leased,
      // the dispatch has not been completed yet.
      const rows = await poll(
        () => attempts(deliveryId),
        (r) => r.length >= count,
        `attempt ${count} to be claimed on ${deliveryId}`,
        timeoutMs,
      );
      return rows;
    },
    async waitForState(deliveryId, states, timeoutMs = 3_000) {
      return poll(
        () => delivery(deliveryId),
        (d) => states.includes(d.state),
        `state ${states.join('|')} on ${deliveryId}`,
        timeoutMs,
      );
    },
    async advanceToDue(deliveryId) {
      const current = await delivery(deliveryId);
      if (!current.next_attempt_at) return null;
      clock.set(current.next_attempt_at.getTime());
      return current.next_attempt_at;
    },
  };
}

/** Convenience: the loop's endpoint URL path used by assertions/messages. */
export function loopEndpointUrl(receiver: ReceiverApp, endpointId: string): string {
  return `http://127.0.0.1:${receiver.port}/hook/${endpointId}`;
}
