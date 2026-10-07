import type { IncomingMessage, ServerResponse } from 'node:http';
import type { Clock } from '../common/clock';
import { canonicalJson } from '../common/canonical-json';
import { sha256Hex } from '../common/hash';
import { verifyWebhookSignature } from '../modules/webhooks/signing';
import type { WebhookEnvelope } from '../domain/types';
import { ENDPOINT_WIDE, ReceiverMode, RECEIVER_MODES, type ReceiverRepository } from './repository';

export interface ReceiverHandlerConfig {
  /** Signature timestamp tolerance, in seconds (PDF: 300). */
  timestampToleranceSec: number;
  /** Reject oversized inbound bodies rather than buffering unbounded. */
  maxBodyBytes: number;
  /** Serve the /__control test surface (failure modes, inspection, reset). */
  testControls: boolean;
}

const WEBHOOK_PATH = /^\/hook\/([0-9a-fA-F-]{36})$/;

/**
 * The mock receiver: an independent, durable business endpoint used to prove
 * at-least-once delivery + exactly-once business effects.
 *
 * Order of operations is deliberate and mirrors what a real receiver must do
 * (PDF section 14): verify the timestamp FIRST (cheap replay protection), then
 * recompute the HMAC over the exact bytes with a timing-safe comparison, and
 * ONLY then touch deduplication/business logic. Nothing is applied for a request
 * that fails verification.
 *
 * Dedup identity is (endpointId, eventId) enforced by a UNIQUE constraint, so
 * the effect and its dedup record are written by the same statement - there is
 * no check-then-write race.
 */
export function createReceiverHandler(repo: ReceiverRepository, clock: Clock, config: ReceiverHandlerConfig) {
  return async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    try {
      await route(req, res);
    } catch (err) {
      // An unexpected internal failure must be a 5xx the sender can retry,
      // not a crashed process or a hung socket.
      json(res, 500, { code: 'receiver_error' });
      if (process.env.NODE_ENV !== 'test') {
        // eslint-disable-next-line no-console
        console.error('receiver error:', err instanceof Error ? err.message : err);
      }
    }
  };

  async function route(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', 'http://receiver.local');
    const path = url.pathname;

    if (req.method === 'GET' && path === '/health') {
      return json(res, 200, { status: 'ok' });
    }

    if (path.startsWith('/__control')) {
      if (!config.testControls) {
        return json(res, 403, { code: 'test_controls_disabled' });
      }
      return handleControl(req, res, path, url);
    }

    const match = WEBHOOK_PATH.exec(path);
    if (!match || req.method !== 'POST') {
      return json(res, 404, { code: 'not_found' });
    }
    return handleWebhook(req, res, match[1]);
  }

  async function handleWebhook(req: IncomingMessage, res: ServerResponse, endpointId: string): Promise<void> {
    let body: Buffer;
    try {
      body = await readBody(req, config.maxBodyBytes);
    } catch {
      return json(res, 413, { code: 'payload_too_large' });
    }
    const headers = req.headers;
    const eventId = str(headers['x-event-id']);
    const deliveryId = str(headers['x-delivery-id']);
    const attemptId = str(headers['x-attempt-id']);
    const rawTimestamp = str(headers['x-webhook-timestamp']);
    const presentedSignature = str(headers['x-webhook-signature']);

    const secret = await repo.endpointSecret(endpointId);
    if (!secret) {
      // No configured identity for this destination: reject, apply nothing.
      await safeRecord(repo, { endpointId, eventId, deliveryId, attemptId, signatureOk: false, mode: 'unknown_endpoint' });
      return json(res, 401, { code: 'unknown_endpoint' });
    }

    // 1. Structural completeness and timestamp freshness (before any work).
    const timestampSec = Number(rawTimestamp);
    const fresh =
      Number.isInteger(timestampSec) &&
      Math.abs(clock.nowUnixSeconds() - timestampSec) <= config.timestampToleranceSec;

    // 2. Recompute the HMAC over the exact received bytes and compare
    //    in constant time.
    const signatureOk =
      !!eventId &&
      !!attemptId &&
      fresh &&
      verifyWebhookSignatureLenient(secret, timestampSec, body, presentedSignature);

    if (!signatureOk) {
      await safeRecord(repo, { endpointId, eventId, deliveryId, attemptId, signatureOk: false, mode: 'rejected' });
      return json(res, 401, { code: fresh ? 'invalid_signature' : 'stale_timestamp' });
    }

    // 3. Only now parse and apply the business effect.
    let envelope: WebhookEnvelope;
    try {
      envelope = JSON.parse(body.toString('utf8')) as WebhookEnvelope;
    } catch {
      return json(res, 400, { code: 'invalid_body' });
    }
    if (envelope.eventId !== eventId || envelope.deliveryId !== deliveryId) {
      // Header/body disagreement is tampering, not a duplicate.
      return json(res, 400, { code: 'identity_mismatch' });
    }

    const mode = await repo.takeMode(endpointId, eventId);
    const modeName = mode?.mode ?? ReceiverMode.SUCCESS;
    await safeRecord(repo, { endpointId, eventId, deliveryId, attemptId, signatureOk: true, mode: modeName });

    // Failure modes that decline the delivery apply nothing.
    if (mode && mode.mode !== ReceiverMode.SUCCESS && !APPLIES_EFFECT.includes(mode.mode)) {
      return respondForMode(res, mode.mode, mode.retry_after);
    }

    const contentHash = sha256Hex(canonicalJson(envelope));
    const outcome = await repo.applyEffect(endpointId, eventId, contentHash);

    if (outcome === 'conflict') {
      return json(res, 409, { code: 'content_conflict', message: 'Same identity, different content' });
    }

    // Accepted: first application or a harmless duplicate - both answer 200 so
    // the sender stops retrying, and only the first one changed state.
    const applied = outcome === 'applied';

    if (mode?.mode === ReceiverMode.LOST_RESPONSE) {
      // Effect is durably committed; then the response disappears (PDF 22.5).
      res.destroy();
      return;
    }
    if (mode?.mode === ReceiverMode.SLOW && mode.delay_ms) {
      await delay(mode.delay_ms);
    }
    return json(res, 200, { ok: true, applied, deduplicated: !applied, eventId, attemptId });
  }

  async function handleControl(req: IncomingMessage, res: ServerResponse, path: string, url: URL): Promise<void> {
    if (path === '/__control/modes' && req.method === 'PUT') {
      const body = await readBody(req, 8192);
      let input: {
        endpointId?: string;
        eventId?: string;
        mode?: string;
        remaining?: number | null;
        retryAfter?: number | null;
        delayMs?: number | null;
      };
      try {
        input = JSON.parse(body.toString('utf8'));
      } catch {
        return json(res, 400, { code: 'invalid_json' });
      }
      if (!input.endpointId || !input.mode || !RECEIVER_MODES.includes(input.mode as ReceiverMode)) {
        return json(res, 400, { code: 'bad_mode_request' });
      }
      await repo.setMode({
        endpointId: input.endpointId,
        eventId: input.eventId ?? ENDPOINT_WIDE,
        mode: input.mode as ReceiverMode,
        remaining: input.remaining ?? null,
        retryAfter: input.retryAfter ?? null,
        delayMs: input.delayMs ?? null,
      });
      return json(res, 200, { ok: true });
    }
    if (path === '/__control/modes' && req.method === 'DELETE') {
      await repo.clearModes();
      return json(res, 200, { ok: true });
    }
    if (path === '/__control/requests' && req.method === 'GET') {
      const endpointId = url.searchParams.get('endpointId') ?? undefined;
      return json(res, 200, { requests: await repo.listRequests(endpointId) });
    }
    if (path === '/__control/effects' && req.method === 'GET') {
      const endpointId = url.searchParams.get('endpointId') ?? undefined;
      return json(res, 200, { effects: await repo.listEffects(endpointId) });
    }
    if (path === '/__control/reset' && req.method === 'POST') {
      await repo.resetAll();
      return json(res, 200, { ok: true });
    }
    return json(res, 404, { code: 'unknown_control' });
  }
}

/** Modes that still represent a receiver that processed the webhook. */
const APPLIES_EFFECT: ReceiverMode[] = [ReceiverMode.LOST_RESPONSE, ReceiverMode.SLOW];

/** Length-guarded wrapper so a malformed signature can never throw. */
function verifyWebhookSignatureLenient(
  secret: string,
  timestampSec: number,
  body: Buffer,
  presented: string | null,
): boolean {
  if (!presented || !/^[0-9a-f]{64}$/i.test(presented)) return false;
  return verifyWebhookSignature(secret, timestampSec, body, presented);
}

function respondForMode(
  res: ServerResponse,
  mode: ReceiverMode,
  retryAfter: number | null,
): void {
  switch (mode) {
    case ReceiverMode.TEMP_FAILURE:
      return json(res, 503, { code: 'temporary_failure' });
    case ReceiverMode.PERM_FAILURE:
      return json(res, 500, { code: 'permanent_failure' });
    case ReceiverMode.RATE_LIMITED:
      return json(res, 429, { code: 'rate_limited' }, { 'retry-after': String(retryAfter ?? 1) });
    case ReceiverMode.REJECT_400:
      return json(res, 400, { code: 'rejected' });
    case ReceiverMode.REDIRECT:
      return json(res, 302, { code: 'redirect' }, { location: 'http://127.0.0.1:1/nowhere' });
    default:
      return json(res, 200, { ok: true });
  }
}

function json(
  res: ServerResponse,
  status: number,
  payload: unknown,
  extraHeaders: Record<string, string> = {},
): void {
  if (res.writableEnded || res.destroyed) return;
  const body = Buffer.from(JSON.stringify(payload), 'utf8');
  res.writeHead(status, { 'content-type': 'application/json', ...extraHeaders });
  res.end(body);
}

function readBody(req: IncomingMessage, maxBytes: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    let failed = false;
    req.on('data', (c: Buffer) => {
      if (failed) return;
      size += c.byteLength;
      if (size > maxBytes) {
        // Stop buffering, but keep the connection alive so the caller can still
        // answer 413 - the sender needs a status, not a reset.
        failed = true;
        reject(new Error('payload_too_large'));
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      if (!failed) resolve(Buffer.concat(chunks));
    });
    req.on('error', (err) => {
      if (!failed) {
        failed = true;
        reject(err);
      }
    });
  });
}

function delay(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function str(value: string | string[] | undefined): string | null {
  if (Array.isArray(value)) return value[0] ?? null;
  return value ?? null;
}

/** Recording failures must never change the response the sender sees. */
async function safeRecord(
  repo: ReceiverRepository,
  entry: {
    endpointId: string;
    eventId: string | null;
    deliveryId: string | null;
    attemptId: string | null;
    signatureOk: boolean;
    mode: string;
  },
): Promise<void> {
  try {
    await repo.recordRequest(entry);
  } catch {
    // best-effort instrumentation
  }
}
