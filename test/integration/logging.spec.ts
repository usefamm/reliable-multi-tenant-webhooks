import type { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { createTestApp } from '../helpers/app';
import { resetDatabase } from '../helpers/db';
import { SEED, TEST_TOKENS } from '../helpers/test-env';
import type { Logger } from '../../src/common/logger';

interface CapturedLine {
  level: string;
  msg: string;
  fields: Record<string, unknown>;
}

/**
 * A capture logger, deliberately NOT pino: it records exactly the object the call
 * site handed it. pino is configured to censor a `payload` key on the way out, so
 * a "no payload in the logs" assertion made against pino would pass even if the
 * code logged the payload. Capturing the input proves the call site instead.
 */
function createCaptureLogger(lines: CapturedLine[]): Logger {
  const record =
    (level: string) =>
    (fields: unknown, msg?: string): void => {
      lines.push({
        level,
        msg: msg ?? '',
        fields:
          typeof fields === 'object' && fields !== null
            ? (fields as Record<string, unknown>)
            : { value: fields },
      });
    };
  return {
    info: record('info'),
    warn: record('warn'),
    error: record('error'),
    debug: record('debug'),
    trace: record('trace'),
    fatal: record('fatal'),
  } as unknown as Logger;
}

const authA = { Authorization: `Bearer ${TEST_TOKENS.tenantA}` };
const PAYLOAD_MARKER = 'pan-4242-4242-4242-marker';

function publishBody() {
  return {
    endpointId: SEED.endpointA1,
    eventType: 'order.created',
    payload: { orderId: 'ord_1', pan: PAYLOAD_MARKER },
  };
}

/**
 * Observability requirements (PDF: structured logs with correlation identifiers
 * where appropriate; never log credentials or complete payloads):
 *  - an accepted publication carries requestId + eventId + deliveryId, which is
 *    the only place the API's correlation id meets the ids the workers log;
 *  - the caller's x-request-id is honoured, and one is generated when absent;
 *  - no log line is handed the payload, the endpoint secret or the bearer token.
 */
describe('observability: correlation ids and log hygiene', () => {
  let app: INestApplication;
  const lines: CapturedLine[] = [];

  beforeAll(async () => {
    app = await createTestApp({ logger: createCaptureLogger(lines) });
  });
  afterAll(async () => {
    await app.close();
  });
  beforeEach(async () => {
    lines.length = 0;
    await resetDatabase();
  });

  async function publish(headers: Record<string, string> = {}) {
    return request(app.getHttpServer())
      .post('/events')
      .set(authA)
      .set('Idempotency-Key', 'key-log')
      .set(headers)
      .send(publishBody())
      .expect(202);
  }

  it('logs the accepted publication with requestId, eventId and deliveryId', async () => {
    const res = await publish({ 'x-request-id': 'req-trace-1' });

    const accepted = lines.filter((l) => l.msg === 'event accepted');
    expect(accepted).toHaveLength(1);
    expect(accepted[0].level).toBe('info');
    expect(accepted[0].fields).toMatchObject({
      requestId: 'req-trace-1',
      eventId: res.body.eventId,
      deliveryId: res.body.deliveryId,
      endpointId: SEED.endpointA1,
      eventType: 'order.created',
    });
    // The id in the log is the id the caller can quote back from the response.
    expect(res.headers['x-request-id']).toBe('req-trace-1');
  });

  it('generates a correlation id when the caller sends none, and logs that id', async () => {
    const res = await publish();
    const generated = res.headers['x-request-id'] as string;

    expect(generated).toBeTruthy();
    expect(lines.find((l) => l.msg === 'event accepted')?.fields.requestId).toBe(generated);
  });

  it('never hands the payload, the endpoint secret or the bearer token to the logger', async () => {
    await publish({ 'x-request-id': 'req-trace-2' });

    // Every line a successful publication produced, serialized as it was given.
    const serialized = JSON.stringify(lines);
    expect(serialized).not.toContain(PAYLOAD_MARKER);
    expect(serialized).not.toContain(SEED.endpointSecretA1);
    expect(serialized).not.toContain(TEST_TOKENS.tenantA);
    expect(lines.every((l) => l.fields.payload === undefined)).toBe(true);
    expect(lines.every((l) => l.fields.secret === undefined)).toBe(true);
    expect(lines.every((l) => l.fields.authorization === undefined)).toBe(true);
  });
});
