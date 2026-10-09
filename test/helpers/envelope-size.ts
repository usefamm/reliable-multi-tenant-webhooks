import { buildEnvelope } from '../../src/modules/webhooks/envelope';

/**
 * Envelope sizing for boundary tests.
 *
 * `MAX_ENVELOPE_BYTES` bounds the envelope, not the request body, so a test that
 * wants to sit exactly on that limit has to know how much the wrapper adds.
 * Measuring it with the real builder (fixed-width UUID ids, fixed timestamp) is
 * exact rather than estimated: for a payload whose only value is ASCII, the
 * payload's contribution to `JSON.stringify(envelope)` is precisely
 * `JSON.stringify(payload)`.
 */

const PROBE_EVENT_ID = '11111111-1111-4111-8111-111111111111';
const PROBE_DELIVERY_ID = '22222222-2222-4222-8222-222222222222';
const PROBE_OCCURRED_AT_MS = 1_760_000_000_000;

/** The payload shape every sized test uses: one ASCII string field. */
function payloadOfBlobLength(length: number): { blob: string } {
  return { blob: 'x'.repeat(length) };
}

function payloadJsonLength(length: number): number {
  return Buffer.byteLength(JSON.stringify(payloadOfBlobLength(length)), 'utf8');
}

/** Bytes the envelope wrapper (ids, eventType, occurredAt, braces, commas) adds. */
export function envelopeOverheadBytes(eventType: string): number {
  const probeLength = 0;
  const { bytes } = buildEnvelope({
    eventId: PROBE_EVENT_ID,
    deliveryId: PROBE_DELIVERY_ID,
    eventType,
    occurredAt: new Date(PROBE_OCCURRED_AT_MS),
    payload: payloadOfBlobLength(probeLength),
  });
  return bytes.length - payloadJsonLength(probeLength);
}

/** A payload whose envelope is exactly `envelopeBytes` long. */
export function payloadForEnvelopeSize(eventType: string, envelopeBytes: number): { blob: string } {
  const blobLength = envelopeBytes - envelopeOverheadBytes(eventType) - payloadJsonLength(0);
  if (blobLength < 0) {
    throw new Error(`envelope target of ${envelopeBytes} bytes is below the wrapper itself`);
  }
  return payloadOfBlobLength(blobLength);
}
