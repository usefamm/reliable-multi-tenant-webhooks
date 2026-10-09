import { sha256Hex } from '../../common/hash';
import type { WebhookEnvelope } from '../../domain/types';

/**
 * Build the webhook envelope and its exact serialized bytes.
 *
 * The envelope is serialized ONCE at publication and the bytes are persisted
 * (deliveries.envelope_bytes). Every attempt - including retries and operator
 * redrives - reuses those exact bytes; we never re-serialize, so the body a
 * receiver signs/verifies is byte-identical across attempts.
 *
 * Field order is fixed by construction here. `occurredAt` is ISO-8601 UTC and is
 * stable for the life of the event.
 *
 * Size is checked by the caller, not here: publication refuses an over-sized
 * envelope before any row exists, whereas a worker refusing to dispatch would
 * strand a delivery the client was already told was accepted.
 */
export function buildEnvelope(input: {
  eventId: string;
  deliveryId: string;
  eventType: string;
  occurredAt: Date;
  payload: unknown;
}): { envelope: WebhookEnvelope; bytes: Buffer; hash: string } {
  const envelope: WebhookEnvelope = {
    eventId: input.eventId,
    deliveryId: input.deliveryId,
    eventType: input.eventType,
    occurredAt: input.occurredAt.toISOString(),
    payload: input.payload,
  };
  const bytes = Buffer.from(JSON.stringify(envelope), 'utf8');
  return { envelope, bytes, hash: sha256Hex(bytes) };
}
