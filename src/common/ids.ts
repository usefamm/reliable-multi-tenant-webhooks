import { randomUUID } from 'node:crypto';

/** Opaque identifier helpers. All persisted IDs are UUIDv4 strings. */
export function newUuid(): string {
  return randomUUID();
}

/** A per-attempt identifier. Distinct from deliveryId: changes on every dispatch. */
export function newAttemptId(): string {
  return randomUUID();
}

/** A per-inbound-request identifier for log correlation. */
export function newRequestId(): string {
  return randomUUID();
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function isUuid(value: string): boolean {
  return UUID_RE.test(value);
}
