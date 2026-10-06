/**
 * The authenticated caller. Identity is derived ONLY from the bearer token,
 * never from a client-supplied tenantId.
 *
 * - tenant principal: acts on behalf of exactly one tenant.
 * - operator principal: privileged; may redrive and read operational status,
 *   but is NOT a tenant and cannot publish/read tenant events.
 */
export type Principal =
  | { kind: 'tenant'; tenantId: string; label: string }
  | { kind: 'operator'; label: string };

export function isTenant(p: Principal): p is { kind: 'tenant'; tenantId: string; label: string } {
  return p.kind === 'tenant';
}

export function isOperator(p: Principal): p is { kind: 'operator'; label: string } {
  return p.kind === 'operator';
}

/** Extract the tenant id or throw a type-guarded assertion (used after checks). */
export function tenantIdOf(p: Principal): string {
  if (!isTenant(p)) {
    throw new Error('principal is not a tenant');
  }
  return p.tenantId;
}
