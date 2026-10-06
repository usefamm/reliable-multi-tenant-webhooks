import type { Database } from '../../db/pool';
import { sha256Hex } from '../../common/hash';
import type { Principal } from './principal';

interface AuthTokenRow {
  tenant_id: string | null;
  role: 'tenant' | 'operator';
  label: string;
}

/**
 * Resolves a raw bearer token to a Principal by looking up its SHA-256 hash.
 * The raw token is never stored, logged, or returned. An unknown/invalid token
 * yields `undefined` (the guard turns that into 401).
 */
export class AuthService {
  constructor(private readonly db: Database) {}

  /** Parse an `Authorization: Bearer <token>` header value. */
  extractBearer(headerValue: string | undefined): string | undefined {
    if (!headerValue) return undefined;
    const m = /^Bearer\s+(.+)$/i.exec(headerValue.trim());
    return m ? m[1].trim() : undefined;
  }

  async resolve(token: string | undefined): Promise<Principal | undefined> {
    if (!token) return undefined;
    const hash = sha256Hex(token);
    const { rows } = await this.db.query<AuthTokenRow>(
      'SELECT tenant_id, role, label FROM auth_tokens WHERE token_hash = $1',
      [hash],
    );
    const row = rows[0];
    if (!row) return undefined;
    if (row.role === 'operator') {
      return { kind: 'operator', label: row.label };
    }
    // role === 'tenant' guarantees tenant_id NOT NULL via a DB CHECK constraint.
    if (!row.tenant_id) return undefined;
    return { kind: 'tenant', tenantId: row.tenant_id, label: row.label };
  }
}
