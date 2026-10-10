import type { Database } from '../../db/pool';
import { sha256Hex } from '../../common/hash';
import { AuthTokenRepository } from '../../db/repositories/auth-token.repository';
import type { Principal } from './principal';

/**
 * Resolves a raw bearer token to a Principal by looking up its SHA-256 hash.
 * The raw token is never stored, logged, or returned. An unknown/invalid token
 * yields `undefined` (the guard turns that into 401).
 */
export class AuthService {
  constructor(
    private readonly db: Database,
    private readonly tokens: AuthTokenRepository = new AuthTokenRepository(),
  ) {}

  /** Parse an `Authorization: Bearer <token>` header value. */
  extractBearer(headerValue: string | undefined): string | undefined {
    if (!headerValue) return undefined;
    const m = /^Bearer\s+(.+)$/i.exec(headerValue.trim());
    return m ? m[1].trim() : undefined;
  }

  async resolve(token: string | undefined): Promise<Principal | undefined> {
    if (!token) return undefined;
    const hash = sha256Hex(token);
    const row = await this.tokens.findByHash(this.db, hash);
    if (!row) return undefined;
    if (row.role === 'operator') {
      return { kind: 'operator', label: row.label };
    }
    // role === 'tenant' guarantees tenant_id NOT NULL via a DB CHECK constraint.
    if (!row.tenantId) return undefined;
    return { kind: 'tenant', tenantId: row.tenantId, label: row.label };
  }
}
