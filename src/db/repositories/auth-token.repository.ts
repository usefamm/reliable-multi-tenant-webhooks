import type { Queryable } from '../pool';

export interface AuthTokenRecord {
  tenantId: string | null;
  role: 'tenant' | 'operator';
  label: string;
}

export class AuthTokenRepository {
  async findByHash(q: Queryable, tokenHash: string): Promise<AuthTokenRecord | null> {
    const { rows } = await q.query<{
      tenant_id: string | null;
      role: 'tenant' | 'operator';
      label: string;
    }>('SELECT tenant_id, role, label FROM auth_tokens WHERE token_hash = $1', [tokenHash]);
    const row = rows[0];
    return row ? { tenantId: row.tenant_id, role: row.role, label: row.label } : null;
  }
}
