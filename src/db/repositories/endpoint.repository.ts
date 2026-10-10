import type { Queryable } from '../pool';

/** Where and how to deliver: trusted deployment configuration, never caller input. */
export interface DispatchTarget {
  url: string;
  secret: string;
}

export class EndpointRepository {
  /** The tenant that owns the endpoint, or null when it does not exist. */
  async findOwnerTenantId(q: Queryable, endpointId: string): Promise<string | null> {
    const { rows } = await q.query<{ tenant_id: string }>(
      'SELECT tenant_id FROM endpoints WHERE id = $1',
      [endpointId],
    );
    return rows[0]?.tenant_id ?? null;
  }

  /** Destination and signing secret for a tenant's endpoint; null when not found. */
  async findDispatchTarget(
    q: Queryable,
    endpointId: string,
    tenantId: string,
  ): Promise<DispatchTarget | null> {
    const { rows } = await q.query<DispatchTarget>(
      'SELECT url, secret FROM endpoints WHERE id = $1 AND tenant_id = $2',
      [endpointId, tenantId],
    );
    return rows[0] ?? null;
  }
}
