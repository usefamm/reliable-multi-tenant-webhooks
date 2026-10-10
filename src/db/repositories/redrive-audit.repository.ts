import type { Queryable } from '../pool';

export interface NewRedriveAudit {
  id: string;
  deliveryId: string;
  /** Operator label from the authenticated principal - never the raw token. */
  operator: string;
  reason: string;
  idempotencyKey: string;
}

export class RedriveAuditRepository {
  async insert(q: Queryable, audit: NewRedriveAudit): Promise<void> {
    await q.query(
      `INSERT INTO redrive_audit (id, delivery_id, operator, reason, idempotency_key)
       VALUES ($1, $2, $3, $4, $5)`,
      [audit.id, audit.deliveryId, audit.operator, audit.reason, audit.idempotencyKey],
    );
  }
}
