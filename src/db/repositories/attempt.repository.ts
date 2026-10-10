import type { Queryable } from '../pool';
import type { AttemptOutcomeKind } from '../../domain/attempt';

export interface NewAttempt {
  id: string;
  deliveryId: string;
  /** Lifetime attempt number (1..N). */
  attemptNumber: number;
  cycle: number;
  /** The X-Attempt-Id sent to the receiver. */
  attemptId: string;
  leaseOwner: string;
  leaseGeneration: string;
  startedAt: Date;
}

export interface AttemptResult {
  outcome: AttemptOutcomeKind;
  httpStatus: number | null;
  errorCode: string | null;
  responseSnippet: string | null;
  finishedAt: Date;
}

export class AttemptRepository {
  /** Persist the attempt BEFORE dispatch, outcome UNKNOWN until a result is recorded. */
  async insertPending(q: Queryable, a: NewAttempt): Promise<void> {
    await q.query(
      `INSERT INTO delivery_attempts
         (id, delivery_id, attempt_number, cycle, attempt_id,
          lease_owner, lease_generation, started_at, outcome)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'UNKNOWN')`,
      [
        a.id,
        a.deliveryId,
        a.attemptNumber,
        a.cycle,
        a.attemptId,
        a.leaseOwner,
        a.leaseGeneration,
        a.startedAt,
      ],
    );
  }

  /**
   * Record what the dispatching worker truthfully observed. Deliberately NOT
   * fenced: even a worker that lost its lease leaves an accurate history row.
   */
  async recordResult(q: Queryable, attemptRowId: string, r: AttemptResult): Promise<void> {
    await q.query(
      `UPDATE delivery_attempts
          SET finished_at       = $2,
              outcome           = $3,
              http_status       = $4,
              error_code        = $5,
              response_snippet  = $6
        WHERE id = $1`,
      [attemptRowId, r.finishedAt, r.outcome, r.httpStatus, r.errorCode, r.responseSnippet],
    );
  }
}
