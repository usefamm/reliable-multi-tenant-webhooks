import { PgDeliveryQueue } from '../../src/db/pg-delivery-queue';
import { FakeClock } from '../../src/common/clock';
import { DeliveryRepository } from '../../src/db/repositories/delivery.repository';
import { DeliveryState } from '../../src/domain/types';
import { q, resetDatabase, testDb } from '../helpers/db';
import { BASE_MS, insertDelivery } from '../helpers/worker';
import { SEED } from '../helpers/test-env';

/**
 * The persistence seams: the transaction helper, the repositories' tenant scoping
 * and the queue's refusal of transitions the domain does not allow. These run
 * against the real database; nothing here goes through HTTP.
 */
describe('persistence layer', () => {
  afterEach(async () => {
    await q(`DELETE FROM tenants WHERE id IN ('rolled-back', 'committed')`);
    await resetDatabase();
  });

  describe('Database.withTransaction', () => {
    it('rolls back every statement when the callback throws', async () => {
      await expect(
        testDb.withTransaction(async (tx) => {
          await tx.query(`INSERT INTO tenants (id, name) VALUES ('rolled-back', 'x')`);
          throw new Error('boom');
        }),
      ).rejects.toThrow('boom');

      expect(await q(`SELECT 1 FROM tenants WHERE id = 'rolled-back'`)).toHaveLength(0);
    });

    it('commits when the callback resolves', async () => {
      await testDb.withTransaction(async (tx) => {
        await tx.query(`INSERT INTO tenants (id, name) VALUES ('committed', 'x')`);
      });
      expect(await q(`SELECT 1 FROM tenants WHERE id = 'committed'`)).toHaveLength(1);
    });

    it('honours the requested isolation level', async () => {
      const level = await testDb.withTransaction(
        async (tx) => (await tx.query<{ iso: string }>("SELECT current_setting('transaction_isolation') AS iso")).rows[0].iso,
        { isolation: 'REPEATABLE READ' },
      );
      expect(level).toBe('repeatable read');
    });
  });

  describe('DeliveryRepository.listForTenant', () => {
    it('never returns another tenant’s deliveries', async () => {
      const mine = await insertDelivery(testDb, { tenantId: SEED.tenantAId, endpointId: SEED.endpointA1 });
      await insertDelivery(testDb, { tenantId: SEED.tenantBId, endpointId: SEED.endpointB1 });

      const rows = await new DeliveryRepository().listForTenant(testDb, SEED.tenantAId, { limit: 50 });

      expect(rows.map((r) => r.id)).toEqual([mine.deliveryId]);
    });
  });

  describe('PgDeliveryQueue.completeAttempt', () => {
    async function claimed() {
      await insertDelivery(testDb, { state: 'READY' });
      const queue = new PgDeliveryQueue(testDb, new FakeClock(BASE_MS));
      const work = (await queue.claimNext('worker-a', 30_000))!;
      return { queue, work };
    }

    const base = {
      outcome: 'RETRYABLE' as const,
      httpStatus: 503,
      errorCode: 'http_503',
      responseSnippet: null,
    };

    it('refuses a RETRY_WAIT completion that has no schedule, and changes nothing', async () => {
      const { queue, work } = await claimed();
      await expect(
        queue.completeAttempt({
          ...base,
          deliveryId: work.deliveryId,
          attemptRowId: work.attemptRowId,
          leaseOwner: work.leaseOwner,
          leaseGeneration: work.leaseGeneration,
          nextState: DeliveryState.RETRY_WAIT,
          nextAttemptAt: null,
        }),
      ).rejects.toThrow(/illegal completion/);

      const [row] = await q<{ state: string }>('SELECT state FROM deliveries WHERE id = $1', [work.deliveryId]);
      expect(row.state).toBe(DeliveryState.IN_FLIGHT);
      const [attempt] = await q<{ finished_at: Date | null }>(
        'SELECT finished_at FROM delivery_attempts WHERE id = $1',
        [work.attemptRowId],
      );
      expect(attempt.finished_at).toBeNull();
    });

    it.each([DeliveryState.READY, DeliveryState.IN_FLIGHT])(
      'refuses to complete into %s',
      async (nextState) => {
        const { queue, work } = await claimed();
        await expect(
          queue.completeAttempt({
            ...base,
            deliveryId: work.deliveryId,
            attemptRowId: work.attemptRowId,
            leaseOwner: work.leaseOwner,
            leaseGeneration: work.leaseGeneration,
            nextState,
            nextAttemptAt: new Date(BASE_MS + 1_000),
          }),
        ).rejects.toThrow(/illegal completion/);
      },
    );
  });
});
