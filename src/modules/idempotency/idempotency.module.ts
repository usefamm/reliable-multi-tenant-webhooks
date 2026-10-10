import { Module } from '@nestjs/common';
import { DATABASE } from '../../common/tokens';
import { IdempotencyService } from './idempotency.service';
import { IdempotencyRepository } from '../../db/repositories/idempotency.repository';
import type { Database } from '../../db/pool';

/**
 * Idempotency feature module. Shared by event publication (M5) and operator
 * redrive (M12) so both operations get the same database-backed dedup semantics.
 */
@Module({
  providers: [
    {
      provide: IdempotencyService,
      useFactory: (db: Database, records: IdempotencyRepository) =>
        new IdempotencyService(db, records),
      inject: [DATABASE, IdempotencyRepository],
    },
  ],
  exports: [IdempotencyService],
})
export class IdempotencyModule {}
