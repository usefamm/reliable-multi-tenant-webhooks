import { Module } from '@nestjs/common';
import { DATABASE } from '../../api/tokens';
import { IdempotencyService } from './idempotency.service';
import type { Database } from '../../db/pool';

/**
 * Idempotency feature module. Shared by event publication (M5) and operator
 * redrive (M12) so both operations get the same database-backed dedup semantics.
 */
@Module({
  providers: [
    {
      provide: IdempotencyService,
      useFactory: (db: Database) => new IdempotencyService(db),
      inject: [DATABASE],
    },
  ],
  exports: [IdempotencyService],
})
export class IdempotencyModule {}
