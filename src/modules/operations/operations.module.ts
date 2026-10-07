import { Module } from '@nestjs/common';
import { CLOCK, DATABASE } from '../../api/tokens';
import { OperationsController } from './operations.controller';
import { RedriveService } from './redrive.service';
import { IdempotencyModule } from '../idempotency/idempotency.module';
import { IdempotencyService } from '../idempotency/idempotency.service';
import type { Database } from '../../db/pool';
import type { Clock } from '../../common/clock';

/**
 * Operator operations: redrive of DEAD deliveries (with its idempotency record
 * and audit row written in the same transaction as the state transition).
 */
@Module({
  imports: [IdempotencyModule],
  controllers: [OperationsController],
  providers: [
    {
      provide: RedriveService,
      useFactory: (db: Database, clock: Clock, idempotency: IdempotencyService) =>
        new RedriveService(db, clock, idempotency),
      inject: [DATABASE, CLOCK, IdempotencyService],
    },
  ],
  exports: [RedriveService],
})
export class OperationsModule {}
