import { Module } from '@nestjs/common';
import { CLOCK, DATABASE } from '../../api/tokens';
import { OperationsController } from './operations.controller';
import { RedriveService } from './redrive.service';
import { StatusController } from './status.controller';
import { StatusService } from './status.service';
import { IdempotencyModule } from '../idempotency/idempotency.module';
import { IdempotencyService } from '../idempotency/idempotency.service';
import type { Database } from '../../db/pool';
import type { Clock } from '../../common/clock';

/**
 * Operator surface: redrive of DEAD deliveries (idempotency record and audit row
 * written in the same transaction as the state transition) and the queue's
 * operational counters.
 */
@Module({
  imports: [IdempotencyModule],
  controllers: [OperationsController, StatusController],
  providers: [
    {
      provide: RedriveService,
      useFactory: (db: Database, clock: Clock, idempotency: IdempotencyService) =>
        new RedriveService(db, clock, idempotency),
      inject: [DATABASE, CLOCK, IdempotencyService],
    },
    {
      provide: StatusService,
      useFactory: (db: Database) => new StatusService(db),
      inject: [DATABASE],
    },
  ],
  exports: [RedriveService, StatusService],
})
export class OperationsModule {}
