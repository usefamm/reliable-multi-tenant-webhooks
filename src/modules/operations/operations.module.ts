import { Module } from '@nestjs/common';
import { CLOCK, DATABASE } from '../../common/tokens';
import { OperationsController } from './operations.controller';
import { RedriveService } from './redrive.service';
import { StatusController } from './status.controller';
import { StatusService } from './status.service';
import { IdempotencyModule } from '../idempotency/idempotency.module';
import { IdempotencyService } from '../idempotency/idempotency.service';
import { DeliveryRepository } from '../../db/repositories/delivery.repository';
import { QueueStatsRepository } from '../../db/repositories/queue-stats.repository';
import { RedriveAuditRepository } from '../../db/repositories/redrive-audit.repository';
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
      useFactory: (
        db: Database,
        clock: Clock,
        idempotency: IdempotencyService,
        deliveries: DeliveryRepository,
        audits: RedriveAuditRepository,
      ) => new RedriveService(db, clock, idempotency, deliveries, audits),
      inject: [DATABASE, CLOCK, IdempotencyService, DeliveryRepository, RedriveAuditRepository],
    },
    {
      provide: StatusService,
      useFactory: (db: Database, stats: QueueStatsRepository) => new StatusService(db, stats),
      inject: [DATABASE, QueueStatsRepository],
    },
  ],
  exports: [RedriveService, StatusService],
})
export class OperationsModule {}
