import { Module } from '@nestjs/common';
import { CLOCK, DATABASE } from '../../api/tokens';
import { EventsService } from './events.service';
import { EventsController } from './events.controller';
import { IdempotencyModule } from '../idempotency/idempotency.module';
import { IdempotencyService } from '../idempotency/idempotency.service';
import type { Database } from '../../db/pool';
import type { Clock } from '../../common/clock';

/**
 * Events feature module: publication and tenant-scoped read model.
 * EventsService is a plain class constructed with the shared Database, Clock and
 * IdempotencyService, so the same service is reusable by workers/tests outside
 * the Nest container.
 */
@Module({
  imports: [IdempotencyModule],
  controllers: [EventsController],
  providers: [
    {
      provide: EventsService,
      useFactory: (db: Database, clock: Clock, idempotency: IdempotencyService) =>
        new EventsService(db, clock, idempotency),
      inject: [DATABASE, CLOCK, IdempotencyService],
    },
  ],
  exports: [EventsService],
})
export class EventsModule {}

