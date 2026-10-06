import { Module } from '@nestjs/common';
import { CLOCK, DATABASE } from '../../api/tokens';
import { EventsService } from './events.service';
import { EventsController } from './events.controller';
import type { Database } from '../../db/pool';
import type { Clock } from '../../common/clock';

/**
 * Events feature module: publication and tenant-scoped read model.
 * EventsService is a plain class constructed with the shared Database and Clock,
 * so the same service is reusable by workers/tests outside the Nest container.
 */
@Module({
  controllers: [EventsController],
  providers: [
    {
      provide: EventsService,
      useFactory: (db: Database, clock: Clock) => new EventsService(db, clock),
      inject: [DATABASE, CLOCK],
    },
  ],
  exports: [EventsService],
})
export class EventsModule {}
