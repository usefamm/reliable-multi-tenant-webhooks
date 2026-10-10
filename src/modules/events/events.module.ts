import { Module } from '@nestjs/common';
import { CLOCK, DATABASE } from '../../common/tokens';
import { EventsService } from './events.service';
import { EventsController } from './events.controller';
import { IdempotencyModule } from '../idempotency/idempotency.module';
import { IdempotencyService } from '../idempotency/idempotency.service';
import { DeliveryRepository } from '../../db/repositories/delivery.repository';
import { EndpointRepository } from '../../db/repositories/endpoint.repository';
import { EventRepository } from '../../db/repositories/event.repository';
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
      useFactory: (
        db: Database,
        clock: Clock,
        idempotency: IdempotencyService,
        endpoints: EndpointRepository,
        events: EventRepository,
        deliveries: DeliveryRepository,
      ) => new EventsService(db, clock, idempotency, endpoints, events, deliveries),
      inject: [
        DATABASE,
        CLOCK,
        IdempotencyService,
        EndpointRepository,
        EventRepository,
        DeliveryRepository,
      ],
    },
  ],
  exports: [EventsService],
})
export class EventsModule {}

