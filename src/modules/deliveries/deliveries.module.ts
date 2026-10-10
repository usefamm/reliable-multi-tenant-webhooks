import { Module } from '@nestjs/common';
import { DATABASE } from '../../common/tokens';
import { DeliveriesService } from './deliveries.service';
import { DeliveriesController } from './deliveries.controller';
import { DeliveryRepository } from '../../db/repositories/delivery.repository';
import type { Database } from '../../db/pool';

/** Deliveries feature module: tenant-scoped listing endpoint. */
@Module({
  controllers: [DeliveriesController],
  providers: [
    {
      provide: DeliveriesService,
      useFactory: (db: Database, deliveries: DeliveryRepository) =>
        new DeliveriesService(db, deliveries),
      inject: [DATABASE, DeliveryRepository],
    },
  ],
  exports: [DeliveriesService],
})
export class DeliveriesModule {}
