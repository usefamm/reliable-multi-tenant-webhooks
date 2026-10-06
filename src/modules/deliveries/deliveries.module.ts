import { Module } from '@nestjs/common';
import { DATABASE } from '../../api/tokens';
import { DeliveriesService } from './deliveries.service';
import { DeliveriesController } from './deliveries.controller';
import type { Database } from '../../db/pool';

/** Deliveries feature module: tenant-scoped listing endpoint. */
@Module({
  controllers: [DeliveriesController],
  providers: [
    {
      provide: DeliveriesService,
      useFactory: (db: Database) => new DeliveriesService(db),
      inject: [DATABASE],
    },
  ],
  exports: [DeliveriesService],
})
export class DeliveriesModule {}
