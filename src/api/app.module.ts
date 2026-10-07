import { Module } from '@nestjs/common';
import { APP_FILTER } from '@nestjs/core';
import { CoreModule } from './core.module';
import { AuthModule } from '../modules/auth/auth.module';
import { EventsModule } from '../modules/events/events.module';
import { DeliveriesModule } from '../modules/deliveries/deliveries.module';
import { OperationsModule } from '../modules/operations/operations.module';
import { HealthController } from './health.controller';
import { AllExceptionsFilter } from './exception.filter';

/**
 * Root API module. Wires global infrastructure (CoreModule), authentication
 * (AuthModule installs the global guard), feature modules, and the global
 * exception filter. Request-id and body parsing are express-level middleware
 * applied in http-setup (see configureHttp/finalizeHttp) so they wrap the router.
 */
@Module({
  imports: [CoreModule, AuthModule, EventsModule, DeliveriesModule, OperationsModule],
  controllers: [HealthController],
  providers: [{ provide: APP_FILTER, useClass: AllExceptionsFilter }],
})
export class AppModule {}
