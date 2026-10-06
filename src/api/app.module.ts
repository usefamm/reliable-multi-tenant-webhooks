import { MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { APP_FILTER } from '@nestjs/core';
import { CoreModule } from './core.module';
import { AuthModule } from '../modules/auth/auth.module';
import { HealthController } from './health.controller';
import { RequestIdMiddleware } from './request-id.middleware';
import { AllExceptionsFilter } from './exception.filter';

/**
 * Root API module. Wires global infrastructure (CoreModule), authentication
 * (AuthModule installs the global guard), the request-id middleware, and the
 * global exception filter. Feature modules (events, deliveries, operations) are
 * added in later milestones.
 */
@Module({
  imports: [CoreModule, AuthModule],
  controllers: [HealthController],
  providers: [{ provide: APP_FILTER, useClass: AllExceptionsFilter }],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(RequestIdMiddleware).forRoutes('*');
  }
}
