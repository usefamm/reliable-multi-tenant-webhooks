import { Global, Module } from '@nestjs/common';
import { AttemptRepository } from './repositories/attempt.repository';
import { AuthTokenRepository } from './repositories/auth-token.repository';
import { DeliveryRepository } from './repositories/delivery.repository';
import { EndpointRepository } from './repositories/endpoint.repository';
import { EventRepository } from './repositories/event.repository';
import { IdempotencyRepository } from './repositories/idempotency.repository';
import { QueueStatsRepository } from './repositories/queue-stats.repository';
import { RedriveAuditRepository } from './repositories/redrive-audit.repository';

const REPOSITORIES = [
  AttemptRepository,
  AuthTokenRepository,
  DeliveryRepository,
  EndpointRepository,
  EventRepository,
  IdempotencyRepository,
  QueueStatsRepository,
  RedriveAuditRepository,
];

/**
 * The persistence layer's repositories. They hold no state and take their
 * executor (Database or transaction client) per call, so one instance of each is
 * shared by every feature module. Feature modules depend on these classes, never
 * on SQL.
 */
@Global()
@Module({
  providers: REPOSITORIES,
  exports: REPOSITORIES,
})
export class PersistenceModule {}
