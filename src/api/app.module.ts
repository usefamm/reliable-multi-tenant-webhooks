import { Module } from '@nestjs/common';
import { HealthController } from './health.controller';

/**
 * Root API module. Later milestones register feature modules (events, deliveries,
 * operations) and global infrastructure (config, database, auth, error filter).
 */
@Module({
  controllers: [HealthController],
})
export class AppModule {}
