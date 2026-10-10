import { Global, Module } from '@nestjs/common';
import { CONFIG, CLOCK, DATABASE, LOGGER, RANDOM } from '../common/tokens';
import { getConfig, type AppConfig } from '../config/env';
import { Database } from '../db/pool';
import { DatabaseService } from '../db/database.service';
import { SystemClock, type Clock } from '../common/clock';
import { SystemRandom, type RandomSource } from '../common/random';
import { createLogger, type Logger } from '../common/logger';

/**
 * Global infrastructure module. Provides the config, database pool, clock,
 * random source and logger as injectable singletons. Tests override CLOCK/RANDOM
 * with deterministic fakes; production uses the system implementations.
 */
@Global()
@Module({
  providers: [
    { provide: CONFIG, useFactory: (): AppConfig => getConfig() },
    { provide: DATABASE, useFactory: (config: AppConfig) => Database.fromConfig(config), inject: [CONFIG] },
    { provide: CLOCK, useFactory: (): Clock => new SystemClock() },
    { provide: RANDOM, useFactory: (): RandomSource => new SystemRandom() },
    {
      provide: LOGGER,
      useFactory: (config: AppConfig): Logger => createLogger(config, 'api'),
      inject: [CONFIG],
    },
    // Binds the pg pool lifecycle to Nest shutdown (closes on app.close()).
    DatabaseService,
  ],
  exports: [CONFIG, DATABASE, CLOCK, RANDOM, LOGGER],
})
export class CoreModule {}
