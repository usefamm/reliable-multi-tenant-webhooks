import { Module } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { DATABASE } from '../../api/tokens';
import { AuthService } from './auth.service';
import { AuthGuard } from './auth.guard';
import type { Database } from '../../db/pool';

/**
 * Auth module. Registers AuthService and installs AuthGuard as a global guard so
 * every route is authenticated by default; public routes opt out explicitly.
 */
@Module({
  providers: [
    {
      provide: AuthService,
      useFactory: (db: Database) => new AuthService(db),
      inject: [DATABASE],
    },
    { provide: APP_GUARD, useClass: AuthGuard },
  ],
  exports: [AuthService],
})
export class AuthModule {}
