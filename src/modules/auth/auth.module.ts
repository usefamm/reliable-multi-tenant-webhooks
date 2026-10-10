import { Module } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { DATABASE } from '../../common/tokens';
import { AuthService } from './auth.service';
import { AuthGuard } from './auth.guard';
import { AuthTokenRepository } from '../../db/repositories/auth-token.repository';
import type { Database } from '../../db/pool';

/**
 * Auth module. Registers AuthService and installs AuthGuard as a global guard so
 * every route is authenticated by default; public routes opt out explicitly.
 */
@Module({
  providers: [
    {
      provide: AuthService,
      useFactory: (db: Database, tokens: AuthTokenRepository) => new AuthService(db, tokens),
      inject: [DATABASE, AuthTokenRepository],
    },
    { provide: APP_GUARD, useClass: AuthGuard },
  ],
  exports: [AuthService],
})
export class AuthModule {}
