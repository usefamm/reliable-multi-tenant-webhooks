import {
  CanActivate,
  ExecutionContext,
  Inject,
  Injectable,
  SetMetadata,
  type CustomDecorator,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import type { Request } from 'express';
import { AuthService } from './auth.service';
import { forbidden, unauthorized } from '../../common/errors';
import { isOperator } from './principal';
import { OPERATOR_ONLY_KEY, PRINCIPAL_KEY } from './decorators';

/** Metadata key marking a route as public (no authentication required). */
export const PUBLIC_KEY = 'publicRoute';

/** Mark a route as public (e.g. health). No token required. */
export const Public = (): CustomDecorator<string> => SetMetadata(PUBLIC_KEY, true);

/**
 * Global authentication guard.
 *  - Public routes: allowed without a token.
 *  - Missing/invalid token -> 401.
 *  - @OperatorOnly() route + tenant token -> 403 (PDF: 403 for non-operator redrive).
 *  - Tenant-scoped route + operator token -> 403 (operators are not tenants).
 *  - On success attaches the Principal to the request for downstream handlers.
 */
@Injectable()
export class AuthGuard implements CanActivate {
  constructor(
    @Inject(AuthService) private readonly auth: AuthService,
    private readonly reflector: Reflector,
  ) {}

  async canActivate(ctx: ExecutionContext): Promise<boolean> {
    const req = ctx.switchToHttp().getRequest<Request>();
    const handler = ctx.getHandler();
    const cls = ctx.getClass();

    const isPublic =
      this.reflector.getAllAndOverride<boolean | undefined>(PUBLIC_KEY, [handler, cls]) ?? false;
    if (isPublic) {
      return true;
    }

    const token = this.auth.extractBearer(req.header('authorization'));
    const principal = await this.auth.resolve(token);
    if (!principal) {
      throw unauthorized();
    }

    const operatorOnly =
      this.reflector.getAllAndOverride<boolean | undefined>(OPERATOR_ONLY_KEY, [handler, cls]) ??
      false;

    if (operatorOnly && !isOperator(principal)) {
      throw forbidden('This operation requires an operator token');
    }
    if (!operatorOnly && isOperator(principal)) {
      throw forbidden('Operator token cannot access tenant-scoped resources');
    }

    req[PRINCIPAL_KEY] = principal;
    return true;
  }
}
