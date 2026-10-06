import { createParamDecorator, ExecutionContext, SetMetadata, type CustomDecorator } from '@nestjs/common';
import type { Request } from 'express';
import type { Principal } from './principal';

/** Header used to echo the request id back to the caller and into error bodies. */
export const REQUEST_ID_HEADER = 'x-request-id';

/** Request-scoped keys we attach to the express Request object. */
export const PRINCIPAL_KEY = 'principal';
export const REQUEST_ID_KEY = 'requestId';

/** Augment express Request with our attached fields. */
declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace Express {
    interface Request {
      principal?: Principal;
      requestId?: string;
    }
  }
}

/** Metadata key marking a route as operator-only. */
export const OPERATOR_ONLY_KEY = 'operatorOnly';

/** Mark a route as requiring the operator token (else 403 for tenant tokens). */
export const OperatorOnly = (): CustomDecorator<string> => SetMetadata(OPERATOR_ONLY_KEY, true);

/** Inject the authenticated Principal into a handler. */
export const CurrentPrincipal = createParamDecorator(
  (_data: unknown, ctx: ExecutionContext): Principal => {
    const req = ctx.switchToHttp().getRequest<Request>();
    // The guard guarantees a principal is attached on protected routes.
    return req.principal as Principal;
  },
);

/** Inject the request id into a handler. */
export const CurrentRequestId = createParamDecorator(
  (_data: unknown, ctx: ExecutionContext): string => {
    const req = ctx.switchToHttp().getRequest<Request>();
    return req.requestId ?? '';
  },
);
