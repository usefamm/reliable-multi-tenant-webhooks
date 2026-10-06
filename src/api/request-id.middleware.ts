import { Injectable, type NestMiddleware } from '@nestjs/common';
import type { NextFunction, Request, Response } from 'express';
import { newRequestId } from '../common/ids';
import { REQUEST_ID_HEADER, REQUEST_ID_KEY } from '../modules/auth/decorators';

/**
 * Assigns a correlation id to every inbound request (or reuses a caller-provided
 * one) and echoes it in the `x-request-id` response header. The id appears in
 * structured logs and in every error body.
 */
@Injectable()
export class RequestIdMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction): void {
    const incoming = req.header(REQUEST_ID_HEADER);
    const requestId =
      incoming && incoming.length > 0 && incoming.length <= 200 ? incoming : newRequestId();
    req[REQUEST_ID_KEY] = requestId;
    res.setHeader(REQUEST_ID_HEADER, requestId);
    next();
  }
}
