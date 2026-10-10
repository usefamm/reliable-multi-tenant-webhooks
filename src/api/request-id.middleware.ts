import type { NextFunction, Request, Response } from 'express';
import { newRequestId } from '../common/ids';
import { REQUEST_ID_HEADER, REQUEST_ID_KEY } from '../modules/auth/decorators';

/**
 * Express middleware assigning a correlation id to every request (or reusing a
 * caller-provided one) and echoing it in the `x-request-id` response header.
 * Registered before the body parser so even a 413 carries a request id.
 */
export function requestIdHandler(req: Request, res: Response, next: NextFunction): void {
  const incoming = req.header(REQUEST_ID_HEADER);
  const requestId =
    incoming && incoming.length > 0 && incoming.length <= 200 ? incoming : newRequestId();
  req[REQUEST_ID_KEY] = requestId;
  res.setHeader(REQUEST_ID_HEADER, requestId);
  next();
}
