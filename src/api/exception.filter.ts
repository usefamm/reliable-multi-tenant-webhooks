import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  Inject,
  Injectable,
} from '@nestjs/common';
import type { Request, Response } from 'express';
import { HttpError } from '../common/errors';
import type { Logger } from '../common/logger';
import { LOGGER } from './tokens';
import { REQUEST_ID_KEY } from '../modules/auth/decorators';

interface ErrorBody {
  code: string;
  message: string;
  requestId: string;
}

/**
 * Global exception filter. Produces the required error envelope
 * { code, message, requestId } and never leaks stack traces or secrets.
 *
 * - HttpError (our domain errors) -> its status/code/message.
 * - Nest HttpException (e.g. body-parser 413) -> mapped to our envelope.
 * - Anything else -> 500 internal_error (logged server-side with the requestId,
 *   but the cause is not returned to the client).
 */
@Catch()
@Injectable()
export class AllExceptionsFilter implements ExceptionFilter {
  constructor(@Inject(LOGGER) private readonly logger: Logger) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const res = ctx.getResponse<Response>();
    const req = ctx.getRequest<Request>();
    const requestId = (req[REQUEST_ID_KEY] as string) ?? '';

    let status = 500;
    let body: ErrorBody = { code: 'internal_error', message: 'Internal server error', requestId };

    if (exception instanceof HttpError) {
      status = exception.status;
      body = { code: exception.code, message: exception.message, requestId };
    } else if (exception instanceof HttpException) {
      status = exception.getStatus();
      body = { code: this.mapNestCode(status), message: this.safeMessage(exception, status), requestId };
    } else if (this.isHttpErrorLike(exception)) {
      // Errors raised by pre-route express middleware (notably the body parser):
      // 413 entity.too.large, 400 entity.parse.failed. They carry status/type/expose.
      const mapped = this.mapHttpErrorLike(exception);
      status = mapped.status;
      body = { code: mapped.code, message: mapped.message, requestId };
    } else {
      // Unknown error: log with correlation id, return a generic envelope.
      this.logger.error({ requestId, err: exception }, 'unhandled exception');
    }

    if (status >= 500 && !(exception instanceof HttpError)) {
      this.logger.error({ requestId, status }, 'request failed');
    }

    res.status(status).json(body);
  }

  /** Shape of errors produced by `http-errors` / body-parser. */
  private isHttpErrorLike(err: unknown): err is { status?: number; type?: string; expose?: boolean } {
    return (
      typeof err === 'object' &&
      err !== null &&
      (typeof (err as { status?: unknown }).status === 'number' ||
        typeof (err as { type?: unknown }).type === 'string')
    );
  }

  private mapHttpErrorLike(err: { status?: number; type?: string }): {
    status: number;
    code: string;
    message: string;
  } {
    if (err.status === 413 || err.type === 'entity.too.large') {
      return {
        status: 413,
        code: 'payload_too_large',
        message: 'Request body exceeds the maximum allowed size',
      };
    }
    if (err.type === 'entity.parse.failed') {
      return { status: 400, code: 'bad_request', message: 'Malformed JSON body' };
    }
    const status = err.status ?? 500;
    return { status, code: this.mapNestCode(status), message: 'Request could not be processed' };
  }

  private mapNestCode(status: number): string {
    switch (status) {
      case 400:
        return 'bad_request';
      case 401:
        return 'unauthorized';
      case 403:
        return 'forbidden';
      case 404:
        return 'not_found';
      case 409:
        return 'conflict';
      case 413:
        return 'payload_too_large';
      default:
        return 'internal_error';
    }
  }

  private safeMessage(exception: HttpException, status: number): string {
    if (status === 413) return 'Request body exceeds the maximum allowed size';
    const res = exception.getResponse();
    if (typeof res === 'string') return res;
    if (res && typeof res === 'object' && 'message' in res) {
      const m = (res as { message: unknown }).message;
      if (typeof m === 'string') return m;
      if (Array.isArray(m)) return m.join('; ');
    }
    return exception.message;
  }
}
