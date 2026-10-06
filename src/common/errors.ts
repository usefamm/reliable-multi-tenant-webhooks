/**
 * Domain error model. Every client-facing error carries a stable `code`, a safe
 * `message`, and the HTTP `status`. Stack traces and secrets never leave the process.
 *
 * The API layer serializes these to: { code, message, requestId }.
 */
export type ErrorCode =
  | 'bad_request'
  | 'payload_too_large'
  | 'unauthorized'
  | 'forbidden'
  | 'not_found'
  | 'conflict'
  | 'internal_error';

export class HttpError extends Error {
  readonly code: ErrorCode;
  readonly status: number;
  /** Optional machine-readable detail for logs (never secrets). */
  readonly detail?: Record<string, unknown>;

  constructor(status: number, code: ErrorCode, message: string, detail?: Record<string, unknown>) {
    super(message);
    this.name = 'HttpError';
    this.status = status;
    this.code = code;
    this.detail = detail;
  }
}

export const badRequest = (message: string, detail?: Record<string, unknown>) =>
  new HttpError(400, 'bad_request', message, detail);

export const payloadTooLarge = (message = 'Request body exceeds the maximum allowed size') =>
  new HttpError(413, 'payload_too_large', message);

export const unauthorized = (message = 'Missing or invalid authentication token') =>
  new HttpError(401, 'unauthorized', message);

export const forbidden = (message = 'Caller is not authorized for this operation') =>
  new HttpError(403, 'forbidden', message);

/**
 * Unknown AND other-tenant resources both map to 404 so existence is never leaked
 * across tenant boundaries.
 */
export const notFound = (message = 'Resource not found') => new HttpError(404, 'not_found', message);

export const conflict = (message: string, detail?: Record<string, unknown>) =>
  new HttpError(409, 'conflict', message, detail);
