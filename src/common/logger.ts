import pino, { type Logger } from 'pino';
import type { AppConfig } from '../config/env';

/**
 * Structured JSON logger. Every log line can carry correlation identifiers
 * (requestId, eventId, deliveryId, attemptId). Secrets, tokens and full payloads
 * are never logged - callers log bounded identifiers/codes only.
 *
 * One format, in every environment: a line is a JSON object on stdout, whether
 * this is a laptop or a log pipeline. A "pretty" switch would change what a human
 * sees without changing what is stored, so the parseable form is the only one.
 */
export function createLogger(config: Pick<AppConfig, 'LOG_LEVEL'>, name: string): Logger {
  return pino({
    name,
    level: config.LOG_LEVEL,
    base: { service: name },
    // Redact anything that might accidentally be passed; defence in depth.
    redact: {
      paths: [
        'secret',
        '*.secret',
        'authorization',
        'token',
        '*.token',
        'x-webhook-signature',
        'payload',
        '*.payload',
      ],
      censor: '[redacted]',
    },
    timestamp: pino.stdTimeFunctions.isoTime,
  });
}

export type { Logger };
