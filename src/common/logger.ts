import pino, { type Logger } from 'pino';
import type { AppConfig } from '../config/env';

/**
 * Structured JSON logger. Every log line can carry correlation identifiers
 * (requestId, eventId, deliveryId, attemptId). Secrets, tokens and full payloads
 * are never logged - callers log bounded identifiers/codes only.
 */
export function createLogger(config: Pick<AppConfig, 'LOG_LEVEL' | 'NODE_ENV'>, name: string): Logger {
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
    // Pretty-print only when a human runs it locally in development.
    transport:
      config.NODE_ENV === 'development' && process.env.LOG_PRETTY === '1'
        ? { target: 'pino/file', options: { destination: 1 } }
        : undefined,
  });
}

export type { Logger };
