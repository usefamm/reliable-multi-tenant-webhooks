import { Controller, Get, Inject } from '@nestjs/common';
import { Public } from '../modules/auth/auth.guard';
import { serviceUnavailable } from '../common/errors';
import type { Database } from '../db/pool';
import type { Logger } from '../common/logger';
import { DATABASE, LOGGER } from '../common/tokens';

/**
 * Readiness probe. Not a pure liveness check: this service has exactly one
 * dependency and every request it can serve needs it, so a process that answers
 * HTTP while the database is unreachable is not ready and must not pass an
 * orchestrator's healthcheck.
 *
 * The failure message is fixed. Driver errors name host, port and user - those
 * are logged server-side and never returned to a caller.
 */
@Controller()
export class HealthController {
  constructor(
    @Inject(DATABASE) private readonly db: Database,
    @Inject(LOGGER) private readonly logger: Logger,
  ) {}

  @Public()
  @Get('health')
  async health(): Promise<{ status: 'ok'; uptimeSec: number; database: 'ok' }> {
    try {
      await this.db.ping();
    } catch (err) {
      this.logger.error({ err }, 'health probe: database unreachable');
      throw serviceUnavailable('Service is not ready');
    }
    return { status: 'ok', uptimeSec: Math.round(process.uptime()), database: 'ok' };
  }
}
