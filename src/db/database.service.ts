import { Inject, Injectable, type OnApplicationShutdown } from '@nestjs/common';
import { DATABASE } from '../common/tokens';
import type { Database } from './pool';

/**
 * Lifecycle-aware wrapper so Nest closes the pg pool on application shutdown.
 * Inject `Database` directly in domain services; this exists only to bind the
 * pool's lifecycle to the Nest app (and to supertest's app.close()).
 */
@Injectable()
export class DatabaseService implements OnApplicationShutdown {
  constructor(@Inject(DATABASE) readonly db: Database) {}

  async onApplicationShutdown(): Promise<void> {
    await this.db.close();
  }
}
