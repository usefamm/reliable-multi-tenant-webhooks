import { config as loadDotenv } from 'dotenv';
import { existsSync } from 'node:fs';
import { resolve } from 'node:path';

/**
 * Load `.env` from the project root into process.env if it exists.
 * Called at the very top of every entrypoint (api, worker, receiver, migrate,
 * seed) so local runs work without exporting variables by hand. In Docker the
 * environment is provided by compose and no .env file is present.
 */
export function loadEnvFile(): void {
  const path = resolve(__dirname, '../../.env');
  if (existsSync(path)) {
    loadDotenv({ path });
  }
}
