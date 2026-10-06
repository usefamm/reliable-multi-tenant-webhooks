import { readdirSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { Pool } from 'pg';

/**
 * Minimal, transparent SQL migration runner.
 *
 * - Migration files live in /migrations as `NNN_name.sql`, applied in lexical order.
 * - Applied versions are tracked in `schema_migrations`.
 * - Each file is applied inside its own transaction (all-or-nothing).
 * - `down` drops every known table, returning the DB to empty (used by tests to
 *   prove migrations are reproducible from scratch).
 *
 * We intentionally avoid an ORM's implicit schema generation (no synchronize=true):
 * the schema is explicit SQL, reviewable and reproducible.
 */
const MIGRATIONS_DIR = resolve(__dirname, '../../migrations');

const MIGRATION_FILE_RE = /^(\d{3})_[a-z0-9_]+\.sql$/;

interface MigrationFile {
  version: string;
  filename: string;
  sql: string;
}

function readMigrations(): MigrationFile[] {
  let entries: string[];
  try {
    entries = readdirSync(MIGRATIONS_DIR);
  } catch {
    return [];
  }
  return entries
    .map((filename) => {
      const m = MIGRATION_FILE_RE.exec(filename);
      if (!m) return null;
      const sql = readFileSync(join(MIGRATIONS_DIR, filename), 'utf8');
      return { version: m[1], filename, sql };
    })
    .filter((x): x is MigrationFile => x !== null)
    .sort((a, b) => a.version.localeCompare(b.version));
}

async function ensureMigrationsTable(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version     text PRIMARY KEY,
      filename    text NOT NULL,
      applied_at  timestamptz NOT NULL DEFAULT now()
    );
  `);
}

export async function migrateUp(connectionString: string): Promise<string[]> {
  const pool = new Pool({ connectionString });
  const applied: string[] = [];
  try {
    await ensureMigrationsTable(pool);
    const { rows } = await pool.query<{ version: string }>(
      'SELECT version FROM schema_migrations',
    );
    const done = new Set(rows.map((r) => r.version));

    for (const mig of readMigrations()) {
      if (done.has(mig.version)) continue;
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query(mig.sql);
        await client.query(
          'INSERT INTO schema_migrations (version, filename) VALUES ($1, $2)',
          [mig.version, mig.filename],
        );
        await client.query('COMMIT');
        applied.push(mig.filename);
      } catch (err) {
        await client.query('ROLLBACK');
        throw new Error(`Migration ${mig.filename} failed: ${(err as Error).message}`);
      } finally {
        client.release();
      }
    }
    return applied;
  } finally {
    await pool.end();
  }
}

/** Drop all application tables (reverse order) - returns the DB to empty. */
export async function migrateDown(connectionString: string): Promise<void> {
  const pool = new Pool({ connectionString });
  try {
    await pool.query('DROP TABLE IF EXISTS schema_migrations CASCADE');
    await pool.query(`
      DROP TABLE IF EXISTS
        receiver_requests,
        receiver_effects,
        receiver_modes,
        redrive_audit,
        idempotency_records,
        delivery_attempts,
        deliveries,
        events,
        endpoints,
        tenants,
        auth_tokens
      CASCADE;
    `);
    // Custom enum types are not dropped by DROP TABLE; remove them so `up`
    // is reproducible from an empty database.
    await pool.query('DROP TYPE IF EXISTS attempt_outcome');
    await pool.query('DROP TYPE IF EXISTS delivery_state');
  } finally {
    await pool.end();
  }
}

/** CLI entrypoint: `ts-node src/db/migrate.ts up|down` */
async function main(): Promise<void> {
  const cmd = process.argv[2] ?? 'up';
  const url = process.env.DATABASE_URL;
  if (!url) {
    // eslint-disable-next-line no-console
    console.error('DATABASE_URL is required');
    process.exit(1);
  }
  if (cmd === 'up') {
    const applied = await migrateUp(url);
    // eslint-disable-next-line no-console
    console.log(applied.length ? `applied: ${applied.join(', ')}` : 'no pending migrations');
  } else if (cmd === 'down') {
    await migrateDown(url);
    // eslint-disable-next-line no-console
    console.log('dropped all application tables');
  } else {
    // eslint-disable-next-line no-console
    console.error(`unknown command: ${cmd} (use up|down)`);
    process.exit(1);
  }
}

if (require.main === module) {
  main().catch((err) => {
    // eslint-disable-next-line no-console
    console.error(err);
    process.exit(1);
  });
}
