import { loadEnvFile } from '../config/load-env';
import { migrateUp } from './migrate';
import { sha256Hex } from '../common/hash';
import { Pool } from 'pg';

loadEnvFile();

/**
 * Deterministic seed data (PDF: "Seed data must be deterministic").
 *
 * Fixed UUIDs and fixed dev secrets/tokens make every environment reproducible.
 * These are DEVELOPMENT fixtures only - the values are public in .env.example and
 * must never be used in production. Only the SHA-256 hash of each token is stored.
 *
 * Endpoint URLs are trusted deployment configuration pointing at the local mock
 * receiver; they are never caller-supplied.
 */

const TENANT_A_ID = 'aaaaaaaa-0000-4000-8000-00000000000a';
const TENANT_B_ID = 'bbbbbbbb-0000-4000-8000-00000000000b';

interface EndpointSeed {
  id: string;
  tenantId: string;
  name: string;
  secret: string;
}

const ENDPOINTS: EndpointSeed[] = [
  {
    id: 'eeeeeeee-0000-4000-8000-0000000000a1',
    tenantId: TENANT_A_ID,
    name: 'tenant-a-endpoint-1',
    secret: 'dev-secret-a1',
  },
  {
    id: 'eeeeeeee-0000-4000-8000-0000000000a2',
    tenantId: TENANT_A_ID,
    name: 'tenant-a-endpoint-2',
    secret: 'dev-secret-a2',
  },
  {
    id: 'eeeeeeee-0000-4000-8000-0000000000b1',
    tenantId: TENANT_B_ID,
    name: 'tenant-b-endpoint-1',
    secret: 'dev-secret-b1',
  },
  {
    id: 'eeeeeeee-0000-4000-8000-0000000000b2',
    tenantId: TENANT_B_ID,
    name: 'tenant-b-endpoint-2',
    secret: 'dev-secret-b2',
  },
];

function receiverBaseUrl(): string {
  return (process.env.RECEIVER_BASE_URL ?? 'http://127.0.0.1:4000').replace(/\/$/, '');
}

function endpointUrl(endpointId: string): string {
  return `${receiverBaseUrl()}/hook/${endpointId}`;
}

/** Idempotent upserts: running the seed twice is a no-op, never a duplicate. */
export async function seed(connectionString: string): Promise<void> {
  const pool = new Pool({ connectionString });
  try {
    await pool.query('BEGIN');

    await pool.query(
      `INSERT INTO tenants (id, name) VALUES ($1, $2)
       ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`,
      [TENANT_A_ID, 'Tenant A'],
    );
    await pool.query(
      `INSERT INTO tenants (id, name) VALUES ($1, $2)
       ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`,
      [TENANT_B_ID, 'Tenant B'],
    );

    for (const ep of ENDPOINTS) {
      await pool.query(
        `INSERT INTO endpoints (id, tenant_id, name, url, secret)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (id) DO UPDATE
           SET tenant_id = EXCLUDED.tenant_id,
               name = EXCLUDED.name,
               url = EXCLUDED.url,
               secret = EXCLUDED.secret`,
        [ep.id, ep.tenantId, ep.name, endpointUrl(ep.id), ep.secret],
      );
    }

    // Token -> identity mapping. Store only the hash of each dev token.
    const tokenA = process.env.TENANT_A_TOKEN ?? 'dev-token-tenant-a';
    const tokenB = process.env.TENANT_B_TOKEN ?? 'dev-token-tenant-b';
    const tokenOp = process.env.OPERATOR_TOKEN ?? 'dev-token-operator';

    const tokens: Array<[string, string | null, string, string]> = [
      [sha256Hex(tokenA), TENANT_A_ID, 'tenant', 'tenant-a'],
      [sha256Hex(tokenB), TENANT_B_ID, 'tenant', 'tenant-b'],
      [sha256Hex(tokenOp), null, 'operator', 'operator'],
    ];
    for (const [hash, tenantId, role, label] of tokens) {
      await pool.query(
        `INSERT INTO auth_tokens (token_hash, tenant_id, role, label)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (token_hash) DO UPDATE
           SET tenant_id = EXCLUDED.tenant_id,
               role = EXCLUDED.role,
               label = EXCLUDED.label`,
        [hash, tenantId, role, label],
      );
    }

    await pool.query('COMMIT');
  } catch (err) {
    await pool.query('ROLLBACK');
    throw err;
  } finally {
    await pool.end();
  }
}

async function main(): Promise<void> {
  const url = process.env.DATABASE_URL;
  if (!url) {
    // eslint-disable-next-line no-console
    console.error('DATABASE_URL is required');
    process.exit(1);
  }
  // Ensure schema exists before seeding (reproducible from an empty database).
  await migrateUp(url);
  await seed(url);
  // eslint-disable-next-line no-console
  console.log('seed complete: 2 tenants, 4 endpoints, 3 tokens (hashed)');
}

if (require.main === module) {
  main().catch((err) => {
    // eslint-disable-next-line no-console
    console.error(err);
    process.exit(1);
  });
}
