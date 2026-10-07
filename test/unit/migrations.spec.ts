/**
 * The migration runner's file discovery, in isolation from the database.
 *
 * migrateUp() is exercised on every test run (jest's globalSetup builds the
 * schema with it), but the rules that decide WHAT it applies live in
 * readMigrations() and were only implicitly covered: a version collision or a
 * missing directory used to surface as a confusing database error far from the
 * cause. These tests pin the discovery contract and the shipped migration set.
 */
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { readMigrations } from '../../src/db/migrate';

describe('migration discovery', () => {
  const tempDirs: string[] = [];

  function tempDirWith(files: Record<string, string>): string {
    const dir = mkdtempSync(join(tmpdir(), 'migrations-'));
    tempDirs.push(dir);
    for (const [name, content] of Object.entries(files)) {
      writeFileSync(join(dir, name), content, 'utf8');
    }
    return dir;
  }

  afterEach(() => {
    while (tempDirs.length) rmSync(tempDirs.pop()!, { recursive: true, force: true });
  });

  it('applies the shipped migrations in version order', () => {
    const migrations = readMigrations();
    // Pinned on purpose: this is the audit trail behind "4 migrations", and a
    // renamed or added file must be a deliberate change rather than a surprise.
    expect(migrations.map((m) => m.filename)).toEqual([
      '001_core_schema.sql',
      '002_receiver_schema.sql',
      '003_auth_tokens.sql',
      '004_delivery_listing_index.sql',
    ]);
    // Every file is read, not just named: an empty migration would record a
    // version that created nothing.
    expect(migrations.every((m) => m.sql.trim().length > 0)).toBe(true);
  });

  it('ignores files that are not migrations', () => {
    const dir = tempDirWith({
      '001_first.sql': 'SELECT 1;',
      '02_second.sql': 'SELECT 2;',
      '003_notes.md': '# not a migration',
      '004_upper.SQL': 'SELECT 4;',
    });
    expect(readMigrations(dir).map((m) => m.filename)).toEqual(['001_first.sql']);
  });

  it('refuses to run against an unreadable migrations directory', () => {
    // Before this, a deployment that forgot to ship /migrations printed
    // "no pending migrations" and the schema error arrived later, elsewhere.
    const dir = tempDirWith({});
    const absent = join(dir, 'absent');
    expect(() => readMigrations(absent)).toThrow(/is unreadable/);
    expect(() => readMigrations(absent)).toThrow(absent);
  });

  it('names both files when two migrations claim the same version', () => {
    const dir = tempDirWith({
      '001_a.sql': 'SELECT 1;',
      '001_b.sql': 'SELECT 2;',
    });
    expect(() => readMigrations(dir)).toThrow(
      'Duplicate migration version 001: 001_a.sql and 001_b.sql',
    );
  });
});
