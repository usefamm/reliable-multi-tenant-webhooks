/**
 * Deterministic JSON canonicalization used for idempotency comparison.
 *
 * Rules (documented requirement):
 *  - Object key order is IRRELEVANT: keys are sorted lexicographically at every depth.
 *  - Array order is SIGNIFICANT: arrays are preserved as-is.
 *  - `undefined` values are dropped (JSON.stringify semantics).
 *  - Numbers are serialized via JSON.stringify (no reformatting surprises for finite values).
 *  - Non-finite numbers (NaN/Infinity) are rejected: JSON cannot represent them.
 *
 * The output is a stable string; hashing it (SHA-256) yields a comparison fingerprint.
 */
export function canonicalJson(value: unknown): string {
  return stringify(value);
}

function stringify(value: unknown): string {
  if (value === null) return 'null';

  const t = typeof value;

  if (t === 'number') {
    const n = value as number;
    if (!Number.isFinite(n)) {
      throw new TypeError('canonicalJson: non-finite numbers are not representable in JSON');
    }
    return JSON.stringify(n);
  }
  if (t === 'boolean') return value ? 'true' : 'false';
  if (t === 'string') return JSON.stringify(value);
  if (t === 'bigint') {
    throw new TypeError('canonicalJson: bigint is not representable in JSON');
  }
  if (t === 'undefined' || t === 'function' || t === 'symbol') {
    // At the top level these are not valid JSON; inside objects they are omitted by callers.
    throw new TypeError('canonicalJson: value is not JSON-representable');
  }

  if (Array.isArray(value)) {
    const items = value.map((v) => (v === undefined ? 'null' : stringify(v)));
    return `[${items.join(',')}]`;
  }

  if (t === 'object') {
    const record = value as Record<string, unknown>;
    const keys = Object.keys(record)
      .filter((k) => record[k] !== undefined)
      .sort();
    const parts = keys.map((k) => `${JSON.stringify(k)}:${stringify(record[k])}`);
    return `{${parts.join(',')}}`;
  }

  throw new TypeError(`canonicalJson: unsupported type ${t}`);
}
