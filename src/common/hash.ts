import { createHash } from 'node:crypto';

/** SHA-256 hex digest of a UTF-8 string or raw bytes. */
export function sha256Hex(input: string | Buffer): string {
  return createHash('sha256').update(input).digest('hex');
}
