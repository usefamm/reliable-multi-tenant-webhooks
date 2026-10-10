/**
 * Byte-bounding helpers. Used to cap captured response bodies and error details
 * so storage never grows unbounded (PDF: response details <= 4 KiB).
 */
export function truncateToBytes(input: string, maxBytes: number): string {
  const buf = Buffer.from(input, 'utf8');
  if (buf.byteLength <= maxBytes) return input;
  // Slice on a byte boundary, then drop a possibly-split trailing multibyte char.
  const sliced = buf.subarray(0, maxBytes);
  let end = sliced.length;
  // Walk back over any partial UTF-8 sequence (continuation bytes are 0b10xxxxxx).
  while (end > 0 && (sliced[end - 1] & 0xc0) === 0x80) end -= 1;
  if (end > 0 && (sliced[end - 1] & 0x80) !== 0) end -= 1;
  return sliced.subarray(0, Math.max(end, 0)).toString('utf8');
}

export function byteLength(input: string | Buffer): number {
  return Buffer.isBuffer(input) ? input.byteLength : Buffer.byteLength(input, 'utf8');
}
