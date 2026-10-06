import { canonicalJson } from '../../src/common/canonical-json';
import { FakeClock } from '../../src/common/clock';
import { FakeRandom } from '../../src/common/random';
import { truncateToBytes } from '../../src/common/bytes';

describe('canonicalJson', () => {
  it('ignores object key order at every depth', () => {
    const a = canonicalJson({ a: 1, b: { x: 1, y: 2 } });
    const b = canonicalJson({ b: { y: 2, x: 1 }, a: 1 });
    expect(a).toEqual(b);
  });

  it('treats array order as significant', () => {
    expect(canonicalJson([1, 2, 3])).not.toEqual(canonicalJson([3, 2, 1]));
  });

  it('drops undefined object values but keeps null', () => {
    expect(canonicalJson({ a: undefined, b: null })).toEqual('{"b":null}');
  });

  it('rejects non-finite numbers', () => {
    expect(() => canonicalJson({ a: Number.NaN })).toThrow();
  });
});

describe('FakeClock', () => {
  it('advances deterministically', () => {
    const c = new FakeClock(1000);
    expect(c.nowMs()).toBe(1000);
    c.advance(500);
    expect(c.nowMs()).toBe(1500);
    expect(c.nowUnixSeconds()).toBe(1);
  });
});

describe('FakeRandom', () => {
  it('produces reproducible jitter', () => {
    const r = new FakeRandom([0.5]);
    expect(r.intBelow(250)).toBe(125);
    expect(r.intBelow(250)).toBe(125);
  });
});

describe('truncateToBytes', () => {
  it('caps by byte length without splitting multibyte chars', () => {
    const s = 'a'.repeat(10) + 'é'.repeat(10);
    const out = truncateToBytes(s, 12);
    expect(Buffer.byteLength(out, 'utf8')).toBeLessThanOrEqual(12);
  });
});
