/**
 * Injectable randomness source. Production uses crypto-grade or Math.random;
 * tests inject a deterministic generator so jitter is reproducible.
 */
export interface RandomSource {
  /** Integer in [0, maxMs). Returns 0 when maxMs <= 0. */
  intBelow(maxMs: number): number;
}

export class SystemRandom implements RandomSource {
  intBelow(maxMs: number): number {
    if (maxMs <= 0) return 0;
    return Math.floor(Math.random() * maxMs);
  }
}

/** Deterministic random for tests: cycles through a fixed sequence of fractions. */
export class FakeRandom implements RandomSource {
  private i = 0;
  constructor(private readonly fractions: number[] = [0]) {}
  intBelow(maxMs: number): number {
    if (maxMs <= 0) return 0;
    const f = this.fractions[this.i % this.fractions.length];
    this.i += 1;
    return Math.floor(f * maxMs);
  }
}
