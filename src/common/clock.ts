/**
 * Injectable time source. Production uses the real system clock; tests inject a
 * controllable fake so retry scheduling and lease expiry can be exercised without
 * sleeping in real time.
 */
export interface Clock {
  /** Current time in milliseconds since epoch (UTC). */
  nowMs(): number;
  /** Current time as a Date (UTC). */
  now(): Date;
  /** Current Unix time in whole seconds (used for webhook timestamps). */
  nowUnixSeconds(): number;
}

export class SystemClock implements Clock {
  nowMs(): number {
    return Date.now();
  }
  now(): Date {
    return new Date();
  }
  nowUnixSeconds(): number {
    return Math.floor(Date.now() / 1000);
  }
}

/** Deterministic, manually advanced clock for tests. */
export class FakeClock implements Clock {
  constructor(private ms: number = Date.UTC(2026, 0, 1, 0, 0, 0)) {}
  nowMs(): number {
    return this.ms;
  }
  now(): Date {
    return new Date(this.ms);
  }
  nowUnixSeconds(): number {
    return Math.floor(this.ms / 1000);
  }
  advance(deltaMs: number): void {
    this.ms += deltaMs;
  }
  set(ms: number): void {
    this.ms = ms;
  }
}
