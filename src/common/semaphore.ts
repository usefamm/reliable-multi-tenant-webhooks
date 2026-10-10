/**
 * A counting semaphore that bounds concurrent work.
 *
 * The worker uses this to guarantee it never holds more than WORKER_CONCURRENCY
 * in-flight outbound HTTP calls, and - critically - never builds an unbounded
 * in-memory queue. The worker only claims a delivery from the database when a
 * permit is free, so the durable Postgres queue (not memory) is the source of
 * truth for pending work.
 *
 * acquire() resolves immediately when a permit is available, otherwise it parks
 * the caller until one is released. release() must always be called (try/finally).
 */
export class Semaphore {
  private permits: number;
  private readonly waiters: Array<() => void> = [];

  constructor(permits: number) {
    if (!Number.isInteger(permits) || permits < 1) {
      throw new Error('Semaphore permits must be a positive integer');
    }
    this.permits = permits;
  }

  /** Number of permits currently free. */
  get available(): number {
    return this.permits;
  }

  async acquire(): Promise<void> {
    if (this.permits > 0) {
      this.permits -= 1;
      return;
    }
    await new Promise<void>((resolve) => this.waiters.push(resolve));
  }

  release(): void {
    const next = this.waiters.shift();
    if (next) {
      // Hand the permit directly to the next waiter without inflating the count.
      next();
      return;
    }
    this.permits += 1;
  }
}
