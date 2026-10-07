import type { Logger } from '../common/logger';
import { Semaphore } from '../common/semaphore';
import type { DeliveryQueue } from './delivery-queue';
import type { RetryPolicy } from './retry-policy';
import type { DeliveryProcessor, ClaimedWork, DeliveryAttemptResult } from './types';

export interface DeliveryWorkerOptions {
  queue: DeliveryQueue;
  policy: RetryPolicy;
  processor: DeliveryProcessor;
  logger: Logger;
  /** Stable worker identity used as the lease owner and fencing subject. */
  owner: string;
  concurrency: number;
  leaseTtlMs: number;
  pollIntervalMs: number;
  claimBatchSize: number;
  shutdownGraceMs: number;
}

/**
 * The delivery worker process logic.
 *
 * Lifecycle per unit of work (PDF section 11):
 *   1. claim under a bounded lease + pre-allocate the attempt (one short tx)
 *   2. dispatch over HTTP with NO database transaction or lock held
 *   3. complete the attempt + transition state, fenced on the lease (one short tx)
 *
 * Concurrency is bounded by a Semaphore with `concurrency` permits. The worker
 * only claims a delivery when a permit is free, so it never claims work it
 * cannot immediately process and never builds an unbounded in-memory queue - the
 * Postgres `deliveries` table is the queue.
 *
 * Shutdown: stop() flips a flag so the poll loop claims no more work, wakes the
 * loop, and waits up to shutdownGraceMs for in-flight dispatches to finish. Work
 * that does not finish keeps its lease, which expires and is recovered by another
 * worker (at-least-once). We never hold a DB transaction across the HTTP call, so
 * an abandoned dispatch cannot leave locks behind.
 */
export class DeliveryWorker {
  private readonly opts: DeliveryWorkerOptions;
  private readonly semaphore: Semaphore;
  private readonly inFlight = new Set<Promise<void>>();
  private stopping = false;
  private loopPromise: Promise<void> | null = null;
  private wake: (() => void) | null = null;

  constructor(opts: DeliveryWorkerOptions) {
    this.opts = opts;
    this.semaphore = new Semaphore(opts.concurrency);
  }

  /** Number of dispatches currently in flight (for tests/observability). */
  get inFlightCount(): number {
    return this.inFlight.size;
  }

  start(): void {
    if (this.loopPromise) return;
    this.stopping = false;
    this.loopPromise = this.loop();
  }

  async stop(): Promise<void> {
    this.stopping = true;
    this.wake?.();
    if (this.loopPromise) await this.loopPromise;
    await this.drainInFlight();
  }

  private async loop(): Promise<void> {
    const { logger, owner, claimBatchSize, leaseTtlMs, pollIntervalMs } = this.opts;
    logger.info(
      { owner, concurrency: this.opts.concurrency, leaseTtlMs, pollIntervalMs },
      'worker started',
    );

    while (!this.stopping) {
      let claimedAny = false;

      for (let i = 0; i < claimBatchSize; i += 1) {
        if (this.stopping) break;
        // Never claim more than we can process right now.
        if (this.semaphore.available === 0) break;

        await this.semaphore.acquire();
        let work: ClaimedWork | null;
        try {
          work = await this.opts.queue.claimNext(owner, leaseTtlMs);
        } catch (err) {
          this.semaphore.release();
          logger.error({ err: errMsg(err) }, 'claim failed');
          break;
        }
        if (!work) {
          this.semaphore.release();
          break; // no due work; back off for a poll interval
        }

        claimedAny = true;
        this.spawnHandle(work);
      }

      if (this.stopping) break;
      // If we just claimed work and may still have capacity, loop again quickly
      // to fill up to the concurrency bound; otherwise wait a poll interval.
      await this.sleep(claimedAny && this.semaphore.available > 0 ? 0 : pollIntervalMs);
    }

    logger.info({ owner }, 'worker poll loop stopped');
  }

  private spawnHandle(work: ClaimedWork): void {
    const task = this.handle(work).finally(() => {
      this.semaphore.release();
      this.inFlight.delete(task);
    });
    this.inFlight.add(task);
  }

  private async handle(work: ClaimedWork): Promise<void> {
    const { logger, queue, policy } = this.opts;
    const logCtx = {
      deliveryId: work.deliveryId,
      eventId: work.eventId,
      attemptId: work.attemptId,
      attemptNumber: work.attemptNumber,
      leaseGeneration: work.leaseGeneration,
    };

    let result: DeliveryAttemptResult;
    try {
      result = await this.opts.processor(work);
    } catch (err) {
      // A processor bug must not lose the delivery: record UNKNOWN and let the
      // retry policy reschedule. We never fabricate a success/failure outcome.
      logger.error({ ...logCtx, err: errMsg(err) }, 'processor threw; recording UNKNOWN');
      result = {
        outcome: 'UNKNOWN',
        httpStatus: null,
        errorCode: 'processor_error',
        responseSnippet: null,
        retryAfterMs: null,
      };
    }

    const decision = policy.decide(work, result);
    try {
      const { applied } = await queue.completeAttempt({
        deliveryId: work.deliveryId,
        attemptRowId: work.attemptRowId,
        leaseOwner: work.leaseOwner,
        leaseGeneration: work.leaseGeneration,
        outcome: result.outcome,
        httpStatus: result.httpStatus,
        errorCode: result.errorCode,
        responseSnippet: result.responseSnippet,
        nextState: decision.nextState,
        nextAttemptAt: decision.nextAttemptAt,
      });
      if (applied) {
        logger.info({ ...logCtx, state: decision.nextState }, 'delivery transitioned');
      } else {
        // Fenced out: another worker recovered the lease and owns newer state.
        logger.warn({ ...logCtx }, 'stale worker: completion fenced out (lease lost)');
      }
    } catch (err) {
      // Completion failed after dispatch. The attempt row may be UNKNOWN and the
      // lease will expire, allowing recovery. Do not retry completion in-memory.
      logger.error({ ...logCtx, err: errMsg(err) }, 'completion failed; lease will expire');
    }
  }

  private async drainInFlight(): Promise<void> {
    const { logger, owner, shutdownGraceMs } = this.opts;
    if (this.inFlight.size === 0) return;

    logger.info({ owner, count: this.inFlight.size }, 'draining in-flight work');
    const pending = Promise.allSettled([...this.inFlight]);
    const timeout = new Promise<'timeout'>((resolve) =>
      setTimeout(() => resolve('timeout'), shutdownGraceMs),
    );
    const outcome = await Promise.race([pending.then(() => 'drained' as const), timeout]);
    if (outcome === 'timeout') {
      logger.warn(
        { owner, remaining: this.inFlight.size },
        'shutdown grace elapsed; abandoning in-flight work (leases will expire and be recovered)',
      );
    }
  }

  private sleep(ms: number): Promise<void> {
    if (ms <= 0) return new Promise((resolve) => setImmediate(resolve));
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.wake = null;
        resolve();
      }, ms);
      this.wake = () => {
        clearTimeout(timer);
        this.wake = null;
        resolve();
      };
    });
  }
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
