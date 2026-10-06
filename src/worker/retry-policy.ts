import type { Clock } from '../common/clock';
import type { RandomSource } from '../common/random';
import { DeliveryState } from '../domain/types';
import type { AppConfig } from '../config/env';
import type { ClaimedWork, DeliveryAttemptResult, RetryDecision } from './types';

type RetryConfig = Pick<
  AppConfig,
  'RETRY_MAX_ATTEMPTS_PER_CYCLE' | 'RETRY_BACKOFF_BASE_MS' | 'RETRY_JITTER_MAX_MS' | 'RETRY_AFTER_CAP_MS'
>;

/**
 * Maps one attempt's outcome to the next delivery state and (for retries) the
 * next due time. Pure decision logic: no I/O beyond the injected Clock/Random,
 * so it is fully deterministic under fakes and unit-testable in isolation.
 *
 * Rules (PDF section 16):
 *   - SUCCESS (2xx)                      -> DELIVERED
 *   - NON_RETRYABLE (other status / 3xx) -> DEAD
 *   - RETRYABLE (timeout, conn failure, 408, 429, 5xx) and UNKNOWN -> retry
 *       until the per-cycle attempt budget is exhausted, then DEAD.
 *   - Backoff before retry n: base * 2^(n-1) => 1s, 2s, 4s, 8s, plus 0-250ms jitter.
 *   - For 429 with a valid Retry-After: delay = max(normal_backoff, retry_after),
 *     capped at 60s.
 *
 * UNKNOWN (e.g. a lost response after the receiver may have committed) is treated
 * as retryable: we never invent a result, we simply try again and rely on
 * receiver-side deduplication to keep the business effect exactly-once.
 */
export class RetryPolicy {
  constructor(
    private readonly clock: Clock,
    private readonly random: RandomSource,
    private readonly config: RetryConfig,
  ) {}

  decide(work: ClaimedWork, result: DeliveryAttemptResult): RetryDecision {
    if (result.outcome === 'SUCCESS') {
      return { nextState: DeliveryState.DELIVERED, nextAttemptAt: null };
    }
    if (result.outcome === 'NON_RETRYABLE') {
      return { nextState: DeliveryState.DEAD, nextAttemptAt: null };
    }

    // RETRYABLE or UNKNOWN: respect the per-cycle attempt budget.
    if (work.attemptsInCycle >= this.config.RETRY_MAX_ATTEMPTS_PER_CYCLE) {
      return { nextState: DeliveryState.DEAD, nextAttemptAt: null };
    }

    const delayMs = this.computeDelayMs(work.attemptsInCycle, result.retryAfterMs);
    return {
      nextState: DeliveryState.RETRY_WAIT,
      nextAttemptAt: new Date(this.clock.nowMs() + delayMs),
    };
  }

  /** Exponential backoff + jitter, honouring Retry-After and the 60s cap. */
  computeDelayMs(attemptsInCycle: number, retryAfterMs: number | null): number {
    const backoff = this.config.RETRY_BACKOFF_BASE_MS * 2 ** (attemptsInCycle - 1);
    const jitter = this.random.intBelow(this.config.RETRY_JITTER_MAX_MS + 1); // 0..250
    let delay = backoff + jitter;
    if (retryAfterMs !== null && retryAfterMs > 0) {
      delay = Math.max(delay, retryAfterMs);
    }
    return Math.min(delay, this.config.RETRY_AFTER_CAP_MS);
  }
}
