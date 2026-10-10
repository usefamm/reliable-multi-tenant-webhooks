import { FakeClock } from '../../src/common/clock';
import { FakeRandom } from '../../src/common/random';
import { RetryPolicy } from '../../src/domain/retry-policy';
import { DeliveryState } from '../../src/domain/types';
import type { ClaimedWork, DeliveryAttemptResult } from '../../src/domain/attempt';

const CONFIG = {
  RETRY_MAX_ATTEMPTS_PER_CYCLE: 5,
  RETRY_BACKOFF_BASE_MS: 1000,
  RETRY_JITTER_MAX_MS: 250,
  RETRY_AFTER_CAP_MS: 60000,
};

const BASE_MS = Date.UTC(2026, 0, 1, 0, 0, 0);

function work(attemptsInCycle: number): ClaimedWork {
  return {
    deliveryId: 'd',
    eventId: 'e',
    tenantId: 't',
    endpointId: 'ep',
    envelopeBytes: Buffer.from('{}'),
    attemptRowId: 'ar',
    attemptId: 'a',
    attemptNumber: attemptsInCycle,
    cycle: 1,
    attemptsInCycle,
    leaseOwner: 'w',
    leaseGeneration: '1',
  };
}

function result(over: Partial<DeliveryAttemptResult> = {}): DeliveryAttemptResult {
  return {
    outcome: 'RETRYABLE',
    httpStatus: 503,
    errorCode: 'http_5xx',
    responseSnippet: null,
    retryAfterMs: null,
    ...over,
  };
}

describe('RetryPolicy', () => {
  // Zero jitter so delays are exactly the exponential backoff ladder.
  function policy(fractions: number[] = [0]) {
    const clock = new FakeClock(BASE_MS);
    return { clock, p: new RetryPolicy(clock, new FakeRandom(fractions), CONFIG) };
  }

  it('SUCCESS -> DELIVERED with no next attempt', () => {
    const { p } = policy();
    const d = p.decide(work(1), result({ outcome: 'SUCCESS', httpStatus: 200 }));
    expect(d.nextState).toBe(DeliveryState.DELIVERED);
    expect(d.nextAttemptAt).toBeNull();
  });

  it('NON_RETRYABLE -> DEAD', () => {
    const { p } = policy();
    const d = p.decide(work(1), result({ outcome: 'NON_RETRYABLE', httpStatus: 400 }));
    expect(d.nextState).toBe(DeliveryState.DEAD);
    expect(d.nextAttemptAt).toBeNull();
  });

  it('exhausting the per-cycle budget -> DEAD', () => {
    const { p } = policy();
    const d = p.decide(work(5), result({ outcome: 'RETRYABLE' }));
    expect(d.nextState).toBe(DeliveryState.DEAD);
    expect(d.nextAttemptAt).toBeNull();
  });

  it('UNKNOWN is retryable while budget remains', () => {
    const { p } = policy();
    const d = p.decide(work(2), result({ outcome: 'UNKNOWN', httpStatus: null }));
    expect(d.nextState).toBe(DeliveryState.RETRY_WAIT);
    expect(d.nextAttemptAt).not.toBeNull();
  });

  it('applies the 1s/2s/4s/8s exponential backoff ladder', () => {
    const { p } = policy([0]); // zero jitter
    expect(p.computeDelayMs(1, null)).toBe(1000);
    expect(p.computeDelayMs(2, null)).toBe(2000);
    expect(p.computeDelayMs(3, null)).toBe(4000);
    expect(p.computeDelayMs(4, null)).toBe(8000);
  });

  it('adds jitter in [0, 250] ms', () => {
    // intBelow(251) with fraction 0.5 -> floor(125.5) = 125 ms of jitter.
    const { p } = policy([0.5]);
    expect(p.computeDelayMs(1, null)).toBe(1000 + 125);
  });

  it('schedules nextAttemptAt relative to the clock', () => {
    const { p, clock } = policy([0]);
    const d = p.decide(work(1), result({ outcome: 'RETRYABLE' }));
    expect(d.nextAttemptAt!.getTime()).toBe(clock.nowMs() + 1000);
  });

  it('429 Retry-After uses max(backoff, retryAfter)', () => {
    const { p } = policy([0]);
    // backoff for attempt 1 is 1000ms; Retry-After of 5000ms wins.
    expect(p.computeDelayMs(1, 5000)).toBe(5000);
    // backoff for attempt 4 is 8000ms; a small Retry-After loses.
    expect(p.computeDelayMs(4, 2000)).toBe(8000);
  });

  it('caps Retry-After at 60s', () => {
    const { p } = policy([0]);
    expect(p.computeDelayMs(1, 120_000)).toBe(60_000);
  });
});
