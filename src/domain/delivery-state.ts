import { DeliveryState, TERMINAL_STATES } from './types';

/**
 * The delivery state machine, in one place.
 *
 *   READY ──claim──▶ IN_FLIGHT ──success──────────────▶ DELIVERED   (terminal)
 *     ▲                 │  ▲
 *     │                 │  └──lease expired, re-claimed──┐ (IN_FLIGHT → IN_FLIGHT)
 *   redrive             ├──retryable, budget left──▶ RETRY_WAIT ──claim──▶ IN_FLIGHT
 *     │                 └──non-retryable / exhausted──▶ DEAD
 *     └────────────────────────────────────────────────┘
 *
 * Persistence code (repositories, queue) executes these transitions; it does not
 * decide them. Keeping the rules here means a rule change is one edit, and the
 * rules are unit-testable without a database.
 */

/** States from which a due delivery may be claimed by a worker. */
export const CLAIMABLE_STATES: readonly DeliveryState[] = [
  DeliveryState.READY,
  DeliveryState.RETRY_WAIT,
];

/** States an attempt's completion may move an IN_FLIGHT delivery into. */
export const COMPLETION_STATES: readonly DeliveryState[] = [
  DeliveryState.RETRY_WAIT,
  DeliveryState.DELIVERED,
  DeliveryState.DEAD,
];

export function isTerminal(state: DeliveryState): boolean {
  return TERMINAL_STATES.includes(state);
}

/** Only DEAD (automatic attempts stopped) may be redriven by an operator. */
export function canRedrive(state: DeliveryState): boolean {
  return state === DeliveryState.DEAD;
}

/**
 * Is `(nextState, nextAttemptAt)` a legal outcome for completing an attempt?
 * A terminal state has no schedule; RETRY_WAIT must have one.
 */
export function isValidCompletion(nextState: DeliveryState, nextAttemptAt: Date | null): boolean {
  if (!COMPLETION_STATES.includes(nextState)) return false;
  return isTerminal(nextState) ? nextAttemptAt === null : nextAttemptAt !== null;
}
