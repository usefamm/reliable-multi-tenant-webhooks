import {
  CLAIMABLE_STATES,
  COMPLETION_STATES,
  canRedrive,
  isTerminal,
  isValidCompletion,
} from '../../src/domain/delivery-state';
import { DeliveryState } from '../../src/domain/types';

const ALL_STATES = Object.values(DeliveryState);
const SOON = new Date(Date.UTC(2026, 0, 1, 0, 0, 1));

describe('delivery state machine', () => {
  describe('isTerminal', () => {
    it.each([
      [DeliveryState.DELIVERED, true],
      [DeliveryState.DEAD, true],
      [DeliveryState.READY, false],
      [DeliveryState.IN_FLIGHT, false],
      [DeliveryState.RETRY_WAIT, false],
    ])('%s -> %s', (state, expected) => {
      expect(isTerminal(state)).toBe(expected);
    });
  });

  describe('canRedrive', () => {
    it('allows only DEAD', () => {
      expect(ALL_STATES.filter(canRedrive)).toEqual([DeliveryState.DEAD]);
    });

    it('refuses DELIVERED: the receiver already has the effect', () => {
      expect(canRedrive(DeliveryState.DELIVERED)).toBe(false);
    });
  });

  describe('claimable and completion sets', () => {
    it('only non-terminal waiting states are claimable', () => {
      expect([...CLAIMABLE_STATES].sort()).toEqual(
        [DeliveryState.READY, DeliveryState.RETRY_WAIT].sort(),
      );
      expect(CLAIMABLE_STATES.some(isTerminal)).toBe(false);
    });

    it('never completes back to READY or IN_FLIGHT', () => {
      expect(COMPLETION_STATES).not.toContain(DeliveryState.READY);
      expect(COMPLETION_STATES).not.toContain(DeliveryState.IN_FLIGHT);
    });
  });

  describe('isValidCompletion', () => {
    it.each([DeliveryState.DELIVERED, DeliveryState.DEAD])(
      '%s is valid only without a schedule',
      (state) => {
        expect(isValidCompletion(state, null)).toBe(true);
        expect(isValidCompletion(state, SOON)).toBe(false);
      },
    );

    it('RETRY_WAIT is valid only WITH a schedule (a retry that never runs is lost work)', () => {
      expect(isValidCompletion(DeliveryState.RETRY_WAIT, SOON)).toBe(true);
      expect(isValidCompletion(DeliveryState.RETRY_WAIT, null)).toBe(false);
    });

    it.each([DeliveryState.READY, DeliveryState.IN_FLIGHT])(
      'rejects %s as a completion target',
      (state) => {
        expect(isValidCompletion(state, SOON)).toBe(false);
        expect(isValidCompletion(state, null)).toBe(false);
      },
    );
  });
});
