/**
 * NestJS injection tokens for framework-agnostic infrastructure. Domain services
 * are plain classes constructed with these dependencies, so the same services run
 * inside the Nest API, inside the standalone worker process, and inside tests.
 */
export const DATABASE = Symbol('DATABASE');
export const CLOCK = Symbol('CLOCK');
export const RANDOM = Symbol('RANDOM');
export const LOGGER = Symbol('LOGGER');
export const CONFIG = Symbol('CONFIG');
