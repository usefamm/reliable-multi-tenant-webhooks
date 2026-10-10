/**
 * Condition-based waiting. Tests wait for a durable fact (a row, a counter, a
 * recorded request) with a deadline, never for "long enough". The deadline only
 * bounds a FAILING run; a passing run returns the moment the condition holds.
 */
export async function waitUntil<T>(
  read: () => Promise<T> | T,
  done: (value: T) => boolean,
  what: string,
  timeoutMs = 3_000,
  intervalMs = 5,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: T | undefined;
  while (Date.now() < deadline) {
    last = await read();
    if (done(last)) return last;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(
    `timed out after ${timeoutMs}ms waiting for ${what}; last seen: ${JSON.stringify(last)}`,
  );
}

/** Yield to the event loop `turns` times (lets already-scheduled I/O callbacks run). */
export async function yieldTurns(turns = 5): Promise<void> {
  for (let i = 0; i < turns; i += 1) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
}

/** A promise a test resolves by hand, to hold something open until it chooses. */
export interface Gate {
  readonly promise: Promise<void>;
  release(): void;
  readonly released: boolean;
}

export function createGate(): Gate {
  let release!: () => void;
  let released = false;
  const promise = new Promise<void>((resolve) => {
    release = () => {
      released = true;
      resolve();
    };
  });
  return {
    promise,
    release,
    get released() {
      return released;
    },
  };
}
