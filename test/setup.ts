/**
 * Per-test-file setup. Runs before each test file's suite.
 * Keeps unhandled rejections loud so flaky async code cannot pass silently.
 */
process.on('unhandledRejection', (reason) => {
  // eslint-disable-next-line no-console
  console.error('Unhandled rejection during tests:', reason);
});

export {};
