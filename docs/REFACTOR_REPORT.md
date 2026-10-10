# Report: architecture and test cleanup

Nothing is committed. The full suite passed at the end: 292 tests in 28 suites, up from 236 in 25.

## Architecture problems and fixes

| Issue | Fix |
|---|---|
| Raw SQL was spread across about 13 files, including services, the worker processor and the health controller. | All SQL now lives in `src/db/repositories/`, with one repository per table group. Services and the processor call repositories, and the health check calls `db.ping()`. |
| Business modules imported the DI tokens from `api/`, and `webhook.client` imported types from `worker/` while the worker imported the client back. | Tokens moved to `common/tokens.ts`. The attempt types and `RetryPolicy` moved to `domain/`. The cycles are gone. |
| The state rules were scattered across SQL strings. | `domain/delivery-state.ts` now holds them. Redrive uses it, and the queue rejects illegal completions such as a `RETRY_WAIT` with no schedule. |
| The worker was tied to the concrete Postgres queue. | A `WorkQueue` interface in `domain/ports.ts` now sits between them, and `PgDeliveryQueue` implements it. A Redis or broker queue could replace it. |
| `webhook.client.ts` mixed several concerns. | The destination policy and the response classification moved into their own files. The old exports still work. |
| The README's project structure listed folders that don't exist. | Rewritten to match the real layout. |

The transaction boundaries stayed in the services. Repositories take the executor (`Database` or a transaction client) as an argument, so the calling service decides where a transaction starts and ends. A transaction helper (`withTransaction`) already existed; only an isolation option was added to it.

## Test problems and fixes

| Issue | Fix |
|---|---|
| Tests used fixed sleeps (120 ms, 400 ms and others), which are flaky on slow machines. | The worker now exposes `pollCount`, and tests use `waitForIdlePolls()` to wait until it has looked for work N times and found none. A shared `waitUntil` helper replaced the hand-written polling loops. |
| The timeout and concurrency tests depended on wall-clock timing. | A manual gate holds the destination's reply until the test releases it. The concurrency tests now prove the bound is exactly 4 instead of "between 2 and 4". |
| A failing test could leave its worker running and hang the cleanup. | Cleanup now stops workers before closing their database pools. |
| Few tests could run without a database. | Added 56 tests: the state machine, the response classification, and a persistence spec covering transaction rollback, isolation level, tenant scoping and illegal completions. |
| There was no CI and no coverage setup. | Added `.github/workflows/ci.yml` (Postgres service, typecheck, lint, coverage) and an `npm run test:coverage` script. The workflow has not been run, since it needs GitHub. |

## Not done
- **Large spec files:** `receiver`, `redrive`, `delivery-loop` and `webhook-client` are not split, so they are still 400–550 lines each.
- **`receiver/handler.ts`:** not split.

## Running the tests
The suite needs a Postgres reachable at `TEST_DATABASE_URL` (default `postgres://yousef@127.0.0.1:5432/webhook_test`). It drops and recreates that database itself.

```bash
npm run typecheck
npm run lint
npm test
```
