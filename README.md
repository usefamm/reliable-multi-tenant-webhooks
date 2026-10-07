# Reliable Multi-Tenant Webhook Delivery

A webhook delivery service that accepts authenticated, tenant-scoped events and delivers them to
customer-configured HTTP destinations over **at-least-once delivery with idempotent receiver-side
business effects**.

The service is built around one refusal: *there is no such thing as exactly-once webhook delivery.*
The network can lose a response after the receiver already applied the effect, and a delivery worker
can be killed between the HTTP call and the database write. Any system that promises exactly-once
delivery is either lying or holding a transaction open across the network. So this system promises
something weaker on the wire and stronger in the database: every event is durably accepted, every
attempt is honestly recorded, duplicates may reach the receiver, and **the receiver's business
effect happens exactly once** because deduplication is a database constraint rather than a check.

Everything below is implemented, tested and runnable. Test results quoted are actual output
(reproduced by `npm test`), never aspirational.

---

## Table of contents

1. [The guarantee, stated precisely](#1-the-guarantee-stated-precisely)
2. [Architecture](#2-architecture)
3. [Quick start with Docker Compose](#3-quick-start-with-docker-compose)
4. [Quick start without Docker](#4-quick-start-without-docker)
5. [Configuration reference](#5-configuration-reference)
6. [Deterministic seed data](#6-deterministic-seed-data)
7. [API reference](#7-api-reference)
8. [Worked example: publish to delivered](#8-worked-example-publish-to-delivered)
9. [Worked example: failure, retry, redrive](#9-worked-example-failure-retry-redrive)
10. [Data model](#10-data-model)
11. [Delivery state machine](#11-delivery-state-machine)
12. [Publication: atomicity and idempotency](#12-publication-atomicity-and-idempotency)
13. [The queue: claiming work with leases](#13-the-queue-claiming-work-with-leases)
14. [Bounded concurrency across two workers](#14-bounded-concurrency-across-two-workers)
15. [Fencing: the stale worker problem](#15-fencing-the-stale-worker-problem)
16. [Retry policy: backoff, jitter, Retry-After](#16-retry-policy-backoff-jitter-retry-after)
17. [Cycles, budget and operator redrive](#17-cycles-budget-and-operator-redrive)
18. [The webhook signature contract](#18-the-webhook-signature-contract)
19. [Envelope stability and duplicate semantics](#19-envelope-stability-and-duplicate-semantics)
20. [Outcome honesty: what UNKNOWN means](#20-outcome-honesty-what-unknown-means)
21. [Crash recovery walk-through](#21-crash-recovery-walk-through)
22. [Mock receiver and its eight failure modes](#22-mock-receiver-and-its-eight-failure-modes)
23. [Receiver-side deduplication](#23-receiver-side-deduplication)
24. [Tenant isolation model](#24-tenant-isolation-model)
25. [Outbound safety and SSRF](#25-outbound-safety-and-ssrf)
26. [Error model](#26-error-model)
27. [Observability: logs and counters](#27-observability-logs-and-counters)
28. [Testing strategy: determinism without mocks](#28-testing-strategy-determinism-without-mocks)
29. [Test results (actual)](#29-test-results-actual)
30. [Acceptance evidence map (PDF tests 1-9)](#30-acceptance-evidence-map-pdf-tests-1-9)
31. [Design decisions and trade-offs](#31-design-decisions-and-trade-offs)
32. [Limitations](#32-limitations)
33. [Production considerations](#33-production-considerations)
34. [Discussion topics the brief asks about](#34-discussion-topics-the-brief-asks-about)
35. [Repository layout](#35-repository-layout)
36. [Postman collection](#36-postman-collection)
37. [Time spent and disclosure](#37-time-spent-and-disclosure)

---

## 1. The guarantee, stated precisely

**Claimed:**

| Property | How it is achieved | Where it is proven |
|---|---|---|
| An accepted event is never silently lost | `POST /events` commits event + delivery + idempotency claim in one transaction before returning 202 | `test/integration/events.spec.ts`, `test/acceptance/t6` (crash right after that commit) |
| Retries never reset the schedule or the budget | `attempt_count`, `cycle`, `attempts_in_cycle` and `next_attempt_at` live on the row, not in a process | `test/integration/delivery-loop.spec.ts`, `test/acceptance/t5`, `t6`, `t8` |
| The receiver's business effect happens exactly once per (endpoint, event) | `UNIQUE (endpoint_id, event_id)`; the effect and its dedup record are the same statement | `test/integration/receiver.spec.ts`, `test/acceptance/t4` (across a real receiver restart) |
| A stale worker cannot overwrite newer state | Completion is fenced on `(lease_owner, lease_generation)` | `test/integration/worker-fencing.spec.ts`, `test/acceptance/t7` |
| Unknown outcomes stay unknown | Timeouts and lost responses are recorded as `RETRYABLE`/`UNKNOWN` with the real status (or `NULL`), never as a fabricated result | `test/integration/webhook-client.spec.ts`, `test/acceptance/t4`, `t9` |
| One tenant cannot read, name or affect another tenant's resources | Every query pins `tenant_id` to the authenticated principal; unknown and foreign both answer 404 | `test/integration/auth.spec.ts`, `test/acceptance/t2` |
| A caller cannot steer where a webhook goes | Destinations are read from the `endpoints` table only; the request body is a closed schema | `test/acceptance/t2` |
| Per-worker outbound concurrency is genuinely bounded | A counting semaphore gates claims; measured at the destination socket | `test/acceptance/t9` |

**Explicitly not claimed:** exactly-once HTTP delivery. Duplicate requests to a receiver are a normal,
expected event in this system's lifetime, and the design offloads the correctness requirement to the
receiver's deduplication - which is the only place in the topology that can actually enforce it.

---

## 2. Architecture

Five processes, one database. The database is the queue, the lock manager, the idempotency store and
the audit trail - there is no second copy of the truth to keep in sync.

```mermaid
flowchart LR
    subgraph Client["Tenant caller"]
        C[Event producer<br/>Bearer token + Idempotency-Key]
    end

    subgraph Edge["API process (NestJS)"]
        A1[AuthGuard<br/>token -> Principal]
        A2[POST /events<br/>atomic publish]
        A3[GET /events/:id<br/>GET /deliveries]
        A4[POST /ops/.../redrive<br/>GET /ops/status]
    end

    subgraph Workers["Two independent worker processes"]
        W1["worker-a<br/>semaphore(4)"]
        W2["worker-b<br/>semaphore(4)"]
    end

    subgraph DB["PostgreSQL - the only durable store"]
        T1[(events)]
        T2[(deliveries<br/>state + lease + fencing)]
        T3[(delivery_attempts)]
        T4[(idempotency_records)]
        T5[(redrive_audit)]
        T6[(receiver_effects)]
    end

    subgraph Receiver["Mock receiver (customer stand-in)"]
        R1[HMAC + freshness verify]
        R2[durable dedup<br/>UNIQUE endpoint+event]
        R3["/__control/*<br/>test-only failure modes"]
    end

    C --> A1 --> A2 --> T1 & T2 & T4
    C --> A3
    A4 --> T2 & T4 & T5
    W1 -->|claim/complete, SKIP LOCKED| T2
    W2 -->|claim/complete, SKIP LOCKED| T2
    W1 & W2 -->|pre-allocate| T3
    W1 & W2 -.->|HTTPS POST, signed, never a redirect| R1
    R1 --> R2 --> T6
```

The worker has exactly two database operations (`claimNext`, `completeAttempt`), each a short
transaction, and **no transaction ever spans the HTTP call**. That single rule is what makes crash
recovery, fencing and two-process safety tractable.

---

## 3. Quick start with Docker Compose

```bash
git clone <your-fork-url> reliable-webhook-delivery
cd reliable-webhook-delivery
cp .env.example .env          # optional: compose already injects the dev fixtures
docker compose up --build
```

Compose brings up: `postgres` (14.13) → a one-shot `bootstrap` container that applies every migration
from an empty database and writes the deterministic seed → then `api` (port 3000), **two** worker
processes (`worker-a`, `worker-b`) and the mock `receiver` (port 4000).

Smoke test in another terminal:

```bash
curl -s http://127.0.0.1:3000/health
curl -s -X POST http://127.0.0.1:3000/events \
  -H 'Authorization: Bearer dev-token-tenant-a' \
  -H 'Idempotency-Key: demo-1' \
  -H 'Content-Type: application/json' \
  -d '{"endpointId":"eeeeeeee-0000-4000-8000-0000000000a1","eventType":"order.created","payload":{"orderId":"ord_demo"}}'
```

Then read it back and count receiver effects:

```bash
curl -s http://127.0.0.1:3000/events/<eventId> -H 'Authorization: Bearer dev-token-tenant-a'
curl -s 'http://127.0.0.1:4000/__control/effects?endpointId=eeeeeeee-0000-4000-8000-0000000000a1'
```

Tear down (including the volume): `docker compose down -v`.

**Verification status, stated honestly.** The stack was brought up and exercised against a live daemon
(Compose v2.12.1, Docker 20.10.20). What was observed:

```
docker compose ps     api running (healthy)   postgres running (healthy)
                      worker-a / worker-b / receiver running
                      bootstrap exited (0)            # one-shot migrate + seed
curl :3000/health     {"status":"ok","uptimeSec":35,"database":"ok"}
POST /events          202 {"eventId":"a1e2056d-…","status":"READY",…}
GET  /events/:id      +1s -> state DELIVERED, totalAttempts 1, lastHttpStatus 200
receiver effects      1 effect for that event
GET  /ops/status      {"ready":0,"inFlight":0,"retryWait":0,"delivered":1,"dead":0,"expiredLeases":0}
```

The crash story was then run for real rather than only in jest: with `worker-b` stopped, `worker-a` was
**paused** mid-dispatch (`docker compose pause worker-a`), leaving the delivery `IN_FLIGHT` with a lease
nobody would ever complete. `worker-b` was started, the 30 s lease lapsed, and it recovered the row -
`state DELIVERED, totalAttempts 2, cycle 1` with attempt rows

```
 attempt_number | lease_owner | lease_generation | outcome   | error_code
              1 | worker-a    |                1 | RETRYABLE | timeout
              2 | worker-b    |                2 | SUCCESS   |
```

and `worker-a` was then unpaused: its late write was rejected with the log line
`"stale worker: completion fenced out (lease lost)"`, the delivery stayed `DELIVERED`, and the receiver
still held **one** effect for **two** signed requests with two distinct attempt ids. Tear-down
(`docker compose down -v`) removed containers, network and volume cleanly.

Running it is also what found a real defect: only `bootstrap` declared a `build:` key, so the four
image-only services made Compose attempt a registry pull of the local-only tag and abort with
`pull access denied`. Every application service now declares the same build context and tag
(`x-build` anchor). The one thing that could not be exercised is `up --build` itself, because buildx is
blocked by a root-owned `~/.docker/buildx/current` on this machine (§32, item 14) - the image was built
with the classic builder and started with `--no-build`.

Once the stack is up, [the Postman collection](#36-postman-collection) is the fastest way to walk the whole
API surface instead of hand-writing `curl` calls.

---

## 4. Quick start without Docker

Requires Node.js >= 20 and a reachable PostgreSQL instance.

```bash
npm ci

# 1. point the app at your database
export DATABASE_URL=postgres://webhook:webhook@127.0.0.1:5432/webhook
export TENANT_A_TOKEN=dev-token-tenant-a
export TENANT_B_TOKEN=dev-token-tenant-b
export OPERATOR_TOKEN=dev-token-operator

# 1b. that URL is the Compose service's. On a host-only Postgres use your own role
#     and database, or the connection aborts with `role "webhook" does not exist`
#     (Postgres surfaces it from InitializeSessionUserId):
export DATABASE_URL=postgres://<your-user>@127.0.0.1:5432/webhook_dev
#     destinations are deployment-owned config, so pin them too:
export RECEIVER_BASE_URL=http://127.0.0.1:4000
export WEBHOOK_ALLOWED_HOSTS=127.0.0.1:4000

# 2. schema + deterministic seed (seed runs migrate up itself)
npm run seed

# 3. three terminals: API, workers, receiver
npm run start:api
WORKER_NAME=worker-a npm run start:worker
WORKER_NAME=worker-b npm run start:worker
npm run start:receiver
```

Tests need their own database and are pointed at it explicitly:

```bash
export TEST_DATABASE_URL=postgres://<user>@127.0.0.1:5432/webhook_test
npm test
```

`test/global-setup.ts` drops and recreates `webhook_test` on every run, migrates from empty and seeds
it - so the suite doubles as proof that the schema is reproducible from scratch. The migration needs
`CREATE DATABASE` permission; if your server needs credentials, put them in `TEST_DATABASE_URL`.

| Script | What it runs |
|---|---|
| `npm test` | every suite, `--runInBand` (suites share one database, so they must not overlap) |
| `npm run test:unit` | pure logic: canonical JSON, HMAC, backoff/Retry-After |
| `npm run test:integration` | real Postgres + real HTTP per milestone |
| `npm run test:acceptance` | the nine PDF acceptance proofs |
| `npm run typecheck` | `tsc --noEmit` over src **and** test |
| `npm run lint` | ESLint over src and test |
| `npm run build` | compile to `dist/` (what Compose runs) |
| `npm run migrate:up` / `migrate:down` / `seed` | schema and fixtures |

**Why `--runInBand` is not optional** - measured, not theorised. Running the suites in parallel with bare
`npx jest` on this machine produced **18 failed suites / 7 passed, 67 failed tests / 166 passed in 72.2 s**,
and every error was cross-suite interference on the one shared database: `violates foreign key constraint
"deliveries_event_id_fkey"` when another suite's teardown deleted the `events` row a test had just inserted
(50 occurrences), `deadlock detected` inside `resetDatabase()`'s `TRUNCATE … CASCADE` (20), and
`waitForAttempts` timeouts because a neighbouring suite truncated the queue mid-dispatch (12).
`npm test` on the same tree, immediately afterwards: **25 passed / 233 passed in 15.6 s**. So the
parallel run is a real trap for a reviewer who types `jest`, and the honest fix is one database per
worker process - not a change to the delivery logic, which those failures never touched.

---

## 5. Configuration reference

Every tunable lives in `src/config/env.ts` and is validated at process start with zod; an invalid
environment stops the process instead of degrading silently. `.env.example` documents all of these with
placeholder values only - no real credential belongs in the repository.

| Variable | Default | Meaning / why it exists |
|---|---|---|
| `NODE_ENV` | `development` | Validated mode. `test` additionally silences the two ad-hoc `console` fallbacks (`src/db/pool.ts`, `src/receiver/handler.ts`) |
| `LOG_LEVEL` | `info` | pino level |
| `DATABASE_URL` | **required** | the one dependency; also the queue |
| `API_PORT` | `3000` | HTTP API listen port |
| `WORKER_NAME` | `worker-a` | lease owner identity. **Must differ per process** |
| `WORKER_CONCURRENCY` | `4` | outbound HTTP dispatches in flight per worker (hard bound, `max 64`) |
| `WORKER_LEASE_TTL_MS` | `30000` | how long a claim is honoured; expiry enables recovery |
| `WORKER_POLL_INTERVAL_MS` | `250` | idle scan interval (this bounds delivery latency, not correctness) |
| `WORKER_CLAIM_BATCH_SIZE` | `4` | claims per poll pass, capped by free semaphore permits |
| `WORKER_SHUTDOWN_GRACE_MS` | `5000` | drain budget on SIGTERM; unfinished work keeps its lease |
| `WEBHOOK_TIMEOUT_MS` | `2000` | **total** timeout: connect + response + body |
| `WEBHOOK_MAX_RESPONSE_BYTES` | `4096` | response capture bound; reading stops at the bound |
| `WEBHOOK_ALLOWED_HOSTS` | empty | SSRF boundary, comma-separated `host` or `host:port`; empty means the endpoints table is the only destination source |
| `RETRY_MAX_ATTEMPTS_PER_CYCLE` | `5` | automatic attempts per cycle, initial one included |
| `RETRY_BACKOFF_BASE_MS` | `1000` | ladder base: 1s, 2s, 4s, 8s |
| `RETRY_JITTER_MAX_MS` | `250` | uniform jitter 0..250ms added to each backoff |
| `RETRY_AFTER_CAP_MS` | `60000` | ceiling on any scheduled delay, `Retry-After` included |
| `RECEIVER_PORT` | `4000` | mock receiver listen port |
| `RECEIVER_TIMESTAMP_TOLERANCE_SEC` | `300` | signature freshness window (±5 minutes) |
| `RECEIVER_TEST_CONTROLS` | `1` | gates `/__control/*`. **`0` in any production-like deployment** |
| `RECEIVER_BASE_URL` | `http://127.0.0.1:4000` | used by the seed to build destination URLs (deployment-owned config) |
| `TENANT_A_TOKEN` / `TENANT_B_TOKEN` / `OPERATOR_TOKEN` | **required** | dev fixtures; only their SHA-256 hash is stored in `auth_tokens` |

Tests read `TEST_DATABASE_URL` (default `postgres://yousef@127.0.0.1:5432/webhook_test`).

---

## 6. Deterministic seed data

Fixed UUIDs and fixed dev secrets make every environment byte-identical (PDF: "seed data must be
deterministic"). The values are public in `.env.example` and are **development fixtures only**.

| Identity | Value |
|---|---|
| Tenant A | `aaaaaaaa-0000-4000-8000-00000000000a` |
| Tenant B | `bbbbbbbb-0000-4000-8000-00000000000b` |
| Tenant A endpoints | `eeeeeeee-0000-4000-8000-0000000000a1`, `...a2` |
| Tenant B endpoints | `eeeeeeee-0000-4000-8000-0000000000b1`, `...b2` |
| Signing secrets | `dev-secret-a1`, `dev-secret-a2`, `dev-secret-b1`, `dev-secret-b2` (column `endpoints.secret`) |
| Tokens | `dev-token-tenant-a`, `dev-token-tenant-b`, `dev-token-operator` |

Each endpoint's destination URL is `${RECEIVER_BASE_URL}/hook/${endpointId}`, written into the
`endpoints` table by the seed. The seed is idempotent (`ON CONFLICT ... DO UPDATE`): running it twice
is a no-op, never a duplicate.

---

## 7. API reference

Authentication is `Authorization: Bearer <token>`. Tenant tokens reach tenant routes; the operator
token reaches only `/ops/*`. A tenant token on an operator route is **403**, an operator token on a
tenant route is **403**, and a missing/unknown token is **401** - the two privilege levels never mix.

### `POST /events` — publish an event (202)

| | |
|---|---|
| Auth | tenant token |
| Required header | `Idempotency-Key` (1..200 chars) |
| Optional header | `X-Request-Id` (reused if present, else generated; always echoed back) |
| Body limit | 64 KiB → `413 payload_too_large` |
| Body | `{"endpointId": "<uuid>", "eventType": "<1..100 chars>", "payload": { ... }}` |
| Closed schema | unrecognised **top-level** fields are rejected with 400. `payload` is open by design |

202 response:

```json
{ "eventId": "0f9c...", "deliveryId": "77ab...", "status": "READY", "statusUrl": "/events/0f9c..." }
```

Errors: `400 bad_request` (missing key, invalid body, unknown field), `401 unauthorized`,
`404 not_found` (endpoint unknown **or** another tenant's - identical response, no existence leak),
`409 conflict` (same key, different input), `413 payload_too_large`.

### `GET /events/:id` — event and delivery state (200)

Tenant-scoped. Unknown id and foreign id both produce the same `404`, as does a malformed UUID.

```json
{
  "eventId": "0f9c...",
  "endpointId": "eeeeeeee-0000-4000-8000-0000000000a1",
  "eventType": "order.created",
  "occurredAt": "2026-01-01T00:00:00.000Z",
  "createdAt": "2026-01-01T00:00:00.000Z",
  "delivery": {
    "deliveryId": "77ab...",
    "state": "RETRY_WAIT",
    "totalAttempts": 2,
    "cycle": 1,
    "nextAttemptAt": "2026-01-01T00:00:03.121Z",
    "lastHttpStatus": 503,
    "lastErrorCode": "http_503"
  }
}
```

Never returned: `envelope_bytes`, endpoint URL, endpoint secret, attempt history.

### `GET /deliveries` — tenant delivery listing (200)

Query: `limit` (1..100, default 50), `state` (one of the five states), `cursor` (opaque keyset token).
Ordering is stable: `(created_at DESC, id DESC)` via keyset pagination - no `OFFSET`, so new arrivals
cannot shift rows between pages and a page costs the same at row 10 and row 10 million.

```json
{
  "data": [
    { "deliveryId": "77ab...", "eventId": "0f9c...", "endpointId": "eeee...a1",
      "state": "RETRY_WAIT", "attemptCount": 2, "cycle": 1, "attemptsInCycle": 2,
      "nextAttemptAt": "2026-01-01T00:00:03.121Z", "lastHttpStatus": 503,
      "lastErrorCode": "http_503", "createdAt": "...", "updatedAt": "..." }
  ],
  "pagination": { "limit": 50, "nextCursor": "eyJjIjoiMjAyNi0wMS0wMVQwMDowMDowMFoi..." }
}
```

### `POST /ops/deliveries/:id/redrive` — operator redrive (202)

| | |
|---|---|
| Auth | operator token (a tenant token gets 403) |
| Required header | `Idempotency-Key` |
| Body | `{"reason": "<1..500 chars, must contain a non-whitespace character>"}` |

202 response (a replay is byte-identical apart from the request id):

```json
{ "deliveryId": "77ab...", "eventId": "0f9c...", "state": "READY", "cycle": 2,
  "attemptCount": 5, "attemptsInCycle": 0, "nextAttemptAt": "2026-01-01T00:00:15.000Z" }
```

`reason` is mandatory because a redrive is a privileged action that spends fresh delivery effort; the
audit row must be explainable. Errors: `400`, `401`, `403`, `404` (unknown or malformed id), `409`
(state is not `DEAD`, or the same key with a different reason).

### `GET /ops/status` — operational counters (200, operator only)

See [section 27](#27-observability-logs-and-counters).

### `GET /health` — readiness (public)

`200 {"status":"ok","uptimeSec":12,"database":"ok"}` when `SELECT 1` succeeds; otherwise
`503 {"code":"service_unavailable","message":"Service is not ready","requestId":"..."}`. It is a
readiness probe, not a liveness probe: this service has exactly one dependency and every request needs
it, so a process that answers HTTP while the database is unreachable is not ready. The driver's error
(which names host, port and user) is logged server-side and never returned.

---

## 8. Worked example: publish to delivered

Publishing with the seed's dev token (host mode, receiver on 4000):

```bash
curl -sX POST http://127.0.0.1:3000/events \
  -H 'Authorization: Bearer dev-token-tenant-a' \
  -H 'Idempotency-Key: ord-1001-attempt-1' \
  -H 'Content-Type: application/json' \
  -d '{
    "endpointId": "eeeeeeee-0000-4000-8000-0000000000a1",
    "eventType": "order.created",
    "payload": { "orderId": "ord_1001", "amount": {"currency":"USD","cents":4200}, "items":["a","b"] }
  }'
```

Replay the **same key** with the same body → the identical 202 body (the original
`eventId`/`deliveryId`), and exactly one event, one delivery and one dispatch:

```bash
curl -sX POST http://127.0.0.1:3000/events \
  -H 'Authorization: Bearer dev-token-tenant-a' \
  -H 'Idempotency-Key: ord-1001-attempt-1' \
  -H 'Content-Type: application/json' \
  -d '{"endpointId":"eeeeeeee-0000-4000-8000-0000000000a1","eventType":"order.created","payload":{"amount":{"cents":4200,"currency":"USD"},"items":["a","b"],"orderId":"ord_1001"}}'
```

Note the second body reorders the nested `amount` keys and the top-level fields: **object key order is
irrelevant** to the fingerprint. But `items: ["b","a"]` would be a *different* request → `409`.

Change the payload under the same key:

```bash
curl -siX POST http://127.0.0.1:3000/events \
  -H 'Authorization: Bearer dev-token-tenant-a' \
  -H 'Idempotency-Key: ord-1001-attempt-1' \
  -H 'Content-Type: application/json' \
  -d '{"endpointId":"eeeeeeee-0000-4000-8000-0000000000a1","eventType":"order.created","payload":{"orderId":"ord_1002"}}'
# HTTP/1.1 409 ... {"code":"conflict","message":"Idempotency-Key was reused with different input; ...","requestId":"..."}
```

Cross-tenant: tenant B's token with tenant A's endpoint id → `404 not_found`, byte-identical to the
response for an endpoint id that has never existed.

Watch the effect land exactly once, even after duplicates:

```bash
EP=eeeeeeee-0000-4000-8000-0000000000a1
curl -s "http://127.0.0.1:4000/__control/requests?endpointId=$EP" | node -e 'let d="";process.stdin.on("data",c=>d+=c).on("end",()=>console.log("requests:",JSON.parse(d).requests.length))'
curl -s "http://127.0.0.1:4000/__control/effects?endpointId=$EP"   | node -e 'let d="";process.stdin.on("data",c=>d+=c).on("end",()=>console.log("effects:",JSON.parse(d).effects.length))'
```

Observed in a live host run of exactly these commands (four processes, `webhook_dev` database):

```
PUBLISH  -> {"eventId":"db9c55af-…","deliveryId":"32e95c2b-…","status":"READY",
             "statusUrl":"/events/db9c55af-…"}
+2s      -> GET /events/db9c55af-…  delivery.state "DELIVERED", totalAttempts 1,
            cycle 1, nextAttemptAt null, lastHttpStatus 200, lastErrorCode null
REPLAY   -> the same eventId and deliveryId; key order in the body was reordered
CHANGED  -> 409 {"code":"conflict","message":"Idempotency-Key was reused with
            different input; the original response cannot be replayed","requestId":…}
TENANT B -> 404 {"code":"not_found","message":"Event not found","requestId":…}
UNKNOWN  -> 404, identical shape and message
"url"    -> 400 {"code":"bad_request","message":"Invalid request body: (body):
            Unrecognized key(s) in object: 'url'","requestId":…}
receiver -> requests: 1   effects: 1   (signature_ok true)
/ops/status -> {"ready":0,"inFlight":0,"retryWait":0,"delivered":3,"dead":0,
                "oldestPending":null,"expiredLeases":0,"workers":[]}
                (counted after the three events of section 9 were published)
```

A `GET /health` on each process answers `{"status":"ok","uptimeSec":…,"database":"ok"}`, and the worker
logs for the same run contain only identifiers and state - no token, no secret, no payload
(the `owner`/`name` fields are elided here, ids are truncated for width):

```
{"level":30,"service":"worker-a","deliveryId":"60bde071-…","eventId":"5d57e923-…",
 "attemptId":"d4f65664-…","attemptNumber":1,"leaseGeneration":"1","state":"RETRY_WAIT",
 "msg":"delivery transitioned"}
```

---

## 9. Worked example: failure, retry, redrive

```bash
EP=eeeeeeee-0000-4000-8000-0000000000a1

# 1. make the receiver refuse with 503 for the next 2 calls
curl -sX PUT http://127.0.0.1:4000/__control/modes \
  -d "{\"endpointId\":\"$EP\",\"mode\":\"temp_failure\",\"remaining\":2}"

# 2. publish; poll until the retry budget is exhausted
curl -sX POST http://127.0.0.1:3000/events -H 'Authorization: Bearer dev-token-tenant-a' \
  -H 'Idempotency-Key: ord-1002' -H 'Content-Type: application/json' \
  -d "{\"endpointId\":\"$EP\",\"eventType\":\"order.created\",\"payload\":{\"orderId\":\"ord_1002\"}}"

curl -s http://127.0.0.1:3000/events/<eventId> -H 'Authorization: Bearer dev-token-tenant-a'
# ... state RETRY_WAIT, totalAttempts 1, nextAttemptAt = now + ~1s ... then 2s, 4s, 8s ... then DEAD

# 3. a 429 with Retry-After wins over the ladder (capped at 60s)
curl -sX PUT http://127.0.0.1:4000/__control/modes \
  -d "{\"endpointId\":\"$EP\",\"eventId\":\"<eventId>\",\"mode\":\"rate_limited\",\"retryAfter\":20,\"remaining\":1}"

# 4. operator buys exactly one new automatic cycle
curl -sX POST http://127.0.0.1:3000/ops/deliveries/<deliveryId>/redrive \
  -H 'Authorization: Bearer dev-token-operator' \
  -H 'Idempotency-Key: redrive-2026-01-01-1' -H 'Content-Type: application/json' \
  -d '{"reason":"receiver certificate fixed in INC-4821"}'

# 5. whole-system view
curl -s http://127.0.0.1:3000/ops/status -H 'Authorization: Bearer dev-token-operator'
```

The receiver's per-event mode (step 3) takes precedence over the endpoint-wide one, which is how the
acceptance suite builds a cycle where one attempt is rate-limited and the next is a plain 503.

Measured while running this against a live four-process host deployment (real `curl`, real sockets,
no fake timers):

| Step | Observed |
|---|---|
| endpoint-wide `temp_failure`, publish | at +1.3 s: `state RETRY_WAIT`, `totalAttempts 1`, `lastErrorCode "http_503"`, `nextAttemptAt` = completion time **+1041 ms** (the 1 s ladder plus 41 ms of *unforced* jitter) |
| clear the modes, wait | `DELIVERED`, `totalAttempts 2`, `cycle 1`, `lastHttpStatus 200` - the retry landed on the second attempt and the receiver kept **1** effect for **2** requests |
| endpoint-wide `reject_400`, publish | `DEAD` within 1 s: `totalAttempts 1`, `lastHttpStatus 400`, `lastErrorCode "http_400"`, `nextAttemptAt null` - a 4xx other than 408/429 burns no retry budget |
| redrive with a tenant token | `403 {"code":"forbidden","message":"This operation requires an operator token",…}`, DEAD row untouched |
| redrive with the operator token | `202 {"state":"READY","cycle":2,"attemptCount":1,"attemptsInCycle":0,…}` - lifetime attempts preserved, cycle budget refilled |
| after that redrive | `DELIVERED`, `totalAttempts 2`, `cycle 2`; receiver side: **2** requests for the event, **1** effect |
| `GET /ops/status` at rest | all counters consistent with the rows (`delivered 3`, `ready/inFlight/retryWait/dead 0`, `expiredLeases 0`) |

This whole sequence - publish, fail, retry, reject, redrive - is packaged as runnable, asserted requests in
[the Postman collection](#36-postman-collection), folder `06`.

---

## 10. Data model

Four migrations, applied in lexical version order by `src/db/migrate.ts`, which records each applied
version in `schema_migrations` and applies each file in its own transaction (all-or-nothing). It skips
versions already applied; it does **not** detect a version gap (adding `006` after `004` would simply be
applied), so ordering discipline is a review concern - noted in [limitations](#32-limitations).
`migrate down` drops every known table *and* the two custom enum types so `up` can be re-run against a
clean database; the test suite proves the from-empty path differently - `test/global-setup.ts` drops and
recreates the whole database and then migrates `up` on every run. `migrate down` itself is exercised
manually, not by CI.

### `migrations/001_core_schema.sql`

| Table | Purpose | The one thing to remember |
|---|---|---|
| `tenants` | identity | - |
| `endpoints` | destination config: `url`, `secret` | trusted **deployment** data; never caller-supplied, never returned by an API |
| `events` | immutable published facts | `payload` jsonb + `occurred_at` fixed at publication |
| `deliveries` | the queue row and the state machine | `event_id` is **UNIQUE**: one logical delivery per event, which is what makes "atomically published" verifiable |
| `delivery_attempts` | history | allocated **before** dispatch, with `outcome='UNKNOWN'`, `finished_at NULL` |
| `idempotency_records` | replay store | `UNIQUE (tenant_id, operation, idempotency_key)` |
| `redrive_audit` | operator trail | stores the operator **label**, never the raw token |

`deliveries` columns that carry the whole reliability story:

```
state enum(READY,IN_FLIGHT,RETRY_WAIT,DELIVERED,DEAD)   attempt_count       -- lifetime, never reset
envelope_bytes bytea  envelope_hash text               cycle               -- automatic cycle number
next_attempt_at timestamptz                           attempts_in_cycle   -- budget within a cycle
lease_owner text   lease_generation bigint   lease_expires_at timestamptz
last_http_status integer   last_error_code text        -- NULL/absent when genuinely unknown
```

`envelope_bytes` is `bytea` on purpose: the body is serialized once and reused byte-for-byte forever, so
a retry can never invalidate a signature by re-formatting it.

### Indexes, and why each one exists

| Index | Serves |
|---|---|
| `deliveries_due_idx` **partial** `(next_attempt_at) WHERE state IN ('READY','RETRY_WAIT')` | the claim scan. Partial because terminal rows must not bloat the hot path; this is the query that runs every 250ms |
| `deliveries_lease_expiry_idx` **partial** `(lease_expires_at) WHERE state='IN_FLIGHT'` | finding abandoned leases without scanning history |
| `deliveries_tenant_state_idx` `(tenant_id, state, created_at DESC, id DESC)` | `GET /deliveries?state=...` in stable order, no sort step |
| `deliveries_tenant_created_idx` `(tenant_id, created_at DESC, id DESC)` (004) | the same listing **without** a state filter |
| `deliveries_state_idx` `(state)` | `/ops/status` `GROUP BY state` |
| `delivery_attempts_delivery_idx` `(delivery_id, attempt_number)` | attempt history read in order |
| `endpoints_tenant_idx` `(tenant_id)` | ownership checks |
| `events_tenant_created_idx` `(tenant_id, created_at DESC, id DESC)` | tenant event reads |
| `idempotency_unique` `(tenant_id, operation, idempotency_key)` | the serialization point for duplicate publications/redrives |
| `deliveries.event_id UNIQUE` | one delivery per event |
| `receiver_effect_identity UNIQUE (endpoint_id, event_id)` | **the deduplication** |
| `receiver_requests_event_idx`, `receiver_requests_endpoint_idx` | request inspection for tests |
| `auth_tokens_tenant_idx`, `redrive_audit_delivery_idx` | token lookup per tenant; audit per delivery |

### `002_receiver_schema.sql`

`receiver_effects` (durable dedup), `receiver_requests` (every inbound request, with `signature_ok` and
the mode in effect), `receiver_modes` (test-only failure modes keyed by `(endpoint_id, event_id)` where
`''` means "endpoint-wide" - a sentinel rather than NULL because a primary-key column is implicitly
NOT NULL).

### `003_auth_tokens.sql`

`auth_tokens(token_hash PK, tenant_id NULL, role CHECK, label)`. Only the SHA-256 hash of a token is
stored, and a table-level `CHECK` makes `tenant` ⇒ `tenant_id NOT NULL` / `operator` ⇒ `NULL`, so the
role/identity pair cannot become inconsistent from application code.

---

## 11. Delivery state machine

```mermaid
stateDiagram-v2
    [*] --> READY : publication commits (due immediately)
    READY --> IN_FLIGHT : claimNext - lease + generation+1 + attempt allocated
    RETRY_WAIT --> IN_FLIGHT : next_attempt_at reached
    IN_FLIGHT --> IN_FLIGHT : lease expired -> another worker recovers it (generation bumps)
    IN_FLIGHT --> DELIVERED : 2xx SUCCESS (lease released)
    IN_FLIGHT --> DEAD : NON_RETRYABLE (4xx other than 408/429, any 3xx)
    IN_FLIGHT --> RETRY_WAIT : RETRYABLE / UNKNOWN, budget left -> 1s/2s/4s/8s + jitter
    IN_FLIGHT --> DEAD : budget exhausted (attempts_in_cycle = 5)
    DEAD --> READY : operator redrive, cycle+1, attempts_in_cycle=0
    DELIVERED --> [*]
    DEAD --> [*]
```

`READY`, `RETRY_WAIT` and expired-`IN_FLIGHT` are the three forms of "claimable". `DELIVERED` and `DEAD`
are terminal: no lease is held and `next_attempt_at` is NULL. The lifetime `attempt_count` and the
`cycle` counter are the two numbers that make a restart or a redrive a non-event for the schedule.

---

## 12. Publication: atomicity and idempotency

```mermaid
sequenceDiagram
    autonumber
    participant C as Caller
    participant A as API process
    participant P as PostgreSQL
    participant W as Worker

    C->>A: POST /events (Bearer, Idempotency-Key, body)
    A->>A: requestId, 64 KiB parse, strict schema
    A->>P: BEGIN
    A->>P: SELECT idempotency_records (tenant, publish_event, key)
    alt key already used
        A->>P: ROLLBACK
        A-->>C: replay stored 202, or 409 if the fingerprint differs
    else key is new
        A->>P: INSERT idempotency_records (CLAIM BEFORE WORK)
        A->>P: SELECT endpoint WHERE id=$1 -> compare tenant_id
        A->>P: INSERT events (id, tenant, endpoint, type, payload, occurred_at)
        A->>P: INSERT deliveries (state READY, envelope_bytes, due now)
        A->>P: UPDATE record with the real status+body, COMMIT
        A-->>C: 202 {eventId, deliveryId, status, statusUrl}
    end
    W->>P: claimNext -> SKIP LOCKED lease
```

Three consequences of putting the claim, the event and the delivery in **one** transaction:

1. **There is no such thing as an event without a delivery.** A partial commit is not expressible.
2. **Concurrent duplicates serialize on the unique index.** Twenty simultaneous publications of the
   same key: one transaction wins, nineteen get `23505`, roll back, re-read the committed record and
   replay it. All twenty see one `eventId`.
3. **A request that fails validation or ownership consumed nothing.** It rolled back with its claim, so
   the key stays usable. This is deliberate: a typo must not burn an idempotency key forever.
   (`test/acceptance/t9` proves the same property for an injected *database* write failure.)

The fingerprint is `sha256(canonicalJson({endpointId, eventType, payload}))`. `canonicalJson`
(`src/common/canonical-json.ts`) recursively sorts **object** keys while preserving **array** order -
because `{"a":1,"b":2}` and `{"b":2,"a":1}` are the same request, while `items:["a","b"]` and
`items:["b","a"]` are not. That asymmetry is a semantic decision, and it is unit-tested.

One finding worth knowing: a replayed body comes back through a `jsonb` column, and **`jsonb`
reorders keys**. The replay is semantically identical but not necessarily byte-identical, so the
acceptance suite asserts semantic equality via `canonicalJson`, not raw bytes. A client must treat the
stored response as data, not as a byte snapshot.

---

## 13. The queue: claiming work with leases

```mermaid
sequenceDiagram
    autonumber
    participant WA as worker-a
    participant WB as worker-b
    participant P as PostgreSQL
    participant R as Receiver

    WA->>P: BEGIN + claimNext(owner=worker-a, ttl=30s)
    Note over P: WITH candidate (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)<br/>UPDATE ... SET state=IN_FLIGHT, lease_owner, generation+1,<br/>attempt_count+1, attempts_in_cycle+1 RETURNING
    P->>P: INSERT delivery_attempts (outcome UNKNOWN, finished_at NULL)
    P-->>WA: COMMIT: ClaimedWork (envelope bytes, attemptId, generation)
    WB->>P: claimNext(owner=worker-b)
    P-->>WB: a DIFFERENT row (SKIP LOCKED never blocks, never double-claims)
    WA->>R: POST, signed with fresh timestamp (no DB transaction held)
    R-->>WA: 200
    WA->>P: completeAttempt (attempt row + fenced state UPDATE)
    Note over P: WHERE id AND lease_owner AND lease_generation<br/>rowCount 0 => stale worker, newer state survives
```

The claim is one statement: a CTE selects at most one due/abandoned row with `FOR UPDATE SKIP LOCKED`
and an `UPDATE ... FROM candidate ... RETURNING` takes the lease and bumps both counters. The attempt
row is inserted **in the same transaction, before any HTTP**. That ordering matters:

* A worker that dies immediately after the commit leaves a leased `IN_FLIGHT` row and an `UNKNOWN`
  attempt - recoverable, and already honest about what happened.
* A worker that dies before the commit has done nothing at all.

`completeAttempt` writes the attempt row unconditionally (so even a fenced-out worker records *its*
truthful outcome) and the delivery state **fenced**. On a terminal transition the lease is released in
the same statement.

---

## 14. Bounded concurrency across two workers

Each worker process holds a `Semaphore` with `WORKER_CONCURRENCY` (4) permits and **acquires a permit
before it claims**. It therefore cannot claim work it has nowhere to put, and no unbounded in-memory
queue ever forms - the unbounded buffer is the `deliveries` table, which is exactly what a database is
for.

Total system concurrency is `workers x 4 = 8` dispatches; that is a property of the deployment, not of
one process, and it is why `worker-a` and `worker-b` are separate containers rather than two threads.
`test/acceptance/t9` measures the bound at the **destination**: 12 deliveries, each response held open
for 120ms, peak concurrency observed by the receiving socket `<= 4` and `> 1` (the lower bound proves
the assertion is not vacuous).

Shutdown: `stop()` flips a flag, wakes the poll loop so it claims nothing more, and drains in-flight
dispatches up to `WORKER_SHUTDOWN_GRACE_MS`. Anything unfinished keeps its lease and expires into
recovery - an honest "we might have sent this" rather than a fabricated success.

---

## 15. Fencing: the stale worker problem

A lease alone does not make delivery safe. The dangerous interleaving:

1. `worker-a` claims (generation 1) and stalls - a GC pause, a VM suspend, a laptop sleeping.
2. Its lease expires. `worker-b` recovers the row (generation 2) and dispatches.
3. `worker-b` gets 200 → `DELIVERED`.
4. `worker-a` wakes up, its HTTP call returns 503, and it tries to write `RETRY_WAIT`.

Without fencing, step 4 would move a successfully delivered event back into the retry queue - the
classic "duplicate work resurrected" bug. Because every state write carries
`AND lease_owner = $8 AND lease_generation = $9`, step 4 matches zero rows and is reported as
`applied: false`. The worker logs it and stops; the newer state survives.

`lease_generation` is bumped by the database (`d.lease_generation + 1`) inside the claim statement, so
no worker can mint a token and no clock is involved. `test/acceptance/t7` runs this against real HTTP:
both attempt rows are kept with their own generations, exactly one receiver effect exists, and the
stale worker provably cannot produce a third dispatch.

Two honest limits: fencing protects *state*, not the *network* - a paused worker still sends its
request, so duplicate HTTP is expected; and the lease TTL is a heuristic, not a bound on a pause. Both
are why correctness lives in the receiver's dedup constraint.

---

## 16. Retry policy: backoff, jitter, Retry-After

`RetryPolicy.decide` (`src/worker/retry-policy.ts`) is pure: it takes the claimed work and the attempt
result, and returns `(nextState, nextAttemptAt)`. Injected `Clock` and `Random` make it deterministic.

```
delay(n) = min( 1000 * 2^(n-1) + uniform(0..250) , 60_000 )        n = attempts_in_cycle
if 429 with a valid Retry-After: delay = max(delay, retry_after)   then cap at 60_000
```

| Attempt in cycle | Backoff | Total in a cycle |
|---|---|---|
| 1 | 1s + jitter | |
| 2 | 2s + jitter | |
| 3 | 4s + jitter | |
| 4 | 8s + jitter | |
| 5 | next failure ⇒ `DEAD` | ≈15s + jitter of retrying |

The ladder is indexed by **attempts in the current cycle**, so after a redrive it starts again at 1s -
but `attempt_number` (lifetime) keeps climbing, which is what the history and the API's `totalAttempts`
report.

`Retry-After` handling is intentionally narrow: only a plain delta-seconds value is honoured
(`0 < n <= 86400`); an HTTP-date or a garbage value falls back to normal backoff. A `Retry-After`
shorter than the computed backoff does **not** shorten it - a client must not be able to be pushed
faster than the policy's floor.

Jitter exists to prevent a fleet of workers from retrying in lockstep after a shared outage. The value
comes from the injected `RandomSource`, so a test fixes it at 0 and asserts exact due times.

```mermaid
flowchart TD
    O{attempt outcome}
    O -- SUCCESS --> D1["DELIVERED<br/>next_attempt_at NULL<br/>lease released"]
    O -- NON_RETRYABLE --> X["DEAD<br/>no automatic retry<br/>operator redrive only"]
    O -- "RETRYABLE / UNKNOWN" --> B{"attempts_in_cycle<br/>hit the 5-attempt budget?"}
    B -- yes --> X
    B -- no --> C["backoff = 1000 * 2 to the (n-1)<br/>n = attempts_in_cycle"]
    C --> J["delay = backoff + uniform(0..250)"]
    J --> RA{"429 with a plain<br/>delta-seconds Retry-After?"}
    RA -- yes --> M["delay = max(delay, retry_after)"]
    RA -- "no (HTTP-date / garbage / absent)" --> K["keep the computed delay<br/>a client cannot shorten the floor"]
    M --> CAP["delay = min(delay, 60000)"]
    K --> CAP
    CAP --> W["RETRY_WAIT<br/>next_attempt_at = now + delay"]
```

`UNKNOWN` taking the retry branch is the whole point of the diagram: an outcome we cannot confirm is
never upgraded to success and never downgraded to failure, it is retried and deduplication absorbs the
duplicate.

---

## 17. Cycles, budget and operator redrive

A **cycle** is one automatic attempt budget. `RETRY_MAX_ATTEMPTS_PER_CYCLE = 5` counts the initial
dispatch, so a cycle that never succeeds ends in `DEAD` after 5 attempts.

```mermaid
sequenceDiagram
    autonumber
    participant O as Operator
    participant A as API
    participant P as PostgreSQL
    participant W as Worker
    participant R as Receiver

    Note over P: state DEAD, attempt_count 5, cycle 1
    O->>A: POST /ops/deliveries/:id/redrive (Idempotency-Key, reason)
    A->>P: BEGIN, claim idempotency record (tenant, redrive, key)
    A->>P: SELECT delivery FOR UPDATE
    alt state != DEAD
        A->>P: ROLLBACK
        A-->>O: 409 "Only a DEAD delivery can be redriven (current state: X)"
    else state == DEAD
        A->>P: UPDATE state=READY, cycle=cycle+1, attempts_in_cycle=0,<br/>next_attempt_at=now, lease cleared
        A->>P: INSERT redrive_audit (operator label, reason, key)
        A->>P: COMMIT
        A-->>O: 202 {cycle: 2, attemptsInCycle: 0, attemptCount: 5}
    end
    W->>P: claim -> attempt_number 6, cycle 2
    W->>R: POST the SAME envelope bytes, SAME eventId/deliveryId
    R->>R: dedup identity is (endpoint, event) -> single effect either way
```

What a redrive deliberately does **not** touch: `eventId`, `deliveryId`, `envelope_bytes`,
`attempt_count`, the attempt history, or the cycle-1 attempts. The receiver's dedup identity is the
*event*, so changing the identity would apply the business effect twice. Restarting the lifetime
counter would make the budget unbounded. A redrive therefore buys exactly 5 more automatic attempts and
nothing more.

Concurrency: the key is claimed first, then the row is locked `FOR UPDATE`. Two operators using
*different* keys cannot start two cycles - the second blocks, then sees a non-`DEAD` row and gets 409.
Six concurrent requests with the *same* key replay one response and write one audit row
(`test/acceptance/t8`). Replaying a redrive whose cycle is already spending its budget does not reset
it: the stored snapshot replays and the live row is unchanged.

---

## 18. The webhook signature contract

**Signed input** (exactly this, in this order):

```
string_to_sign = UTF8( "<unix_seconds>" + "." + <exact raw body bytes> )
signature      = lowercase hex( HMAC_SHA256( endpoint_secret, string_to_sign ) )
```

**Headers sent on every attempt** - these five plus `content-type` and `content-length`, and nothing
else:

| Header | Stability |
|---|---|
| `X-Event-Id` | stable for the life of the event |
| `X-Delivery-Id` | stable across attempts and redrives |
| `X-Attempt-Id` | **fresh per attempt** |
| `X-Webhook-Timestamp` | **fresh per attempt** (Unix seconds) |
| `X-Webhook-Signature` | **recomputed per attempt** over the fresh timestamp + the same bytes |

The body is the persisted `deliveries.envelope_bytes`, re-sent byte-for-byte on every attempt and every
redrive - it is never re-serialized, which is what makes a signature reproducible on the receiver side.
The signing secret is only ever passed to `signWebhook`; it is not a header, not logged, and never
returned by an API.

**Receiver verification recipe** (what `src/receiver/handler.ts` does, and what a customer must do):

1. Read the raw bytes first; never verify a re-stringified body.
2. Reject unless `|now - X-Webhook-Timestamp| <= 300`s. Timestamp freshness is checked **before** any
   dedup or business work: it is the cheap replay defence.
3. Recompute the HMAC over `timestamp + "." + bytes` with the endpoint secret and compare
   **constant-time** (`timingSafeEqual` over equal-length buffers; a structural mismatch fails closed
   without leaking comparison timing).
4. Only then consult deduplication and apply the effect.

`test/acceptance/t3` proves this end-to-end: two attempts of one delivery carry distinct attempt ids,
distinct timestamps (exactly one second apart, because the fake clock was advanced deliberately),
distinct signatures, both valid over byte-identical bodies; then the captured bytes replayed 301s later
are rejected `401 stale_timestamp` with zero effects, and the same bytes re-signed with a fresh
timestamp are accepted with exactly one effect.

Rejections: tampered body, wrong secret and stale timestamp all yield 401 and **no** effect applied.

---

## 19. Envelope stability and duplicate semantics

The delivered JSON is fixed at publication:

```json
{
  "eventId": "0f9c...",
  "deliveryId": "77ab...",
  "eventType": "order.created",
  "occurredAt": "2026-01-01T00:00:00.000Z",
  "payload": { "orderId": "ord_1001" }
}
```

`occurredAt` is ISO-8601 UTC and never changes; `payload` is the caller's object verbatim. Field order
is fixed by construction (the object is built in this order and `JSON.stringify` preserves insertion
order), so `envelope_hash` is a stable integrity reference for debugging.

Duplicate semantics, stated plainly: a duplicate dispatch is **normal**. It happens whenever a response
is lost, a lease expires while a request is in flight, or an operator redrives. The receiver answers 200
for both the first application (`applied: true`) and a harmless duplicate (`applied: false,
deduplicated: true`) so the sender stops retrying; only the first one changes state. A *different* body
presenting the *same* identity is a `409 content_conflict` - a genuine integrity alarm, not a duplicate.

---

## 20. Outcome honesty: what UNKNOWN means

| Situation | Recorded as | Why |
|---|---|---|
| 2xx completed response | `SUCCESS` + status | - |
| 408, 429, 5xx | `RETRYABLE` + status | the receiver said "come back" |
| Other 4xx, any 3xx | `NON_RETRYABLE` + status | a permanent fact about this delivery |
| Total timeout, nothing received | `RETRYABLE`, `error_code='timeout'`, `http_status NULL` | we do not know whether it was processed; we know we have no answer |
| Headers arrived, body stream broke | `RETRYABLE`/`UNKNOWN` with `stream_error` | a rejection status stays authoritative; an unfinished **2xx becomes `UNKNOWN`**, never a fabricated success |
| Connection failure / reset | `UNKNOWN`, `transport_error`, `http_status NULL` | the request may have been processed before the socket died |
| Worker killed between dispatch and completion write | attempt stays `UNKNOWN`, `finished_at NULL` | the record of "we sent something and lost the answer" survives the worker |
| Processor throws (bug) | `UNKNOWN`, `processor_error` | a bug must not lose a delivery, and must not invent a result |

`UNKNOWN` and `RETRYABLE` both consume a retry and both respect the per-cycle budget: uncertainty is
treated like a transient failure because the receiver's dedup makes the retry safe. The rule running
through all of it: **report what was observed. Where nothing was observed, record the absence.**

---

## 21. Crash recovery walk-through

```mermaid
sequenceDiagram
    autonumber
    participant WA as worker-crashed
    participant P as PostgreSQL
    participant R as Receiver
    participant WB as worker-recovery

    WA->>P: claimNext (gen 1) + attempt 1 allocated
    WA->>R: POST signed
    R->>P: effect committed (endpoint,event) UNIQUE
    Note over WA: process killed before the completion write
    Note over P: state IN_FLIGHT, lease held, attempt 1 = UNKNOWN / finished_at NULL
    R-->>WB: (nothing to do: dedup already durable, 1 effect exists)
    Note over P: lease_expires_at passes
    WB->>P: claimNext recovers expired lease (gen 2, attempt 2)
    WB->>R: POST the same bytes
    R->>P: INSERT ... ON CONFLICT DO NOTHING -> duplicate
    R-->>WB: 200 {applied:false, deduplicated:true}
    WB->>P: completeAttempt fenced -> DELIVERED, lease released
    Note over P: attempt_count 2, cycle 1, attempts_in_cycle 2, receiver effects 1
```

The recovery path is the *same* query as the normal claim path: `IN_FLIGHT` with
`lease_expires_at <= now` is claimable. There is no separate "repair job" and no in-memory state to
rebuild. Proven in `test/acceptance/t6` (crash after publication commit; crash after dispatch, before
completion commit; crash mid-cycle where the recovery must **continue the same cycle** rather than
re-issue the budget).

---

## 22. Mock receiver and its eight failure modes

A standalone `node:http` process (`src/receiver/`), deliberately **not** a Nest app: it stands in for a
third-party customer endpoint, so it must not depend on the delivery service's framework.

| Mode (`ReceiverMode`) | Response | Applies the effect? |
|---|---|---|
| `success` (default) | 200 `{ok,applied,deduplicated,eventId,attemptId}` | yes |
| `temp_failure` | 503 `temporary_failure` | no |
| `perm_failure` | 500 `permanent_failure` | no |
| `rate_limited` | 429 `rate_limited` + `Retry-After: <n>` | no |
| `reject_400` | 400 `rejected` | no |
| `redirect` | 302 + `Location` (never followed by the sender) | no |
| `slow` | 200 after `delayMs` | yes |
| `lost_response` | effect committed, then `res.destroy()` | **yes** — the response disappears after the commit |

`lost_response` is the most important row: it is the PDF's "commit the receiver effect and drop the
response" case, and it is why `UNKNOWN` exists as an outcome.

A naming caveat worth stating before someone trips over it: `perm_failure` means "fails until I clear
it", not "non-retryable". It returns **500**, which the sender classifies as `RETRYABLE`, so it is the
mode that burns a whole cycle into `DEAD`. The genuinely non-retryable mode is `reject_400`, which
produces immediate `DEAD` on the first attempt.

Modes are keyed per endpoint and optionally per event (event-scoped wins), with `remaining` for counted
modes so a mode can fail exactly N times and then recover. They are served only from `/__control/*`:

| Control endpoint | Method | Purpose |
|---|---|---|
| `/__control/modes` | PUT | set `{endpointId, eventId?, mode, remaining?, retryAfter?, delayMs?}` |
| `/__control/modes` | DELETE | clear all modes |
| `/__control/requests` | GET | every inbound request, with `signature_ok` and the mode in effect |
| `/__control/effects` | GET | the durable dedup records |
| `/__control/reset` | POST | wipe observed state (test helper only) |

Two rules the PDF insists on, implemented literally: modes are **never** selectable through a public
event field (the request schema has no way to express them and rejects unknown top-level keys), and the
whole control surface returns `403 test_controls_disabled` unless `RECEIVER_TEST_CONTROLS` is on - so a
production-like receiver cannot have its failure behaviour steered from outside.

---

## 23. Receiver-side deduplication

```mermaid
flowchart TD
    A[POST /hook/:endpointId<br/>raw bytes captured] --> B{endpoint has a secret?}
    B -- no --> X1[401 unknown_endpoint<br/>no effect]
    B -- yes --> C{timestamp integer and within 300s?}
    C -- no --> X2[401 stale_timestamp<br/>no effect]
    C -- yes --> D{HMAC recomputed over exact bytes,<br/>timing-safe compare ok?}
    D -- no --> X3[401 invalid_signature<br/>no effect]
    D -- yes --> E{body parses and headers match<br/>envelope eventId/deliveryId?}
    E -- no --> X4[400 invalid_body / identity_mismatch]
    E -- yes --> F["INSERT receiver_effects<br/>ON CONFLICT (endpoint_id,event_id) DO NOTHING<br/>RETURNING id"]
    F -- row inserted --> G["200 applied:true<br/>effect happened"]
    F -- conflict --> H{stored content_hash == this hash?}
    H -- yes --> I["200 applied:false deduplicated:true"]
    H -- no --> J[409 content_conflict<br/>same identity, different body]
```

The insert *is* the deduplication check. Because the effect and its dedup record are written by one
statement, there is no check-then-write window for two concurrent duplicates to race through - which is
the exact bug a `SELECT ... then INSERT ...` receiver has. `content_hash` is
`sha256(canonicalJson(envelope))`, so a duplicate is recognized as harmless while a same-identity
*different-content* delivery - which can only mean a bug or an attack - is a 409.

Because the record lives in PostgreSQL, dedup survives a receiver restart. `test/acceptance/t4` proves
it with a real restart: bind the same port, replace the process and its connection pool, keep the same
clock - two or more HTTP attempts, and one durable effect whose `applied_at` and `content_hash` are
unchanged across the restart.

Request recording (`receiver_requests`) is deliberately best-effort and never changes the response:
instrumentation must not be able to alter a delivery outcome.

---

## 24. Tenant isolation model

Threat: an authenticated tenant probing, naming or mutating another tenant's resources.

* **Identity comes from the token, never the body.** A tenant id is not a request field at all; the
  guard resolves `Bearer` → SHA-256 hash → `auth_tokens` row → `Principal`, and every service call
  takes `tenantIdOf(principal)`.
* **404 for both "does not exist" and "not yours".** `loadOwnedEndpoint` fetches by id and compares
  `tenant_id` in application code, throwing the same `notFound` either way. Malformed UUIDs short-circuit
  to the same 404 - a probe cannot distinguish "typo" from "exists elsewhere".
* **Isolation is in the query, not the filter.** `GET /deliveries` builds `WHERE tenant_id = $1` before
  any optional predicate, so forgetting it is not possible without breaking the function signature.
* **Privilege levels do not mix.** Tenant token on `/ops/*` → 403; operator token on a tenant route →
  403; anonymous → 401. `/ops/status` is intentionally *not* tenant-scoped (it describes the shared
  queue), which is why it is operator-only: a tenant must not infer another tenant's volume from it.
* **Secrets are never readable.** `url` and `secret` are excluded from every response type; the worker
  reads them server-side at dispatch time.
* **Idempotency keys are tenant-scoped.** Uniqueness is `(tenant_id, operation, idempotency_key)`, so two
  tenants may legitimately reuse `retry-1` and neither can see or block the other's record.

Proven in `test/integration/auth.spec.ts` and `test/acceptance/t2` (including the identical-response
check, so isolation does not leak through response *shape*).

---

## 25. Outbound safety and SSRF

`WebhookClient.dispatch` is the only code in the system that makes outbound HTTP calls.

* **POST only**, `content-type: application/json`, `content-length` set, body = the exact persisted bytes.
* **Redirects are never followed.** `node:http.request` does not follow them by default and the client
  classifies any 3xx as `NON_RETRYABLE` with `error_code='redirect'`. `test/acceptance/t9` proves the
  strong version: a live second server sits at the `Location` target and records **zero** requests.
  Otherwise the delivery worker would be a general-purpose SSRF proxy - pointed at internal addresses by
  whoever controlled a receiver's response.
* **Hard total timeout** (`WEBHOOK_TIMEOUT_MS`, default 2s) covering connect, response and body; expiry
  destroys the socket. A per-request `setTimeout` alone would not bound the body read.
* **Bounded response capture** (4 KiB). Reading stops and the request is settled at the bound, so a
  hostile or chatty destination cannot make a worker buffer unboundedly. The same 4 KiB is the bound on
  what is *retained*: secret redaction runs **before** truncation, because the `[redacted:…]` replacement
  is longer than the word it hides, and `test/integration/webhook-client.spec.ts` asserts
  `octet_length(response_snippet) <= 4096` in the table rather than only the returned string.
* **Caller headers are never forwarded.** The only outbound headers are `content-type`,
  `content-length` and the five identity headers. The inbound `Authorization` token is *never* copied -
  proven at the wire in `t3` (`authorization` absent from the captured request).
* **Destination is server-side data only.** `endpoints.url` is read at dispatch time; the request schema is
  `.strict()`, so `{"url": ...}` is a 400 rather than a silently ignored field. Ignoring it would let a
  caller believe they had steered the delivery.
* **Optional network-level allowlist.** `WEBHOOK_ALLOWED_HOSTS` pins dispatch to explicit `host[:port]`
  values; a violation raises `DestinationNotAllowedError` **before the socket opens**, and is classified
  `NON_RETRYABLE` because misconfiguration must not burn the retry budget. Compose sets
  `WEBHOOK_ALLOWED_HOSTS=receiver:4000`.
* **Scheme restriction**: `http:`/`https:` only; anything else is `unsupported_scheme`, non-retryable.
* **URLs are redacted** in error messages (protocol + host + port only) so userinfo and query strings
  cannot leak into logs.

Remaining production hardening (honest): the allowlist matches **hostnames**, so a public hostname that
resolves to an internal address is still reachable. A real deployment needs DNS-level or egress-proxy
enforcement, private-link destinations, and (for `https:`) certificate validation with a pinned CA
store.

---

## 26. Error model

Every error is `{ "code": ..., "message": ..., "requestId": ... }`. No stack traces, no SQL, no
driver messages, no secrets - `AllExceptionsFilter` (`src/api/exception.filter.ts`) maps
`HttpError` → its status/code/message, Nest `HttpException` and body-parser-style errors
(`entity.too.large` → 413, `entity.parse.failed` → 400) → the same envelope, and anything unexpected →
500 `internal_error` with the cause logged server-side against the `requestId`.

| HTTP | `code` | Raised by |
|---|---|---|
| 400 | `bad_request` | missing/oversized `Idempotency-Key`, schema violations, bad query params |
| 401 | `unauthorized` | missing/unknown bearer token; receiver-side signature failures use the receiver's own 401 codes |
| 403 | `forbidden` | privilege mismatch (tenant on `/ops/*`, operator on tenant routes), test controls disabled |
| 404 | `not_found` | unknown or cross-tenant resource, malformed UUID |
| 409 | `conflict` | idempotency key reused with different input; redrive on a non-`DEAD` delivery |
| 413 | `payload_too_large` | body over 64 KiB |
| 500 | `internal_error` | anything unexpected |
| 503 | `service_unavailable` | readiness probe: database unreachable |

`X-Request-Id` is echoed on **every** response, including successes and 413s (the middleware runs
before the body parser). Success bodies carry no `requestId` - correlation belongs to the header and the
error envelope.

---

## 27. Observability: logs and counters

**Structured logs** (pino, JSON lines). Correlation fields are `requestId`, `eventId`, `deliveryId`,
`attemptId`, plus `attemptNumber`, `leaseGeneration` and `owner` on worker lines. `createLogger` also
redacts defensively: `secret`, `*.secret`, `authorization`, `token`, `*.token`, `x-webhook-signature`,
`payload`, `*.payload` all print as `[redacted]` even if a caller passes them by mistake. What is logged
is bounded identifiers and codes - never secrets, never full payloads. Notable lines: `delivery
transitioned`, `stale worker: completion fenced out (lease lost)`, `processor threw; recording UNKNOWN`,
`completion failed; lease will expire`, `shutdown grace elapsed; abandoning in-flight work`.

**`GET /ops/status`** (operator token) reads everything from durable state, never from worker memory, so
a restarted process reports the same numbers:

```json
{
  "snapshotAt": "2026-01-01T00:00:15.000Z",
  "counts": { "ready": 0, "inFlight": 1, "retryWait": 3, "delivered": 12, "dead": 2 },
  "oldestPending": { "deliveryId": "77ab...", "state": "RETRY_WAIT",
                     "scheduledFor": "2026-01-01T00:00:03.121Z", "overdueMs": 11879 },
  "expiredLeases": 0,
  "workers": [ { "owner": "worker-a", "inFlight": 1 } ]
}
```

Design details worth defending:

* The whole snapshot runs in one **`REPEATABLE READ` transaction** so it is a single MVCC snapshot: a
  delivery transitioning mid-read cannot be counted as both `DEAD` and `READY` in one response.
* Time comparisons use the **database clock** (`now()`), never one process's clock against another's
  timestamps - that is how an "age" metric goes negative under clock skew.
* `oldestPending` is shaped to match `deliveries_due_idx`, so "what is the queue stuck on?" reads a
  partial index instead of the table.
* `expiredLeases > 0` means work is **parked, not lost** - a worker died mid-attempt and recovery has
  not reached it yet. That is the single most useful alerting signal in the system.
* All five states are listed explicitly in the response, so a state with no rows reports `0` rather than
  disappearing, and a future state added without updating the mapping is a compile error.

Suggested alerting: `expiredLeases` non-zero for > 2 lease TTLs; `oldestPending.overdueMs` past an SLO;
`counts.dead` rising; `workers[].inFlight` pinned at the concurrency bound while `ready` grows.

---

## 28. Testing strategy: determinism without mocks

**Nothing simulated that matters.** Every integration and acceptance suite runs against a real
PostgreSQL and real HTTP sockets - including the receiver, which is the actual `src/receiver` process
code, not a stub. The two things that make the suite fast and deterministic are injected:

* **`FakeClock`** - one shared instance per scenario, used by the API app, the worker loop and the
  receiver. "Advance to the retry instant" is a function call, so a 4-second backoff costs 0ms of real
  time and an assertion can demand the *exact* due timestamp.
* **`FakeRandom([0])`** - jitter becomes 0, so the backoff ladder is asserted arithmetically.

Consequences of a frozen clock that the suites handle explicitly, because each one is otherwise an
unfalsifiable claim:

* Waits poll the **database for a durable fact** (`waitForAttempts`, `waitForState`), never sleep.
* A retry cannot be claimed until something moves the clock to its due instant, so tests call
  `advanceToDue` - that is the test harness acting as the wall clock.
* `waitForAttempts(n)` waits for the nth attempt to carry an **outcome**, not for a row to exist: the row
  is allocated before dispatch, so counting rows would race the HTTP call.

Crash modelling, stated honestly: a crash is "work is durably committed, and then the code that would
have moved it on simply never runs" - no completion write, no shutdown drain, nothing faked. Recovery is
performed by a **different worker identity** through the queue's own rules. The injected write failures in
`t9` are real database errors raised by plpgsql triggers on `deliveries`, so the rollback proven is
Postgres' own.

Two harness pieces worth knowing about:

* `test/helpers/delivery-loop.ts` - the whole end-to-end loop (worker + receiver + queue + API publish)
  with one shared clock and DB-polled waits.
* `test/helpers/capture-endpoint.ts` - a real HTTP destination that keeps the raw bytes, parses the
  identity headers, recomputes the HMAC and counts genuinely concurrent in-flight requests. The mock
  receiver proves *receiver-side* semantics; this answers "what did **our** service actually put on the
  wire?"

`resetDatabase()` runs per suite (jest file order is random, so no suite may depend on another's
leftovers). `test/global-setup.ts` recreates the database from empty before each run.

One finding that changed an assertion: replayed idempotency responses come back from a `jsonb` column
with reordered keys. The original suite compared raw bytes and failed - correctly. Semantic equality
(`canonicalJson`) is the right contract for a replayed response, and the harness now documents it.

---

## 29. Test results (actual)

Command: `npm test` (jest, `--runInBand`), against PostgreSQL 14.13 (Homebrew) on the host.

```
Test Suites: 25 passed, 25 total
Tests:       233 passed, 233 total
Snapshots:   0 total
Time:        15.564 s
```

This is the run that immediately follows the failed bare-`npx jest` attempt recorded in §4, on the tree
after the final audit fixes - so the number describes the shipped code, not an earlier snapshot of it.
The suite and test counts are deterministic; the wall-clock moves by a second or two between runs (the
runs recorded here measured 16.1 s, 17.0 s under `--coverage`, 15.6 s, and 16.2 s re-run after the Postman
collection was added - the collection itself is not in that count).

`npm run typecheck` (tsc over src **and** test): clean. `npm run lint` (ESLint, `no-unused-vars` as
error, explicit module boundaries): clean. `npm run build`: clean.

The [Postman collection](#36-postman-collection) is separate, manual evidence and is **not** part of
`npm test`. Run against the live four-process host stack with `npx newman@6`: 77 request executions, 77
assertions, 0 failed. The executed-request count moves between runs because two requests poll for
asynchronous states; `0 failed` is the claim, not the total.

Per suite:

| Suite | Tests | What it establishes |
|---|---|---|
| `test/unit/common.spec.ts` | 7 | `canonicalJson` (object key order irrelevant, array order significant), byte truncation on character boundaries, id/request-id shapes |
| `test/unit/webhook-signing.spec.ts` | 19 | HMAC vector over `timestamp + "." + bytes`, lowercase hex, timing-safe verify, tamper/wrong-secret/length-mismatch fail closed |
| `test/unit/retry-policy.spec.ts` | 9 | ladder 1s/2s/4s/8s + jitter, 5-attempt exhaustion → DEAD, `max(backoff, Retry-After)` capped at 60s, `SUCCESS`→DELIVERED, `NON_RETRYABLE`→DEAD, UNKNOWN retried |
| `test/unit/migrations.spec.ts` | 4 | migration discovery without a database: the shipped ordered set, the `NNN_name.sql` pattern, unreadable directory refused, duplicate version named |
| `test/integration/auth.spec.ts` | 10 | 401/403 matrix, token-hash lookup, tenant scoping of every route |
| `test/integration/events.spec.ts` | 22 | atomic publish, 202 shape, strict schema (every non-object payload shape: array, string, number, boolean, null), 413, ownership 404, event read model |
| `test/integration/logging.spec.ts` | 3 | the fields the publication call site hands the logger (request/event/delivery/endpoint ids only), a generated request id when the caller sends none, and no payload bytes, endpoint secret or API token anywhere in the logged fields |
| `test/integration/idempotency.spec.ts` | 8 | claim-before-work, replay, 409 on changed input, key not consumed by failures, tenant-scoped keys |
| `test/integration/deliveries.spec.ts` | 14 | keyset pagination stability, state filter, limit bounds, no secret/URL leakage |
| `test/integration/worker-claim.spec.ts` | 10 | `SKIP LOCKED` no double-claim, lease fields, attempt allocated pre-dispatch, expired-lease recovery |
| `test/integration/worker-fencing.spec.ts` | 3 | fenced completion `applied:false`, truthful attempt history under a lost lease, interleaved claims |
| `test/integration/webhook-client.spec.ts` | 14 | exact bytes, five headers, fresh valid signature, no auth forwarding, no redirect following, total timeout, honest classification, and the 4 KiB response bound from both sides: the client stops *reading* at the limit instead of buffering it, and the snippet stays ≤4 KiB on the wire and in `delivery_attempts.response_snippet` even when redaction expands the text |
| `test/integration/receiver.spec.ts` | 30 | verification order, freshness ±300s, dedup identity, `content_conflict`, all eight modes, control surface gating |
| `test/integration/delivery-loop.spec.ts` | 15 | full loop: success, retry to recovery, exhaustion to DEAD, redrive, restart preserving schedule |
| `test/integration/redrive.spec.ts` | 15 | DEAD-only, cycle semantics, budget preservation, audit row, idempotent replay, concurrency |
| `test/integration/ops-status.spec.ts` | 10 | counters, snapshot consistency, `oldestPending`, `expiredLeases`, operator gating |
| `test/acceptance/t1-publication-concurrency.spec.ts` | 4 | **PDF test 1** |
| `test/acceptance/t2-ownership-validation.spec.ts` | 15 | **PDF test 2** |
| `test/acceptance/t3-signature-verification.spec.ts` | 3 | **PDF test 3** |
| `test/acceptance/t4-lost-response-restart.spec.ts` | 3 | **PDF test 4** |
| `test/acceptance/t5-retry-scheduling.spec.ts` | 2 | **PDF test 5** |
| `test/acceptance/t6-crash-boundaries.spec.ts` | 3 | **PDF test 6** |
| `test/acceptance/t7-stale-worker.spec.ts` | 2 | **PDF test 7** |
| `test/acceptance/t8-redrive-concurrency.spec.ts` | 3 | **PDF test 8** |
| `test/acceptance/t9-bounds-rollback.spec.ts` | 5 | **PDF test 9** |

Coverage, measured with `npx jest --coverage --runInBand` over the same 233 tests:

| Scope | Stmts | Branch | Funcs | Lines |
|---|---|---|---|---|
| **all files** | **88.70%** | **76.55%** | **89.32%** | **89.22%** |
| `src/worker` | 91.78 | 79.31 | 93.33 | 93.18 |
| `src/modules/events` | 98.75 | 88.88 | 100 | 98.68 |
| `src/modules/idempotency` | 93.87 | 87.50 | 91.66 | 95.55 |
| `src/modules/operations` | 98.96 | 88.23 | 100 | 98.88 |
| `src/modules/deliveries` | 96.15 | 87.09 | 100 | 95.94 |
| `src/modules/webhooks` | 91.93 | 71.01 | 92.85 | 91.22 |
| `src/modules/auth` | 97.56 | 86.36 | 100 | 98.57 |
| `src/receiver` | 89.69 | 82.50 | 94.11 | 90.38 |
| `src/common` | 87.20 | 59.45 | 84.21 | 87.73 |
| `src/config` | 82.60 | 37.50 | 83.33 | 81.81 |
| `src/api` | 77.96 | 48.78 | 86.66 | 79.24 |
| `src/db` | 55.79 | 37.03 | 60.00 | 55.30 |

Two readings of that table matter more than the number itself:

* **`src/db` understates reality.** `migrate.ts` shows 35.21% (up from 12.3% once its discovery rules got
  unit tests) because `migrateUp()`'s body, `migrateDown()` and the CLI `main()` run from jest's
  `globalSetup` - code istanbul does not instrument. The *function* is exercised on every run (the suite
  would not boot without it), and the CLI wrappers were driven by hand against a throwaway database
  (`migrate:down`, `migrate:up`, `migrate:up` again, `seed`, `seed` again) as recorded in §36.
* **The gaps are where I would expect them.** `retry-policy.ts` and `delivery-queue.ts` are at 100%
  statements - the two files that decide state transitions. `api/exception.filter.ts` (52.7%) is
  uncovered in the branch that classifies a *foreign* `HttpException`; every typed error the app raises
  (401/403/404/409/413) is asserted, and `common/logger.ts` redaction and `domain/types.ts` are at 100%.

The requirement map in §30 remains the primary evidence: a percentage says lines were executed, not that
the guarantee holds.

---

## 30. Acceptance evidence map (PDF tests 1-9)

The PDF's requirement: "Use a real database and actual HTTP mock for worker and recovery tests. Assert
durable state, attempt history and receiver effect counts, not only API responses." Each row is the
verbatim requirement and where it is proven.

| # | PDF requirement (verbatim) | Proven by |
|---|---|---|
| 1 | "Twenty identical concurrent publications produce one event and delivery. Reordered object keys replay correctly; changed payload returns 409. Separate tenants may reuse the same key." | `t1`: 20 concurrent publications → 1 event, 1 delivery, 1 claim, 1 attempt, 1 receiver request, 1 effect; 20 distinct request ids; reordered nested keys replay; changed payload 409 with counts unchanged; cross-tenant key reuse |
| 2 | "Reject cross-tenant access, invalid input, oversized bodies and unauthorised redrives. A caller cannot replace the configured destination." | `t2`: invalid-input table (400 + zero durable rows), 413, cross-tenant 404 with identical shape, `url`/`destination`/`secret` fields rejected then the corrected body accepted under the same key, tenant token 403 / anonymous 401 on redrive with the DEAD row untouched, delivery dispatched only at the **configured path** while a diverted endpoint records nothing |
| 3 | "Accept valid raw bytes; reject body tampering, a wrong secret and stale timestamps. A valid retry has a fresh timestamp and stable event identity." | `t3`: captured wire bytes verified independently; attempt 1 vs 2 same `eventId`/`deliveryId`, distinct `attemptId`, timestamps exactly `t` and `t+1`, distinct signatures, both valid, bodies byte-identical to `envelope_bytes`; replay at +301s → 401 `stale_timestamp` and **0 effects**; re-signed → 200 with 1 effect; tampered body / wrong secret → 401 |
| 4 | "Commit the receiver effect and drop the response. Restart the receiver and retry. Assert two or more HTTP attempts but exactly one durable effect." | `t4`: `lost_response` → attempt 1 `UNKNOWN`/`transport_error` with the effect already durable → restart on the **same port** with a new pool → `DELIVERED`; outcomes `['UNKNOWN','SUCCESS']`, 2 requests from 2 processes, 1 effect with unchanged `applied_at`/`content_hash`; plus a slow-then-killed variant and a both-sides-restart variant |
| 5 | "Verify 503 recovery, 429 delay, five-attempt exhaustion and immediate DEAD on 400. Restart during RETRY_WAIT and preserve schedule and budget." | `t5` (mixed cycle: 429 `Retry-After: 20` → due +20s, then 503 → due +22s from the lifetime ladder, then success) + `delivery-loop.spec.ts` / `retry-policy.spec.ts` for exhaustion→DEAD and 400→immediate DEAD; `t5` second test stops the worker during `RETRY_WAIT` and proves the replacement cannot attempt early, keeps `cycle 1` and finishes with `attempt_count 3` |
| 6 | "Crash after publication commit and before dispatch; recover accepted work. Crash after dispatch but before completion commit; recover without losing history or duplicating receiver effects." | `t6`: 202 with no worker → row `READY` due at `BASE_MS`, 0 attempts, 0 requests → a later worker delivers; manual claim + real dispatch + **no completion write** → leased `IN_FLIGHT`, attempt 1 `UNKNOWN`/`finished_at NULL`, 1 effect already durable → lease expiry → recovery worker: attempts `[1,2]`, owners `['worker-crashed','worker-recovery']`, generations 1→2, **2 requests / 1 effect**; plus mid-cycle crash continuing the **same cycle** |
| 7 | "Pause worker A beyond its lease, let B recover, then resume A. Assert fenced local updates and a single receiver effect, even if duplicate HTTP requests occur." | `t7` (real HTTP, lease TTL 300ms vs 1200ms dispatch) + `worker-fencing.spec.ts` (deterministic SQL-level fencing): A's late write `applied: false`, both attempt rows kept with their own generations, final `DELIVERED` with `lease_owner NULL`, 2 requests with 2 attempt ids, **1 effect**, and provably no third dispatch |
| 8 | "Concurrent redrives of DEAD produce one retry cycle. Replay does not reset its budget; changed input under the same key conflicts. DELIVERED cannot be redriven." | `t8`: six concurrent same-key redrives → all 202 with one identical body, 6 request ids, **one** `READY` cycle-2 reset, **one** audit row, then delivered by a replacement worker with cycles `[1,1,1,1,1,2]`; replay while cycle 2 is mid-budget → byte-equal snapshot, row unchanged, audit still 1; same key different reason → 409 row unchanged; `DELIVERED` → 409 and still 1 request / 1 effect / 1 attempt |
| 9 | "Prove per-worker concurrency and timeout bounds. Reject redirects without contacting their target. Inject a local write failure and prove atomic rollback of event creation or completion state." | `t9`: 12 deliveries at concurrency 4 with responses held 120ms → destination-measured peak concurrency `<= 4` and `> 1`; 1.5s response vs 150ms timeout → `RETRYABLE` `timeout` with `http_status NULL` in under 1s real, then success on retry; 302 with a **live** `Location` target → `DEAD`/`redirect` with origin 1 request and target **0**; trigger-injected failure on `INSERT deliveries` → 500 `internal_error` with no stack/trigger detail, `{events:0,deliveries:0,claims:0}`, and the same key succeeding after the trigger is dropped; trigger-injected failure on the completion UPDATE → `completeAttempt` rejects, row stays `IN_FLIGHT` with its lease and the attempt stays `UNKNOWN`/`finished_at NULL`, then the same fenced completion applies after the trigger is dropped |

---

## 31. Design decisions and trade-offs

| Decision | Alternative | Why this one |
|---|---|---|
| **Postgres is the queue** (`FOR UPDATE SKIP LOCKED`) | Redis / Kafka / RabbitMQ / BullMQ | The delivery state, the attempt history, the idempotency record and the queue must commit **together**. A broker adds a second copy of the truth whose sync *is* the bug class this exercise is about. Also: the brief forbids the extra infrastructure. Cost: ~10k deliveries/s ceiling and polling latency, both acceptable here |
| **Two-phase transaction** (claim+allocate / dispatch with no transaction / fenced completion) | one transaction spanning the HTTP call | A transaction open across a 2s network call holds row locks and pool connections for the duration; with any real concurrency that is a outage. Cost: an extra round-trip per attempt, and a window where a dispatch is unacknowledged - which is exactly what leases + fencing + receiver dedup are for |
| **Attempts allocated before dispatch** | written after the response | If the process dies mid-call, "we sent something" is still on record. Without this, history silently under-reports and a duplicate looks like a first attempt |
| **`bytea` envelope, serialized once** | re-serialize per attempt | Re-serialization changes bytes, which invalidates the signature and makes the body a receiver deduped on differ from the one it verified. Also cheap integrity: `envelope_hash` |
| **Fencing token in the state write** | lease expiry alone | A lease cannot bound a pause. Generation numbers turn "who may write" into a `WHERE` clause the database evaluates atomically |
| **Receiver dedup as a UNIQUE constraint** | check-then-insert | The check-then-write window is where two duplicates both apply. `INSERT ... ON CONFLICT DO NOTHING RETURNING` collapses the check and the write into one statement |
| **`UNKNOWN` as a first-class outcome** | treat a timeout as failure or success | A timeout has no answer. Recording the absence keeps the retry schedule honest and preserves the possibility that the receiver already committed |
| **Validation failures roll back the idempotency claim** | consume the key on failure | A caller with a typo must not permanently poison their own key. Cost: a failed call cannot be "replayed" - but it never produced a resource either |
| **NestJS with symbol-token DI, raw `pg`** | Express + an ORM | Guards/filters/DI keep tenant isolation structurally impossible to forget; an ORM would hide the exact SQL (`SKIP LOCKED`, `ON CONFLICT`, fenced `UPDATE`) that the whole design is built on. No `synchronize`, ever |
| **`.strict()` request schema** | ignore unknown fields | A silently dropped `url` teaches the caller that they steered the destination. Rejecting it states where destinations actually come from |
| **Polling instead of `LISTEN/NOTIFY`** | notification-driven claiming | `NOTIFY` is not durable: a worker can miss one while reconnecting and then depend on a repair sweep anyway. Polling is one mechanism whose correctness never depends on a signal being delivered. Cost: up to `WORKER_POLL_INTERVAL_MS` of latency |

---

## 32. Limitations

Honestly listed, in rough order of how much they would matter in production:

1. **Single-node Postgres.** No HA, no replica reads, no failover. The queue's durability *is* the
   database's durability.
2. **Throughput ceiling.** Due-scan claiming costs one indexed query per claim per worker; a
   `LIMIT 1`-per-claim design does not scale to very high volumes without batching. `claimBatchSize`
   bounds the batch, not the row.
3. **No tenant fairness.** A few noisy tenants can occupy every permit. `ORDER BY next_attempt_at` is
   deliberately tenant-blind, so one tenant's backlog can delay another's. Fix: per-tenant due slots or
   a weighted claim (`ROW_NUMBER() OVER (PARTITION BY tenant_id)` with a per-tenant cap).
4. **No global rate limiting** (per-destination or per-tenant). Two workers × 4 permits is the only
   bound, so scaling workers scales load onto a receiver that may be asking for slower via `Retry-After`.
5. **No endpoint registration API** - by design. Destinations and secrets are seeded configuration,
   which is what makes the SSRF surface small. A real product needs an authenticated registration flow
   with ownership verification (e.g. a signed challenge) and per-tenant secret storage.
6. **No secret rotation implementation** (design discussion only, §34). One secret per endpoint; a
   rotation would break in-flight signed attempts.
7. **No retention or archival.** `receiver_requests` and `delivery_attempts` grow without bound.
8. **Schema migrations are forward-only in effect**: the runner neither detects a version gap nor
   enforces ordering discipline, `migrate down` is not exercised by CI as a safe-deployment path, and
   there is no online-migration (`CONCURRENTLY`, expand-then-contract) discipline (§34).
9. **Polling latency floor** of `WORKER_POLL_INTERVAL_MS` (250ms) for a due-but-unclaimed delivery.
10. **`Retry-After` HTTP-date form unsupported** - falls back to normal backoff.
11. **Per-process concurrency bound only.** System concurrency = workers × 4; there is no shared
    semaphore, so a deployment with 10 workers sends 40 concurrent requests.
12. **`DestinationNotAllowedError` is non-retryable by design**, which means a temporary allowlist
    misconfiguration requires a redrive rather than an automatic retry.
13. **SSRF defence is hostname-based**, so a public hostname resolving to an internal address still
    passes (§25).
14. **`docker compose up --build` needs a working BuildKit/buildx.** The stack itself was run
    end-to-end (§3), but on the machine this was verified on `~/.docker/buildx/current` is owned by
    root, so Compose's build step aborts with `permission denied`. The image was therefore built with
    `DOCKER_BUILDKIT=0 docker build -t reliable-webhook-delivery:local .` and started with
    `docker compose up --no-build`. Same Dockerfile, same tag, same result — a reviewer on a clean
    install can use the one-line `up --build`.
15. **Test suites share one database and must run `--runInBand`.** This is not hypothetical: bare
    `npx jest` on this machine failed 18 of 25 suites (67 of 233 tests) through cross-suite interference
    (deadlocked `TRUNCATE … CASCADE`, teardowns deleting a neighbour's rows) while `npm test` passed 233
    of 233 seconds later — see §4. Parallelising it properly means one database per jest worker.
16. **Coverage is reported, not enforced** (§29). There is no `coverageThreshold` in `jest.config.js`
    and no `collectCoverageFrom`, so a run only instruments files the suite actually loads and a drop in
    the percentage would not fail anything. The gate in this repository is the 233 tests, not the
    number.

---

## 33. Production considerations

* **TLS on the API edge.** The service terminates nothing; a reverse proxy must provide TLS, and
  `https:` destinations should be validated with CA pinning. Bearer tokens over plain HTTP are a leak.
* **Secret storage.** Endpoint signing secrets belong in a KMS/secrets manager with per-tenant
  encryption, never in a plaintext column or an env file. `endpoints.secret` is a deliberate
  simplification for a self-contained demo - it is the first thing a real deployment replaces.
* **Token storage.** `auth_tokens` keeps only a SHA-256 hash. For real traffic, hash is the floor, not
  the ceiling: prefer per-token derived keys with rotation and revocation records.
* **Network egress.** Run workers in a subnet whose only egress is an allowlisting proxy; enforce the
  destination allowlist at the network layer, not only in the client. Publish no database port (compose
  already doesn't).
* **Least privilege.** DB role with DML only (no `SUPERUSER`, no `CREATE EXTENSION` at runtime);
  `security_opt: no-new-privileges`; separate service accounts for API, worker and receiver.
* **Body size and rate limits at the edge** in addition to the in-app 64 KiB cap, so a flood is not
  absorbed by the API process.
* **Payload content.** Business payloads are stored verbatim in `events.payload` and `envelope_bytes`;
  production needs PII classification, field-level encryption or a "reference, not content" payload
  design, plus a documented retention window.
* **Clocks.** Signature freshness is ±300s by contract. NTP everywhere; a skewed receiver clock fails
  valid signatures or accepts a 5-minute replay.
* **Observability backend.** JSON lines with `requestId` are already correlation-ready; ship them to a
  log pipeline and alert on `expiredLeases`, `oldestPending.overdueMs` and `counts.dead` (§27).
* **What the receiver must do** (the contract handed to a customer): read raw bytes, verify the
  timestamp window, recompute the HMAC in constant time, dedup on `(endpoint, X-Event-Id)` **durably**,
  return 2xx only after the effect is committed, and return a retryable status for a transient failure.

---

## 34. Discussion topics the brief asks about

Not implemented; this is how I would do each, and what I would protect against.

**Endpoint secret rotation.** Dual-secret windows are the only safe shape: store
`secret_current` + `secret_previous` + `active_from`/`retire_at`, sign with the current secret, and
verify against *both* while the previous is live. The trap is in-flight work: an envelope signed at
publication and retried hours later (t3 shows the retry path re-signs at dispatch time, which is why
rotation is survivable at all - the signature is always fresh, so rotation only needs to cover the
*verify* side). Add a receiver-facing `GET /endpoints/:id/jwks-like` publication of key metadata
(kid + validity), an alert when a receiver rejects with `invalid_signature` at a rate that spikes on
rotation, and a forced re-verification test before retiring the old secret. Never rotate by editing a
row while workers hold connections: they read the secret per attempt, so a deploy-time cache would need
busting.

**Tenant fairness.** Three levels: (1) make the claim query per-tenant-fair - rank due rows by
`(tenant_id, next_attempt_at)` and take one per tenant round-robin, so no tenant is starved by
another's backlog; (2) a per-tenant inflight/reservation cap so a noisy tenant cannot hold every permit;
(3) token-bucket egress per tenant and per destination, with `Retry-After`-driven backoff stored
per-destination so one slow receiver throttles only its own tenant. Cost to watch: per-tenant fairness
reduces batching efficiency, and Postgres-side `ROW_NUMBER()` over the due set is expensive without a
`(tenant_id, next_attempt_at)` partial index.

**Retention.** Event/delivery/attempt/request data is hot for days and audit-grade for years. Shape:
time-partition `delivery_attempts` and `receiver_requests` monthly, `pg_drop` expired partitions (cheap,
no `DELETE` bloat), move terminal deliveries older than N days to a columnar archive by
`created_at`, keep `deliveries` for live state only. The constraint that must not be broken: retention
must never delete a delivery whose `state` is non-terminal or whose receiver dedup identity could still
be replayed - so `receiver_effects` retention has to be *longer* than the total retry+redrive window,
otherwise a very late duplicate applies a second effect. That coupling is the one I would want reviewed
by the business, not decided by an engineer.

**Safe deployment migrations.** Rules: (1) expand-then-contract - a release only ever *adds*
nullable columns/indexes; drops and type changes land in a later release once no deployed code uses
them; (2) `CREATE INDEX CONCURRENTLY` (outside a transaction) so a hot table isn't locked; (3) no
`SET NOT NULL` without a prior validated backfill plus `CHECK` constraint as an interim; (4) enum
evolution: `ALTER TYPE ... ADD VALUE` before any code that uses the value, never inside the same
transaction that uses it on older Postgres; (5) a migration runner that holds an advisory lock so two
API instances cannot both migrate; (6) gate rollout on the runner (compose already has the one-shot
`bootstrap` pattern - in production that becomes a pre-deploy job with a lock and a rollback plan);
(7) every migration tested by the suite from an empty database (which `global-setup` already does) and
by a rehearsal against a production snapshot, including the *down* path.

---

## 35. Repository layout

```
src/
  api/            Nest app: bootstrap, DI tokens, guards/filters wiring, /health, request-id
  common/         clock (System/Fake), random, logger, semaphore, canonical JSON, hash,
                  bytes, ids, error model          <- framework-free utilities
  config/         env schema + validator, .env loader
  db/             pg pool + Database wrapper, migration runner, deterministic seed
  domain/         DeliveryState / AttemptOutcome / envelope types shared by every layer
  modules/
    auth/         bearer -> Principal, guard, @OperatorOnly/@Public, principal types
    events/       POST /events, GET /events/:id, atomic publish, strict DTO
    idempotency/  claim-before-work, fingerprint, replay/conflict semantics
    deliveries/   tenant-scoped keyset listing
    operations/   operator redrive (+audit) and /ops/status counters
    webhooks/     envelope builder, HMAC sign/verify, outbound client (timeout,
                  no-redirect, bound, allowlist, outcome classification)
  worker/         DeliveryQueue (claim/complete SQL), RetryPolicy, DeliveryWorker
                  (semaphore, lease, drain), processor, process entrypoint
  receiver/       standalone mock endpoint: handler, durable repo, modes, entrypoint
migrations/       001 core, 002 receiver, 003 auth tokens, 004 listing index
test/
  unit/           canonical JSON, HMAC, retry policy
  integration/    per-milestone suites against real Postgres + real HTTP
  acceptance/     t1..t9 - the PDF acceptance proofs
  helpers/        app factory, db helpers, receiver, delivery loop, capture endpoint, fixtures
docs/             IMPLEMENTATION_PLAN.md, DESIGN.md
Dockerfile        multi-stage: build -> prune dev deps -> runtime as non-root
docker-compose.yml  postgres + bootstrap + api + worker-a + worker-b + receiver
.env.example      every variable, placeholders only, no real credential
postman/          webhook-delivery.postman_collection.json (63 requests, all asserted)
                  webhook-delivery-local.postman_environment.json (non-secret values only)
```

Dependency direction is deliberate: `common` → `db`/`config` → `domain` → `modules` → `api`/`worker` /
`receiver`. The worker and the receiver import no Nest code; the API owns the DI container. That is why
`src/worker/worker-main.ts` can be run as a plain `node dist/worker/worker-main.js` process.

---

## 36. Postman collection

`postman/webhook-delivery.postman_collection.json` is a Collection v2.1 file that exercises every route
the service exposes, from both sides of the wire: **63 requests across 8 folders, and every one of them
carries test assertions** (no click-and-eyeball requests). It is the same surface the automated suite
covers, but readable as a script of what a reviewer can do by hand after `docker compose up`.

| Folder | What it proves |
|---|---|
| `00 Health & readiness` | API and receiver are separate processes, each with its own `/health` |
| `01 Publish + idempotency` | 202 with exactly four keys, byte-identical replay, 409 on a reused key with a different body, 400 without the header, 401 without a token |
| `02 Event status + tenant isolation` | owner reads 200, other tenant reads **404** (no existence leak), never-published UUID 404, malformed id 404, operator token 403 |
| `03 Delivery listing (keyset pages)` | page 1 → `nextCursor` → page 2, state filter, tenant B sees only its own rows, 400 on `limit=101` / bogus state / garbage cursor |
| `04 Payload + outbound security contract` | caller-supplied `url` and caller-supplied secret are **rejected**, non-object payload rejected, over-long `eventType` rejected, another tenant's endpoint 404, >64 KiB body 413 |
| `05 Drive the retry engine (receiver failure modes)` | arms `temp_failure` for three counted attempts, publishes a probe event, reads its delivery snapshot and then `/__control/requests` to prove the retries physically arrived; then arms `rate_limited` (with `Retry-After: 30`), `perm_failure`, `lost_response`, `slow`, `reject_400` and `redirect` in turn, and finally disarms. Those six assert that the control plane accepted the mode - see the note below on why they are not each driven to a terminal state |
| `06 Operator: queue status + redrive` | `/ops/status` authorization matrix, then a full DEAD → redrive → DELIVERED cycle: arm `reject_400`, publish a doomed event, wait for DEAD **on exactly one attempt**, capture the id, disarm, redrive once, replay the same `Idempotency-Key` and assert the cycle counter did **not** move, then 409 on a non-DEAD delivery and 403/400/404 on the redrive route |
| `07 Receiver: signature + dedup, called directly` | the receiver's own contract, bypassing the worker: a correctly signed call applies the effect, the same ids on a new attempt return 200 **deduplicated**, the same ids with different content return 409 `content_conflict`, a bad signature and a stale timestamp both return 401, an unknown endpoint returns 401 |

### What is prefilled, and what is deliberately empty

Nothing secret is committed. Five variables ship with values - `baseUrl`, `receiverUrl`, and the three
seeded endpoint UUIDs (`endpointA1`, `endpointA2`, `endpointB1`), which are deployment data the seed
writes and are not credentials. The four variables that hold credentials ship **empty on purpose**:

| Variable | Where to get it |
|---|---|
| `tenantAToken` | `TENANT_A_TOKEN` in your local `.env` (or `src/db/seed.ts` for the default) |
| `tenantBToken` | `TENANT_B_TOKEN` |
| `operatorToken` | `OPERATOR_TOKEN` |
| `endpointA1Secret` | `ENDPOINT_A1_SECRET` - folder 07 HMACs its own request bodies with it |

Requests read all four with `pm.variables.get(...)` rather than `pm.collectionVariables.get(...)`, so the
value can come from either the collection layer or an environment layer. `postman/webhook-delivery-local.postman_environment.json`
declares only the non-secret keys; it omits the credential keys entirely on purpose, because an *empty*
environment variable would shadow a filled collection variable and produce a confusing wall of 401s.

### Running it

1. Start the stack (`docker compose up`, or the host run in [section 4](#4-quick-start-without-docker)) and seed it.
2. Import both files into Postman, select the imported environment, and paste the four credential values into the collection variables.
3. Run folders `00` → `07` **in order**. Order is load-bearing: each folder stores ids (`eventId`, `deliveryId`, `deadDeliveryId`, cursors) that later requests consume, and folders 05/06 arm receiver failure modes they also disarm.

Headless, the same run is CI-shaped without being part of CI:

```bash
npx newman run postman/webhook-delivery.postman_collection.json \
  -e postman/webhook-delivery-local.postman_environment.json \
  --env-var "tenantAToken=$TENANT_A_TOKEN" \
  --env-var "tenantBToken=$TENANT_B_TOKEN" \
  --env-var "operatorToken=$OPERATOR_TOKEN" \
  --env-var "endpointA1Secret=$ENDPOINT_A1_SECRET"
```

### Three things in the scripts worth explaining

**Waiting is a loop, not a sleep.** The delivery lifecycle is asynchronous, so folder 06 has to observe
states the worker produces after the request returns (a delivery reaching DEAD, then the redriven delivery
landing). Those two requests re-enter themselves via `setNextRequest` until the expected state appears,
capped at 200 polls. While still waiting they emit a *soft* test (`still waiting on the worker (poll N)`)
that only asserts the state is one of `READY / IN_FLIGHT / RETRY_WAIT / DEAD`, and on the final iteration
they assert the real expectation (`state DEAD` with `totalAttempts 1` and `lastErrorCode "http_400"`, or
`DELIVERED` on a higher cycle after redrive). A timeout therefore fails loudly on the assertion instead of
passing because the sleep happened to be long enough. Folder 05 takes the lighter approach: its poll
request reads the delivery once, logs the whole progression line to the console, and asserts only that the
state is one of the five documented values - enough to make the retry engine's work visible in the run
output without depending on which attempt the run happened to catch. `pm.setNextRequest` does not exist in
newman's sandbox (only the deprecated `postman.setNextRequest` does), so the scripts feature-detect both;
that is the one place the collection is written for two runtimes rather than one.

**Folder 06 creates its own precondition.** Rather than assuming a DEAD row exists in the database, it
arms `reject_400` on the receiver, publishes an event whose first attempt is therefore non-retryable, and
waits for exactly that delivery to reach DEAD with `totalAttempts 1` and `lastErrorCode "http_400"`. It
disarms *after* DEAD is observed, never before - an earlier version of this collection raced its own
cleanup, and the doomed delivery got rescued by the worker on a second attempt.

**Why folder 05 arms six modes without driving each to a terminal state.** The receiver stores one
endpoint-wide mode row keyed `(endpoint_id, '')` with `ON CONFLICT DO UPDATE`, so arming the next mode
overwrites the previous one: a single automated pass can only exercise the mode that is in effect when an
event is published, which here is `temp_failure`. The other six are a menu for hands-on use - in Postman,
run one arm request, then run folder 05's `POST /events - probe event` and watch folder 06's wait pattern
against it. Each of those behaviours *is* proven to a terminal state, but in the deterministic suites
([section 28](#28-testing-strategy-determinism-without-mocks) and the
[acceptance map](#30-acceptance-evidence-map-pdf-tests-1-9)), which use fake clocks to assert the exact
backoff without waiting 30 s for a real `Retry-After`. Reproducing all six end to end in the collection
would mean tens of seconds of wall-clock backoff per run, and one flakier duplicate of an assertion the
suite already makes - so the collection stops at showing the control plane accepts each mode.

### What a repeat run does to the database

The collection never deletes API-side rows, so running it repeatedly accumulates events and deliveries
(and folder 06's `state=DEAD` capture stays correct because it filters by state). `POST /__control/reset`
on the receiver clears receiver-side request/effect state; the API side is cleared by rebuilding the
schema (`npm run migrate:down && npm run migrate:up && npm run seed`). The suite in
[section 28](#28-testing-strategy-determinism-without-mocks) is the one that guarantees isolation, since
`test/global-setup.ts` drops and recreates its database on every run - the collection is a demonstration
tool, not a test oracle.

Measured again with newman 6.2.2 against the running four-process host stack on this machine, immediately
before committing these files: **77 request executions, 77 test-scripts, 77 assertions, 0 failed**, total
run duration 1340 ms, exit code 0. The executed count is above 63 because the two wait loops re-enter
themselves - this run spent **14** of those executions on soft `still waiting on the worker (poll N)`
iterations before the terminal states appeared, so treat the request total as run-dependent and the
`failed 0` as the number that matters.

---

## 37. Time spent and disclosure

**Wall-clock** (from the git history, checkable with `git log --format='%ci %s'`):

| Session | Local times | Commits | What happened |
|---|---|---|---|
| 1 | 2026-10-06 21:59 – 23:02 | 7 | plan, scaffold, schema + seed, auth/isolation, atomic publication, idempotency, delivery listing |
| — | ~4 h gap, no commits | | |
| 2 | 2026-10-07 02:58 – 05:45 | 9 | worker claim loop, fencing, envelope + HMAC + outbound client, receiver, retry engine, redrive, observability, Compose, acceptance evidence |
| 3 | 2026-10-07 06:14 – 06:59 | 6 | the three documents, then the live host run and the live Compose run that found and fixed two defects |
| 4 | 2026-10-07 08:33 – 13:09 | 3 | documentation alignment passes after the build itself: the pre-build plan brought in line with the system that was actually built, the measured parallel-jest trap recorded, local-only working notes removed from the tree and the history |
| 5 | 2026-10-07 13:46 → | — | final engineering audit of the built system: the 4 KiB response-detail bound enforced on the wire and after redaction, the object-only payload contract asserted for every non-object JSON shape, one correlation log line per accepted publication, and the migration runner's two silent failure modes made explicit |

Total span **16h26m as of this writing** (first commit 21:59:33, previous commit 14:25:30). Re-derive it
with `git log --format='%ci %s'` — like the commit count, it moves every time this file is corrected,
including by the commit that records the correction, which is why session 5 has no end time and no
commit count. Sessions 1–3 each have a first and last commit timestamp and sum to **4h34m**
(1h02m45s + 2h46m46s + 0h44m55s, window end-to-end, not net of pauses), which is an upper bound on the
active work inside them — thinking without a tool call leaves no git trace.
`git rev-list --count HEAD` gives the
commit count exactly - it moves every time this file is corrected, including by the commit that rewrote
this sentence, so it is checked rather than claimed. The
milestone-by-milestone rule (implement → run tests → inspect the diff → Conventional Commit) is what
makes that history readable: one milestone per commit, never one giant drop.

**Where the time actually went**, by the numbers: 4,385 lines of TypeScript across 57 source files,
6,342 lines across 35 test files, 233 tests in 25 suites, 4 migrations. The expensive parts were not the
API - they were the concurrency semantics (lease/fencing/`SKIP LOCKED` SQL, the two-phase transaction
boundaries), making the tests deterministic while still using real sockets and a real database, and the
honest-outcome model (`UNKNOWN` is harder to get right than it looks).

**Process notes, in the interest of not hiding anything:**
* Built with AI assistance (this author's coding agent) plus standard libraries; the brief explicitly
  permits both with disclosure. Design decisions in §31 and §34 are the ones I would defend; each is
  grounded in code a reviewer can open.
* Nine things surfaced by measuring instead of trusting. Eight were real defects, and all eight are fixed;
  the ninth is a documented trap rather than a bug:
  1. the capture endpoint's in-flight counter never decremented, so its "peak concurrency" was really
     its request count (found while writing `t9`);
  2. a replayed idempotency response is not byte-identical, because `jsonb` reorders keys (found by
     asserting on a replay);
  3. a permissive request schema silently dropped caller-supplied `url`/`secret` fields (found by
     writing the rejection test first);
  4. four Compose services had only an `image:` key, so `docker compose up --build` tried to *pull* the
     local tag and aborted (`pull access denied`) - found by actually running the stack (§3);
  5. `LOG_PRETTY` was documented as enabling human-readable logs while the transport it selected was
     `pino/file`, i.e. the same JSON - the switch and its documentation are removed, so there is now one
     log format and no phantom option.
  6. `sanitizeSnippet` truncated the captured response to 4 KiB **before** redacting it, and the
     replacement text is longer than the word it hides: a realistic 4 KiB receiver error body produced
     an 11,587-byte snippet that was returned to the worker and stored in
     `delivery_attempts.response_snippet`. Found by asserting the bound on the wire, fixed by redacting
     first and truncating last; the test now checks `octet_length()` in the table, not just the returned
     string.
  7. The migration runner treated a missing `/migrations` directory as "no pending migrations" - a
     deployment that forgot to ship it looked healthy until a later `relation does not exist` - and two
     files sharing a version aborted mid-run on `schema_migrations`' primary key. Both are refused up
     front, naming the path or the colliding pair.
  8. `.env.example` shipped `PGHOST`/`PGPORT`/`PGUSER`/`PGPASSWORD`/`PGDATABASE`. node-postgres reads
     those only as fallbacks for a key the connection config omits, and every pool here is built from a
     complete `DATABASE_URL`, so they were dead configuration that looked required.
  9. `npx jest` (parallel, i.e. not the documented command) fails **18 of 25 suites** on this machine
     without touching the delivery logic: the suites share one database, so `TRUNCATE … CASCADE` in one
     suite deadlocks against another's writes and teardowns delete rows a neighbour just inserted. The
     full output and the immediately following clean serial run are recorded in §4. `npm test` pins
     `--runInBand`; the proper fix is one database per jest worker, which is test infrastructure the
     timebox did not buy.
* Nothing in this README quotes an unrun test. Coverage was measured (§29) rather than estimated, and
  the two things it does **not** prove are stated in the same place: the `migrate:up`/`migrate:down`/`seed`
  CLI wrappers have no automated test of their wiring (their file-discovery rules are unit-tested, and the
  wrappers were driven by hand against a throwaway database - `down`, `up`, `up` again, `seed`, `seed`
  again, then dropped), and no load/soak measurement was taken, so "4 concurrent dispatches per worker"
  is a proven bound, not a proven throughput.

**Licence/data:** no proprietary code, no production data, no real credentials. The tokens, secrets and
UUIDs in the seed and in `.env.example` are published development fixtures and are unusable against
anything real. If you publish this repository, replace them before pointing the service at a live
database.
