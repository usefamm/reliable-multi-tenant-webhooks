# Implementation Plan — Reliable Multi-Tenant Webhook Delivery

> Source of truth: `56052348.pdf` ("Senior Backend Engineering Challenge — Reliable Multi Tenant Webhook Delivery", bitex, 4 pages).
> This plan extracts the requirements, fixes the architecture, and defines milestones. Where the PDF is silent, the choice is marked **[IMPL DECISION]**.

---

## 1. Requirements extracted from the PDF

### 1.1 Scope & stack
- TypeScript + Node.js, PostgreSQL, Docker Compose.
- One **API**, **two independent worker processes**, one **local mock receiver**.
- **Database-backed queue** is sufficient. No Redis/broker/Kafka required (explicitly optional). No UI, no cloud deploy, no real external destination.
- Timebox 10–12h. Small working system with convincing failure tests > unfinished platform.

### 1.2 Fixtures / seed
- **2 tenants**, **2 endpoints per tenant**, fixed **development tokens** mapped server-side to tenant identities.
- **1 separate operator token**.
- Endpoint URLs + signing secrets are **trusted deployment config**, never caller-supplied.
- All seeded endpoints point to the local mock receiver.
- Out of scope: endpoint creation/editing, secret rotation, event fan-out.

### 1.3 Event publication — `POST /events`
- Body: `endpointId`, `eventType`, `payload` (JSON object). Header: `Idempotency-Key`.
- Validate: `eventType` non-empty, ≤ 100 chars; **raw request body ≤ 64 KiB**; endpoint belongs to authenticated tenant.
- Tenant resolved **from token**, never from a request `tenantId`.
- On success → **HTTP 202** with `eventId`, `deliveryId`, status URL, `status`.
- **Event + its one logical delivery commit atomically** in a single DB transaction. Never event-without-delivery or delivery-without-event.

### 1.4 Publication idempotency
- Keyed by **tenant + operation + Idempotency-Key**.
- Same key + **equivalent validated input** → replay original 202 response.
- Same key + changed `endpointId` / `eventType` / `payload` → **409**.
- **Canonical comparison**: JSON object key order irrelevant; **array order significant**.
- Different tenants may reuse the same key.
- Keep successful keys indefinitely (this exercise). Document whether validation failures consume a key. **[IMPL DECISION: validation failures do NOT consume a key — the key record is only written inside the successful publication transaction.]**

### 1.5 Query APIs
- `GET /events/:id` → tenant-scoped event metadata, delivery state, total attempts, `nextAttemptAt`, latest bounded error. Unknown **and** other-tenant ids both → **404** (no existence leak).
- `GET /deliveries` → tenant-scoped, pagination, state filter, stable ordering, documented max page size, **no secrets**.
- `POST /ops/deliveries/:id/redrive` → **operator only**; accepts `reason` + `Idempotency-Key`; **DEAD only** (other states → 409); audits operator/time/reason; concurrent redrives → **one** new retry cycle; same key+input replays response; changed input → 409.

### 1.6 Status codes
- 400 malformed/invalid, 413 oversized body, 401 missing/invalid token, 403 non-operator redrive, 404 unavailable/other-tenant resource, 409 idempotency/state conflict, 202 accepted.
- Error body: `{ code, message, requestId }` — **no stack traces, no secrets**.

### 1.7 Delivery state machine
- States: **READY, IN_FLIGHT, RETRY_WAIT, DELIVERED, DEAD**.
- DELIVERED terminal. DEAD = automatic attempts stopped (does **not** mean receiver did nothing).
- Operator may redrive DEAD preserving eventId, deliveryId, attempt history.
- **No ordering guarantee between different events** — document explicitly.
- All timestamps UTC. Bound error details. Never log credentials/full payloads by default.

### 1.8 Outbound webhook
- POST JSON envelope: `eventId`, `deliveryId`, `eventType`, `occurredAt`, `payload`.
- **Store exact envelope bytes once; reuse for all attempts** (never re-serialize differently).
- Headers: `X-Event-Id`, `X-Delivery-Id`, `X-Attempt-Id`, `X-Webhook-Timestamp`, `X-Webhook-Signature`.
- `attemptId`/timestamp/signature change per attempt; `eventId`/`deliveryId` stable.
- Timestamp = **Unix seconds**. Signature = **lowercase hex HMAC-SHA256** over `UTF8(timestamp + "." + exact raw body bytes)` using the endpoint secret.
- Outbound safety: POST only, JSON, **2s total timeout**, **no redirects**, capture response details **≤ 4 KiB**, never forward API auth tokens, only contact configured destinations, reject caller URL override (SSRF-safe).

### 1.9 Receiver
- Verify signature **before** deduplication, **timing-safe** compare, reject timestamps > **300s** from its clock.
- Persist a **unique effect per (endpointId + eventId)** atomically with the business update.
- Duplicate same content → 200, no second effect. Conflicting content same identity → reject.
- Deduplication **survives restart** (durable). Records every request. Exposes test-only attempt & effect counts.
- Reject: invalid signature, wrong secret, stale timestamp, body tampering.

### 1.10 Retry policy
- 2xx → DELIVERED.
- Retry: timeout, connection failure, 408, 429, 5xx.
- Non-retryable → DEAD: other statuses, redirects.
- **Max 5 attempts per automatic cycle** (including initial). Exhaustion → DEAD (payload + history retained).
- Backoff before retries 1–4: **1s, 2s, 4s, 8s** + jitter **0–250ms**.
- 429 with valid `Retry-After` delta-seconds → `delay = max(normalBackoff, retryAfter)`, **capped 60s**. Invalid/unsupported → normal backoff.
- Persist `nextAttemptAt`; restart must not reset schedule or budget.
- **Persist an attempt BEFORE dispatch.** A crash can leave an attempt with unknown outcome and no HTTP send; it still consumes budget. Preserve uncertainty (do not invent a response).

### 1.11 Concurrent workers
- Two workers on shared durable storage. Claim due work with a **bounded lease**. Recover abandoned claims after lease expiry. **Fence** completion writes so a stale worker cannot overwrite a newer owner.
- **No DB transaction/row lock held during HTTP.**
- **Max 4 concurrent requests per worker.** Claim only work for available slots; **no unbounded in-memory queue.**
- Requests already at the receiver cannot be recalled after lease expiry → correctness must NOT depend on leases preventing every duplicate HTTP.
- Do not reset attempt budgets after crashes. Stop acquiring new work on shutdown; document in-flight treatment. Failing destination gets a future retry time (no busy spin).

### 1.12 Persistence & observability
- Persist events, logical deliveries, attempts, redrive audit records.
- State transitions + retry scheduling + attempt outcomes consistent in **one local transaction**.
- Attempt record keeps: attempt number, cycle, lease ownership evidence, start/finish times, HTTP status when known, bounded error code.
- No distributed transaction with receiver.
- Protected status endpoint/counters: ready, in-flight, retry-wait, dead, oldest pending age.
- Structured logs include requestId, eventId, deliveryId, attemptId. Document indexes for due-work scans and tenant listing.

### 1.13 Deterministic mock modes (fixtures/test-only control, never public event fields)
`success`, `temp_failure` (503 a bounded number of times, then success), `perm_failure` (500 forever), `rate_limited` (429 + `Retry-After`), `lost_response` (commit the effect, then destroy the connection), `slow` (respond later than the sender's timeout), `reject_400` (non-retryable), `redirect` (3xx the sender must never follow) — eight modes for what the PDF lists as seven cases, because "temporary/permanent failure" is two different behaviours worth observing separately. Gated behind `RECEIVER_TEST_CONTROLS`, selected per `(endpointId, eventId)` (or endpoint-wide), never from a public event field.
- Injected clock, scheduler hooks, deterministic randomness → fast reproducible tests.

### 1.14 Acceptance tests (real DB + real HTTP mock; assert durable state, attempt history, receiver effect counts)
1. Publication concurrency (20 identical → 1 event/1 delivery; reordered keys replay; changed payload 409; cross-tenant key reuse).
2. Ownership & validation (cross-tenant reject, invalid input, oversized body, unauthorized redrive, caller cannot replace destination).
3. Signature verification (valid accepted; tamper/wrong-secret/stale rejected; retry fresh timestamp + stable identity).
4. Lost response & restart (≥2 HTTP attempts, exactly 1 durable effect).
5. Retry scheduling (503 recovery, 429 delay, 5-attempt exhaustion, immediate DEAD on 400, restart during RETRY_WAIT preserves schedule+budget).
6. Crash boundaries (crash after publication commit before dispatch → recover; crash after dispatch before completion commit → recover without losing history or duplicating effects).
7. Stale worker (pause A beyond lease, B recovers, resume A → fenced updates, single receiver effect despite duplicate HTTP).
8. Redrive concurrency (concurrent redrives → one cycle; replay doesn't reset budget; changed input 409; DELIVERED cannot redrive).
9. Bounds & rollback (per-worker concurrency + timeout bounds; redirects rejected without contacting target; injected write failure → atomic rollback).

### 1.15 Scoring weights (drives priority order)
- 30: durable state + concurrent worker safety.
- 25: deterministic failure/recovery tests.
- 20: retry, redrive, delivery semantics.
- 15: tenant isolation, signing, outbound safety.
- 10: reproducibility, observability, explanation.
- **Critical defects**: silent event loss, cross-tenant access, repeated receiver business effects.

---

## 2. Architecture

```
                 ┌───────────────┐
                 │    Client     │  (dev tokens / operator token)
                 └───────┬───────┘
                         │ HTTP (JSON)
                         ▼
                 ┌───────────────┐
                 │   API Server  │  NestJS: /events, /events/:id,
                 │  (NestJS)     │  /deliveries, /ops/.../redrive, /ops/status
                 └───────┬───────┘
                         │ SQL (pg)
                         ▼
        ┌────────────────────────────────────┐
        │            PostgreSQL              │  ← durable queue + source of truth
        │ tenants, endpoints, events,        │
        │ deliveries, delivery_attempts,     │
        │ idempotency_records,               │
        │ redrive_audit, receiver_effects    │
        └───────┬────────────────────┬───────┘
                │ claim (SKIP LOCKED)│ claim
                ▼                    ▼
        ┌──────────────┐     ┌──────────────┐
        │   Worker A   │     │   Worker B   │  each ≤4 concurrent HTTP, bounded lease
        └──────┬───────┘     └──────┬───────┘
               │  signed webhook POST (2s timeout, no redirect)
               └─────────┬──────────┘
                         ▼
                 ┌───────────────┐
                 │ Mock Receiver │  verifies HMAC, durable dedup per (endpoint,event)
                 └───────────────┘
```

- **Workers share PostgreSQL.** DB is the durable queue and the only source of truth. No in-memory queue as truth.
- **No transaction/lock held during HTTP.** Claim-and-persist-attempt in txn #1 → commit → HTTP → complete in txn #2.

### 2.1 Components
| Component | Responsibility |
|---|---|
| `config` | Env-driven config, validation (zod/`@nestjs/config`), no magic constants. |
| `database` | pg `Pool`, migration runner, transaction helper, repositories. |
| `auth` | Bearer token → tenant identity (dev tokens) or operator role. Guards. |
| `tenants`/`endpoints` | Seeded ownership lookups; endpoint destination + secret resolved server-side. |
| `events` | `POST /events`, `GET /events/:id`; atomic event+delivery publish. |
| `idempotency` | Canonical JSON hashing; key record read/write inside publish txn. |
| `deliveries` | State model, `GET /deliveries`, state transitions repository. |
| `workers` | Lease claim loop, bounded concurrency, dispatch, completion, shutdown. |
| `webhooks` | Envelope builder (stable bytes), HMAC signer, outbound HTTP client (safety). |
| `receiver` | Mock receiver app: verify, dedup, effect, test-only modes + counters. |
| `operations` | Operator redrive (+ its idempotency), status/counters endpoint. |
| `observability` | Structured logger (pino), requestId context, counters. |

### 2.2 Framework / library choices **[IMPL DECISION]**
- **NestJS** (clean DI, guards, pipes, modules) — allowed and encouraged if clean within timebox.
- **pg** (node-postgres) directly — "well-understood database access layer", full control of `FOR UPDATE SKIP LOCKED` and transaction boundaries. No heavy ORM (`synchronize` forbidden).
- **Migrations**: plain `.sql` files in `migrations/` + a small hand-written runner (`src/db/migrate.ts`) that applies them in lexical order and records each version in `schema_migrations`. Chosen over `node-pg-migrate` so the runner has no extra dependency and stays readable end to end, and so `globalSetup` can import `migrateUp()` directly.
- **Validation**: `zod` for DTO + config.
- **Logging**: `pino` (structured JSON).
- **Testing**: `jest` + `supertest` against a **real PostgreSQL** selected by `TEST_DATABASE_URL` (dropped, recreated, migrated and seeded in `test/global-setup.ts`) and a real HTTP receiver. No testcontainers dependency: the suite needs only a reachable server, which keeps CI simple and the run deterministic.
- **Clock/random injection**: a `Clock` and `Random` provider interface; deterministic fakes in tests.

---

## 3. Database model

Identifiers are `text` PKs holding UUID values (`crypto.randomUUID()`) — the id in the API response, the wire header and the database is one string with no casting, and adding a wire-visible `attempt_id` never needed a schema change. All timestamps `timestamptz`, all stored and compared in UTC. Four migrations: `001_core_schema` → `004_delivery_listing_index`; this section is the **as-built** schema, not a sketch.

**tenants** `(id, name, created_at)`

**endpoints** `(id, tenant_id→tenants, url, secret, created_at)`
- `url`/`secret` = deployment config. Never returned by APIs.

**events** `(id, tenant_id→tenants, endpoint_id→endpoints, event_type, payload jsonb, occurred_at, created_at)`
- `occurred_at` fixed at publication; envelope embeds it (stable identity).

**deliveries** (one logical delivery per event)
```
(id, event_id→events UNIQUE, tenant_id, endpoint_id,
 state delivery_state,               -- READY|IN_FLIGHT|RETRY_WAIT|DELIVERED|DEAD
 envelope_bytes bytea,               -- exact body, written once
 envelope_hash text,                 -- sha256 hex of envelope_bytes (integrity/debugging)
 attempt_count int,                  -- lifetime attempts (not reset by redrive)
 cycle int,                          -- automatic cycle number; redrive increments
 attempts_in_cycle int,              -- attempts used in current cycle (≤5)
 next_attempt_at timestamptz,        -- due time; null when terminal
 lease_owner text,                   -- worker id
 lease_generation bigint,            -- fencing token, incremented on each claim
 lease_expires_at timestamptz,       -- bounded lease
 last_error_code text,               -- bounded
 last_http_status int,
 created_at, updated_at)
```
- One-to-one `events ↔ deliveries` enforced by `UNIQUE(event_id)` → guarantees atomic pairing (both inserted in same txn).

**delivery_attempts**
```
(id, delivery_id→deliveries, attempt_number int, cycle int,
 attempt_id text NOT NULL,                     -- the X-Attempt-Id actually sent (fresh per attempt)
 lease_owner text, lease_generation bigint,   -- ownership evidence
 started_at, finished_at,                      -- finished_at NULL = outcome unknown
 http_status int,                              -- null when unknown
 outcome attempt_outcome DEFAULT 'UNKNOWN',    -- SUCCESS|RETRYABLE|NON_RETRYABLE|UNKNOWN
 error_code text,                              -- bounded
 response_snippet text)                        -- ≤4 KiB
```
- Inserted in the **claim** transaction, before any HTTP: a row with `outcome='UNKNOWN'` and `finished_at IS NULL` is the durable evidence that an attempt was allocated and its outcome never confirmed.

**idempotency_records**
```
(id, tenant_id, operation text,               -- 'publish_event' | 'redrive'
 idempotency_key text, request_hash text,      -- canonical hash of validated input
 response_status int, response_body jsonb,     -- original response to replay
 resource_id text,                             -- eventId (publish) / deliveryId (redrive)
 created_at,
 UNIQUE(tenant_id, operation, idempotency_key))
```
- Unique constraint scoped by tenant → different tenants reuse the same key.

**redrive_audit** `(id, delivery_id→deliveries, operator, reason, idempotency_key, created_at)` — `operator` is the token's non-secret label (never the raw token), indexed by `(delivery_id, created_at)`.

**receiver_effects** (mock receiver, durable dedup)
```
(id, endpoint_id, event_id, content_hash,      -- canonical hash of envelope/payload
 applied_at, UNIQUE(endpoint_id, event_id))
```
- Unique constraint = idempotent business effect; conflicting content_hash under same identity → reject.

**receiver_requests** (mock receiver log) `(id, endpoint_id, event_id, delivery_id, attempt_id, signature_ok bool, mode, received_at)` — records **every** inbound request, including rejected ones, so tests assert `requests vs effects` rather than trusting a response. Two indexes: `(event_id, received_at)`, `(endpoint_id, received_at)`.

**receiver_modes** (test-only) `(endpoint_id, event_id DEFAULT '', mode, remaining, retry_after, delay_ms, updated_at, PRIMARY KEY(endpoint_id, event_id))` — `event_id = ''` is an endpoint-wide scope sentinel; `''` instead of NULL because a PK column is implicitly NOT NULL. `remaining` is decremented on consumption, which is what makes "503 twice then succeed" reproducible.

**auth_tokens** `(token_hash text PK, tenant_id→tenants NULL for operator, role, label, created_at)` — the PK is `sha256` of the raw bearer token, so the **plaintext token is never stored** and lookup is one indexed hash read; the raw token only ever appears in an `Authorization` header. Two CHECKs enforce the role/tenant consistency that the guards otherwise have to trust: `role IN ('tenant','operator')` and `(tenant ⇒ tenant_id NOT NULL) / (operator ⇒ NULL)`. `label` is what the logs and redrive audit may name.

### 3.1 Indexes (as shipped, each commented in the migration itself)
- `deliveries_due_idx ON deliveries (next_attempt_at) WHERE state IN ('READY','RETRY_WAIT')` → the due-work scan; the partial predicate keeps hot rows only, since `IN_FLIGHT`/terminal rows are not claimable on the due branch.
- `deliveries_lease_expiry_idx ON deliveries (lease_expires_at) WHERE state='IN_FLIGHT'` → lease-expiry recovery.
- `deliveries_tenant_state_idx ON deliveries (tenant_id, state, created_at DESC, id DESC)` → tenant listing **with** a state filter, in the exact order the query uses.
- `deliveries_tenant_created_idx ON deliveries (tenant_id, created_at DESC, id DESC)` (migration 004) → the same listing **without** a state filter, so the keyset scan needs no sort.
- `deliveries_state_idx ON deliveries (state)` → the `/ops/status` counters `GROUP BY`.
- `events_tenant_created_idx ON events (tenant_id, created_at DESC, id DESC)`; `endpoints_tenant_idx`; `delivery_attempts_delivery_idx ON (delivery_id, attempt_number)` → ordered history; `redrive_audit_delivery_idx ON (delivery_id, created_at)`; `auth_tokens_tenant_idx`.
- `idempotency_records` is covered by its `UNIQUE (tenant_id, operation, idempotency_key)` constraint; `receiver_effects` by `UNIQUE (endpoint_id, event_id)` — both are the lookup index *and* the correctness rule, which is why no separate index is needed.

---

## 4. Delivery state machine

```
            publish
               │
               ▼
            READY ──claim──▶ IN_FLIGHT ──2xx──▶ DELIVERED (terminal)
               ▲                 │
               │                 ├──retryable & budget left──▶ RETRY_WAIT ──due──▶ READY/claim
               │                 │
               │                 ├──non-retryable | budget exhausted──▶ DEAD
               │                 │
               │                 └──lease expired──▶ (recoverable) claim by new worker
               │                                        (fencing rejects stale writer)
            DEAD ──operator redrive──▶ READY (cycle+1, attempts_in_cycle=0, keeps history)
```

**Valid transitions** (enforced in SQL with `WHERE state = expected`):
- READY → IN_FLIGHT (claim)
- IN_FLIGHT → DELIVERED | RETRY_WAIT | DEAD (completion)
- IN_FLIGHT (lease expired) → IN_FLIGHT (new claim, generation++) — recovery
- RETRY_WAIT → IN_FLIGHT (claim when due; `next_attempt_at <= now`)
- DEAD → READY (redrive only)
- DELIVERED → (none; terminal)

Retry-wait is claimed directly by the due scan (`state IN (READY, RETRY_WAIT) AND next_attempt_at <= now`), so RETRY_WAIT→IN_FLIGHT is the claim edge.

---

## 5. Worker lifecycle

Per worker (id = `WORKER_NAME`, e.g. `worker-a` / `worker-b`, used verbatim as `lease_owner` — the deployment gives the processes distinct names instead of the runtime guessing a `pid` suffix):
1. Loop while running. Each pass attempts up to `WORKER_CLAIM_BATCH_SIZE` claims and **acquires a semaphore slot before each claim**, breaking immediately when no slot is free — so a claim is never made for work that cannot be dispatched.
2. **Txn #1 (claim)** — one statement per unit of work, not a batch `LIMIT n`. Expired-lease recovery is *not* a separate step; it is one OR-branch of the due predicate:
   ```sql
   WITH candidate AS (
     SELECT id FROM deliveries
      WHERE (state IN ('READY','RETRY_WAIT') AND next_attempt_at <= $1)
         OR (state = 'IN_FLIGHT' AND lease_expires_at IS NOT NULL AND lease_expires_at <= $1)
      ORDER BY next_attempt_at ASC NULLS LAST, id ASC
      FOR UPDATE SKIP LOCKED
      LIMIT 1)
   UPDATE deliveries d SET state='IN_FLIGHT', lease_owner=$2,
          lease_generation=d.lease_generation+1, lease_expires_at=$3,
          attempt_count=d.attempt_count+1, attempts_in_cycle=d.attempts_in_cycle+1,
          updated_at=$1
     FROM candidate WHERE d.id=candidate.id
   RETURNING ...
   ```
   `$1` is the **injected clock**, never Postgres `now()` — one clock for both processes is what makes the retry schedule testable deterministically. The CTE locks and picks the row, the outer `UPDATE … FROM` writes it, `RETURNING` hands back the fenced coordinates in the same round trip.
   - Same transaction then **inserts the `delivery_attempts` row** (fresh `attempt_id`, `outcome='UNKNOWN'`, `started_at`, `lease_owner`, `lease_generation`). Commit.
3. **Dispatch (no txn open):** build headers (fresh attemptId, fresh Unix-seconds timestamp, HMAC over stored envelope bytes), POST with 2s timeout, no redirect, capture ≤4KiB.
4. **Txn #2 (complete):**
   - `UPDATE delivery_attempts SET finished_at, outcome, http_status, error_code, response_snippet WHERE id=?` — **no lease fence**: this worker's own attempt row always records what it actually observed, even if it is already stale. History lies less than state does.
   - `UPDATE deliveries SET ... WHERE id=? AND lease_owner=me AND lease_generation=?`. **0 rows → stale; the outcome is discarded, the newer state is preserved.** On a terminal transition (`next_attempt_at IS NULL`) the same statement clears `lease_owner` and `lease_expires_at`.
   - Transition: 2xx→DELIVERED; retryable & `attempts_in_cycle<5`→RETRY_WAIT + `next_attempt_at=clock.now()+backoff`; else→DEAD.
5. Slot freed → loop. Idle → poll after short interval.
6. **Shutdown:** stop claiming; document in-flight treatment (**[IMPL DECISION]**: wait up to `WORKER_SHUTDOWN_GRACE_MS` for in-flight HTTP to finish and commit; on timeout, leave leases to expire → recovered by another worker; attempt already persisted so budget is correct).

**Bounded concurrency:** an in-memory counting semaphore of `WORKER_CONCURRENCY` (default 4). The slot is taken *before* the claim, and each claim fetches one row — so concurrency is capped by construction, there is no batch `LIMIT` to size, and no unbounded in-memory queue can form.

---

## 6. Lease & fencing strategy

- **Lease** = `(lease_owner, lease_generation, lease_expires_at)`, TTL ≈ 30s **[IMPL DECISION]**, > 2s HTTP timeout with margin.
- **Claim** uses `FOR UPDATE SKIP LOCKED` so two workers never claim the same row; expired `IN_FLIGHT` leases are reclaimable.
- **Fencing token** = monotonically increasing `lease_generation`. Every completion write carries `WHERE lease_owner=me AND lease_generation=myGeneration`.
- **Stale worker**: A claims (gen=1) → pauses past TTL → B reclaims (gen=2) → B completes (gen=2 accepted). A resumes, writes `WHERE gen=1` → **0 rows** → A's write is rejected. Newer state preserved.
- **Key honesty (from PDF):** a duplicate HTTP request may still physically reach the receiver (lease cannot recall it). Correctness of the *business effect* is guaranteed by **receiver dedup**, not by leases. Leases/fencing guarantee the *delivery state* is not corrupted.

---

## 7. Transaction boundaries

| Boundary | Contents | Guarantee |
|---|---|---|
| **Publish txn** | validate → insert event + delivery(READY) + idempotency_record (with the replayable status/body) (all-or-nothing) | event ⟺ delivery atomic; idempotent |
| **Claim txn** | lock due rows (SKIP LOCKED) → set IN_FLIGHT + lease + generation++ → insert attempt(UNKNOWN) | attempt persisted **before** dispatch |
| **HTTP** | *outside any txn* | no locks held during network |
| **Complete txn** | fenced update delivery state + attempt outcome + schedule | stale writes rejected; state consistent |
| **Redrive txn** | check DEAD → idempotency lookup → set READY, cycle++, attempts_in_cycle=0, next_attempt_at=clock.now(), **lease_owner/lease_expires_at cleared** → audit | one cycle under concurrency; no stale lease can fence the fresh attempts |
| **Receiver effect txn** | verify → insert effect `ON CONFLICT (endpoint_id,event_id)` + business update | **one durable effect per (endpoint,event), however many requests arrive** — enforced by the receiver, never promised by the sender |

No distributed transaction across API/worker/receiver.

---

## 8. Retry strategy

- Classification: 2xx→SUCCESS; {timeout, ECONNRESET/ECONNREFUSED, 408, 429, 5xx}→RETRYABLE; everything else incl. 3xx/redirect→NON_RETRYABLE.
- Budget: 5 attempts/cycle. `attempts_in_cycle` gate; exhaustion → DEAD.
- Backoff before retry k (k=1..4): `base=[1,2,4,8][k-1]s` + `jitter∈[0,250]ms`.
- 429 + valid `Retry-After` (delta-seconds integer): `delay=min(max(base+jitter, retryAfter), 60s)`. Non-numeric / HTTP-date / negative / >cap → fall back to base+jitter. **[IMPL DECISION: only delta-seconds honoured, per PDF; HTTP-date treated as unsupported.]**
- `next_attempt_at = now() + delay` persisted → restart-safe schedule + budget.
- Deterministic tests: inject `Clock` (now) and `Random` (jitter) → compute schedule without real sleeping.

---

## 9. Idempotency strategy

- **Canonicalization [IMPL DECISION, documented]:** recursively sort object keys (stable), preserve array order, drop `undefined`, serialize with stable number/string formatting → SHA-256 hex = `request_hash`. (Uses a small deterministic `canonicalJson` — no reliance on JS key insertion order.)
- Publish: inside txn, look up `(tenant, 'publish_event', key)`.
  - Found + hash matches → replay stored 202 response.
  - Found + hash differs → **409**.
  - Not found → insert event+delivery+idempotency_record together. Concurrent duplicates: UNIQUE(tenant,operation,key) → one wins, losers re-read and replay (catch unique violation).
- Validation failures (400/413/401) happen **before** the txn → do **not** consume a key.
- Redrive uses the same mechanism with operation `'redrive'`, hashing `{deliveryId, reason}`.

---

## 10. Receiver deduplication strategy

- Verify HMAC (timing-safe `crypto.timingSafeEqual`) + timestamp freshness (±300s) **before** any effect.
- Effect identity = `(endpoint_id, event_id)`; store `content_hash` = `sha256(canonicalJson(full envelope))` — the whole envelope, not just the payload, so a replayed `eventId` with *any* changed field is detected as a conflict.
- Insert effect in one txn `ON CONFLICT (endpoint_id,event_id) DO NOTHING`:
  - inserted → apply business effect → 200.
  - conflict + same content_hash → 200, no second effect.
  - conflict + different content_hash → **409 reject**.
- Durable (Postgres) → survives restart. Records every request in `receiver_requests` for assertions; exposes test-only attempt/effect counters.

---

## 11. Security strategy

- Tokens: seeded dev tokens (tenant A/B) + operator token; resolved server-side; never trust request `tenantId`.
- Tenant isolation: every owned-resource query filters by `tenant_id`; other-tenant/unknown → 404 (no existence leak).
- Secrets: endpoint `secret` never returned/logged; HMAC signing only; auth tokens never forwarded to receiver.
- SSRF: destination = endpoint config only; caller cannot override URL; **redirects disabled**; local HTTP allowed only for test setup.
- Bounds: 64 KiB request body (413), 4 KiB response capture, bounded error codes, no full payload logging.
- Production notes (DESIGN.md): HTTPS/mTLS, secret manager (Vault/KMS) + rotation via dual-secret grace window, private destinations via egress allow-list, endpoint registration authz.
- No secrets committed; `.env.example` placeholders only.

---

## 12. Testing strategy

- **Real Postgres + real HTTP receiver.** `test/global-setup.ts` drops and recreates the database named by `TEST_DATABASE_URL`, applies every migration from empty and writes the deterministic seed; integration tests talk to a live receiver over real sockets. No mocking the DB, no testcontainers.
- **Deterministic time/random**: `Clock`/`Random` providers injected; fakes advance time and fix jitter → retry-schedule tests run without 8s sleeps.
- **Crash injection**: a crash is modelled the honest way — work is durably committed, then the worker is abandoned and the injected clock moves past the lease expiry, so recovery is claimed as "another worker finds leased `IN_FLIGHT` work". One test strands a genuinely slow in-flight worker so a second worker reclaims it mid-dispatch and the first is fenced on completion. The process-level version of the same story (pause a real container until its lease lapses, then let the other worker take over) was verified through Docker Compose and is recorded in the README rather than in `jest`.
- **Failure harness**: receiver modes selected via test-only control endpoint (never public event fields).
- Coverage = the 9 PDF acceptance tests (mapped in §1.14) as integration/failure tests + focused unit tests (canonicalJson, backoff/Retry-After, HMAC, fencing SQL, state transitions).
- Assert **durable state, attempt history, receiver effect counts** — not just API responses.
- Never report unrun tests as passing.

---

## 13. Implementation milestones (Conventional Commits per milestone)

| M | Milestone | Commit(s) |
|---|---|---|
| M0 | Repo inspection + this plan | `chore: initialize project`, `docs: add implementation plan` |
| M1 | TS/NestJS scaffold, config, logger, health | `chore: configure typescript and nestjs`, `feat: add config and structured logging` |
| M2 | Postgres + migrations + deterministic seed (2 tenants/4 endpoints/tokens) | `feat: add database migrations`, `chore: add seed data` |
| M3 | Auth (dev/operator tokens) + tenant isolation guards | `feat: add token auth and tenant isolation` |
| M4 | `POST /events` atomic event+delivery txn, `GET /events/:id` | `feat: add atomic event publication` |
| M5 | Publication idempotency + canonical JSON | `feat: add publication idempotency` |
| M6 | Delivery state model + `GET /deliveries` (pagination/filter/order) | `feat: add delivery listing` |
| M7 | Worker claim loop + bounded leases + concurrency cap | `feat: add worker lease claiming` |
| M8 | Fencing + stale-worker protection + lease recovery | `feat: add delivery fencing` |
| M9 | Webhook envelope (stable bytes) + HMAC signing + safe outbound client | `feat: add webhook signing` |
| M10 | Mock receiver: verify + durable dedup + modes + counters | `feat: add mock receiver` |
| M11 | Retry engine: backoff, jitter, Retry-After, schedule persistence | `feat: add retry scheduling` |
| M12 | Operator redrive + redrive idempotency + audit | `feat: add operator redrive` |
| M13 | Observability: `/ops/status` counters, log context | `feat: add operational status endpoint` |
| M14 | Docker Compose (postgres, api, worker A/B, receiver), `.env.example` | `chore: add docker compose` |
| M15 | Acceptance/failure tests (PDF tests 1–9) | `test: add acceptance and failure suites` |
| M16 | Docs: README, DESIGN.md, Mermaid diagrams | `docs: add design note and README` |
| M17 | Final cleanup, full test run, git verification | `chore: finalize submission` |

Rule: after each milestone → run relevant tests → inspect diff → Conventional Commit → do not proceed while fundamentally broken.

**This table was written before the build.** The `Commit(s)` column is *planned* subject text, and the real history follows the diff rather than this table: some rows landed as one commit where they were planned as two, and some milestones split into an implementation commit plus a `test:` commit (fencing and the retry/recovery evidence both did). The subjects are Conventional Commits throughout and every commit is one coherent milestone-sized change — but `git log --oneline` is the authority here, not this column.

---

## 14. Explicit assumptions **[IMPL DECISION]**
1. Lease TTL = 30s (`WORKER_LEASE_TTL_MS`); poll interval when the queue is empty = 250ms (`WORKER_POLL_INTERVAL_MS`); one claim pass claims at most `WORKER_CLAIM_BATCH_SIZE` (default 4) deliveries **and** stops as soon as the outbound semaphore has no free slot — never claim work you cannot dispatch.
2. `occurredAt` set at publication and embedded in envelope (stable across attempts/redrive).
3. Validation failures do not consume an idempotency key.
4. Only `Retry-After` delta-seconds honoured; HTTP-date treated as unsupported → normal backoff.
5. Receiver `content_hash` = canonical hash of the full envelope (eventId+deliveryId+eventType+occurredAt+payload).
6. Status endpoint protected by operator token.
7. `GET /deliveries` page size: default 50 (`DEFAULT_PAGE_SIZE`), max 100 (`MAX_PAGE_SIZE`), out-of-range `limit` → 400; stable order `created_at DESC, id DESC` (newest first, `id` breaks `created_at` ties).
8. Redrive resets `attempts_in_cycle` to 0 and increments `cycle`; lifetime `attempt_count` retained.
9. Envelope stored as `bytea` (exact bytes) to guarantee byte-identical reuse.
10. Lease owner (`lease_owner`) = `WORKER_NAME` verbatim — no `pid` suffix. Distinct identity across processes is a **deployment** concern, not a runtime guess: Compose sets `worker-a` and `worker-b`, so two workers on one host cannot share an owner string and fence each other out. A name collision would be visible immediately in `/ops/status`.

## 15. Trade-offs
- **pg + plain SQL migrations (hand-written 149-line runner) over TypeORM/Prisma/node-pg-migrate**: more SQL to write, but full control of `SKIP LOCKED`, fencing, and exact transaction boundaries — the core of the challenge. No `synchronize`, and the migration list is literally a folder of `.sql` files you can read top to bottom.
- **DB-backed queue over Redis/broker**: simpler, matches PDF, transactional with state; lower throughput ceiling (acceptable; fairness/rate-limit are discussion topics).
- **NestJS**: structure + DI + guards speed up clean code; small overhead vs bare Express.
- **Two-phase txn (claim / complete) around HTTP**: extra round-trips, but mandatory (no locks during HTTP) and enables crash-safe recovery.
- **Lease cannot prevent all duplicate HTTP**: accepted; correctness delegated to receiver dedup (as PDF requires) rather than over-engineering distributed locks.

## 16. Known limitations (to document, not hide)
- Single-node Postgres; no HA/replication.
- No tenant fairness / global rate limiting (PDF: discussion topics only).
- No secret rotation implemented (design note only).
- No retention/archival job (design note only).
- Due-scan polling (not `LISTEN/NOTIFY`) → small latency vs simplicity.
- In-memory semaphore per worker; total system concurrency = workers × 4.
- HTTP-date `Retry-After` unsupported.
- **No CI pipeline** — there is no `.github/workflows` file. The suite is run by hand and the README records the command and the real output; adding CI means a Postgres service container, which is deployment work rather than a gap in the design.
- **Tests need a reachable Postgres.** `TEST_DATABASE_URL` selects the server; `globalSetup` drops/recreates/migrates/seeds that database but does not provision one. No container is started for you, so pointing it at the wrong host destroys a real database — which is why the name is `webhook_test` and the compose stack publishes no Postgres port.
- **Coverage is reported, not gated.** `jest.config.js` sets no `coverageThreshold`: the measured numbers are in the README, but nothing fails a run for dropping below them.
- **The suite must run serially.** Tests reset shared transactional tables in `afterEach` (`TRUNCATE … RESTART IDENTITY CASCADE`) against one `webhook_test` database, so `npm test` pins `--runInBand`; running bare `npx jest` was measured at 18 failed suites of 25 from deadlocked truncates and teardowns deleting a neighbour's rows, while the serial run passed 233/233. Fixing it properly means one database per jest worker.

---

## 17. Deliverables checklist
- Source (NestJS API, worker entrypoint, receiver), migrations, deterministic seed, tests, Docker Compose, `.env.example` (secret-free).
- README (setup/test commands, example requests, **actual** test results, time spent, limitations, AI disclosure).
- `docs/DESIGN.md` (≤2 pages: txn boundaries, duplicate semantics, fencing, production security).
- Mermaid diagrams (architecture, publication, claim/lease, retry, crash recovery, redrive, receiver dedup, state machine).
- Clean git history (Conventional Commits, no secrets, no node_modules/.env).
