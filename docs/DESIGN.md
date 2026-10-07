# Design note

Two pages, four subjects: **transaction boundaries**, **duplicate delivery semantics**, **fencing**,
**production security**. The README carries the diagrams and the test evidence; this document is the
argument.

---

## 1. Transaction boundaries

The rule the whole design stands on: **no database transaction spans an HTTP call.** Every unit of work
is a short transaction whose duration is bounded by the database, not by a customer's endpoint. There
are four transactions in the system.

**T1 — publication** (`POST /events`, one transaction, `src/modules/events/events.service.ts`).
Claims the idempotency record, verifies endpoint ownership, inserts `events`, inserts `deliveries`
(`state=READY`, `envelope_bytes` written once), finalizes the record with the response body, commits.
Three properties follow: an event without a delivery is not expressible; concurrent duplicates serialize
on `UNIQUE (tenant_id, operation, idempotency_key)` and losers replay the winner instead of racing to
redo work; and a request that fails validation or ownership **rolled its claim back too**, so a caller's
typo does not burn their own key. The claim is written *before* the work deliberately: claiming after
would let two concurrent requests both do the work and then both fail a uniqueness precondition, which
surfaces as a spurious 409.

**T2 — claim** (`DeliveryQueue.claimNext`, one statement plus the attempt insert).
A CTE selects at most one row that is `READY`/`RETRY_WAIT` and due, or `IN_FLIGHT` with an expired
lease, using `FOR UPDATE SKIP LOCKED`; `UPDATE ... FROM candidate ... RETURNING` takes the lease,
increments `lease_generation`, `attempt_count` and `attempts_in_cycle`, then the attempt row is inserted
in the same transaction with `outcome='UNKNOWN'`, `finished_at NULL`. Committing *before* dispatch is
the point: the delivery of "we decided to send this" is durable, so a crash immediately after the commit
costs a lease, not an event. `SKIP LOCKED` is what makes two workers structurally unable to double-claim
or block each other.

**T3 — completion** (`DeliveryQueue.completeAttempt`, one transaction, fenced).
The attempt row is updated unconditionally with the truthful outcome (so even a worker that has lost its
lease leaves an accurate record of what it saw), then the `deliveries` state write carries
`WHERE id = $1 AND lease_owner = $8 AND lease_generation = $9`. `rowCount = 0` means "someone else owns
this row now" and is reported as `applied: false` — not an error to retry, because retrying a stale
worker's write is precisely the bug being prevented. Terminal transitions release the lease in the same
statement.

**T4 — redrive** (`RedriveService`, one transaction).
Claims the idempotency record, locks the delivery `FOR UPDATE`, refuses anything that is not `DEAD`,
increments `cycle`, resets `attempts_in_cycle`, clears the lease, writes `redrive_audit`. Because the key
is claimed first and the row locked second, two operators with *different* keys cannot start two cycles:
the second blocks on the lock, then sees a non-`DEAD` row and gets 409.

The HTTP dispatch happens **between T2 and T3, outside any transaction**, holding no row lock and no
pool connection for longer than the query. Cost: an extra round-trip per attempt and a window in which a
dispatch is unacknowledged. That window is not closed by a bigger transaction — it is closed by leases,
fencing, and receiver-side deduplication.

Injection note: two of the acceptance proofs (`test/acceptance/t9`) install plpgsql triggers that raise
real errors on `INSERT INTO deliveries` and on the completion `UPDATE`. The rollback they assert is
Postgres' own, not a stub's — which is the only way to demonstrate that a partially-written T1 or T3
leaves no trace.

---

## 2. Duplicate delivery semantics

**Stated plainly: delivery is at-least-once, duplicates are normal, and the receiver's business effect
is exactly-once.** A system that claims exactly-once delivery is either lying or holding a transaction
open across the network.

Duplicates arise from four real situations, none of them exotic: a response lost after the receiver
committed (`lost_response`, timeout, connection reset); a lease expiring while the original dispatch is
still in flight (a GC pause, a suspended VM); an operator redrive; a worker killed between dispatch and
completion. In each, the honest answer to "did the receiver get it?" is *unknown*, and the system records
exactly that (`UNKNOWN`/`RETRYABLE` with the real status, or `NULL` when nothing was observed) rather
than inventing a result.

Uncertainty is then made harmless by three choices:

1. **Byte-stable identity.** `envelope_bytes` is serialized once at publication and re-sent verbatim on
   every attempt and redrive, so `eventId`/`deliveryId` are stable and a retry never invalidates a
   signature by reformatting a body. Only `X-Attempt-Id`, `X-Webhook-Timestamp` and the signature are
   fresh per attempt.
2. **Dedup as a constraint, not a check.** `receiver_effects` has `UNIQUE (endpoint_id, event_id)` and
   the effect is applied by `INSERT ... ON CONFLICT DO NOTHING RETURNING id`. The insert *is* the check,
   so there is no check-then-write window for two concurrent duplicates to slip through — the bug every
   "SELECT then INSERT" receiver has. `content_hash = sha256(canonicalJson(envelope))` distinguishes a
   harmless duplicate (`200 applied:false, deduplicated:true`) from same-identity-different-content
   (`409 content_conflict`), which can only mean a defect or an attack.
3. **Durable, so it survives the receiver.** Because the record is a database row, restarting the
   receiver changes nothing about deduplication. `test/acceptance/t4` proves it with a real restart —
   same port, new process, new pool: two or more HTTP attempts, one effect, and the pre-existing
   `applied_at`/`content_hash` byte-unchanged.

Both first application and duplicate return 2xx, deliberately: the sender must stop retrying in both
cases, and only the first one changed state.

---

## 3. Fencing

A lease bounds *time*, not *execution*: nothing about `lease_expires_at` prevents a paused worker from
resuming and sending a late write. The interleaving that must not corrupt state:

1. `worker-a` claims (generation 1) and stalls beyond its TTL.
2. The lease expires; `worker-b` recovers the row — generation 2, attempt 2.
3. `worker-b` gets 200 → `DELIVERED`, lease released.
4. `worker-a` wakes, its call returned 503, and it tries to write `RETRY_WAIT`.

Without fencing, step 4 resurrects a delivered event into the retry queue. Because every state write in
T3 is fenced on `(lease_owner, lease_generation)`, step 4 matches zero rows and is reported as
`applied: false`. `worker-a`'s own attempt row still records its truthful 503 — history keeps both
dispatches, state keeps the newer one.

Two properties worth defending:

* **The token is minted by the database.** `lease_generation = d.lease_generation + 1` inside the claim
  statement means no worker can choose its own token and no clock participates in ordering. A clock skew
  cannot produce two workers with the same generation.
* **Recovery needs no new mechanism.** An expired-lease row is claimable by the *same* query as normal
  due work, so there is no repair job and no in-memory state to rebuild. A restarted worker finds
  everything the row already says: `attempt_count`, `cycle`, `attempts_in_cycle`, `next_attempt_at`.

Fencing protects **state**, not the **network**: a paused worker still sends its request, so `t7`
asserts 2 requests with 2 distinct attempt ids, generations 1→2, one effect, `DELIVERED` with no lease
held, and provably no third dispatch. That is the correct expectation, and it is the reason §2 exists.

---

## 4. Production security

**Tenant isolation is structural, not filtered.** Identity comes only from the resolved token
(`Bearer` → SHA-256 → `auth_tokens` → `Principal`); `tenantId` is not a request field anywhere. Every
query pins `tenant_id` from the principal before any optional predicate, so forgetting it is a type
error. Unknown resources and other-tenant resources return byte-identical 404s, and a malformed UUID
short-circuits to the same 404 — existence never leaks through status *or* response shape. The two
privilege levels cannot mix: tenant on `/ops/*` → 403, operator on a tenant route → 403, anonymous →
401.

**Signing.** `HMAC-SHA256(secret, "<unix_seconds>" + "." + <exact raw bytes>)`, lowercase hex. The
receiver verifies the ±300s freshness window **before** touching dedup or business logic, recomputes
over the raw received bytes, and compares in constant time (`timingSafeEqual` over equal-length buffers;
a structural mismatch fails closed without leaking where it diverged). A timestamp window is replay
protection, not a substitute for dedup — the two are complementary, and a receiver needs both.

**Secrets and tokens never travel.** `endpoints.secret` is passed only to `signWebhook`; it is not a
header, not logged, never in a response. Inbound `Authorization` is never forwarded outbound — the
client sends `content-type`, `content-length` and the five identity headers and nothing else, verified at
the wire. Tokens are stored hashed. `redrive_audit` records the operator *label*, not the token. The
logger redacts `secret`, `authorization`, `token`, `x-webhook-signature` and `payload` defensively, and
captured response snippets are bounded to 4 KiB and scrubbed of secret-looking keys. The readiness probe
returns a fixed message because driver errors name host, port and user.

**Outbound surface is minimized on purpose.** Destinations are read from the `endpoints` table and are
not expressible in a request: the publish schema is `.strict()`, so `{"url": ...}` is a 400 rather than a
silently dropped field that would teach the caller they had steered the delivery. `WEBHOOK_ALLOWED_HOSTS`
adds a deployment-owned network boundary; a violation raises before the socket opens and is classified
`NON_RETRYABLE`, because misconfiguration must not consume a retry budget. Redirects are never followed —
proven against a *live* target server that records zero requests (`t9`), since a followed redirect turns
the delivery worker into an SSRF proxy. Schemes are restricted to `http`/`https`, URLs are redacted in
errors, and timeouts are total (connect + response + body) with bounded response capture.

**What must change before this is production.** Endpoint secrets and API tokens move to a KMS/secrets
manager (the plaintext `endpoints.secret` column is a demo simplification). TLS terminates at the edge —
bearer tokens over plain HTTP are a leak. The destination allowlist becomes an egress proxy or DNS-level
policy, because hostname matching is bypassable by a public name resolving to an internal address, and
`https:` destinations get CA pinning. `/health` and `/ops/status` move behind an internal interface.
`RECEIVER_TEST_CONTROLS=0` everywhere. Body-size and rate limits are added at the proxy, since payloads
are stored verbatim and need a documented retention and PII decision rather than an engineer's guess.
