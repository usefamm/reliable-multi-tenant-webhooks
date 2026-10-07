# Reliable Multi-Tenant Webhook Delivery

A production-oriented webhook delivery service built with **Node.js, TypeScript, PostgreSQL, and Docker Compose**.

The system accepts tenant events, persists delivery work durably, and delivers signed webhooks with:

- Idempotent event publication
- Automatic retries with exponential backoff
- Crash recovery
- Worker leases and fencing
- HMAC-SHA256 webhook signatures
- Receiver-side durable deduplication
- Multi-tenant isolation
- Operator redrive of dead deliveries
- Bounded worker concurrency

> **Delivery semantics:** the system does not claim exactly-once HTTP delivery. A webhook may be sent more than once after an uncertain outcome, but the mock receiver applies the business effect only once.

---

## Architecture

```text
Client
  |
  | POST /events
  v
API
  |
  | PostgreSQL transaction
  v
PostgreSQL
  |
  | durable delivery queue
  |
  +-------------------+
  |                   |
Worker A            Worker B
  |                   |
  +---------+---------+
            |
            | signed HTTP
            v
      Mock Webhook Receiver
            |
            | durable deduplication
            v
       Business Effect
```

### Main flow

1. Client publishes an event with an `Idempotency-Key`.
2. API authenticates the tenant.
3. Event + delivery + idempotency record are committed atomically.
4. A worker claims the delivery from PostgreSQL.
5. The worker creates an attempt record **before** making the HTTP request.
6. The webhook is signed with HMAC-SHA256.
7. Receiver verifies the signature and deduplicates using `endpointId + eventId`.
8. Successful delivery becomes `DELIVERED`.
9. Retryable failures become `RETRY_WAIT`.
10. Exhausted deliveries become `DEAD`.
11. An operator can redrive a `DEAD` delivery without changing its `eventId` or `deliveryId`.

---

## Important Design Decisions

### 1. PostgreSQL as the durable queue

No Redis or Kafka is required.

PostgreSQL stores:

- Events
- Logical deliveries
- Delivery attempts
- Idempotency records
- Redrive audit records

Workers claim due deliveries using PostgreSQL row locking with `FOR UPDATE SKIP LOCKED`.

The HTTP request is made **outside the database transaction**, so database locks are not held during network calls.

---

### 2. Idempotent publication

The same tenant cannot publish another event using the same idempotency key.

Equivalent requests return the original result.

A changed request using the same key returns `409 Conflict`.

JSON object key ordering does not affect the idempotency comparison.

---

### 3. Worker crash recovery

A worker receives a bounded lease when it claims a delivery.

If the worker crashes, another worker can recover the delivery after the lease expires.

A lease alone cannot prevent duplicate HTTP requests, because the first request may already have reached the receiver.

Therefore the system uses:

**durable attempt history + fencing + receiver-side deduplication.**

---

### 4. Fencing stale workers

Consider:

```text
Worker A claims delivery
       |
       | lease expires
       v
Worker B claims delivery
       |
       | completes
       v
New state committed

Worker A wakes up
       |
       X
       Cannot overwrite Worker B
```

The delivery state contains ownership/generation information so an old worker cannot overwrite newer state.

---

### 5. Delivery semantics

The system intentionally does **not** claim exactly-once delivery.

For example:

```text
Worker -> Receiver
        |
        | receiver commits effect
        |
        X response lost
```

The worker does not know whether the request succeeded.

It may retry:

```text
Attempt 1 -> effect applied -> response lost
Attempt 2 -> duplicate -> 200
```

Result:

```text
HTTP requests: 2
Business effects: 1
```

This is the expected behavior.

---

## Retry Policy

| Response           | Behavior                             |
| ------------------ | ------------------------------------ |
| 2xx                | `DELIVERED`                          |
| Timeout            | Retry                                |
| Connection failure | Retry                                |
| 408                | Retry                                |
| 429                | Retry using `Retry-After` when valid |
| 5xx                | Retry                                |
| Other status       | `DEAD`                               |

Automatic delivery cycles have a maximum of **5 attempts**.

Backoff:

```text
1s
2s
4s
8s
```

with small random jitter.

Retry scheduling is persisted in PostgreSQL, so restarting a worker does not reset the retry schedule.

---

## Webhook Security

Every attempt contains:

```text
X-Event-Id
X-Delivery-Id
X-Attempt-Id
X-Webhook-Timestamp
X-Webhook-Signature
```

The signature is:

```text
HMAC-SHA256(
  secret,
  timestamp + "." + rawBody
)
```

The receiver:

1. Validates the timestamp.
2. Recomputes the HMAC.
3. Uses a timing-safe comparison.
4. Deduplicates the event.
5. Applies the business effect only once.

Secrets and destination URLs are deployment configuration, not caller-controlled input.

Outbound requests:

- Do not follow redirects.
- Have a 2-second timeout.
- Bound captured response data.
- Never forward API authentication tokens.

---

## Multi-Tenancy

Every authenticated request is resolved to a tenant from the server-side token.

Tenant IDs are never trusted from the request body.

A tenant can only access:

- Its own events
- Its own deliveries
- Its own endpoints

Unknown and cross-tenant resources return `404`.

---

## API

### Publish event

```http
POST /events
Idempotency-Key: order-123
Authorization: Bearer <tenant-token>
```

```json
{
  "endpointId": "endpoint-1",
  "eventType": "order.created",
  "payload": {
    "orderId": "123"
  }
}
```

Returns:

```json
{
  "eventId": "...",
  "deliveryId": "...",
  "status": "READY"
}
```

### Get event

```http
GET /events/:id
```

Returns delivery state, attempt count, next retry time, and the latest bounded error.

### List deliveries

```http
GET /deliveries
```

Supports tenant-scoped pagination and state filtering.

### Redrive

```http
POST /ops/deliveries/:id/redrive
```

Only `DEAD` deliveries can be redriven.

A redrive:

- Preserves `eventId`
- Preserves `deliveryId`
- Preserves previous attempt history
- Starts a new retry cycle

Concurrent redrives are idempotent.

---

## Failure Scenarios Covered

The test suite covers the important failure boundaries:

- Concurrent duplicate publication
- Cross-tenant access
- Invalid requests
- Oversized requests
- Invalid signatures
- Lost receiver response
- Receiver restart
- 503 retry/recovery
- 429 `Retry-After`
- 400 permanent failure
- Retry exhaustion
- Worker crash before dispatch
- Worker crash after dispatch
- Stale worker after lease expiry
- Concurrent redrive
- Redirect rejection
- Worker concurrency limits
- Atomic transaction rollback

The project uses deterministic clock/scheduler/randomness hooks where needed to make failure tests reproducible.

---

## Running Locally

Requirements:

- Docker
- Docker Compose

Start the system:

```bash
docker compose up --build
```

Run tests:

```bash
npm test
```

See the full setup instructions and example requests in the repository documentation.

---

## Project Structure

```text
src/
  api/          HTTP API
  domain/       domain models and rules
  application/  use cases
  infrastructure/
                PostgreSQL and HTTP implementations
  workers/      delivery workers
  receiver/     mock webhook receiver

migrations/     database migrations
seed/           development data
test/           integration and acceptance tests
docs/
  DESIGN.md
  IMPLEMENTATION_PLAN.md
```

---

## Documentation

For deeper discussion:

- [`docs/DESIGN.md`](docs/DESIGN.md) — architecture and reliability decisions
- [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) — implementation details
