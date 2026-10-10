-- 001_core_schema.sql
-- Core application schema: tenants, endpoints, events, deliveries,
-- delivery_attempts, idempotency_records, redrive_audit.
--
-- Conventions:
--   * All PKs are UUID (text) generated in application code (UUIDv4).
--   * All timestamps are timestamptz, stored in UTC.
--   * deliveries.event_id is UNIQUE -> enforces exactly one logical delivery per
--     event, which is what makes the atomic event+delivery publish verifiable.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------------
-- Tenants and endpoints (endpoint url/secret are trusted deployment config)
-- ---------------------------------------------------------------------------
CREATE TABLE tenants (
  id          text PRIMARY KEY,
  name        text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE endpoints (
  id          text PRIMARY KEY,
  tenant_id   text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  name        text NOT NULL,
  url         text NOT NULL,              -- destination; NEVER caller-supplied, NEVER returned by APIs
  secret      text NOT NULL,              -- HMAC signing secret; NEVER logged or returned
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Tenant-scoped endpoint lookup (ownership checks on every publish).
CREATE INDEX endpoints_tenant_idx ON endpoints (tenant_id);

-- ---------------------------------------------------------------------------
-- Events (immutable published facts)
-- ---------------------------------------------------------------------------
CREATE TABLE events (
  id           text PRIMARY KEY,
  tenant_id    text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  endpoint_id  text NOT NULL REFERENCES endpoints(id),
  event_type   text NOT NULL,
  payload      jsonb NOT NULL,
  occurred_at  timestamptz NOT NULL,       -- fixed at publication; embedded in the envelope
  created_at   timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT events_event_type_len CHECK (char_length(event_type) BETWEEN 1 AND 100)
);

-- Tenant event queries ordered by recency (GET /events/:id lookups use PK).
CREATE INDEX events_tenant_created_idx ON events (tenant_id, created_at DESC, id DESC);

-- ---------------------------------------------------------------------------
-- Deliveries (the durable queue row + delivery state machine)
-- ---------------------------------------------------------------------------
CREATE TYPE delivery_state AS ENUM ('READY','IN_FLIGHT','RETRY_WAIT','DELIVERED','DEAD');
CREATE TYPE attempt_outcome AS ENUM ('SUCCESS','RETRYABLE','NON_RETRYABLE','UNKNOWN');

CREATE TABLE deliveries (
  id                 text PRIMARY KEY,
  event_id           text NOT NULL UNIQUE REFERENCES events(id) ON DELETE CASCADE,
  tenant_id          text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  endpoint_id        text NOT NULL REFERENCES endpoints(id),

  state              delivery_state NOT NULL DEFAULT 'READY',

  envelope_bytes     bytea NOT NULL,        -- exact webhook body, written once, reused for all attempts
  envelope_hash      text NOT NULL,         -- sha256 hex of envelope_bytes (integrity / debugging)

  attempt_count      integer NOT NULL DEFAULT 0,   -- lifetime attempts (NOT reset by redrive)
  cycle              integer NOT NULL DEFAULT 1,   -- automatic cycle number; redrive increments
  attempts_in_cycle  integer NOT NULL DEFAULT 0,   -- attempts used in current cycle (<= max)

  next_attempt_at    timestamptz,           -- due time; NULL when terminal (DELIVERED/DEAD)

  lease_owner        text,                  -- worker identity holding the lease
  lease_generation   bigint NOT NULL DEFAULT 0,    -- fencing token; incremented on every claim
  lease_expires_at   timestamptz,           -- bounded lease expiry

  last_http_status   integer,               -- last known HTTP status (NULL when unknown)
  last_error_code    text,                  -- bounded error code (no secrets, no full bodies)

  created_at         timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now()
);

-- Due-work scan for claiming: only rows that can be worked now.
-- Partial index keeps it small and hot.
CREATE INDEX deliveries_due_idx
  ON deliveries (next_attempt_at)
  WHERE state IN ('READY','RETRY_WAIT');

-- Lease-expiry recovery scan: in-flight rows whose lease has lapsed.
CREATE INDEX deliveries_lease_expiry_idx
  ON deliveries (lease_expires_at)
  WHERE state = 'IN_FLIGHT';

-- Tenant-scoped delivery listing with state filter and stable ordering.
CREATE INDEX deliveries_tenant_state_idx
  ON deliveries (tenant_id, state, created_at DESC, id DESC);

-- Operational counters (status endpoint): group by state.
CREATE INDEX deliveries_state_idx ON deliveries (state);

-- ---------------------------------------------------------------------------
-- Delivery attempts (history; persisted BEFORE dispatch)
-- ---------------------------------------------------------------------------
CREATE TABLE delivery_attempts (
  id                text PRIMARY KEY,
  delivery_id       text NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
  attempt_number    integer NOT NULL,        -- lifetime attempt number (1..N)
  cycle             integer NOT NULL,        -- which automatic cycle this attempt belongs to
  attempt_id        text NOT NULL,           -- the X-Attempt-Id sent (fresh per attempt)

  lease_owner       text,                    -- ownership evidence at dispatch time
  lease_generation  bigint,                  -- fencing token this attempt was made under

  started_at        timestamptz NOT NULL,
  finished_at       timestamptz,             -- NULL when outcome unknown (crash before completion)

  outcome           attempt_outcome NOT NULL DEFAULT 'UNKNOWN',
  http_status       integer,                 -- NULL when unknown (lost response / never sent)
  error_code        text,                    -- bounded classification code
  response_snippet  text                     -- captured response, <= 4 KiB
);

-- Attempt history for a delivery, ordered.
CREATE INDEX delivery_attempts_delivery_idx
  ON delivery_attempts (delivery_id, attempt_number);

-- ---------------------------------------------------------------------------
-- Idempotency records (publication + redrive), scoped by tenant + operation
-- ---------------------------------------------------------------------------
CREATE TABLE idempotency_records (
  id               text PRIMARY KEY,
  tenant_id        text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  operation        text NOT NULL,            -- 'publish_event' | 'redrive'
  idempotency_key  text NOT NULL,
  request_hash     text NOT NULL,            -- sha256 hex of canonicalized validated input
  response_status  integer NOT NULL,         -- original HTTP status to replay
  response_body    jsonb NOT NULL,           -- original response body to replay
  resource_id      text,                     -- eventId (publish) / deliveryId (redrive)
  created_at       timestamptz NOT NULL DEFAULT now(),

  -- Same tenant + operation + key can exist only once. Different tenants may
  -- reuse the same key because tenant_id is part of the uniqueness.
  CONSTRAINT idempotency_unique UNIQUE (tenant_id, operation, idempotency_key)
);

-- ---------------------------------------------------------------------------
-- Redrive audit trail (operator actions)
-- ---------------------------------------------------------------------------
CREATE TABLE redrive_audit (
  id            text PRIMARY KEY,
  delivery_id   text NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
  operator      text NOT NULL,               -- operator token id / label (never the raw token)
  reason        text NOT NULL,
  idempotency_key text NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX redrive_audit_delivery_idx ON redrive_audit (delivery_id, created_at);
