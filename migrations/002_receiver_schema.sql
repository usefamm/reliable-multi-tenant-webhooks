-- 002_receiver_schema.sql
-- Mock receiver tables. The receiver shares the same PostgreSQL instance but its
-- state is logically separate: it records every inbound request and applies each
-- event's business effect exactly once via a durable uniqueness constraint.

-- ---------------------------------------------------------------------------
-- Durable deduplication / business effect (survives receiver restart)
-- ---------------------------------------------------------------------------
CREATE TABLE receiver_effects (
  id            text PRIMARY KEY,
  endpoint_id   text NOT NULL,
  event_id      text NOT NULL,
  content_hash  text NOT NULL,             -- sha256 hex of the canonical envelope content
  applied_at    timestamptz NOT NULL DEFAULT now(),

  -- The business effect identity is (endpoint, event). The UNIQUE constraint is
  -- what makes the effect idempotent and durable across restarts.
  CONSTRAINT receiver_effect_identity UNIQUE (endpoint_id, event_id)
);

-- ---------------------------------------------------------------------------
-- Request log: records EVERY inbound webhook request (for test assertions)
-- ---------------------------------------------------------------------------
CREATE TABLE receiver_requests (
  id           text PRIMARY KEY,
  endpoint_id  text NOT NULL,
  event_id     text,
  delivery_id  text,
  attempt_id   text,
  signature_ok boolean NOT NULL,
  mode         text NOT NULL,               -- the test-only mode in effect for this request
  received_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX receiver_requests_event_idx ON receiver_requests (event_id, received_at);
CREATE INDEX receiver_requests_endpoint_idx ON receiver_requests (endpoint_id, received_at);

-- ---------------------------------------------------------------------------
-- Test-only receiver modes (selected via fixtures / control interface,
-- NEVER via public event fields). Keyed by endpoint (and optionally event).
-- ---------------------------------------------------------------------------
CREATE TABLE receiver_modes (
  endpoint_id   text NOT NULL,
  event_id      text,                       -- NULL => applies to all events for the endpoint
  mode          text NOT NULL,              -- success|temp_failure|perm_failure|rate_limited|lost_response|slow|reject_400|redirect
  remaining     integer,                    -- for counted modes: how many more calls fail
  retry_after   integer,                    -- for rate_limited: Retry-After delta-seconds
  delay_ms      integer,                    -- for slow: artificial delay
  updated_at    timestamptz NOT NULL DEFAULT now(),

  -- One mode per (endpoint, event-or-global).
  CONSTRAINT receiver_modes_pkey PRIMARY KEY (endpoint_id, event_id)
);
