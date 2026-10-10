-- Tenant-wide delivery listing without a state filter.
--
-- deliveries_tenant_state_idx (migration 001) covers the state-filtered case.
-- GET /deliveries may also be called with no state filter, ordered by
-- (created_at DESC, id DESC) for stable keyset pagination; this partial-free
-- index supports that scan without forcing a sort.
CREATE INDEX IF NOT EXISTS deliveries_tenant_created_idx
  ON deliveries (tenant_id, created_at DESC, id DESC);
