-- 003_auth_tokens.sql
-- Server-side mapping from a bearer token to a tenant identity (or operator role).
-- Only the SHA-256 hash of a token is stored; the raw token never touches the DB.
-- Tokens are resolved here, so a client-supplied tenantId is never trusted.

CREATE TABLE auth_tokens (
  token_hash  text PRIMARY KEY,             -- sha256 hex of the raw bearer token
  tenant_id   text REFERENCES tenants(id) ON DELETE CASCADE,  -- NULL for operator tokens
  role        text NOT NULL,                -- 'tenant' | 'operator'
  label       text NOT NULL,                -- safe, non-secret label for logs ('tenant-a', 'operator')
  created_at  timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT auth_tokens_role_check CHECK (role IN ('tenant','operator')),
  CONSTRAINT auth_tokens_tenant_consistency CHECK (
    (role = 'tenant'   AND tenant_id IS NOT NULL) OR
    (role = 'operator' AND tenant_id IS NULL)
  )
);

CREATE INDEX auth_tokens_tenant_idx ON auth_tokens (tenant_id);
