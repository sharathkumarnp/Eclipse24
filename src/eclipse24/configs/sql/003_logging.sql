-- src/eclipse24/configs/sql/003_logging.sql

-- ===== ENUM: action_enum (create if missing; then add values if missing) =====
DO
$$
BEGIN
CREATE TYPE action_enum AS ENUM (
    'posted',
    'approve',
    'extend_request',
    'extend_approve',
    'extend_deny',
    'auto_block',
    'reminder'
  );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Extend enum with any additional values you may log now or in future
-- (safe, IF NOT EXISTS supported on PG 9.6+ with this pattern)
DO
$$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_enum e JOIN pg_type t ON e.enumtypid=t.oid
                 WHERE t.typname='action_enum' AND e.enumlabel='validation_passed') THEN
ALTER TYPE action_enum ADD VALUE 'validation_passed';
END IF;
END $$;

DO
$$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_enum e JOIN pg_type t ON e.enumtypid=t.oid
                 WHERE t.typname='action_enum' AND e.enumlabel='blocked') THEN
ALTER TYPE action_enum ADD VALUE 'blocked';
END IF;
END $$;

-- You can append more values later using the same block if needed:
-- DO $$ BEGIN
--   IF NOT EXISTS (...) THEN ALTER TYPE action_enum ADD VALUE 'something_new'; END IF;
-- END $$;

-- ===== TABLE: actions (actor trail for UI/ops) =====
CREATE TABLE IF NOT EXISTS actions
(
    id
    BIGSERIAL
    PRIMARY
    KEY,
    server_id
    TEXT
    REFERENCES
    servers
(
    server_id
),
    action action_enum NOT NULL,
    actor_uid TEXT,
    actor_name TEXT,
    meta JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now
(
)
    );

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_actions_server_created
    ON actions (server_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_actions_action_created
    ON actions (action, created_at DESC);

-- ===== TABLE: audit_log (free-form text action + details) =====
-- Keep 'action' as TEXT to avoid enum coupling; this is your durable audit trail.
CREATE TABLE IF NOT EXISTS audit_log
(
    id
    BIGSERIAL
    PRIMARY
    KEY,
    server_id
    TEXT
    REFERENCES
    servers
(
    server_id
),
    action TEXT NOT NULL,
    actor TEXT,
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now
(
)
    );

CREATE INDEX IF NOT EXISTS idx_audit_server_created
    ON audit_log (server_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_action_created
    ON audit_log (action, created_at DESC);

-- ===== TABLE: extensions (current extension grant) =====
-- One row per server; upserts will keep latest grant & expiry.
CREATE TABLE IF NOT EXISTS extensions
(
    server_id
    TEXT
    PRIMARY
    KEY
    REFERENCES
    servers
(
    server_id
),
    extension_hours INT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    granted_by_uid TEXT,
    granted_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now
(
)
    );

CREATE INDEX IF NOT EXISTS idx_extensions_expires_at
    ON extensions (expires_at);

-- ===== TABLE: reminders (last reminder bookkeeping) =====
CREATE TABLE IF NOT EXISTS reminders
(
    server_id
    TEXT
    PRIMARY
    KEY
    REFERENCES
    servers
(
    server_id
),
    last_reminded_at TIMESTAMPTZ NOT NULL
    );

CREATE INDEX IF NOT EXISTS idx_reminders_last_reminded
    ON reminders (last_reminded_at);