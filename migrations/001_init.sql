-- Idempotent: safe to run on every startup (guarded by an advisory lock in app/db.py).

CREATE TABLE IF NOT EXISTS jobs (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key   TEXT        NOT NULL,
    request_hash      TEXT        NOT NULL,
    payload           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status            TEXT        NOT NULL DEFAULT 'queued'
                      CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    attempts          INT         NOT NULL DEFAULT 0,
    max_attempts      INT         NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    -- Lease: who holds the job and until when (always DB time, never worker time).
    lease_owner       TEXT,
    lease_expires_at  TIMESTAMPTZ,
    -- Fencing token: bumped on every claim. Every write from a worker must present it.
    fencing_token     BIGINT      NOT NULL DEFAULT 0,
    cancel_requested  BOOLEAN     NOT NULL DEFAULT false,
    result            JSONB,
    error             TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    CONSTRAINT jobs_idempotency_key_uniq UNIQUE (idempotency_key),
    -- A running job always has a lease; a non-running job never does.
    CONSTRAINT jobs_lease_consistent CHECK (
        (status = 'running') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    )
);

-- Claim path: oldest queued job first.
CREATE INDEX IF NOT EXISTS jobs_queued_idx  ON jobs (created_at, id) WHERE status = 'queued';
-- Reaper path: expired leases.
CREATE INDEX IF NOT EXISTS jobs_running_idx ON jobs (lease_expires_at) WHERE status = 'running';
-- List / keyset pagination.
CREATE INDEX IF NOT EXISTS jobs_list_idx    ON jobs (created_at, id);

-- The emulated "execution" side effect. Written in the SAME transaction as the
-- fenced transition to 'succeeded', so it commits exactly when the job does.
CREATE TABLE IF NOT EXISTS side_effects (
    id             BIGSERIAL PRIMARY KEY,
    job_id         UUID        NOT NULL REFERENCES jobs (id),
    fencing_token  BIGINT      NOT NULL,
    worker_id      TEXT        NOT NULL,
    written_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Defense in depth: even if the state machine has a bug, a second effect fails loudly.
-- (The chaos harness drops this index for the `naive_complete` mutation run.)
CREATE UNIQUE INDEX IF NOT EXISTS side_effects_job_uniq ON side_effects (job_id);

-- Append-only log of every attempt that started (committed with the claim).
-- Lets us count how many times a job *really ran*, separately from committed effects.
CREATE TABLE IF NOT EXISTS executions (
    id             BIGSERIAL PRIMARY KEY,
    job_id         UUID        NOT NULL,
    worker_id      TEXT        NOT NULL,
    fencing_token  BIGINT      NOT NULL,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS executions_job_idx ON executions (job_id);
