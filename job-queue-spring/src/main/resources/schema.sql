CREATE TABLE IF NOT EXISTS jobs (
    id               UUID        PRIMARY KEY,
    type             TEXT        NOT NULL,
    payload          JSONB       NOT NULL,
    status           TEXT        NOT NULL,   -- PENDING | IN_PROGRESS | COMPLETED | DEAD | CANCELLED
    attempts         INT         NOT NULL DEFAULT 0,
    max_attempts     INT         NOT NULL,
    error            TEXT,
    lease_owner      TEXT,                   -- worker pod that holds the current lease
    lease_expires_at TIMESTAMPTZ,            -- stale-job reaper scans: status=IN_PROGRESS AND lease_expires_at < NOW()
    created_at       TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL,
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);

CREATE INDEX IF NOT EXISTS idx_jobs_lease_expires ON jobs (status, lease_expires_at)
    WHERE status = 'IN_PROGRESS';

CREATE TABLE IF NOT EXISTS job_outbox (
    id         UUID        PRIMARY KEY,
    job_id     UUID        NOT NULL REFERENCES jobs (id),
    job_type   TEXT        NOT NULL,
    published  BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_job_outbox_unpublished ON job_outbox (created_at)
    WHERE published = FALSE;
