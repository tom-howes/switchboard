-- One row per reconciliation sweep: what it checked and what it found.
--
-- records_changed is the headline number. It counts records whose mapped
-- fields differed from what we already had, i.e. changes the webhooks should
-- have delivered and did not. Over time it is the webhook loss rate.
CREATE TABLE sync_run (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_id    uuid        NOT NULL REFERENCES connection(id) ON DELETE CASCADE,
    kind             text        NOT NULL DEFAULT 'reconcile',
    status           text        NOT NULL DEFAULT 'running',
    started_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    records_seen     int         NOT NULL DEFAULT 0,
    records_changed  int         NOT NULL DEFAULT 0,
    records_missing  int         NOT NULL DEFAULT 0,
    records_deleted  int         NOT NULL DEFAULT 0,
    error            text,
    CONSTRAINT sync_run_status_valid CHECK (status IN ('running', 'completed', 'failed'))
);

-- At most one sweep in flight per connection. A second "start" for the same
-- connection hits this and is skipped rather than running twice in parallel.
CREATE UNIQUE INDEX sync_run_one_running ON sync_run (connection_id) WHERE status = 'running';

CREATE INDEX sync_run_recent_idx ON sync_run (connection_id, started_at DESC);

-- When a sweep last saw this record in the provider's own listing. After a
-- complete walk, live records not seen since the walk began are "missing".
ALTER TABLE provider_record ADD COLUMN last_seen_at timestamptz;
