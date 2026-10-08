-- Switchboard schema.
--
-- Layering, outermost in:
--   workspace / connection      who we sync for, and with whose credentials
--   field_mapping               how a provider payload becomes a canonical record (config, not code)
--   provider_record             one row per record per provider, raw payload retained
--   person / person_link        the canonical object, plus an audit trail of how we got there
--   sync_job / sync_cursor      how work is scheduled and resumed
--   webhook_event               replay protection
--   dead_letter                 what we gave up on, and why
--
-- Table order below is also dependency order. Postgres will not accept a
-- foreign key to a table that does not exist yet.

CREATE TABLE workspace (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name        text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- One row per provider per workspace. `credentials` is an AES-256-GCM blob,
-- never plaintext: this is the highest value row in the database.
CREATE TABLE connection (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id        uuid        NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    provider            text        NOT NULL,
    external_account_id text,
    credentials         text        NOT NULL,
    status              text        NOT NULL DEFAULT 'active',
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT connection_provider_once UNIQUE (workspace_id, provider),
    CONSTRAINT connection_status_valid  CHECK (status IN ('active', 'needs_reauth', 'disabled'))
);

-- The mapping document: provider payload -> canonical fields, as DATA.
--
-- This is the decision the whole extensibility story rests on. Because a mapping
-- is a row rather than a function, the LLM layer can draft one, a human can
-- approve it, and a new provider needs no bespoke normalisation code at all.
--
-- `rules` shape:
--   {
--     "external_id":       "$.id",
--     "source_created_at": "$.created",
--     "source_updated_at": "$.updated_at",
--     "fields": [
--       {"canonical": "primary_email", "source": "$.email",
--        "transforms": ["trim", "lower"], "required": true},
--       {"canonical": "given_name", "source": "$.properties.firstname",
--        "transforms": ["trim"]}
--     ]
--   }
CREATE TABLE field_mapping (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    provider       text        NOT NULL,
    canonical_type text        NOT NULL DEFAULT 'person',
    version        int         NOT NULL,
    rules          jsonb       NOT NULL,
    status         text        NOT NULL DEFAULT 'proposed',
    -- provenance, so "who decided firstname maps to given_name" has an answer
    proposed_by    text,
    model          text,
    sample_payload jsonb,
    created_at     timestamptz NOT NULL DEFAULT now(),
    approved_at    timestamptz,
    approved_by    text,
    CONSTRAINT field_mapping_version_once UNIQUE (provider, canonical_type, version),
    CONSTRAINT field_mapping_status_valid CHECK (status IN ('proposed', 'approved', 'rejected', 'superseded'))
);

-- At most one approved mapping per provider per canonical type. A partial
-- unique index is the cleanest way to say "only one row may be live".
CREATE UNIQUE INDEX field_mapping_one_approved
    ON field_mapping (provider, canonical_type)
    WHERE status = 'approved';

-- The canonical object. One row per human per workspace, however many
-- providers happen to know about them.
CREATE TABLE person (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id  uuid        NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    primary_email text        NOT NULL,
    full_name     text,
    given_name    text,
    family_name   text,
    phone         text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT person_email_once UNIQUE (workspace_id, primary_email)
);

-- One row per record per provider.
--
-- `raw` holds the provider payload untouched. The flat columns beside it are a
-- projection of it, not a replacement. Normalisation is lossy and the mapping
-- will be wrong at first; keeping raw means fixing a mapping is a re-normalise
-- over local rows rather than a full refetch from the provider.
CREATE TABLE provider_record (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_id     uuid        NOT NULL REFERENCES connection(id) ON DELETE CASCADE,
    external_id       text        NOT NULL,
    raw               jsonb       NOT NULL,

    primary_email     text,
    full_name         text,
    given_name        text,
    family_name       text,
    phone             text,

    mapping_id        uuid REFERENCES field_mapping(id),
    source_created_at timestamptz,
    source_updated_at timestamptz,
    normalized_at     timestamptz NOT NULL DEFAULT now(),
    fetched_at        timestamptz NOT NULL DEFAULT now(),

    person_id         uuid REFERENCES person(id) ON DELETE SET NULL,

    CONSTRAINT provider_record_identity UNIQUE (connection_id, external_id)
);

CREATE INDEX provider_record_email_idx   ON provider_record (lower(primary_email)) WHERE primary_email IS NOT NULL;
CREATE INDEX provider_record_person_idx  ON provider_record (person_id);
CREATE INDEX provider_record_updated_idx ON provider_record (connection_id, source_updated_at DESC);

-- Why a given provider_record was attached to a given person.
-- Identity resolution that cannot be explained after the fact is not auditable,
-- and "why did these two merge?" is a question that gets asked in this domain.
CREATE TABLE person_link (
    id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    person_id          uuid        NOT NULL REFERENCES person(id) ON DELETE CASCADE,
    provider_record_id uuid        NOT NULL REFERENCES provider_record(id) ON DELETE CASCADE,
    match_rule         text        NOT NULL,
    matched_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT person_link_record_once UNIQUE (provider_record_id)
);

-- Two providers disagreeing about the same field on the same person.
-- The loser is recorded rather than dropped: silently discarding one system's
-- version of the truth is how integrations lose data invisibly.
CREATE TABLE field_conflict (
    id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    person_id          uuid        NOT NULL REFERENCES person(id) ON DELETE CASCADE,
    field              text        NOT NULL,
    winning_value      text,
    winning_provider   text        NOT NULL,
    winning_updated_at timestamptz,
    losing_value       text,
    losing_provider    text        NOT NULL,
    losing_updated_at  timestamptz,
    resolved_at        timestamptz,
    detected_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX field_conflict_open_idx ON field_conflict (person_id, detected_at DESC)
    WHERE resolved_at IS NULL;

-- Where a backfill got to. Persisted per page so a crash resumes
-- rather than starting over.
CREATE TABLE sync_cursor (
    connection_id uuid        NOT NULL REFERENCES connection(id) ON DELETE CASCADE,
    kind          text        NOT NULL,
    cursor        text,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (connection_id, kind)
);

-- The work queue. Postgres rather than Redis: one less thing to run, and
-- SELECT ... FOR UPDATE SKIP LOCKED gives exactly-one-worker-per-row without
-- an external broker.
CREATE TABLE sync_job (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_id uuid        NOT NULL REFERENCES connection(id) ON DELETE CASCADE,
    kind          text        NOT NULL,
    payload       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    status        text        NOT NULL DEFAULT 'pending',
    attempts      int         NOT NULL DEFAULT 0,
    max_attempts  int         NOT NULL DEFAULT 5,
    run_after     timestamptz NOT NULL DEFAULT now(),
    locked_at     timestamptz,
    last_error    text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT sync_job_kind_valid   CHECK (kind IN ('backfill', 'incremental', 'webhook', 'reconcile', 'write')),
    CONSTRAINT sync_job_status_valid CHECK (status IN ('pending', 'running', 'done', 'failed'))
);

-- The claim query's index: work that is pending and due, oldest first.
CREATE INDEX sync_job_claimable_idx ON sync_job (run_after, created_at)
    WHERE status = 'pending';

-- Replay protection. Providers redeliver on any non-2xx, and sometimes on a 2xx
-- they did not hear. Seeing an event id twice must be a no-op, so the unique
-- constraint IS the dedupe mechanism rather than an application-level check
-- that two concurrent workers could both pass.
CREATE TABLE webhook_event (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_id     uuid        NOT NULL REFERENCES connection(id) ON DELETE CASCADE,
    provider_event_id text        NOT NULL,
    topic             text,
    received_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT webhook_event_once UNIQUE (connection_id, provider_event_id)
);

-- Work that exhausted its retries, kept with the payload intact so it can be
-- inspected and replayed. "Where did my record go" deserves a better answer
-- than a log line that has already rotated away.
CREATE TABLE dead_letter (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_id uuid        NOT NULL REFERENCES connection(id) ON DELETE CASCADE,
    job_id        uuid,
    kind          text        NOT NULL,
    payload       jsonb       NOT NULL,
    error         text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    retried_at    timestamptz
);

CREATE INDEX dead_letter_open_idx ON dead_letter (connection_id, created_at DESC)
    WHERE retried_at IS NULL;
