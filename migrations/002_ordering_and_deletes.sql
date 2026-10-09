-- Three things the first real connector exposed.

-- observed_at: the moment this version of a record was known to be true.
--
-- Webhooks arrive out of order and a backfill page can be fetched while a
-- webhook for the same record is in flight, so "last write wins" means "last
-- PROCESSED wins", which is wrong. Every write is guarded on observed_at
-- instead: an older observation never overwrites a newer one.
--
-- It is a separate column from source_updated_at because Stripe customers
-- have no updated timestamp at all. For a Stripe webhook observed_at is the
-- event's `created`; for a backfill row it is when the request started.
ALTER TABLE provider_record
    ADD COLUMN observed_at timestamptz NOT NULL DEFAULT now(),
    -- A tombstone, not a DELETE. If the row vanished, a late-arriving
    -- "created" event for the same id would resurrect a deleted customer.
    ADD COLUMN deleted_at  timestamptz;

-- A finished backfill has cursor NULL, and so does one that has not started.
-- completed_at tells them apart, which is what lets a re-run of an old page
-- job recognise that the backfill has moved on without it.
ALTER TABLE sync_cursor
    ADD COLUMN completed_at timestamptz;
