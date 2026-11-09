-- Conflicts are kept current, not accumulated.
--
-- At most one OPEN conflict per person, field and losing provider. When the
-- values disagree again it is updated in place; when they agree it is closed
-- with resolution = 'converged'. The alternative, a new row per detection,
-- turns the conflicts screen into a log nobody can act on.
ALTER TABLE field_conflict
    ADD COLUMN resolution text,
    ADD CONSTRAINT field_conflict_resolution_valid
        CHECK (resolution IS NULL OR resolution IN ('converged', 'manual'));

CREATE UNIQUE INDEX field_conflict_one_open
    ON field_conflict (person_id, field, losing_provider)
    WHERE resolved_at IS NULL;
