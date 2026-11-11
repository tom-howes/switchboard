-- Claude's reasoning for each field in a proposed mapping, and its confidence,
-- kept with the proposal so the person approving it can see why each choice
-- was made. Human-written mappings leave it null.
ALTER TABLE field_mapping ADD COLUMN notes jsonb;
