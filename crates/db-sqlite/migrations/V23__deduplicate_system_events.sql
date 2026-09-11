ALTER TABLE t_system_event ADD COLUMN dedupe_key TEXT CHECK (length(dedupe_key) <= 512);
CREATE UNIQUE INDEX t_system_event_dedupe_idx
    ON t_system_event(code, deployment_id, dedupe_key)
    WHERE dedupe_key IS NOT NULL;
