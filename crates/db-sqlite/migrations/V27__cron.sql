CREATE TABLE t_cron (
    cron_digest BLOB PRIMARY KEY NOT NULL,
    tracking_execution_id TEXT,
    last_target_execution_id TEXT,
    last_submitted_at TEXT,
    created_at TEXT NOT NULL,
    CHECK ((last_target_execution_id IS NULL) = (last_submitted_at IS NULL))
) STRICT;

CREATE UNIQUE INDEX idx_t_cron_tracking_execution ON t_cron (tracking_execution_id);
