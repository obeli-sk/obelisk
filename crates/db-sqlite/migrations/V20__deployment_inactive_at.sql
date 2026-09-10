ALTER TABLE t_deployment ADD COLUMN inactive_at TEXT;

UPDATE t_deployment
SET inactive_at = strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
WHERE status = 'inactive';

CREATE INDEX idx_t_deployment_inactive_at
    ON t_deployment (inactive_at, deployment_id)
    WHERE status = 'inactive';

CREATE INDEX idx_t_state_retention_updated_at
    ON t_state (updated_at, execution_id)
    WHERE is_top_level = TRUE AND tombstoned = FALSE;
