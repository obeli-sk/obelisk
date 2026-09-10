ALTER TABLE t_deployment ADD COLUMN inactive_at TIMESTAMPTZ;

UPDATE t_deployment SET inactive_at = CURRENT_TIMESTAMP WHERE status = 'inactive';

CREATE INDEX idx_t_deployment_inactive_at
    ON t_deployment (inactive_at, deployment_id)
    WHERE status = 'inactive';

CREATE INDEX idx_t_state_retention_updated_at
    ON t_state (updated_at, execution_id)
    WHERE is_top_level = TRUE AND tombstoned = FALSE;
