ALTER TABLE t_deployment ADD COLUMN updated_at TIMESTAMPTZ;
UPDATE t_deployment SET updated_at = GREATEST(created_at, last_active_at, inactive_at);
ALTER TABLE t_deployment ALTER COLUMN updated_at SET NOT NULL;
