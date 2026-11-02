DROP INDEX idx_t_deployment_inactive_at;
ALTER TABLE t_deployment DROP COLUMN inactive_at;
CREATE INDEX idx_t_deployment_retention_updated_at ON t_deployment (updated_at, deployment_id)
    WHERE status = 'inactive';
-- Fails on databases storing the same content twice; see CHANGELOG.md.
CREATE UNIQUE INDEX idx_t_deployment_digest ON t_deployment (digest);
