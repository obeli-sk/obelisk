DROP INDEX idx_t_deployment_inactive_at;
ALTER TABLE t_deployment DROP COLUMN inactive_at;
-- Full index serves deployment listing; retention filters inactive rows from it.
DROP INDEX idx_t_deployment_created_at;
CREATE INDEX idx_t_deployment_updated_at ON t_deployment (updated_at, deployment_id);
-- Fails on databases storing the same content twice; see CHANGELOG.md.
CREATE UNIQUE INDEX idx_t_deployment_digest ON t_deployment (digest);
