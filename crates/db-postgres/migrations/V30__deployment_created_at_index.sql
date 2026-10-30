-- Full index serves deployment listing; retention filters inactive rows from it.
DROP INDEX idx_t_deployment_retention_created_at;
CREATE INDEX idx_t_deployment_created_at ON t_deployment (created_at, deployment_id);
