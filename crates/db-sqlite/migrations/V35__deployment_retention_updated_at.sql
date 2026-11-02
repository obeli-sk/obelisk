DROP INDEX idx_t_deployment_inactive_at;
ALTER TABLE t_deployment DROP COLUMN inactive_at;
-- Full index serves deployment listing; retention filters inactive rows from it.
DROP INDEX idx_t_deployment_created_at;
CREATE INDEX idx_t_deployment_updated_at ON t_deployment (updated_at, deployment_id);
-- backcompat: 0.43, rows storing the same content as an earlier deployment point to the one kept unique.
ALTER TABLE t_deployment ADD COLUMN superseded_by TEXT CHECK (length(superseded_by) <= 30);
UPDATE t_deployment SET superseded_by = (
    SELECT c.deployment_id FROM t_deployment c WHERE c.digest = t_deployment.digest
    ORDER BY c.status = 'active' DESC, c.status = 'enqueued' DESC, c.updated_at DESC, c.deployment_id DESC
    LIMIT 1
);
UPDATE t_deployment SET superseded_by = NULL WHERE superseded_by = deployment_id;
CREATE UNIQUE INDEX idx_t_deployment_digest ON t_deployment (digest) WHERE superseded_by IS NULL;
