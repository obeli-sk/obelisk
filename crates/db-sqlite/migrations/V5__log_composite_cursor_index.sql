DROP INDEX IF EXISTS idx_t_log_execution_id_created_at;

CREATE INDEX idx_t_log_execution_id_id
    ON t_log (execution_id, id);
