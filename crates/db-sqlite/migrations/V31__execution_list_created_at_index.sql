-- Execution listing orders by `(created_at, execution_id)`.
DROP INDEX idx_t_state_created_at;
CREATE INDEX idx_t_state_created_at ON t_state (created_at, execution_id);
