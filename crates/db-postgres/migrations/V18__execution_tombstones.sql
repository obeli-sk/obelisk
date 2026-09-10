ALTER TABLE t_state ADD COLUMN tombstoned BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX idx_t_state_tombstoned_roots
    ON t_state (execution_id)
    WHERE is_top_level = TRUE AND tombstoned = TRUE;
