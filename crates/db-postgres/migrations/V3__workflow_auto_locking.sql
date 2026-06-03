ALTER TABLE t_state ADD COLUMN incompatible_digest BYTEA;

CREATE INDEX IF NOT EXISTS idx_t_state_lock_pending_auto
ON t_state (state, pending_expires_finished, ffqn, incompatible_digest)
WHERE state = 'pending_at';
