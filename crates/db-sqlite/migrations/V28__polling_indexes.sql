DROP INDEX idx_t_state_lock_pending_by_ffqn;
CREATE INDEX idx_t_state_lock_pending_by_ffqn
    ON t_state (ffqn, pending_expires_finished) WHERE state = 'pending_at';

DROP INDEX idx_t_state_lock_pending_auto;
CREATE INDEX idx_t_state_lock_pending_auto
    ON t_state (ffqn, pending_expires_finished, incompatible_digest) WHERE state = 'pending_at';

DROP INDEX idx_t_state_lock_pending_by_component;
CREATE INDEX idx_t_state_lock_pending_by_component
    ON t_state (component_id_input_digest, pending_expires_finished) WHERE state = 'pending_at';

CREATE INDEX idx_t_state_cancelling ON t_state (lifecycle, created_at);
