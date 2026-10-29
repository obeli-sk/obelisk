pub(crate) const PENDING_BY_FFQN: &str = "
    SELECT execution_id, corresponding_version FROM t_state
    WHERE state = 'pending_at' AND pending_expires_finished <= :pending_expires_finished
    AND ffqn = :ffqn AND lifecycle = 'active' AND tombstoned = FALSE
    ORDER BY pending_expires_finished LIMIT :batch_size";

pub(crate) const PENDING_BY_FFQN_AUTO: &str = "
    SELECT execution_id, corresponding_version FROM t_state
    WHERE state = 'pending_at' AND pending_expires_finished <= :pending_expires_finished
    AND ffqn = :ffqn AND lifecycle = 'active' AND tombstoned = FALSE
    AND (incompatible_digest IS NULL OR incompatible_digest <> :current_digest)
    ORDER BY pending_expires_finished LIMIT :batch_size";

pub(crate) const PENDING_BY_COMPONENT: &str = "
    SELECT execution_id, corresponding_version FROM t_state
    WHERE state = 'pending_at' AND pending_expires_finished <= :pending_expires_finished
    AND component_id_input_digest = :component_id_input_digest
    AND lifecycle = 'active' AND tombstoned = FALSE
    ORDER BY pending_expires_finished LIMIT :batch_size";

pub(crate) const CANCELLING: &str = "
    SELECT execution_id FROM t_state WHERE lifecycle = :lifecycle
    AND NOT EXISTS (SELECT 1 FROM t_state root WHERE root.is_top_level = TRUE
        AND root.tombstoned = TRUE AND (t_state.execution_id = root.execution_id
        OR t_state.execution_id LIKE root.execution_id || '.%'))
    ORDER BY created_at LIMIT :batch_size";

pub(crate) const EXPIRED_DELAYS: &str = "
    SELECT d.execution_id, d.join_set_id, d.delay_id FROM t_delay d
    WHERE d.expires_at <= :at AND NOT d.is_paused
    AND NOT EXISTS (SELECT 1 FROM t_state root WHERE root.is_top_level = TRUE
        AND root.tombstoned = TRUE AND (d.execution_id = root.execution_id
        OR d.execution_id LIKE root.execution_id || '.%'))";

pub(crate) const EXPIRED_LOCKS: &str = "
    SELECT execution_id, last_lock_version, corresponding_version, intermittent_event_count,
        max_retries, retry_exp_backoff_millis, executor_id, run_id, lifecycle
    FROM t_state WHERE pending_expires_finished <= :at AND state = 'locked'";
