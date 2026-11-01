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
        AND root.tombstoned = TRUE AND root.execution_id = substr(t_state.execution_id, 1, instr(t_state.execution_id || '.', '.') - 1))
    ORDER BY created_at LIMIT :batch_size";

pub(crate) const EXPIRED_DELAYS: &str = "
    SELECT d.execution_id, d.join_set_id, d.delay_id FROM t_delay d
    WHERE d.expires_at <= :at AND NOT d.is_paused
    AND NOT EXISTS (SELECT 1 FROM t_state root WHERE root.is_top_level = TRUE
        AND root.tombstoned = TRUE AND root.execution_id = substr(d.execution_id, 1, instr(d.execution_id || '.', '.') - 1))";

pub(crate) const EXPIRED_LOCKS: &str = "
    SELECT execution_id, last_lock_version, corresponding_version, intermittent_event_count,
        max_retries, retry_exp_backoff_millis, executor_id, run_id, lifecycle
    FROM t_state WHERE pending_expires_finished <= :at AND state = 'locked'";
pub(crate) const IDLE_STATE: &str = "
    SELECT EXISTS(SELECT 1 FROM t_state WHERE state = 'locked' AND lifecycle <> 'paused')
        OR EXISTS(SELECT 1 FROM t_state WHERE lifecycle = 'cancelling'),
        (SELECT MIN(run_at) FROM (
            SELECT MIN(pending_expires_finished) AS run_at FROM t_state
            WHERE state = 'pending_at' AND lifecycle = 'active' AND tombstoned = FALSE
            UNION ALL
            SELECT MIN(d.expires_at) AS run_at FROM t_delay d WHERE NOT d.is_paused
            AND NOT EXISTS (SELECT 1 FROM t_state root WHERE root.is_top_level = TRUE
                AND root.tombstoned = TRUE AND root.execution_id = substr(d.execution_id, 1, instr(d.execution_id || '.', '.') - 1))
        ))";
