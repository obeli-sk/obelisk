-- Extend the deployment summary index so `list_deployment_states` (per-state counts:
-- top-level only by default, pending/scheduled split, paused bucket and the finished
-- result-kind breakdown) and `list_executions` state filtering can be answered from
-- the index alone.
DROP INDEX IF EXISTS idx_t_state_deployment_state;
CREATE INDEX IF NOT EXISTS idx_t_state_deployment_state
    ON t_state (deployment_id, is_top_level, state, is_paused, pending_expires_finished, result_kind);
