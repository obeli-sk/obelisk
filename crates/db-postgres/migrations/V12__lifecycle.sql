-- Replace the `is_paused` boolean on t_state with a single `lifecycle` flag that
-- also carries the (later) `cancelling` state, so paused and cancelling are
-- mutually exclusive by construction. Existing paused rows are preserved.
ALTER TABLE t_state ADD COLUMN lifecycle TEXT NOT NULL DEFAULT 'active'
    CHECK (lifecycle IN ('active', 'paused', 'cancelling'));
UPDATE t_state SET lifecycle = 'paused' WHERE is_paused;

-- Rebuild the deployment-summary index to key on `lifecycle` instead of `is_paused`.
DROP INDEX IF EXISTS idx_t_state_deployment_state;
CREATE INDEX IF NOT EXISTS idx_t_state_deployment_state
    ON t_state (deployment_id, is_top_level, state, lifecycle, pending_expires_finished, result_kind);

ALTER TABLE t_state DROP COLUMN is_paused;
