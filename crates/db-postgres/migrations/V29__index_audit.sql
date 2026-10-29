DROP INDEX idx_t_execution_log_execution_id_version;
DROP INDEX idx_t_execution_backtrace_execution_id_version;
DROP INDEX idx_t_log_execution_id_run_id_created_at;
DROP INDEX idx_t_state_execution_id_is_root;
DROP INDEX idx_t_join_set_response_execution_id_id;
DROP INDEX idx_t_state_lock_pending_by_ffqn;
DROP INDEX idx_t_state_lock_pending_auto;
DROP INDEX idx_t_state_lock_pending_by_component;
CREATE INDEX idx_t_state_lock_pending_auto
    ON t_state (ffqn, pending_expires_finished, incompatible_digest)
    WHERE state = 'pending_at' AND lifecycle = 'active' AND tombstoned = FALSE;
CREATE INDEX idx_t_state_lock_pending_by_component
    ON t_state (component_id_input_digest, pending_expires_finished)
    WHERE state = 'pending_at' AND lifecycle = 'active' AND tombstoned = FALSE;
CREATE INDEX idx_t_state_retention_created_at ON t_state (created_at, execution_id)
    WHERE is_top_level = TRUE AND tombstoned = FALSE;
CREATE INDEX idx_t_state_finished_created_at ON t_state (created_at, execution_id)
    WHERE is_top_level = TRUE AND tombstoned = FALSE AND state = 'finished';
CREATE INDEX idx_t_state_finished_updated_at ON t_state (updated_at, execution_id)
    WHERE is_top_level = TRUE AND tombstoned = FALSE AND state = 'finished';
CREATE INDEX idx_t_deployment_retention_created_at ON t_deployment (created_at, deployment_id)
    WHERE status = 'inactive';
CREATE INDEX idx_t_state_deployment_roots ON t_state (deployment_id, execution_id)
    WHERE is_top_level = TRUE AND tombstoned = FALSE;
CREATE INDEX idx_t_component_source_digest ON t_component_source (digest);
CREATE INDEX t_system_event_cas_digest_idx ON t_system_event (cas_digest) WHERE cas_digest IS NOT NULL;
DROP INDEX t_system_event_node_run_idx;
CREATE INDEX t_system_event_node_run_idx ON t_system_event (node_run_id, created_at DESC, event_id DESC);
CREATE INDEX idx_t_file_upload_uploaded_at ON t_file_upload (uploaded_at);
CREATE INDEX t_system_event_http_policy_idx ON t_system_event (
    deployment_id, (details->>'component'), (details->>'component_policy_hash'),
    (details->>'server_policy_hash'), event_id DESC
) WHERE code = 'component.http_policy.applied';

ALTER TABLE t_state ALTER COLUMN execution_id TYPE TEXT COLLATE "C";
ALTER TABLE t_state ALTER COLUMN ffqn TYPE TEXT COLLATE "C";
ALTER TABLE t_execution_log ALTER COLUMN execution_id TYPE TEXT COLLATE "C";
ALTER TABLE t_join_set_response ALTER COLUMN execution_id TYPE TEXT COLLATE "C";
ALTER TABLE t_join_set_response ALTER COLUMN child_execution_id TYPE TEXT COLLATE "C";
ALTER TABLE t_delay ALTER COLUMN execution_id TYPE TEXT COLLATE "C";
ALTER TABLE t_execution_backtrace ALTER COLUMN execution_id TYPE TEXT COLLATE "C";
ALTER TABLE t_log ALTER COLUMN execution_id TYPE TEXT COLLATE "C";
