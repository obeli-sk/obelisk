DROP INDEX t_system_event_server_run_idx;

ALTER TABLE t_system_event
ADD COLUMN node_run_id TEXT NOT NULL DEFAULT 'NodeRun_00000000000000000000000000'
    CHECK (length(node_run_id) = 34);
UPDATE t_system_event SET node_run_id = 'NodeRun_' || substr(server_run_id, 5);
ALTER TABLE t_system_event DROP COLUMN server_run_id;

CREATE INDEX t_system_event_node_run_idx ON t_system_event(node_run_id, event_id DESC);
