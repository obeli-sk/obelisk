ALTER TABLE t_system_event RENAME COLUMN server_run_id TO node_run_id;
ALTER TABLE t_system_event ALTER COLUMN node_run_id TYPE VARCHAR(34);
UPDATE t_system_event
SET node_run_id = 'NodeRun_' || substring(node_run_id FROM 5);
ALTER INDEX t_system_event_server_run_idx RENAME TO t_system_event_node_run_idx;
