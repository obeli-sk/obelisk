ALTER TABLE t_system_event ADD COLUMN server_run_id TEXT;
CREATE INDEX idx_system_event_server_run ON t_system_event(server_run_id, event_id DESC);
