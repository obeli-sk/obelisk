ALTER TABLE t_system_event DROP CONSTRAINT t_system_event_level_check;
ALTER TABLE t_system_event
ADD CONSTRAINT t_system_event_level_check
CHECK (level IN ('debug', 'info', 'warning', 'error'));
