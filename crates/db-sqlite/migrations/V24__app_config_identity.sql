ALTER TABLE t_deployment ADD COLUMN last_active_app_config_digest TEXT;
ALTER TABLE t_system_event ADD COLUMN app_config_digest TEXT;
