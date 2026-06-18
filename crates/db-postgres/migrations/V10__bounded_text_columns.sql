ALTER TABLE t_execution_log
    ALTER COLUMN variant TYPE VARCHAR(32);

DROP INDEX IF EXISTS idx_t_execution_log_execution_id_join_set;
ALTER TABLE t_execution_log DROP COLUMN history_event_type;
ALTER TABLE t_execution_log
    ADD COLUMN history_event_type VARCHAR(20) GENERATED ALWAYS AS (json_value #>> '{history_event,event,type}') STORED;
CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_join_set ON t_execution_log
    (execution_id, join_set_id, history_event_type) WHERE history_event_type='join_next';

ALTER TABLE t_state
    ALTER COLUMN component_type TYPE VARCHAR(16),
    ALTER COLUMN deployment_id TYPE VARCHAR(30),
    ALTER COLUMN state TYPE VARCHAR(20),
    ALTER COLUMN executor_id TYPE VARCHAR(30),
    ALTER COLUMN run_id TYPE VARCHAR(30);

ALTER TABLE t_log
    ALTER COLUMN run_id TYPE VARCHAR(30);

ALTER TABLE t_deployment_component
    ALTER COLUMN deployment_id TYPE VARCHAR(30),
    ALTER COLUMN component_type TYPE VARCHAR(16);

ALTER TABLE t_deployment_file
    ALTER COLUMN deployment_id TYPE VARCHAR(30),
    ALTER COLUMN digest TYPE VARCHAR(71);

ALTER TABLE t_deployment
    ALTER COLUMN deployment_id TYPE VARCHAR(30),
    ALTER COLUMN status TYPE VARCHAR(8),
    ALTER COLUMN description TYPE VARCHAR(1024),
    ALTER COLUMN digest TYPE VARCHAR(71);

ALTER TABLE t_file
    ALTER COLUMN digest TYPE VARCHAR(71);
