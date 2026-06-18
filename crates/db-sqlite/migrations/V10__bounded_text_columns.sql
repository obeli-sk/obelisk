CREATE TABLE _v10_validate_varchar_32 (value TEXT NOT NULL CHECK (length(value) <= 32)) STRICT;
INSERT INTO _v10_validate_varchar_32 SELECT variant FROM t_execution_log;
DROP TABLE _v10_validate_varchar_32;

CREATE TABLE _v10_validate_varchar_20 (value TEXT NOT NULL CHECK (length(value) <= 20)) STRICT;
INSERT INTO _v10_validate_varchar_20
SELECT history_event_type FROM t_execution_log WHERE history_event_type IS NOT NULL;
INSERT INTO _v10_validate_varchar_20 SELECT state FROM t_state;
DROP TABLE _v10_validate_varchar_20;

CREATE TABLE _v10_validate_varchar_16 (value TEXT NOT NULL CHECK (length(value) <= 16)) STRICT;
INSERT INTO _v10_validate_varchar_16 SELECT component_type FROM t_state;
INSERT INTO _v10_validate_varchar_16 SELECT component_type FROM t_deployment_component;
DROP TABLE _v10_validate_varchar_16;

CREATE TABLE _v10_validate_varchar_1024 (value TEXT NOT NULL CHECK (length(value) <= 1024)) STRICT;
INSERT INTO _v10_validate_varchar_1024 SELECT description FROM t_deployment WHERE description IS NOT NULL;
DROP TABLE _v10_validate_varchar_1024;

CREATE TABLE _v10_validate_varchar_8 (value TEXT NOT NULL CHECK (length(value) <= 8)) STRICT;
INSERT INTO _v10_validate_varchar_8 SELECT status FROM t_deployment;
DROP TABLE _v10_validate_varchar_8;

CREATE TABLE _v10_validate_varchar_30 (value TEXT NOT NULL CHECK (length(value) <= 30)) STRICT;
INSERT INTO _v10_validate_varchar_30 SELECT deployment_id FROM t_state;
INSERT INTO _v10_validate_varchar_30 SELECT executor_id FROM t_state WHERE executor_id IS NOT NULL;
INSERT INTO _v10_validate_varchar_30 SELECT run_id FROM t_state WHERE run_id IS NOT NULL;
INSERT INTO _v10_validate_varchar_30 SELECT run_id FROM t_log;
INSERT INTO _v10_validate_varchar_30 SELECT deployment_id FROM t_deployment;
INSERT INTO _v10_validate_varchar_30 SELECT deployment_id FROM t_deployment_component;
INSERT INTO _v10_validate_varchar_30 SELECT deployment_id FROM t_deployment_file;
DROP TABLE _v10_validate_varchar_30;

CREATE TABLE _v10_validate_varchar_71 (value TEXT NOT NULL CHECK (length(value) <= 71)) STRICT;
INSERT INTO _v10_validate_varchar_71 SELECT digest FROM t_deployment;
INSERT INTO _v10_validate_varchar_71 SELECT digest FROM t_file;
INSERT INTO _v10_validate_varchar_71 SELECT digest FROM t_deployment_file;
DROP TABLE _v10_validate_varchar_71;

CREATE TRIGGER t_execution_log_bounded_text_insert
BEFORE INSERT ON t_execution_log
BEGIN
    SELECT RAISE(ABORT, 't_execution_log.variant exceeds 32 characters')
    WHERE length(NEW.variant) > 32;
    SELECT RAISE(ABORT, 't_execution_log.history_event_type exceeds 20 characters')
    WHERE length(NEW.json_value->>'$.history_event.event.type') > 20;
END;

CREATE TRIGGER t_execution_log_bounded_text_update
BEFORE UPDATE OF variant, json_value ON t_execution_log
BEGIN
    SELECT RAISE(ABORT, 't_execution_log.variant exceeds 32 characters')
    WHERE length(NEW.variant) > 32;
    SELECT RAISE(ABORT, 't_execution_log.history_event_type exceeds 20 characters')
    WHERE length(NEW.json_value->>'$.history_event.event.type') > 20;
END;

CREATE TRIGGER t_state_bounded_text_insert
BEFORE INSERT ON t_state
BEGIN
    SELECT RAISE(ABORT, 't_state.component_type exceeds 16 characters')
    WHERE length(NEW.component_type) > 16;
    SELECT RAISE(ABORT, 't_state.deployment_id exceeds 30 characters')
    WHERE length(NEW.deployment_id) > 30;
    SELECT RAISE(ABORT, 't_state.state exceeds 20 characters')
    WHERE length(NEW.state) > 20;
    SELECT RAISE(ABORT, 't_state.executor_id exceeds 30 characters')
    WHERE length(NEW.executor_id) > 30;
    SELECT RAISE(ABORT, 't_state.run_id exceeds 30 characters')
    WHERE length(NEW.run_id) > 30;
END;

CREATE TRIGGER t_state_bounded_text_update
BEFORE UPDATE OF component_type, deployment_id, state, executor_id, run_id ON t_state
BEGIN
    SELECT RAISE(ABORT, 't_state.component_type exceeds 16 characters')
    WHERE length(NEW.component_type) > 16;
    SELECT RAISE(ABORT, 't_state.deployment_id exceeds 30 characters')
    WHERE length(NEW.deployment_id) > 30;
    SELECT RAISE(ABORT, 't_state.state exceeds 20 characters')
    WHERE length(NEW.state) > 20;
    SELECT RAISE(ABORT, 't_state.executor_id exceeds 30 characters')
    WHERE length(NEW.executor_id) > 30;
    SELECT RAISE(ABORT, 't_state.run_id exceeds 30 characters')
    WHERE length(NEW.run_id) > 30;
END;

CREATE TRIGGER t_log_bounded_text_insert
BEFORE INSERT ON t_log
BEGIN
    SELECT RAISE(ABORT, 't_log.run_id exceeds 30 characters')
    WHERE length(NEW.run_id) > 30;
END;

CREATE TRIGGER t_log_bounded_text_update
BEFORE UPDATE OF run_id ON t_log
BEGIN
    SELECT RAISE(ABORT, 't_log.run_id exceeds 30 characters')
    WHERE length(NEW.run_id) > 30;
END;

CREATE TRIGGER t_deployment_bounded_text_insert
BEFORE INSERT ON t_deployment
BEGIN
    SELECT RAISE(ABORT, 't_deployment.deployment_id exceeds 30 characters')
    WHERE length(NEW.deployment_id) > 30;
    SELECT RAISE(ABORT, 't_deployment.status exceeds 8 characters')
    WHERE length(NEW.status) > 8;
    SELECT RAISE(ABORT, 't_deployment.description exceeds 1024 characters')
    WHERE length(NEW.description) > 1024;
    SELECT RAISE(ABORT, 't_deployment.digest exceeds 71 characters')
    WHERE length(NEW.digest) > 71;
END;

CREATE TRIGGER t_deployment_bounded_text_update
BEFORE UPDATE OF deployment_id, status, description, digest ON t_deployment
BEGIN
    SELECT RAISE(ABORT, 't_deployment.deployment_id exceeds 30 characters')
    WHERE length(NEW.deployment_id) > 30;
    SELECT RAISE(ABORT, 't_deployment.status exceeds 8 characters')
    WHERE length(NEW.status) > 8;
    SELECT RAISE(ABORT, 't_deployment.description exceeds 1024 characters')
    WHERE length(NEW.description) > 1024;
    SELECT RAISE(ABORT, 't_deployment.digest exceeds 71 characters')
    WHERE length(NEW.digest) > 71;
END;

CREATE TRIGGER t_deployment_component_bounded_text_insert
BEFORE INSERT ON t_deployment_component
BEGIN
    SELECT RAISE(ABORT, 't_deployment_component.deployment_id exceeds 30 characters')
    WHERE length(NEW.deployment_id) > 30;
    SELECT RAISE(ABORT, 't_deployment_component.component_type exceeds 16 characters')
    WHERE length(NEW.component_type) > 16;
END;

CREATE TRIGGER t_deployment_component_bounded_text_update
BEFORE UPDATE OF deployment_id, component_type ON t_deployment_component
BEGIN
    SELECT RAISE(ABORT, 't_deployment_component.deployment_id exceeds 30 characters')
    WHERE length(NEW.deployment_id) > 30;
    SELECT RAISE(ABORT, 't_deployment_component.component_type exceeds 16 characters')
    WHERE length(NEW.component_type) > 16;
END;

CREATE TRIGGER t_file_bounded_text_insert
BEFORE INSERT ON t_file
BEGIN
    SELECT RAISE(ABORT, 't_file.digest exceeds 71 characters')
    WHERE length(NEW.digest) > 71;
END;

CREATE TRIGGER t_file_bounded_text_update
BEFORE UPDATE OF digest ON t_file
BEGIN
    SELECT RAISE(ABORT, 't_file.digest exceeds 71 characters')
    WHERE length(NEW.digest) > 71;
END;

CREATE TRIGGER t_deployment_file_bounded_text_insert
BEFORE INSERT ON t_deployment_file
BEGIN
    SELECT RAISE(ABORT, 't_deployment_file.deployment_id exceeds 30 characters')
    WHERE length(NEW.deployment_id) > 30;
    SELECT RAISE(ABORT, 't_deployment_file.digest exceeds 71 characters')
    WHERE length(NEW.digest) > 71;
END;

CREATE TRIGGER t_deployment_file_bounded_text_update
BEFORE UPDATE OF deployment_id, digest ON t_deployment_file
BEGIN
    SELECT RAISE(ABORT, 't_deployment_file.deployment_id exceeds 30 characters')
    WHERE length(NEW.deployment_id) > 30;
    SELECT RAISE(ABORT, 't_deployment_file.digest exceeds 71 characters')
    WHERE length(NEW.digest) > 71;
END;
