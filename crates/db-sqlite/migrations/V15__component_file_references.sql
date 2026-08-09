DROP TRIGGER t_deployment_file_bounded_text_insert;
DROP TRIGGER t_deployment_file_bounded_text_update;

ALTER TABLE t_deployment_file RENAME TO t_deployment_file_old;

CREATE TABLE t_deployment_file (
    deployment_id TEXT NOT NULL,
    path          TEXT NOT NULL,
    digest        TEXT NOT NULL,
    PRIMARY KEY (deployment_id, path),
    FOREIGN KEY (deployment_id) REFERENCES t_deployment(deployment_id)
) STRICT;

INSERT INTO t_deployment_file (deployment_id, path, digest)
SELECT deployment_id, path, digest FROM t_deployment_file_old;
DROP TABLE t_deployment_file_old;

CREATE INDEX idx_t_deployment_file_digest ON t_deployment_file (digest);

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

CREATE TABLE t_deployment_component_file (
    deployment_id  TEXT NOT NULL,
    component_name TEXT NOT NULL,
    path           TEXT NOT NULL,
    role           TEXT NOT NULL CHECK (role IN (
        'wasm_component', 'exec_program', 'js_entrypoint', 'js_module', 'backtrace_source'
    )),
    PRIMARY KEY (deployment_id, component_name, path),
    FOREIGN KEY (deployment_id, component_name)
        REFERENCES t_deployment_component(deployment_id, component_name),
    FOREIGN KEY (deployment_id, path)
        REFERENCES t_deployment_file(deployment_id, path)
) STRICT;

CREATE INDEX idx_t_deployment_component_file_path
ON t_deployment_component_file (deployment_id, path);

CREATE TRIGGER t_deployment_component_file_bounded_text_insert
BEFORE INSERT ON t_deployment_component_file
BEGIN
    SELECT RAISE(ABORT, 't_deployment_component_file.deployment_id exceeds 30 characters')
    WHERE length(NEW.deployment_id) > 30;
    SELECT RAISE(ABORT, 't_deployment_component_file.role exceeds 16 characters')
    WHERE length(NEW.role) > 16;
END;
