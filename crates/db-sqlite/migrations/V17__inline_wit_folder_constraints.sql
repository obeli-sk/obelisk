-- Inline-WIT-folder components introduced two new enum values that the V15/V16
-- CHECK constraints predate: the `authored` WIT origin (3) and the `wit_source`
-- component-file role. Widen both. SQLite cannot alter a CHECK in place, so mirror
-- V16's add/copy/drop/rename per column, keeping each table's identity, PK, inbound
-- FKs and indexes intact.

ALTER TABLE t_component_metadata
ADD COLUMN wit_origin_widened INTEGER NOT NULL DEFAULT 1 CHECK (wit_origin_widened IN (1, 2, 3));
UPDATE t_component_metadata SET wit_origin_widened = wit_origin;
ALTER TABLE t_component_metadata DROP COLUMN wit_origin;
ALTER TABLE t_component_metadata RENAME COLUMN wit_origin_widened TO wit_origin;

-- The role trigger references NEW.role, so it must be dropped before the column swap
-- and recreated afterwards.
DROP TRIGGER t_deployment_component_file_bounded_text_insert;

ALTER TABLE t_deployment_component_file
ADD COLUMN role_widened TEXT NOT NULL DEFAULT 'wasm_component' CHECK (role_widened IN (
    'wasm_component', 'exec_program', 'js_entrypoint', 'js_module', 'backtrace_source', 'wit_source'
));
UPDATE t_deployment_component_file SET role_widened = role;
ALTER TABLE t_deployment_component_file DROP COLUMN role;
ALTER TABLE t_deployment_component_file RENAME COLUMN role_widened TO role;

CREATE TRIGGER t_deployment_component_file_bounded_text_insert
BEFORE INSERT ON t_deployment_component_file
BEGIN
    SELECT RAISE(ABORT, 't_deployment_component_file.deployment_id exceeds 30 characters')
    WHERE length(NEW.deployment_id) > 30;
    SELECT RAISE(ABORT, 't_deployment_component_file.role exceeds 16 characters')
    WHERE length(NEW.role) > 16;
END;
