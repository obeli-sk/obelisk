-- Inline-WIT-folder components introduced two new enum values that the V15/V16
-- CHECK constraints predate: the `authored` WIT origin (3) and the `wit_source`
-- component-file role. Widen both.

ALTER TABLE t_component_metadata
DROP CONSTRAINT t_component_metadata_wit_origin_check;
ALTER TABLE t_component_metadata
ADD CONSTRAINT t_component_metadata_wit_origin_check CHECK (wit_origin IN (1, 2, 3));

ALTER TABLE t_deployment_component_file
DROP CONSTRAINT t_deployment_component_file_role_check;
ALTER TABLE t_deployment_component_file
ADD CONSTRAINT t_deployment_component_file_role_check CHECK (role IN (
    'wasm_component', 'exec_program', 'js_entrypoint', 'js_module', 'backtrace_source', 'wit_source'
));
