ALTER TABLE t_deployment_file DROP CONSTRAINT t_deployment_file_pkey;
ALTER TABLE t_deployment_file ADD PRIMARY KEY (deployment_id, path);

CREATE TABLE t_deployment_component_file (
    deployment_id  VARCHAR(30) NOT NULL,
    component_name TEXT NOT NULL,
    path           TEXT NOT NULL,
    role           VARCHAR(16) NOT NULL CHECK (role IN (
        'wasm_component', 'exec_program', 'js_entrypoint', 'js_module', 'backtrace_source'
    )),
    PRIMARY KEY (deployment_id, component_name, path),
    FOREIGN KEY (deployment_id, component_name)
        REFERENCES t_deployment_component(deployment_id, component_name),
    FOREIGN KEY (deployment_id, path)
        REFERENCES t_deployment_file(deployment_id, path)
);

CREATE INDEX idx_t_deployment_component_file_path
ON t_deployment_component_file (deployment_id, path);
