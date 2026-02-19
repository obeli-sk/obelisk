use crate::RunnableComponent;
use concepts::ComponentId;
use concepts::ComponentType;
use concepts::FunctionFqn;
use concepts::FunctionMetadata;
use concepts::FunctionRegistry;
use concepts::PackageIfcFns;
use concepts::StrVariant;
use concepts::component_id::InputContentDigest;
use concepts::storage::LogLevel;
use indexmap::IndexMap;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;
use tracing::error;

/// Holds information about components, used for gRPC services like `ListComponents`
#[derive(Debug, Clone)]
pub struct ComponentConfig {
    pub component_id: ComponentId,
    pub imports: Vec<FunctionMetadata>,
    pub workflow_or_activity_config: Option<ComponentConfigImportable>,
    pub wit: Option<String>,
    pub workflow_replay_info: Option<WorkflowReplayInfo>,
}

#[derive(Debug, Clone)]
pub struct WorkflowReplayInfo {
    pub runnable_component: RunnableComponent,
    pub logs_store_min_level: Option<LogLevel>,
    /// For JS workflows: the JS source code and user's FFQN
    pub js_workflow_info: Option<JsWorkflowReplayInfo>,
}

#[derive(Debug, Clone)]
pub struct JsWorkflowReplayInfo {
    pub js_source: String,
    pub user_ffqn: FunctionFqn,
    pub user_params: Vec<concepts::ParameterType>,
}

#[derive(Debug, Clone)]
// Workflows or Activities (WASM, stub, external), but not Webhooks
pub struct ComponentConfigImportable {
    pub exports_ext: Vec<FunctionMetadata>,
    pub exports_hierarchy_ext: Vec<PackageIfcFns>,
}

#[derive(Default, Debug)]
pub struct ComponentConfigRegistry {
    inner: ComponentConfigRegistryInner,
}

#[derive(Default, Debug)]
struct ComponentConfigRegistryInner {
    exported_ffqns_ext: IndexMap<FunctionFqn, (ComponentId, FunctionMetadata)>,
    export_hierarchy: Vec<PackageIfcFns>,
    ids_to_components: IndexMap<InputContentDigest, ComponentConfig>,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("registering component failed: {0}")]
pub struct ComponentInsertionError(StrVariant);

impl ComponentConfigRegistry {
    pub fn insert(&mut self, component: ComponentConfig) -> Result<(), ComponentInsertionError> {
        // verify that the component or its exports are not already present
        if self
            .inner
            .ids_to_components
            .contains_key(&component.component_id.input_digest)
        {
            return Err(ComponentInsertionError(
                format!(
                    "component {} is already inserted with the same digest",
                    component.component_id
                )
                .into(),
            ));
        }
        if let Some(workflow_or_activity_config) = &component.workflow_or_activity_config {
            for exported_ffqn in workflow_or_activity_config
                .exports_ext
                .iter()
                .map(|f| &f.ffqn)
            {
                if let Some((conflicting_id, _)) = self.inner.exported_ffqns_ext.get(exported_ffqn)
                {
                    return Err(ComponentInsertionError(
                        format!(
                        "function {exported_ffqn} is already exported by component {conflicting_id}, cannot insert {}",
                        component.component_id
                    ).into()));
                }
            }
            // insert to `exported_ffqns_ext`
            for exported_fn_metadata in &workflow_or_activity_config.exports_ext {
                let old = self.inner.exported_ffqns_ext.insert(
                    exported_fn_metadata.ffqn.clone(),
                    (component.component_id.clone(), exported_fn_metadata.clone()),
                );
                assert!(old.is_none());
            }
            // insert to `export_hierarchy`
            self.inner
                .export_hierarchy
                .extend_from_slice(&workflow_or_activity_config.exports_hierarchy_ext);
        }

        self.inner
            .ids_to_components
            .insert(component.component_id.input_digest.clone(), component);

        Ok(())
    }

    /// Verify that each imported function can be matched by looking at the available exports.
    /// This is a best effort to give function-level error messages.
    /// WASI imports and host functions are not validated at the moment, those errors
    /// are caught by wasmtime while pre-instantiation with a message containing the missing interface.
    pub fn verify_registry(
        self,
    ) -> (
        ComponentConfigRegistryRO,
        Option<String>, /* supressed_errors */
    ) {
        let mut errors = Vec::new();
        for examined_component in self.inner.ids_to_components.values() {
            self.verify_imports_component(examined_component, &mut errors);
        }
        let errors = if !errors.is_empty() {
            let errors = errors.join("\n");
            // TODO: Promote to an error when version resolution is implemented
            // https://github.com/bytecodealliance/wasmtime/blob/8dbd5db30d05e96594e2516cdbd7cc213f1c2fa4/crates/environ/src/component/names.rs#L102-L104
            tracing::warn!("component resolution error: \n{errors}");
            Some(errors)
        } else {
            None
        };
        (
            ComponentConfigRegistryRO {
                inner: Arc::new(self.inner),
            },
            errors,
        )
    }

    fn additional_import_allowlist(
        import: &FunctionMetadata,
        component_type: ComponentType,
    ) -> bool {
        match component_type {
            ComponentType::ActivityWasm => {
                // wasi + log
                match import.ffqn.ifc_fqn.namespace() {
                    "wasi" => true,
                    "obelisk" => {
                        import.ffqn.ifc_fqn.deref() == "obelisk:log/log@1.0.0"
                            || import.ffqn.ifc_fqn.deref() == "obelisk:activity/process@1.0.0"
                    }
                    _ => false,
                }
            }
            ComponentType::Workflow => {
                // log + workflow(-support) + types
                matches!(
                    import.ffqn.ifc_fqn.pkg_fqn_name().to_string().as_str(),
                    "obelisk:log@1.0.0"
                        | "obelisk:workflow@4.0.0"
                        | "obelisk:workflow@4.1.0"
                        | "obelisk:workflow@4.2.0"
                        | "obelisk:types@4.0.0"
                        | "obelisk:types@4.1.0"
                )
            }
            ComponentType::WebhookEndpoint => {
                // wasi + log + types (needed for scheduling)
                match import.ffqn.ifc_fqn.namespace() {
                    "wasi" => true,
                    "obelisk" => matches!(
                        import.ffqn.ifc_fqn.pkg_fqn_name().to_string().as_str(),
                        "obelisk:log@1.0.0" | "obelisk:types@4.0.0" | "obelisk:types@4.1.0"
                    ),
                    _ => false,
                }
            }
            ComponentType::ActivityStub | ComponentType::ActivityExternal => false,
        }
    }

    fn verify_imports_component(&self, component: &ComponentConfig, errors: &mut Vec<String>) {
        let component_id = &component.component_id;
        for imported_fn_metadata in &component.imports {
            if let Some((exported_component_id, exported_fn_metadata)) = self
                .inner
                .exported_ffqns_ext
                .get(&imported_fn_metadata.ffqn)
            {
                // check parameters
                if imported_fn_metadata.parameter_types != exported_fn_metadata.parameter_types {
                    error!(
                        "Parameter types do not match: {ffqn} imported by {component_id} , exported by {exported_component_id}",
                        ffqn = imported_fn_metadata.ffqn
                    );
                    error!(
                        "Import {import}",
                        import = serde_json::to_string(imported_fn_metadata).unwrap(), // TODO: print in WIT format
                    );
                    error!(
                        "Export {export}",
                        export = serde_json::to_string(exported_fn_metadata).unwrap(),
                    );
                    errors.push(format!("parameter types do not match: {component_id} imports {imported_fn_metadata} , {exported_component_id} exports {exported_fn_metadata}"));
                }
                if imported_fn_metadata.return_type != exported_fn_metadata.return_type {
                    error!(
                        "Return types do not match: {ffqn} imported by {component_id} , exported by {exported_component_id}",
                        ffqn = imported_fn_metadata.ffqn
                    );
                    error!(
                        "Import {import}",
                        import = serde_json::to_string(imported_fn_metadata).unwrap(), // TODO: print in WIT format
                    );
                    error!(
                        "Export {export}",
                        export = serde_json::to_string(exported_fn_metadata).unwrap(),
                    );
                    errors.push(format!("return types do not match: {component_id} imports {imported_fn_metadata} , {exported_component_id} exports {exported_fn_metadata}"));
                }
            } else if !Self::additional_import_allowlist(
                imported_fn_metadata,
                component_id.component_type,
            ) {
                errors.push(format!(
                    "function imported by {component_id} not found: {imported_fn_metadata}"
                ));
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ComponentConfigRegistryRO {
    inner: Arc<ComponentConfigRegistryInner>,
}

impl ComponentConfigRegistryRO {
    /// Return `None` if component is not found, `Some(None)` if component has no WIT content.
    #[must_use]
    pub fn get_wit(&self, input_digest: &InputContentDigest) -> Option<Option<&str>> {
        self.inner
            .ids_to_components
            .get(input_digest)
            .map(|component_config| component_config.wit.as_deref())
    }

    #[must_use]
    pub fn get_workflow_replay_info(
        &self,
        input_digest: &InputContentDigest,
    ) -> Option<(&ComponentId, &WorkflowReplayInfo)> {
        self.inner
            .ids_to_components
            .get(input_digest)
            .and_then(|component_config| {
                component_config
                    .workflow_replay_info
                    .as_ref()
                    .map(|it| (&component_config.component_id, it))
            })
    }

    #[must_use]
    pub fn find_by_exported_ffqn_submittable(
        &self,
        ffqn: &FunctionFqn,
    ) -> Option<(&ComponentId, &FunctionMetadata)> {
        self.inner
            .exported_ffqns_ext
            .get(ffqn)
            .and_then(|(component_id, fn_metadata)| {
                if fn_metadata.submittable {
                    Some((component_id, fn_metadata))
                } else {
                    None
                }
            })
    }

    #[must_use]
    pub fn find_by_exported_ffqn(
        &self,
        ffqn: &FunctionFqn,
    ) -> Option<(&ComponentId, &FunctionMetadata)> {
        self.inner
            .exported_ffqns_ext
            .get(ffqn)
            .map(|t| (&t.0, &t.1))
    }

    #[must_use]
    pub fn find_by_exported_ffqn_stub(
        &self,
        ffqn: &FunctionFqn,
    ) -> Option<(&ComponentId, &FunctionMetadata)> {
        self.inner
            .exported_ffqns_ext
            .get(ffqn)
            .and_then(|(component_id, fn_metadata)| {
                if component_id.component_type == ComponentType::ActivityStub {
                    assert!(!ffqn.ifc_fqn.is_extension());
                    Some((component_id, fn_metadata))
                } else {
                    None
                }
            })
    }

    /// List comopnents. When `extensions` is set to true, exteded functions are stripped from exports in each component.
    #[must_use]
    pub fn list(&self, extensions: bool) -> Vec<ComponentConfig> {
        self.inner
            .ids_to_components
            .values()
            .cloned()
            .map(|mut component| {
                // If no extensions are requested, retain those that are !ext
                if !extensions && let Some(importable) = &mut component.workflow_or_activity_config
                {
                    importable
                        .exports_ext
                        .retain(|fn_metadata| !fn_metadata.ffqn.ifc_fqn.is_extension());
                    importable
                        .exports_hierarchy_ext
                        .retain(|ifc_fns| !ifc_fns.extension);
                }
                component
            })
            .collect()
    }
}

impl FunctionRegistry for ComponentConfigRegistryRO {
    fn get_by_exported_function(
        &self,
        ffqn: &FunctionFqn,
    ) -> Option<(FunctionMetadata, ComponentId)> {
        if ffqn.ifc_fqn.is_extension() {
            None
        } else {
            self.inner
                .exported_ffqns_ext
                .get(ffqn)
                .map(|(id, metadata)| (metadata.clone(), id.clone()))
        }
    }

    fn all_exports(&self) -> &[PackageIfcFns] {
        &self.inner.export_hierarchy
    }
}
