//! Component registry for a single deployment.
//!
use crate::RunnableComponent;
use concepts::ComponentId;
use concepts::ComponentType;
use concepts::FunctionFqn;
use concepts::FunctionMetadata;
use concepts::FunctionRegistry;
use concepts::IfcFqnName;
use concepts::PackageIfcFns;
use concepts::StrVariant;
use concepts::component_id::ComponentDigest;
use concepts::storage::LogLevel;
use hashbrown::HashMap;
use indexmap::IndexMap;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;
use tracing::error;

/// Origin of a component's WIT: parsed from a real WASM binary, or synthesized from `TypeWrapper`s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display)]
pub enum WitOrigin {
    #[display("wasm")]
    Wasm,
    #[display("synthesized")]
    Synthesized,
}

/// Holds information about components, used for gRPC services like `ListComponents`
#[derive(Debug, Clone)]
pub struct ComponentConfig {
    pub component_id: ComponentId,
    pub imports: Vec<FunctionMetadata>,
    pub workflow_or_activity_config: Option<ComponentConfigImportable>,
    pub wit: String,
    pub workflow_replay_info: Option<WorkflowReplayInfo>,
    /// Origin of this component's WIT (parsed from WASM vs synthesized from `TypeWrapper`s).
    pub wit_origin: WitOrigin,
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
    /// Tracks the origin (synthesized-WIT vs real WASM) of each exported non-extension
    /// interface so that JS / inline-stub interfaces cannot be merged into WASM-owned
    /// interfaces and vice versa.
    export_hierarchy_origin: HashMap<IfcFqnName, WitOrigin>,
    /// Primary index: component name → component config. Names are unique across all component types.
    names_to_components: IndexMap<StrVariant, ComponentConfig>,
    /// Digest-keyed secondary indexes.
    digests_to_wit: IndexMap<ComponentDigest, String>,
    digests_to_replay_info: IndexMap<ComponentDigest, (ComponentId, WorkflowReplayInfo)>,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("registering component failed: {0}")]
pub struct ComponentInsertionError(StrVariant);

impl ComponentConfigRegistry {
    pub fn insert(&mut self, component: ComponentConfig) -> Result<(), ComponentInsertionError> {
        let name = &component.component_id.name;
        // verify that the component is not already present by name
        if self.inner.names_to_components.contains_key(name) {
            return Err(ComponentInsertionError(
                format!("component with name `{name}` is already registered").into(),
            ));
        }

        // component.workflow_or_activity_config == None implies webhook.
        // Webhooks do not have to have a unique component digest - all share the same ffqn.
        // The same webhook source can be configured differently.
        // Webhooks need just to provide source and WIT, so duplication is OK.

        if let Some(workflow_or_activity_config) = &component.workflow_or_activity_config {
            if self
                .inner
                .digests_to_wit
                .contains_key(&component.component_id.component_digest)
            {
                return Err(ComponentInsertionError(
                    format!(
                        "component {} is already inserted with the same digest",
                        component.component_id
                    )
                    .into(),
                ));
            }

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
            // Insert into `export_hierarchy`, merging entries that share the same ifc_fqn.
            for new_ifc_fns in &workflow_or_activity_config.exports_hierarchy_ext {
                if !new_ifc_fns.extension {
                    if let Some(&existing_origin) =
                        self.inner.export_hierarchy_origin.get(&new_ifc_fns.ifc_fqn)
                    {
                        if existing_origin != component.wit_origin {
                            return Err(ComponentInsertionError(
                                format!(
                                    "interface `{}` is already exported by a {} component, cannot insert {} which is a {} component",
                                    new_ifc_fns.ifc_fqn,
                                    existing_origin,
                                    component.component_id,
                                    component.wit_origin,
                                )
                                .into(),
                            ));
                        }
                    } else {
                        self.inner
                            .export_hierarchy_origin
                            .insert(new_ifc_fns.ifc_fqn.clone(), component.wit_origin);
                    }
                }
                if let Some(existing) = self.inner.export_hierarchy.iter_mut().find(|e| {
                    e.ifc_fqn == new_ifc_fns.ifc_fqn && e.extension == new_ifc_fns.extension
                }) {
                    existing.fns.extend(new_ifc_fns.fns.clone());
                } else {
                    self.inner.export_hierarchy.push(new_ifc_fns.clone());
                }
            }

            // Insert into `digests_to_wit`
            let old = self.inner.digests_to_wit.insert(
                component.component_id.component_digest.clone(),
                component.wit.clone(),
            );
            assert!(old.is_none());
            // Insert into `workflow_replay_info`
            if let Some(replay_info) = component.workflow_replay_info.clone() {
                let old = self.inner.digests_to_replay_info.insert(
                    component.component_id.component_digest.clone(),
                    (component.component_id.clone(), replay_info),
                );
                assert!(old.is_none());
            }
        } else if component.component_id.component_type == ComponentType::WebhookEndpoint {
            // first wins for digest-keyed maps (same code = same WIT)
            self.inner
                .digests_to_wit
                .entry(component.component_id.component_digest.clone())
                .or_insert(component.wit.clone());
        } // Cron executions do not expose WIT

        let old = self
            .inner
            .names_to_components
            .insert(name.clone(), component);
        assert!(old.is_none());

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
        for examined_component in self.inner.names_to_components.values() {
            self.verify_imports_component(examined_component, &mut errors);
        }
        let errors = if !errors.is_empty() {
            let errors = errors.join("\n");
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
            ComponentType::Activity => {
                // wasi + log
                match import.ffqn.ifc_fqn.namespace() {
                    "wasi" => true,
                    "obelisk" => import.ffqn.ifc_fqn.deref() == "obelisk:log/log@1.0.0",
                    _ => false,
                }
            }
            ComponentType::Workflow => {
                // log + workflow support + types
                matches!(
                    import.ffqn.ifc_fqn.pkg_fqn_name().to_string().as_str(),
                    "obelisk:log@1.0.0"
                        | "obelisk:workflow@5.0.0"
                        | "obelisk:workflow@5.1.0"
                        | "obelisk:types@4.2.0"
                )
            }
            ComponentType::WebhookEndpoint => {
                // webhook support + wasi + log + types (needed for scheduling)
                match import.ffqn.ifc_fqn.namespace() {
                    "wasi" => true,
                    "obelisk" => matches!(
                        import.ffqn.ifc_fqn.pkg_fqn_name().to_string().as_str(),
                        "obelisk:webhook@5.1.0"
                            | "obelisk:webhook@5.0.0"
                            | "obelisk:log@1.0.0"
                            | "obelisk:types@4.0.0"
                            | "obelisk:types@4.1.0"
                            | "obelisk:types@4.2.0"
                    ),
                    _ => false,
                }
            }
            ComponentType::ActivityStub | ComponentType::Cron => false,
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
    /// Look up WIT by content digest. Returns `None` if the digest is not found.
    #[must_use]
    pub fn get_wit(&self, input_digest: &ComponentDigest) -> Option<&str> {
        self.inner
            .digests_to_wit
            .get(input_digest)
            .map(std::string::String::as_str)
    }

    #[must_use]
    pub fn get_workflow_replay_info(
        &self,
        input_digest: &ComponentDigest,
    ) -> Option<(&ComponentId, &WorkflowReplayInfo)> {
        self.inner
            .digests_to_replay_info
            .get(input_digest)
            .map(|(id, ri)| (id, ri))
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

    /// List components. When `extensions` is set to false, extended functions are stripped from exports in each component.
    #[must_use]
    pub fn list(&self, extensions: bool) -> Vec<ComponentConfig> {
        self.inner
            .names_to_components
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
