use concepts::StrVariant;
use std::{error::Error, fmt::Debug};
use tokio::task::AbortHandle;
use utils::wasm_tools::{self};

pub mod activity;
mod component_logger;
pub mod engines;
pub mod epoch_ticker;
pub mod preopens_cleaner;
pub mod std_output_stream;
pub mod webhook;
pub mod workflow;

#[derive(thiserror::Error, Debug)]
pub enum WasmFileError {
    #[error("cannot read WASM file: {0}")]
    CannotReadComponent(wasmtime::Error),
    #[error("cannot decode: {0}")]
    DecodeError(#[from] wasm_tools::DecodeError),
    #[error("linking error - {context}, details: {err}")]
    LinkingError {
        context: StrVariant,
        err: Box<dyn Error + Send + Sync>,
    },
}

pub struct AbortOnDropHandle(pub AbortHandle);
impl Drop for AbortOnDropHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub mod envvar {
    #[derive(Clone, derive_more::Debug)]
    pub struct EnvVar {
        pub key: String,
        #[debug(skip)]
        pub val: String,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use concepts::{
        ComponentId, ComponentRetryConfig, FnName, FunctionFqn, FunctionMetadata, FunctionRegistry,
        IfcFqnName, PackageIfcFns, ParameterTypes,
    };
    use indexmap::IndexMap;
    use utils::wasm_tools::WasmComponent;

    pub(crate) struct TestingFnRegistry {
        ffqn_to_fn_details:
            hashbrown::HashMap<FunctionFqn, (FunctionMetadata, ComponentId, ComponentRetryConfig)>,
        export_hierarchy: Vec<PackageIfcFns>,
    }

    impl TestingFnRegistry {
        pub(crate) fn new_from_components(
            wasm_components: Vec<(WasmComponent, ComponentId)>,
        ) -> Arc<dyn FunctionRegistry> {
            let mut ffqn_to_fn_details = hashbrown::HashMap::new();
            let mut export_hierarchy: hashbrown::HashMap<
                IfcFqnName,
                IndexMap<FnName, FunctionMetadata>,
            > = hashbrown::HashMap::new();
            for (wasm_component, component_id) in wasm_components {
                for exported_function in wasm_component.exim.get_exports(true) {
                    let ffqn = exported_function.ffqn.clone();
                    ffqn_to_fn_details.insert(
                        ffqn.clone(),
                        (
                            exported_function.clone(),
                            component_id.clone(),
                            ComponentRetryConfig {
                                max_retries: 0,
                                retry_exp_backoff: Duration::ZERO,
                            },
                        ),
                    );

                    let index_map = export_hierarchy.entry(ffqn.ifc_fqn.clone()).or_default();
                    index_map.insert(ffqn.function_name.clone(), exported_function.clone());
                }
            }
            let export_hierarchy = export_hierarchy
                .into_iter()
                .map(|(ifc_fqn, fns)| PackageIfcFns {
                    extension: ifc_fqn.is_extension(),
                    ifc_fqn,
                    fns,
                })
                .collect();
            Arc::from(TestingFnRegistry {
                ffqn_to_fn_details,
                export_hierarchy,
            })
        }
    }

    #[async_trait]
    impl FunctionRegistry for TestingFnRegistry {
        async fn get_by_exported_function(
            &self,
            ffqn: &FunctionFqn,
        ) -> Option<(FunctionMetadata, ComponentId, ComponentRetryConfig)> {
            self.ffqn_to_fn_details.get(ffqn).cloned()
        }

        fn all_exports(&self) -> &[PackageIfcFns] {
            &self.export_hierarchy
        }
    }

    pub(crate) fn fn_registry_dummy(ffqns: &[FunctionFqn]) -> Arc<dyn FunctionRegistry> {
        let component_id = ComponentId::dummy_activity();
        let mut ffqn_to_fn_details = hashbrown::HashMap::new();
        let mut export_hierarchy: hashbrown::HashMap<
            IfcFqnName,
            IndexMap<FnName, FunctionMetadata>,
        > = hashbrown::HashMap::new();
        for ffqn in ffqns {
            let fn_metadata = FunctionMetadata {
                ffqn: ffqn.clone(),
                parameter_types: ParameterTypes::default(),
                return_type: None,
                extension: None,
                submittable: true,
            };
            ffqn_to_fn_details.insert(
                ffqn.clone(),
                (
                    fn_metadata.clone(),
                    component_id.clone(),
                    ComponentRetryConfig {
                        max_retries: 0,
                        retry_exp_backoff: Duration::ZERO,
                    },
                ),
            );
            let index_map = export_hierarchy.entry(ffqn.ifc_fqn.clone()).or_default();
            index_map.insert(ffqn.function_name.clone(), fn_metadata);
        }
        let export_hierarchy = export_hierarchy
            .into_iter()
            .map(|(ifc_fqn, fns)| PackageIfcFns {
                extension: ifc_fqn.is_extension(),
                ifc_fqn,
                fns,
            })
            .collect();
        Arc::new(TestingFnRegistry {
            ffqn_to_fn_details,
            export_hierarchy,
        })
    }

    mod populate_codegen_cache {
        use crate::{
            activity::activity_worker::tests::compile_activity,
            workflow::workflow_worker::tests::compile_workflow,
        };

        #[rstest::rstest(wasm_path => [
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
            ])]
        #[tokio::test]
        async fn fibo(wasm_path: &str) {
            compile_activity(wasm_path).await;
        }

        #[rstest::rstest(wasm_path => [
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            ])]
        #[tokio::test]
        async fn workflow(wasm_path: &str) {
            compile_workflow(wasm_path).await;
        }

        #[cfg(not(madsim))]
        #[rstest::rstest(wasm_path => [
            test_programs_fibo_webhook_builder::TEST_PROGRAMS_FIBO_WEBHOOK
            ])]
        #[test]
        fn webhook(wasm_path: &str) {
            crate::webhook::webhook_trigger::tests::nosim::compile_webhook(wasm_path);
        }
    }
}
