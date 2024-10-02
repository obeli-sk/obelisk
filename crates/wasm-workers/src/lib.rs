use concepts::StrVariant;
use std::{error::Error, fmt::Debug};
use utils::wasm_tools::{self};

mod activity_ctx;
pub mod activity_worker;
mod component_logger;
pub mod engines;
pub mod epoch_ticker;
mod event_history;
pub mod std_output_stream;
pub mod webhook_trigger;
mod workflow_ctx;
pub mod workflow_worker;

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

pub mod envvar {
    use serde::{Deserialize, Deserializer};

    #[derive(Clone, derivative::Derivative)]
    #[derivative(Debug)]
    pub struct EnvVar {
        pub key: String,
        #[derivative(Debug = "ignore")]
        pub val: String,
    }

    struct EnvVarVisitor;

    impl<'de> serde::de::Visitor<'de> for EnvVarVisitor {
        type Value = EnvVar;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str(
                "either key of environment varaible to be forwarded from host, or key=value",
            )
        }

        fn visit_str<E>(self, input: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(match input.split_once('=') {
                None => {
                    let val = match std::env::var(input) {
                        Ok(val) => val,
                        Err(err) => {
                            return Err(E::custom(format!(
                                "cannot get environment variable `{input}` from the host - {err}"
                            )))
                        }
                    };

                    EnvVar {
                        key: input.to_string(),
                        val,
                    }
                }
                Some((k, input)) => EnvVar {
                    key: k.to_string(),
                    val: input.to_string(),
                },
            })
        }
    }
    impl<'de> Deserialize<'de> for EnvVar {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_str(EnvVarVisitor)
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use concepts::{
        ComponentRetryConfig, ComponentType, ConfigId, FnName, FunctionFqn, FunctionMetadata,
        FunctionRegistry, IfcFqnName, PackageIfcFns, ParameterTypes, ReturnType,
    };
    use indexmap::IndexMap;
    use utils::wasm_tools::WasmComponent;

    pub(crate) struct TestingFnRegistry {
        ffqn_to_fn_details: hashbrown::HashMap<
            FunctionFqn,
            (
                FunctionMetadata,
                ConfigId,
                ComponentRetryConfig,
                ComponentType,
            ),
        >,
        export_hierarchy: Vec<PackageIfcFns>,
    }

    impl TestingFnRegistry {
        pub(crate) fn new_from_components(
            wasm_components: Vec<(WasmComponent, ConfigId, ComponentType)>,
        ) -> Arc<dyn FunctionRegistry> {
            let mut ffqn_to_fn_details = hashbrown::HashMap::new();
            let mut export_hierarchy: hashbrown::HashMap<
                IfcFqnName,
                IndexMap<FnName, (ParameterTypes, Option<ReturnType>)>,
            > = hashbrown::HashMap::new();
            for (wasm_component, config_id, component_type) in wasm_components {
                for exported_function in wasm_component.exim.exports_flat {
                    let ffqn = exported_function.ffqn;
                    ffqn_to_fn_details.insert(
                        ffqn.clone(),
                        (
                            FunctionMetadata {
                                ffqn: ffqn.clone(),
                                parameter_types: exported_function.parameter_types.clone(),
                                return_type: exported_function.return_type.clone(),
                            },
                            config_id.clone(),
                            ComponentRetryConfig::default(),
                            component_type,
                        ),
                    );

                    let index_map = export_hierarchy.entry(ffqn.ifc_fqn.clone()).or_default();
                    index_map.insert(
                        ffqn.function_name.clone(),
                        (
                            exported_function.parameter_types,
                            exported_function.return_type,
                        ),
                    );
                }
            }
            let export_hierarchy = export_hierarchy
                .into_iter()
                .map(|(ifc_fqn, fns)| PackageIfcFns { ifc_fqn, fns })
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
        ) -> Option<(
            FunctionMetadata,
            ConfigId,
            ComponentRetryConfig,
            ComponentType,
        )> {
            self.ffqn_to_fn_details.get(ffqn).cloned()
        }

        fn all_exports(&self) -> &[PackageIfcFns] {
            &self.export_hierarchy
        }
    }

    pub(crate) fn fn_registry_dummy(ffqns: &[FunctionFqn]) -> Arc<dyn FunctionRegistry> {
        let component_id = ConfigId::dummy_activity();
        let mut ffqn_to_fn_details = hashbrown::HashMap::new();
        let mut export_hierarchy: hashbrown::HashMap<
            IfcFqnName,
            IndexMap<FnName, (ParameterTypes, Option<ReturnType>)>,
        > = hashbrown::HashMap::new();
        for ffqn in ffqns {
            ffqn_to_fn_details.insert(
                ffqn.clone(),
                (
                    FunctionMetadata {
                        ffqn: ffqn.clone(),
                        parameter_types: ParameterTypes::default(),
                        return_type: None,
                    },
                    component_id.clone(),
                    ComponentRetryConfig::default(),
                    ComponentType::ActivityWasm,
                ),
            );
            let index_map = export_hierarchy.entry(ffqn.ifc_fqn.clone()).or_default();
            index_map.insert(
                ffqn.function_name.clone(),
                (ParameterTypes::default(), None),
            );
        }
        let export_hierarchy = export_hierarchy
            .into_iter()
            .map(|(ifc_fqn, fns)| PackageIfcFns { ifc_fqn, fns })
            .collect();
        Arc::new(TestingFnRegistry {
            ffqn_to_fn_details,
            export_hierarchy,
        })
    }

    mod populate_codegen_cache {
        use crate::{
            activity_worker::tests::compile_activity, workflow_worker::tests::compile_workflow,
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
        #[tokio::test]
        async fn webhook(wasm_path: &str) {
            crate::webhook_trigger::tests::nosim::compile_webhook(wasm_path).await;
        }
    }
}
