use concepts::StrVariant;
use std::{error::Error, fmt::Debug};
use utils::wasm_tools::{self};

mod activity_ctx;
pub mod activity_worker;
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
    DecodeError(wasm_tools::DecodeError),
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
        ComponentRetryConfig, ConfigId, FunctionFqn, FunctionMetadata, FunctionRegistry,
        ParameterTypes,
    };

    pub(crate) struct TestingFnRegistry(
        hashbrown::HashMap<FunctionFqn, (FunctionMetadata, ConfigId, ComponentRetryConfig)>,
    );

    #[async_trait]
    impl FunctionRegistry for TestingFnRegistry {
        async fn get_by_exported_function(
            &self,
            ffqn: &FunctionFqn,
        ) -> Option<(FunctionMetadata, ConfigId, ComponentRetryConfig)> {
            self.0.get(ffqn).cloned()
        }
    }

    pub(crate) fn fn_registry_dummy(ffqns: &[FunctionFqn]) -> Arc<dyn FunctionRegistry> {
        let component_id = ConfigId::dummy();
        let mut map = hashbrown::HashMap::new();
        for ffqn in ffqns {
            map.insert(
                ffqn.clone(),
                (
                    FunctionMetadata {
                        ffqn: ffqn.clone(),
                        parameter_types: ParameterTypes::default(),
                        return_type: None,
                    },
                    component_id.clone(),
                    ComponentRetryConfig::default(),
                ),
            );
        }
        Arc::new(TestingFnRegistry(map))
    }

    mod populate_codegen_cache {
        use crate::{
            activity_worker::{ActivityConfig, ActivityWorker, RecycleInstancesSetting},
            engines::{EngineConfig, Engines},
            tests::fn_registry_dummy,
            webhook_trigger::{self, MethodAwareRouter},
            workflow_worker::{tests::get_workflow_worker, JoinNextBlockingStrategy},
        };
        use concepts::ConfigId;
        use db_tests::Database;
        use hyper::Method;
        use std::{net::SocketAddr, sync::Arc, time::Duration};
        use tokio::net::TcpListener;
        use utils::{time::now, wasm_tools::WasmComponent};

        #[rstest::rstest(path => [
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
            ])]
        #[tokio::test]
        async fn activity(path: &str) {
            let engine =
                Engines::get_activity_engine(EngineConfig::on_demand_testing().await).unwrap();
            ActivityWorker::new_with_config(
                path,
                ActivityConfig {
                    config_id: ConfigId::dummy(),
                    recycle_instances: RecycleInstancesSetting::default(),
                    forward_stdout: None,
                    forward_stderr: None,
                    env_vars: Arc::from([]),
                },
                engine,
                now,
            )
            .unwrap();
        }

        #[rstest::rstest(path => [
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            ])]
        #[tokio::test]
        async fn workflow(path: &str) {
            let (_guard, db_pool) = Database::Memory.set_up().await;
            get_workflow_worker(
                path,
                db_pool,
                now,
                JoinNextBlockingStrategy::default(),
                0,
                fn_registry_dummy(&[]),
            )
            .await;
        }

        #[rstest::rstest(path => [
            test_programs_fibo_webhook_builder::TEST_PROGRAMS_FIBO_WEBHOOK
            ])]
        #[tokio::test]
        async fn webhook(path: &str) {
            let engine =
                Engines::get_webhook_engine(EngineConfig::on_demand_testing().await).unwrap();
            let instance = webhook_trigger::component_to_instance(
                &WasmComponent::new(path, &engine).unwrap(),
                &engine,
                ConfigId::dummy(),
                None,
                None,
                Arc::from([]),
            )
            .unwrap();

            let (_guard, db_pool) = Database::Memory.set_up().await;
            let mut router = MethodAwareRouter::default();
            router.add(Some(Method::GET), "/fibo/:N/:ITERATIONS", instance);
            let tcp_listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .unwrap();
            drop(webhook_trigger::server(
                tcp_listener,
                engine,
                router,
                db_pool.clone(),
                now,
                fn_registry_dummy(&[]),
                crate::webhook_trigger::RetryConfigOverride::default(),
                Duration::from_secs(1),
            ));
        }
    }
}
