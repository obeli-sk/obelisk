//! Worker that acts as wrapper for `activity_worker` or `workflow_worker`.
//! Apply following heuristic to distinguish between an activity and workflow:
//! * Read all imported functions of the component's world
//! * If there are no imports except for standard WASI -> Activity
//! * Otherwise -> Workflow

use crate::{
    activity_worker::{ActivityConfig, ActivityWorker, RecycleInstancesSetting},
    workflow_worker::{JoinNextBlockingStrategy, WorkflowConfig, WorkflowWorker},
    WasmComponent, WasmFileError,
};
use async_trait::async_trait;
use concepts::{prefixed_ulid::ConfigId, storage::DbConnection, FunctionFqn, StrVariant};
use derivative::Derivative;
use executor::worker::Worker;
use itertools::Either;
use std::{sync::Arc, time::Duration};
use utils::time::ClockFn;
use wasmtime::Engine;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct AutoConfig<C: ClockFn> {
    pub config_id: ConfigId,
    pub wasm_path: StrVariant,
    pub activity_recycled_instances: RecycleInstancesSetting,
    pub clock_fn: C,
    pub workflow_join_next_blocking_strategy: JoinNextBlockingStrategy,
    #[derivative(Debug = "ignore")]
    pub workflow_db_connection: Arc<dyn DbConnection>,
    pub workflow_child_retry_exp_backoff: Duration,
    pub workflow_child_max_retries: u32,
}

pub enum AutoWorker<C: ClockFn> {
    ActivityWorker(ActivityWorker<C>),
    WorkflowWorker(WorkflowWorker<C>),
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum Kind {
    Activity,
    Workflow,
}

impl<C: ClockFn> AutoWorker<C> {
    #[tracing::instrument(skip_all, fields(wasm_path = %config.wasm_path, config_id = %config.config_id))]
    pub fn new_with_config(
        config: AutoConfig<C>,
        workflow_engine: Arc<Engine>,
        activity_engine: Arc<Engine>,
    ) -> Result<Self, WasmFileError> {
        let wasm_component = WasmComponent::new(config.wasm_path)?;
        if supported_wasi_imports(
            wasm_component
                .imported_ifc_fns
                .iter()
                .map(|pif| &pif.package_name),
        ) {
            let config = ActivityConfig {
                config_id: config.config_id,
                recycled_instances: config.activity_recycled_instances,
                clock_fn: config.clock_fn,
            };
            ActivityWorker::new_with_config(wasm_component, config, activity_engine)
                .map(Self::ActivityWorker)
        } else {
            let config = WorkflowConfig {
                config_id: config.config_id,
                clock_fn: config.clock_fn,
                join_next_blocking_strategy: config.workflow_join_next_blocking_strategy,
                db_connection: config.workflow_db_connection,
                child_retry_exp_backoff: config.workflow_child_retry_exp_backoff,
                child_max_retries: config.workflow_child_max_retries,
            };
            WorkflowWorker::new_with_config(wasm_component, config, workflow_engine)
                .map(Self::WorkflowWorker)
        }
    }

    pub fn kind(&self) -> Kind {
        match self {
            Self::ActivityWorker(_) => Kind::Activity,
            Self::WorkflowWorker(_) => Kind::Workflow,
        }
    }
}

fn supported_wasi_imports<'a>(
    mut imported_packages: impl Iterator<Item = &'a wit_parser::PackageName>,
) -> bool {
    imported_packages.all(|pkg_name| pkg_name.namespace == "wasi")
}

#[async_trait]
impl<C: ClockFn + 'static> Worker for AutoWorker<C> {
    async fn run(
        &self,
        execution_id: concepts::ExecutionId,
        ffqn: concepts::FunctionFqn,
        params: concepts::Params,
        event_history: Vec<concepts::storage::HistoryEvent>,
        version: concepts::storage::Version,
        execution_deadline: chrono::prelude::DateTime<chrono::prelude::Utc>,
    ) -> executor::worker::WorkerResult {
        match self {
            AutoWorker::WorkflowWorker(w) => {
                w.run(
                    execution_id,
                    ffqn,
                    params,
                    event_history,
                    version,
                    execution_deadline,
                )
                .await
            }
            AutoWorker::ActivityWorker(a) => {
                a.run(
                    execution_id,
                    ffqn,
                    params,
                    event_history,
                    version,
                    execution_deadline,
                )
                .await
            }
        }
    }

    fn supported_functions(&self) -> impl Iterator<Item = &FunctionFqn> {
        (match self {
            AutoWorker::WorkflowWorker(w) => Either::Left(w.supported_functions()),
            AutoWorker::ActivityWorker(a) => Either::Right(a.supported_functions()),
        })
        .into_iter()
    }
}

mod valuable {
    use super::AutoWorker;
    use utils::time::ClockFn;

    impl<C: ClockFn> ::valuable::Structable for AutoWorker<C> {
        fn definition(&self) -> ::valuable::StructDef<'_> {
            match self {
                AutoWorker::WorkflowWorker(w) => w.definition(),
                AutoWorker::ActivityWorker(a) => a.definition(),
            }
        }
    }

    impl<C: ClockFn> ::valuable::Valuable for AutoWorker<C> {
        fn as_value(&self) -> ::valuable::Value<'_> {
            ::valuable::Value::Structable(self)
        }

        fn visit(&self, visitor: &mut dyn ::valuable::Visit) {
            match self {
                AutoWorker::WorkflowWorker(w) => w.visit(visitor),
                AutoWorker::ActivityWorker(a) => a.visit(visitor),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{AutoConfig, AutoWorker};
    use crate::{
        activity_worker::{activity_engine, RecycleInstancesSetting},
        auto_worker::Kind,
        workflow_worker::{workflow_engine, JoinNextBlockingStrategy},
        EngineConfig,
    };
    use concepts::{prefixed_ulid::ConfigId, StrVariant};
    use db::inmemory_dao::DbTask;
    use test_utils::set_up;
    use utils::time::now;

    #[rstest::rstest]
    #[case(
        test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
        Kind::Activity
    )]
    #[case(
        test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
        Kind::Workflow
    )]
    #[tokio::test]
    async fn detection(#[case] file: &'static str, #[case] expected: Kind) {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.as_db_connection().expect("must be open");
        let config = AutoConfig {
            wasm_path: StrVariant::Static(file),
            config_id: ConfigId::generate(),
            activity_recycled_instances: RecycleInstancesSetting::Disable,
            clock_fn: now,
            workflow_join_next_blocking_strategy: JoinNextBlockingStrategy::default(),
            workflow_db_connection: db_connection,
            workflow_child_retry_exp_backoff: Duration::ZERO,
            workflow_child_max_retries: 0,
        };
        let worker = AutoWorker::new_with_config(
            config,
            workflow_engine(EngineConfig::default()),
            activity_engine(EngineConfig::default()),
        )
        .unwrap();
        assert_eq!(expected, worker.kind());
        drop(worker);
        db_task.close().await;
    }
}
