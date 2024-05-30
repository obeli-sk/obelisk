//! Worker that acts as wrapper for `activity_worker` or `workflow_worker`.
//! Apply following heuristic to distinguish between an activity and workflow:
//! * Read all imported functions of the component's world
//! * If there are no imports except for standard WASI -> Activity
//! * Otherwise -> Workflow

use crate::{
    activity_worker::{ActivityConfig, ActivityWorker, RecycleInstancesSetting},
    workflow_worker::{
        JoinNextBlockingStrategy, NonBlockingEventBatching, WorkflowConfig, WorkflowWorker,
    },
    WasmComponent, WasmFileError,
};
use async_trait::async_trait;
use concepts::{
    prefixed_ulid::ConfigId,
    storage::{DbConnection, DbPool},
    ComponentType, FunctionFqn, IfcFqnName, ParameterTypes, ReturnType, StrVariant,
};
use executor::worker::{Worker, WorkerContext};
use itertools::Either;
use std::{ops::Deref, sync::Arc, time::Duration};
use utils::time::ClockFn;
use wasmtime::Engine;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AutoConfig {
    pub config_id: ConfigId,
    pub wasm_path: StrVariant, // TODO: PathBuf
    pub activity_recycled_instances: RecycleInstancesSetting,
    pub workflow_join_next_blocking_strategy: JoinNextBlockingStrategy,
    pub workflow_child_retry_exp_backoff: Duration,
    pub workflow_child_max_retries: u32,
    pub non_blocking_event_batching: NonBlockingEventBatching,
}

pub enum AutoWorker<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    ActivityWorker(ActivityWorker<C>),
    WorkflowWorker(WorkflowWorker<C, DB, P>),
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> AutoWorker<C, DB, P> {
    #[tracing::instrument(skip_all, fields(wasm_path = %config.wasm_path, config_id = %config.config_id))]
    pub fn new_with_config(
        config: AutoConfig,
        workflow_engine: Arc<Engine>,
        activity_engine: Arc<Engine>,
        workflow_db_pool: P,
        clock_fn: C,
    ) -> Result<Self, WasmFileError> {
        // must pick an engine to parse the component
        let wasm_component = WasmComponent::new(config.wasm_path.clone(), &activity_engine)?;
        if supported_wasi_imports(wasm_component.exim.imports.iter().map(|pif| &pif.ifc_fqn)) {
            ActivityWorker::new_with_config(
                wasm_component,
                ActivityConfig {
                    config_id: config.config_id,
                    recycled_instances: config.activity_recycled_instances,
                },
                activity_engine,
                clock_fn,
            )
            .map(Self::ActivityWorker)
        } else {
            // wrong engine was picked
            let wasm_component = WasmComponent::new(config.wasm_path, &workflow_engine)?;
            WorkflowWorker::new_with_config(
                wasm_component,
                WorkflowConfig {
                    config_id: config.config_id,
                    join_next_blocking_strategy: config.workflow_join_next_blocking_strategy,
                    child_retry_exp_backoff: config.workflow_child_retry_exp_backoff,
                    child_max_retries: config.workflow_child_max_retries,
                    non_blocking_event_batching: config.non_blocking_event_batching,
                },
                workflow_engine,
                workflow_db_pool,
                clock_fn,
            )
            .map(Self::WorkflowWorker)
        }
    }

    pub fn component_type(&self) -> ComponentType {
        match self {
            Self::ActivityWorker(_) => ComponentType::WasmActivity,
            Self::WorkflowWorker(_) => ComponentType::WasmWorkflow,
        }
    }
}

fn supported_wasi_imports<'a>(mut imported_packages: impl Iterator<Item = &'a IfcFqnName>) -> bool {
    // FIXME Fail if both wasi and host imports are present
    imported_packages.all(|ifc| ifc.deref().starts_with("wasi:"))
}

#[async_trait]
impl<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static> Worker
    for AutoWorker<C, DB, P>
{
    async fn run(&self, ctx: WorkerContext) -> executor::worker::WorkerResult {
        match self {
            AutoWorker::WorkflowWorker(w) => w.run(ctx).await,
            AutoWorker::ActivityWorker(a) => a.run(ctx).await,
        }
    }

    fn exported_functions(
        &self,
    ) -> impl Iterator<Item = (FunctionFqn, ParameterTypes, ReturnType)> {
        match self {
            AutoWorker::WorkflowWorker(w) => Either::Left(w.exported_functions()),
            AutoWorker::ActivityWorker(a) => Either::Right(a.exported_functions()),
        }
    }

    fn imported_functions(
        &self,
    ) -> impl Iterator<Item = (FunctionFqn, ParameterTypes, ReturnType)> {
        match self {
            AutoWorker::WorkflowWorker(w) => Either::Left(w.imported_functions()),
            AutoWorker::ActivityWorker(a) => Either::Right(a.imported_functions()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AutoConfig, AutoWorker};
    use crate::{
        activity_worker::{activity_engine, RecycleInstancesSetting},
        workflow_worker::{workflow_engine, JoinNextBlockingStrategy, NonBlockingEventBatching},
        EngineConfig,
    };
    use concepts::{prefixed_ulid::ConfigId, StrVariant};
    use concepts::{storage::DbPool, ComponentType};
    use db_tests::Database;
    use std::time::Duration;
    use test_utils::set_up;
    use utils::time::now;

    #[rstest::rstest]
    #[case(
        test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
        ComponentType::WasmActivity
    )]
    #[case(
        test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
        ComponentType::WasmWorkflow
    )]
    #[tokio::test]
    async fn detection(#[case] file: &'static str, #[case] expected: ComponentType) {
        set_up();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let config = AutoConfig {
            wasm_path: StrVariant::Static(file),
            config_id: ConfigId::generate(),
            activity_recycled_instances: RecycleInstancesSetting::Disable,
            workflow_join_next_blocking_strategy: JoinNextBlockingStrategy::default(),
            workflow_child_retry_exp_backoff: Duration::ZERO,
            workflow_child_max_retries: 0,
            non_blocking_event_batching: NonBlockingEventBatching::default(),
        };
        let worker = AutoWorker::new_with_config(
            config,
            workflow_engine(EngineConfig::default()),
            activity_engine(EngineConfig::default()),
            db_pool.clone(),
            now,
        )
        .unwrap();
        assert_eq!(expected, worker.component_type());
        drop(worker);
        db_pool.close().await.unwrap();
    }
}
