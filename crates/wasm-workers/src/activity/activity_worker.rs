use super::activity_ctx::{self, ActivityCtx};
use super::activity_ctx_process::process_support_outer::v1_0_0::obelisk::activity::process as process_support;
use crate::activity::activity_ctx::ActivityPreopenIoError;
use crate::activity::cancel_registry::CancelRegistry;
use crate::component_logger::log_activities;
use crate::envvar::EnvVar;
use crate::std_output_stream::{StdOutputConfig, StdOutputConfigWithSender};
use crate::{RunnableComponent, WasmFileError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::storage::http_client_trace::HttpClientTrace;
use concepts::storage::{LogInfoAppendRow, LogStreamType};
use concepts::time::{ClockFn, Sleep, now_tokio_instant};
use concepts::{ComponentId, FunctionFqn, PackageIfcFns, SupportedFunctionReturnValue, TrapKind};
use concepts::{FunctionMetadata, ResultParsingError};
use executor::worker::{FatalError, WorkerContext, WorkerResult};
use executor::worker::{Worker, WorkerError};
use itertools::Itertools;
use std::path::Path;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};
use utils::wasm_tools::ExIm;
use wasmtime::component::{ComponentExportIndex, InstancePre, Type};
use wasmtime::{Engine, component::Val};
use wasmtime::{Store, UpdateDeadline};

#[derive(Clone, Debug)]
pub struct ActivityConfig {
    pub component_id: ComponentId,
    pub forward_stdout: Option<StdOutputConfig>,
    pub forward_stderr: Option<StdOutputConfig>,
    pub env_vars: Arc<[EnvVar]>,
    pub retry_on_err: bool,
    pub directories_config: Option<ActivityDirectoriesConfig>,
    pub fuel: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct ActivityDirectoriesConfig {
    pub parent_preopen_dir: Arc<Path>,
    pub reuse_on_retry: bool,
    pub process_provider: Option<ProcessProvider>,
}

#[derive(Clone, Copy, Debug)]
pub enum ProcessProvider {
    Native,
}

#[derive(Clone)]
pub struct ActivityWorkerCompiled<C: ClockFn, S: Sleep> {
    engine: Arc<Engine>,
    instance_pre: InstancePre<ActivityCtx<C>>,
    exim: ExIm,
    clock_fn: C,
    sleep: S,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    config: ActivityConfig,
}
impl<C: ClockFn, S: Sleep> ActivityWorkerCompiled<C, S> {
    pub fn new_with_config(
        runnable_component: RunnableComponent,
        config: ActivityConfig,
        engine: Arc<Engine>,
        clock_fn: C,
        sleep: S,
    ) -> Result<Self, WasmFileError> {
        let mut linker = wasmtime::component::Linker::new(&engine);
        // wasi
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)
            .map_err(|err| WasmFileError::linking_error("cannot link wasi", err))?;
        // wasi-http
        wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker)
            .map_err(|err| WasmFileError::linking_error("cannot link wasi-http", err))?;
        // obelisk:log
        log_activities::obelisk::log::log::add_to_linker::<_, ActivityCtx<C>>(&mut linker, |x| x)
            .map_err(|err| WasmFileError::linking_error("cannot link obelisk:log", err))?;
        // obelisk:activity/process
        match config
            .directories_config
            .as_ref()
            .and_then(|dir| dir.process_provider.as_ref())
        {
            Some(ProcessProvider::Native) => {
                process_support::add_to_linker::<_, ActivityCtx<C>>(&mut linker, |x| x).map_err(
                    |err| WasmFileError::linking_error("cannot link process support", err),
                )?;
            }
            None => {}
        }
        // Attempt to pre-instantiate to catch missing imports
        let instance_pre = linker
            .instantiate_pre(&runnable_component.wasmtime_component)
            .map_err(|err| WasmFileError::linking_error("cannot link activity", err))?;

        let exported_ffqn_to_index = runnable_component
            .index_exported_functions()
            .map_err(WasmFileError::DecodeError)?;
        Ok(Self {
            engine,
            exim: runnable_component.wasm_component.exim,
            clock_fn,
            sleep,
            exported_ffqn_to_index,
            config,
            instance_pre,
        })
    }

    pub fn exported_functions_ext(&self) -> &[FunctionMetadata] {
        self.exim.get_exports(true)
    }

    pub fn exports_hierarchy_ext(&self) -> &[PackageIfcFns] {
        self.exim.get_exports_hierarchy_ext()
    }

    pub fn imported_functions(&self) -> &[FunctionMetadata] {
        &self.exim.imports_flat
    }

    pub fn into_worker(
        self,
        cancel_registry: CancelRegistry,
        log_forwarder_sender: &mpsc::Sender<LogInfoAppendRow>,
    ) -> ActivityWorker<C, S> {
        let stdout = StdOutputConfigWithSender::new(
            self.config.forward_stdout,
            log_forwarder_sender,
            LogStreamType::StdOut,
        );
        let stderr = StdOutputConfigWithSender::new(
            self.config.forward_stderr,
            log_forwarder_sender,
            LogStreamType::StdErr,
        );
        ActivityWorker {
            engine: self.engine,
            instance_pre: self.instance_pre,
            exim: self.exim,
            clock_fn: self.clock_fn,
            sleep: self.sleep,
            exported_ffqn_to_index: self.exported_ffqn_to_index,
            config: self.config,
            cancel_registry,
            stdout,
            stderr,
        }
    }
}

#[derive(Clone)]
pub struct ActivityWorker<C: ClockFn, S: Sleep> {
    engine: Arc<Engine>,
    instance_pre: InstancePre<ActivityCtx<C>>,
    exim: ExIm,
    clock_fn: C,
    sleep: S,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    config: ActivityConfig,
    cancel_registry: CancelRegistry,
    stdout: Option<StdOutputConfigWithSender>,
    stderr: Option<StdOutputConfigWithSender>,
}

impl<C: ClockFn + 'static, S: Sleep> ActivityWorker<C, S> {
    pub fn exported_functions_ext(&self) -> &[FunctionMetadata] {
        self.exim.get_exports(true)
    }

    pub fn exports_hierarchy_ext(&self) -> &[PackageIfcFns] {
        self.exim.get_exports_hierarchy_ext()
    }

    pub fn imported_functions(&self) -> &[FunctionMetadata] {
        &self.exim.imports_flat
    }
}

#[async_trait]
impl<C: ClockFn + 'static, S: Sleep + 'static> Worker for ActivityWorker<C, S> {
    fn exported_functions(&self) -> &[FunctionMetadata] {
        self.exim.get_exports(false)
    }

    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        trace!("Params: {params:?}", params = ctx.params);
        assert!(ctx.event_history.is_empty());
        let cancelation_token = self
            .cancel_registry
            .obtain_cancellation_token(ctx.execution_id.clone());

        let started_at = self.clock_fn.now();
        ctx.worker_span.record(
            "execution_deadline",
            tracing::field::display(&ctx.locked_event.lock_expires_at),
        );

        let (mut store, deadline_duration) = match self.create_store(&ctx, started_at).await {
            Ok(store) => store,
            Err(err) => return WorkerResult::Err(err),
        };

        let stopwatch_for_reporting = now_tokio_instant(); // Not using `clock_fn` here is ok, value is only used for log reporting.

        let call_function = {
            let call_func_params = match self.call_func_params(&ctx, &mut store).await {
                Ok(ok) => ok,
                Err(err) => return WorkerResult::Err(err),
            };
            self.call_func(&mut store, call_func_params)
        };

        tokio::select! { // future's liveness: Dropping the loser immediately.
            res = call_function => {
                let activity_ctx = store.into_data();
                let res = self.process_res(res, &ctx, activity_ctx);
                ctx.worker_span.in_scope(|| {
                    if let WorkerResult::Err(err) = &res {
                        info!(%err, duration = ?stopwatch_for_reporting.elapsed(),
                        "Finished with an error");
                    } else {
                        info!(duration = ?stopwatch_for_reporting.elapsed(),
                        "Finished");
                    }
                });
                return res;
            },
            ()  = self.sleep.sleep(deadline_duration) => {
                let activity_ctx = store.into_data();
                ctx.worker_span.in_scope(||
                        info!(duration = ?stopwatch_for_reporting.elapsed(), %started_at,
                        now = %self.clock_fn.now(),
                        "Timed out")
                    );
                let http_client_traces = Some(activity_ctx.http_client_traces
                    .into_iter()
                        .map(|(req, mut resp)| HttpClientTrace {
                            req,
                            resp: resp.try_recv().ok(),
                        })
                        .collect_vec());
                return WorkerResult::Err(WorkerError::TemporaryTimeout{
                    http_client_traces,
                    version: ctx.version,
                });
            }
            cancel_res = cancelation_token => {
                // TODO: Add http traces
                assert!(cancel_res.is_ok(), "only closed channels are dropped");
                return WorkerResult::Err(WorkerError::FatalError(FatalError::Cancelled, ctx.version));
            }
        }
    }
}

struct CallFuncParams {
    func: wasmtime::component::Func,
    params: Arc<[Val]>,
    result_type: Type,
}

impl<C: ClockFn + 'static, S: Sleep + 'static> ActivityWorker<C, S> {
    async fn create_store(
        &self,
        ctx: &WorkerContext,
        started_at: DateTime<Utc>,
    ) -> Result<(Store<ActivityCtx<C>>, Duration /* deadline duration*/), WorkerError> {
        let preopened_dir = if let Some(directories_config) = &self.config.directories_config {
            let preopened_dir = directories_config
                .parent_preopen_dir
                .join(ctx.execution_id.to_string());
            if !directories_config.reuse_on_retry {
                // Attempt to `rm -rf` before (re)creating the directory.
                match tokio::fs::remove_dir_all(&preopened_dir).await {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => {
                        error!(
                            "Cannot remove old preopened directory that is in the way - {err:?}"
                        );
                        return Err(WorkerError::ActivityPreopenedDirError {
                            reason: format!(
                                "cannot remove old preopened directory that is in the way - {err}"
                            ),
                            detail: format!("{err:?}"),
                            version: ctx.version.clone(),
                        });
                    }
                }
            }
            let res = tokio::fs::create_dir_all(&preopened_dir).await;
            if let Err(err) = res {
                error!("cannot create preopened directory - {err:?}");
                return Err(WorkerError::ActivityPreopenedDirError {
                    reason: format!("cannot create preopened directory - {err}"),
                    detail: format!("{err:?}"),
                    version: ctx.version.clone(),
                });
            }
            Some(preopened_dir)
        } else {
            None
        };

        let mut store = match activity_ctx::store(
            &self.engine,
            &ctx.execution_id,
            &self.config,
            ctx.worker_span.clone(),
            self.clock_fn.clone(),
            preopened_dir,
            self.stdout
                .as_ref()
                .map(|it| it.build(&ctx.execution_id, ctx.locked_event.run_id)),
            self.stderr
                .as_ref()
                .map(|it| it.build(&ctx.execution_id, ctx.locked_event.run_id)),
        ) {
            Ok(store) => store,
            Err(ActivityPreopenIoError { err }) => {
                return Err(WorkerError::ActivityPreopenedDirError {
                    reason: format!(
                        "not found although preopened directory was just created - {err}"
                    ),
                    detail: format!("{err:?}"),
                    version: ctx.version.clone(),
                });
            }
        };

        // Set fuel.
        if let Some(fuel) = self.config.fuel {
            store
                .set_fuel(fuel)
                .expect("engine must have `consume_fuel` enabled");
        }

        // Configure epoch callback before running the initialization to avoid interruption
        store.epoch_deadline_callback(|_store_ctx| {
            Ok(UpdateDeadline::YieldCustom(
                1,
                Box::pin(tokio::task::yield_now()),
            ))
        });
        let deadline_delta = ctx.locked_event.lock_expires_at - started_at;
        let Ok(deadline_duration) = deadline_delta.to_std() else {
            ctx.worker_span.in_scope(|| {
                info!(execution_deadline = %ctx.locked_event.lock_expires_at, %started_at,
                    "Timed out - started_at later than execution_deadline");
            });
            return Err(WorkerError::TemporaryTimeout {
                http_client_traces: None,
                version: ctx.version.clone(),
            });
        };
        ctx.worker_span.record(
            "deadline_duration",
            tracing::field::debug(&deadline_duration),
        );
        Ok((store, deadline_duration))
    }

    async fn call_func_params(
        &self,
        ctx: &WorkerContext,
        store: &mut Store<ActivityCtx<C>>,
    ) -> Result<CallFuncParams, WorkerError> {
        let instance = match self.instance_pre.instantiate_async(&mut *store).await {
            Ok(instance) => instance,
            Err(err) => {
                let reason = err.to_string();
                if reason.starts_with("maximum concurrent") {
                    return Err(WorkerError::LimitReached {
                        reason,
                        version: ctx.version.clone(),
                    });
                }
                return Err(WorkerError::FatalError(
                    FatalError::CannotInstantiate {
                        reason: format!("{err}"),
                        detail: format!("{err:?}"),
                    },
                    ctx.version.clone(),
                ));
            }
        };
        let func = {
            let fn_export_index = self
                .exported_ffqn_to_index
                .get(&ctx.ffqn)
                .expect("executor only calls `run` with ffqns that are exported");
            instance
                .get_func(&mut *store, fn_export_index)
                .expect("exported function must be found")
        };

        let component_func = func.ty(store);
        let params = match ctx.params.as_vals(component_func.params()) {
            Ok(params) => params,
            Err(err) => {
                return Err(WorkerError::FatalError(
                    FatalError::ParamsParsingError(err),
                    ctx.version.clone(),
                ));
            }
        };

        let result_types = component_func.results().collect::<Vec<_>>(); // TODO: investigate using the iterator directly.
        assert!(
            result_types.len() == 1,
            "multi-value and void results are not supported, must have been checked in function registry"
        );

        Ok(CallFuncParams {
            func,
            params,
            result_type: result_types
                .into_iter()
                .next()
                .expect("just checked that size == 1"),
        })
    }

    async fn call_func(
        &self,
        store: &mut Store<ActivityCtx<C>>,
        CallFuncParams {
            func,
            params,
            result_type,
        }: CallFuncParams,
    ) -> Result<Result<SupportedFunctionReturnValue, ResultParsingError>, wasmtime::Error> {
        let mut results = vec![Val::Bool(false)];
        let res = func
            .call_async(&mut *store, &params, &mut results)
            .await
            .map(|()| {
                (
                    results.into_iter().next().expect("results size is 1"),
                    result_type,
                )
            });
        match res {
            Ok((val, r#type)) => {
                let result = SupportedFunctionReturnValue::new(val, r#type);
                // post_return is only called if `call` succeeds, after the return value has been processed.
                if let Err(err) = func.post_return_async(store).await {
                    warn!("Error in `post_return_async - {err:?}");
                }
                Ok(result)
            }
            Err(err) => Err(err),
        }
    }

    fn process_res(
        &self,
        res: Result<Result<SupportedFunctionReturnValue, ResultParsingError>, wasmtime::Error>,
        ctx: &WorkerContext,
        activity_ctx: ActivityCtx<C>,
    ) -> WorkerResult {
        let http_client_traces = Some(
            activity_ctx
                .http_client_traces
                .into_iter()
                .map(|(req, mut resp)| HttpClientTrace {
                    req,
                    resp: resp.try_recv().ok(),
                })
                .collect_vec(),
        );
        match res {
            Ok(Ok(result)) => {
                if self.config.retry_on_err {
                    // Interpret any `SupportedFunctionResult::Fallible` Err variant as an retry request (TemporaryError)
                    if let SupportedFunctionReturnValue::Err { err: result_err } = &result {
                        if ctx.can_be_retried {
                            let detail = serde_json::to_string(result_err).expect(
                                "SupportedFunctionReturnValue should be serializable to JSON",
                            );
                            return WorkerResult::Err(WorkerError::ActivityReturnedError {
                                detail: Some(detail),
                                version: ctx.version.clone(),
                                http_client_traces,
                            });
                        }
                        // else: log and pass the retval as is to be stored.
                        ctx.worker_span.in_scope(|| {
                            info!("Execution returned `error()`, not able to retry");
                        });
                    }
                }
                WorkerResult::Ok(result, ctx.version.clone(), http_client_traces)
            }
            Ok(Err(result_parsing_err)) => WorkerResult::Err(WorkerError::FatalError(
                FatalError::ResultParsingError(result_parsing_err),
                ctx.version.clone(),
            )),
            Err(err) => WorkerResult::Err(
                if let Some(trap) = err
                    .source()
                    .and_then(|source| source.downcast_ref::<wasmtime::Trap>())
                {
                    if *trap == wasmtime::Trap::OutOfFuel {
                        WorkerError::ActivityTrap {
                            reason: format!(
                                "total fuel consumed: {}",
                                self.config
                                    .fuel
                                    .expect("must have been set as it was the reason of trap")
                            ),
                            detail: None,
                            trap_kind: TrapKind::OutOfFuel,
                            version: ctx.version.clone(),
                            http_client_traces,
                        }
                    } else {
                        WorkerError::ActivityTrap {
                            reason: trap.to_string(),
                            detail: Some(format!("{err:?}")),
                            trap_kind: TrapKind::Trap,
                            version: ctx.version.clone(),
                            http_client_traces,
                        }
                    }
                } else {
                    WorkerError::ActivityTrap {
                        reason: err.to_string(),
                        trap_kind: TrapKind::HostFunctionError,
                        detail: Some(format!("{err:?}")),
                        version: ctx.version.clone(),
                        http_client_traces,
                    }
                },
            ),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::engines::{EngineConfig, Engines};
    use assert_matches::assert_matches;
    use concepts::prefixed_ulid::RunId;
    use concepts::storage::DbPool;
    use concepts::time::TokioSleep;
    use concepts::{ComponentRetryConfig, ComponentType};
    use concepts::{
        ExecutionId, FunctionFqn, Params, SupportedFunctionReturnValue, prefixed_ulid::ExecutorId,
        storage::CreateRequest, storage::DbPoolCloseable,
    };
    use db_tests::Database;
    use executor::executor::{ExecConfig, ExecTask, LockingStrategy};
    use serde_json::json;
    use std::time::Duration;
    use test_utils::sim_clock::SimClock;
    use val_json::{
        type_wrapper::TypeWrapper,
        wast_val::{WastVal, WastValWithType},
    };

    pub const FIBO_ACTIVITY_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_activity_builder::exports::testing::fibo::fibo::FIBO,
    ); // func(n: u8) -> u64;
    pub const FIBO_10_INPUT: u8 = 10;
    pub const FIBO_10_OUTPUT: u64 = 55;

    fn activity_config(component_id: ComponentId) -> ActivityConfig {
        ActivityConfig {
            component_id,
            forward_stdout: None,
            forward_stderr: None,
            env_vars: Arc::from([]),
            retry_on_err: true,
            directories_config: None,
            fuel: None,
        }
    }

    pub(crate) fn compile_activity(wasm_path: &str) -> (RunnableComponent, ComponentId) {
        let engine = Engines::get_activity_engine_test(EngineConfig::on_demand_testing()).unwrap();
        compile_activity_with_engine(wasm_path, &engine, ComponentType::ActivityWasm)
    }

    pub(crate) fn compile_activity_stub(wasm_path: &str) -> (RunnableComponent, ComponentId) {
        let engine = Engines::get_activity_engine_test(EngineConfig::on_demand_testing()).unwrap();
        compile_activity_with_engine(wasm_path, &engine, ComponentType::ActivityStub)
    }

    pub(crate) fn compile_activity_with_engine(
        wasm_path: &str,
        engine: &Engine,
        component_type: ComponentType,
    ) -> (RunnableComponent, ComponentId) {
        let mut component_id = ComponentId::dummy_activity();
        component_id.component_type = component_type;
        (
            RunnableComponent::new(wasm_path, engine, component_type).unwrap(),
            component_id,
        )
    }

    pub(crate) fn new_activity_worker(
        wasm_path: &str,
        engine: Arc<Engine>,
        clock_fn: impl ClockFn + 'static,
        sleep: impl Sleep + 'static,
    ) -> (Arc<dyn Worker>, ComponentId) {
        new_activity_worker_with_config(wasm_path, engine, clock_fn, sleep, activity_config)
    }

    fn new_activity_worker_with_config(
        wasm_path: &str,
        engine: Arc<Engine>,
        clock_fn: impl ClockFn + 'static,
        sleep: impl Sleep + 'static,
        config_fn: impl FnOnce(ComponentId) -> ActivityConfig,
    ) -> (Arc<dyn Worker>, ComponentId) {
        let cancel_registry = CancelRegistry::new();
        let (wasm_component, component_id) =
            compile_activity_with_engine(wasm_path, &engine, ComponentType::ActivityWasm);
        let (db_forwarder_sender, _) = mpsc::channel(1);
        (
            Arc::new(
                ActivityWorkerCompiled::new_with_config(
                    wasm_component,
                    config_fn(component_id.clone()),
                    engine,
                    clock_fn,
                    sleep,
                )
                .unwrap()
                .into_worker(cancel_registry, &db_forwarder_sender),
            ),
            component_id,
        )
    }

    pub(crate) fn new_activity<C: ClockFn + 'static>(
        db_pool: Arc<dyn DbPool>,
        wasm_path: &'static str,
        clock_fn: C,
        sleep: impl Sleep + 'static,
        retry_config: ComponentRetryConfig,
    ) -> ExecTask<C> {
        new_activity_with_config(
            db_pool,
            wasm_path,
            clock_fn,
            sleep,
            activity_config,
            retry_config,
        )
    }

    pub(crate) fn new_activity_with_config<C: ClockFn + 'static>(
        db_pool: Arc<dyn DbPool>,
        wasm_path: &'static str,
        clock_fn: C,
        sleep: impl Sleep + 'static,
        config_fn: impl FnOnce(ComponentId) -> ActivityConfig,
        retry_config: ComponentRetryConfig,
    ) -> ExecTask<C> {
        let engine = Engines::get_activity_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (worker, component_id) =
            new_activity_worker_with_config(wasm_path, engine, clock_fn.clone(), sleep, config_fn);
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            component_id,
            task_limiter: None,
            executor_id: ExecutorId::generate(),
            retry_config,
            locking_strategy: LockingStrategy::default(),
        };
        ExecTask::new_all_ffqns_test(worker, exec_config, clock_fn, db_pool)
    }

    pub(crate) fn new_activity_fibo<C: ClockFn + 'static>(
        db_pool: Arc<dyn DbPool>,
        clock_fn: C,
        sleep: impl Sleep + 'static,
    ) -> ExecTask<C> {
        new_activity(
            db_pool,
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
            clock_fn,
            sleep,
            ComponentRetryConfig::ZERO,
        )
    }

    #[tokio::test]
    async fn fibo_once() {
        test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_close) = Database::Memory.set_up().await;
        let db_connection = db_pool.connection().await.unwrap();
        let exec = new_activity_fibo(db_pool.clone(), sim_clock.clone(), TokioSleep);
        // Create an execution.
        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let params = Params::from_json_values_test(vec![json!(FIBO_10_INPUT)]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: FIBO_ACTIVITY_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: ComponentId::dummy_activity(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        // tick
        let executed = exec
            .tick_test_await(sim_clock.now(), RunId::generate())
            .await;
        assert_eq!(vec![execution_id.clone()], executed);
        // Check the result.
        let res = db_connection
            .wait_for_finished_result(&execution_id, None)
            .await
            .unwrap();
        let res = assert_matches!(res, SupportedFunctionReturnValue::Ok{ok} => ok);
        let fibo = assert_matches!(res,
            Some(WastValWithType {value: WastVal::U64(val), r#type: TypeWrapper::U64 }) => val);
        assert_eq!(FIBO_10_OUTPUT, fibo);
        drop(db_connection);
        db_close.close().await;
    }

    pub mod wasmtime_nosim {
        use super::*;
        use crate::engines::PoolingOptions;
        use concepts::storage::http_client_trace::{RequestTrace, ResponseTrace};
        use concepts::storage::{Locked, LockedBy, PendingState};
        use concepts::time::Now;
        use concepts::{
            ComponentRetryConfig, ExecutionFailureKind, FinishedExecutionError,
            SUPPORTED_RETURN_VALUE_OK_EMPTY,
        };
        use concepts::{
            prefixed_ulid::RunId,
            storage::{ExecutionRequest, Version},
        };
        use executor::executor::LockingStrategy;
        use insta::assert_debug_snapshot;
        use test_utils::{env_or_default, sim_clock::SimClock};
        use tracing::{debug, info, info_span};

        const EPOCH_MILLIS: u64 = 10;

        pub const SLEEP_LOOP_ACTIVITY_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_sleep_activity_builder::exports::testing::sleep::sleep::SLEEP_LOOP,
        ); // sleep-loop: func(millis: u64, iterations: u32);
        pub const HTTP_GET_SUCCESSFUL_ACTIVITY :FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_http_get_activity_builder::exports::testing::http::http_get::GET_SUCCESSFUL,
        );

        #[tokio::test]
        async fn limit_reached() {
            const FIBO_INPUT: u8 = 10;
            const LOCK_EXPIRY_MILLIS: u64 = 1100;
            const TASKS: u32 = 10;
            const MAX_INSTANCES: u32 = 1;

            test_utils::set_up();
            let fibo_input = env_or_default("FIBO_INPUT", FIBO_INPUT);
            let lock_expiry =
                Duration::from_millis(env_or_default("LOCK_EXPIRY_MILLIS", LOCK_EXPIRY_MILLIS));
            let tasks = env_or_default("TASKS", TASKS);
            let max_instances = env_or_default("MAX_INSTANCES", MAX_INSTANCES);

            let pool_opts = PoolingOptions {
                pooling_total_component_instances: Some(max_instances),
                pooling_total_stacks: Some(max_instances),
                pooling_total_core_instances: Some(max_instances),
                pooling_total_memories: Some(max_instances),
                pooling_total_tables: Some(max_instances),
                ..Default::default()
            };

            let engine =
                Engines::get_activity_engine_test(EngineConfig::pooling_nocache_testing(pool_opts))
                    .unwrap();

            let (fibo_worker, _) = new_activity_worker(
                test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                engine,
                Now,
                TokioSleep,
            );
            // create executions
            let join_handles = (0..tasks)
                .map(|_| {
                    let fibo_worker = fibo_worker.clone();
                    let execution_id = ExecutionId::generate();
                    let ctx = WorkerContext {
                        execution_id: execution_id.clone(),
                        metadata: concepts::ExecutionMetadata::empty(),
                        ffqn: FIBO_ACTIVITY_FFQN,
                        params: Params::from_json_values_test(vec![json!(fibo_input)]),
                        event_history: Vec::new(),
                        responses: Vec::new(),
                        version: Version::new(0),
                        can_be_retried: false,
                        worker_span: info_span!("worker-test"),
                        locked_event: Locked {
                            component_id: ComponentId::dummy_activity(),
                            executor_id: ExecutorId::generate(),
                            run_id: RunId::generate(),
                            lock_expires_at: Now.now() + lock_expiry,
                            retry_config: ComponentRetryConfig::ZERO,
                        },
                    };
                    tokio::spawn(async move { fibo_worker.run(ctx).await })
                })
                .collect::<Vec<_>>();
            let mut limit_reached = 0;
            for jh in join_handles {
                if matches!(
                    jh.await.unwrap(),
                    WorkerResult::Err(WorkerError::LimitReached { .. })
                ) {
                    limit_reached += 1;
                }
            }
            assert!(limit_reached > 0, "Limit was not reached");
        }

        // TODO: Make deterministic
        #[rstest::rstest]
        #[case(
            10,
            100,
            SupportedFunctionReturnValue::ExecutionError(FinishedExecutionError{
                kind: ExecutionFailureKind::TimedOut,
                reason: None, detail: None
            })
        )] // 1s -> timeout
        #[case(10, 10, SUPPORTED_RETURN_VALUE_OK_EMPTY)] // 0.1s -> Ok
        #[case(
            1500,
            1,
            SupportedFunctionReturnValue::ExecutionError(FinishedExecutionError{
                kind: ExecutionFailureKind::TimedOut,
                reason: None, detail: None
            })
        )] // 1s -> timeout
        #[tokio::test]
        async fn flaky_sleep_should_produce_temporary_timeout(
            #[case] sleep_millis: u32,
            #[case] sleep_iterations: u32,
            #[case] expected: concepts::SupportedFunctionReturnValue,
        ) {
            const LOCK_EXPIRY: Duration = Duration::from_millis(500);
            const TICK_SLEEP: Duration = Duration::from_millis(10);
            test_utils::set_up();
            let (_guard, db_pool, db_close) = Database::Memory.set_up().await;
            let timers_watcher_task = executor::expired_timers_watcher::spawn_new(
                db_pool.clone(),
                executor::expired_timers_watcher::TimersWatcherConfig {
                    tick_sleep: TICK_SLEEP,
                    clock_fn: Now,
                    leeway: Duration::ZERO,
                },
            );
            let engine =
                Engines::get_activity_engine_test(EngineConfig::on_demand_testing()).unwrap();
            let _epoch_ticker = crate::epoch_ticker::EpochTicker::spawn_new(
                vec![engine.weak()],
                Duration::from_millis(EPOCH_MILLIS),
            );

            let (worker, _) = new_activity_worker(
                test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                engine,
                Now,
                TokioSleep,
            );

            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: LOCK_EXPIRY,
                tick_sleep: TICK_SLEEP,
                component_id: ComponentId::dummy_activity(),
                task_limiter: None,
                executor_id: ExecutorId::generate(),
                retry_config: ComponentRetryConfig::ZERO,
                locking_strategy: LockingStrategy::default(),
            };
            let exec_task =
                ExecTask::spawn_new(worker, exec_config, Now, db_pool.clone(), TokioSleep);

            // Create an execution.
            let execution_id = ExecutionId::generate();
            info!("Testing {execution_id}");
            let created_at = Now.now();
            let db_connection = db_pool.connection().await.unwrap();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id: execution_id.clone(),
                    ffqn: SLEEP_LOOP_ACTIVITY_FFQN,
                    params: Params::from_json_values_test(vec![
                        json!(
                        {"milliseconds": sleep_millis}),
                        json!(sleep_iterations),
                    ]),
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    component_id: ComponentId::dummy_activity(),
                    scheduled_by: None,
                })
                .await
                .unwrap();
            // Check the result.
            assert_eq!(
                expected,
                assert_matches!(
                    db_connection
                        .wait_for_finished_result(&execution_id, None)
                        .await
                        .unwrap(),
                    actual => actual
                )
            );

            drop(timers_watcher_task);
            exec_task.close().await;
            db_close.close().await;
        }

        #[rstest::rstest]
        #[case(1, 2_000)] // 1ms * 2000 iterations
        #[case(2_000, 1)] // 2s * 1 iteration
        #[tokio::test]
        async fn long_running_execution_should_timeout(
            #[case] sleep_millis: u64,
            #[case] sleep_iterations: u32,
        ) {
            const TIMEOUT: Duration = Duration::from_millis(200);
            test_utils::set_up();

            let engine =
                Engines::get_activity_engine_test(EngineConfig::on_demand_testing()).unwrap();
            let _epoch_ticker = crate::epoch_ticker::EpochTicker::spawn_new(
                vec![engine.weak()],
                Duration::from_millis(EPOCH_MILLIS),
            );
            let sim_clock = SimClock::epoch();
            let (worker, _) = new_activity_worker(
                test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                engine,
                sim_clock.clone(),
                TokioSleep,
            );

            let executed_at = sim_clock.now();
            let version = Version::new(10);
            let ctx = WorkerContext {
                execution_id: ExecutionId::generate(),
                metadata: concepts::ExecutionMetadata::empty(),
                ffqn: SLEEP_LOOP_ACTIVITY_FFQN,
                params: Params::from_json_values_test(vec![
                    json!(
                    {"milliseconds": sleep_millis}),
                    json!(sleep_iterations),
                ]),
                event_history: Vec::new(),
                responses: Vec::new(),
                version: version.clone(),
                can_be_retried: false,
                worker_span: info_span!("worker-test"),
                locked_event: Locked {
                    component_id: ComponentId::dummy_activity(),
                    executor_id: ExecutorId::generate(),
                    run_id: RunId::generate(),
                    lock_expires_at: executed_at + TIMEOUT,
                    retry_config: ComponentRetryConfig::ZERO,
                },
            };
            let WorkerResult::Err(err) = worker.run(ctx).await else {
                panic!()
            };
            let (http_client_traces, actual_version) = assert_matches!(
                err,
                WorkerError::TemporaryTimeout {
                    http_client_traces,
                    version
                }
                => (http_client_traces, version)
            );
            assert_eq!(http_client_traces, Some(Vec::new()));
            assert_eq!(version, actual_version);
        }

        #[tokio::test]
        async fn execution_deadline_before_now_should_timeout() {
            test_utils::set_up();

            let engine =
                Engines::get_activity_engine_test(EngineConfig::on_demand_testing()).unwrap();
            let sim_clock = SimClock::epoch();
            let (worker, _) = new_activity_worker(
                test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                engine,
                sim_clock.clone(),
                TokioSleep,
            );
            // simulate a scheduling problem where deadline < now
            let execution_deadline = sim_clock.now();
            sim_clock.move_time_forward(Duration::from_millis(100));
            let version = Version::new(10);
            let ctx = WorkerContext {
                execution_id: ExecutionId::generate(),
                metadata: concepts::ExecutionMetadata::empty(),
                ffqn: SLEEP_LOOP_ACTIVITY_FFQN,
                params: Params::from_json_values_test(vec![
                    json!(
                    {"milliseconds": 1}),
                    json!(1),
                ]),
                event_history: Vec::new(),
                responses: Vec::new(),
                version: version.clone(),
                can_be_retried: false,
                worker_span: info_span!("worker-test"),
                locked_event: Locked {
                    component_id: ComponentId::dummy_activity(),
                    executor_id: ExecutorId::generate(),
                    run_id: RunId::generate(),
                    lock_expires_at: execution_deadline,
                    retry_config: ComponentRetryConfig::ZERO,
                },
            };
            let WorkerResult::Err(err) = worker.run(ctx).await else {
                panic!()
            };
            let actual_version = assert_matches!(
                err,
                WorkerError::TemporaryTimeout {
                    http_client_traces: None,
                    version: actual_version,
                }
                => actual_version
            );
            assert_eq!(version, actual_version);
        }

        #[tokio::test]
        async fn http_get_simple() {
            use std::ops::Deref;
            use wiremock::{
                Mock, MockServer, ResponseTemplate,
                matchers::{method, path},
            };
            const BODY: &str = "ok";
            test_utils::set_up();
            info!("All set up");
            let sim_clock = SimClock::default();
            let (_guard, db_pool, db_close) = Database::Memory.set_up().await;
            let engine =
                Engines::get_activity_engine_test(EngineConfig::on_demand_testing()).unwrap();
            let (worker, _) = new_activity_worker(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
                engine,
                sim_clock.clone(),
                TokioSleep,
            );
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: Duration::ZERO,
                component_id: ComponentId::dummy_activity(),
                task_limiter: None,
                executor_id: ExecutorId::generate(),
                retry_config: ComponentRetryConfig::ZERO,
                locking_strategy: LockingStrategy::default(),
            };
            let ffqns = Arc::from([HTTP_GET_SUCCESSFUL_ACTIVITY]);
            let exec_task = ExecTask::new_test(
                worker,
                exec_config,
                sim_clock.clone(),
                db_pool.clone(),
                ffqns,
            );

            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let server_address = listener
                .local_addr()
                .expect("Failed to get server address.");
            let uri = format!("http://127.0.0.1:{port}/", port = server_address.port());
            let params = Params::from_json_values_test(vec![json!(uri.clone())]);
            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            let db_connection = db_pool.connection_test().await.unwrap();
            info!("Creating execution");
            let stopwatch = std::time::Instant::now();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id: execution_id.clone(),
                    ffqn: HTTP_GET_SUCCESSFUL_ACTIVITY,
                    params,
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    component_id: ComponentId::dummy_activity(),
                    scheduled_by: None,
                })
                .await
                .unwrap();

            let server = MockServer::builder().listener(listener).start().await;
            Mock::given(method("GET"))
                .and(path("/"))
                .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
                .expect(1)
                .mount(&server)
                .await;

            assert_eq!(
                1,
                exec_task
                    .tick_test(sim_clock.now(), RunId::generate())
                    .await
                    .wait_for_tasks()
                    .await
                    .len()
            );
            let exec_log = db_connection.get(&execution_id).await.unwrap();
            let stopwatch = stopwatch.elapsed();
            info!("Finished in {stopwatch:?}");
            let (res, http_client_traces) = assert_matches!(
                exec_log.last_event().event.clone(),
                ExecutionRequest::Finished { result, http_client_traces: Some(http_client_traces) }
                => (result, http_client_traces));
            let wast_val_with_type = assert_matches!(res, SupportedFunctionReturnValue::Ok{ok: Some(wast_val_with_type)} => wast_val_with_type);
            let val = assert_matches!(wast_val_with_type.value, WastVal::String(val) => val);
            assert_eq!(BODY, val.deref());
            // check types
            assert_matches!(wast_val_with_type.r#type, TypeWrapper::String);
            assert_eq!(1, http_client_traces.len());
            let http_client_trace = http_client_traces.into_iter().next().unwrap();
            let (method, uri_actual) = assert_matches!(
                http_client_trace,
                HttpClientTrace {
                    req: RequestTrace {
                        method,
                        sent_at: _,
                        uri
                    },
                    resp: Some(ResponseTrace {
                        status: Ok(200),
                        finished_at: _
                    })
                }
                => (method, uri)
            );
            assert_eq!("GET", method);
            assert_eq!(uri, *uri_actual);
            drop(db_connection);
            drop(exec_task);
            db_close.close().await;
        }

        #[tokio::test]
        async fn http_get_activity_trap_should_be_turned_into_finished_execution_error_permanent_failure()
         {
            use wiremock::{
                Mock, MockServer, ResponseTemplate,
                matchers::{method, path},
            };
            const STATUS: u16 = 418; // I'm a teapot causes trap
            test_utils::set_up();
            info!("All set up");
            let sim_clock = SimClock::default();
            let (_guard, db_pool, db_close) = Database::Memory.set_up().await;
            let engine =
                Engines::get_activity_engine_test(EngineConfig::on_demand_testing()).unwrap();
            let (worker, _) = new_activity_worker(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
                engine,
                sim_clock.clone(),
                TokioSleep,
            );
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: Duration::ZERO,
                component_id: ComponentId::dummy_activity(),
                task_limiter: None,
                executor_id: ExecutorId::generate(),
                retry_config: ComponentRetryConfig::ZERO,
                locking_strategy: LockingStrategy::default(),
            };
            let ffqns = Arc::from([HTTP_GET_SUCCESSFUL_ACTIVITY]);
            let exec_task = ExecTask::new_test(
                worker,
                exec_config,
                sim_clock.clone(),
                db_pool.clone(),
                ffqns,
            );

            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let server_address = listener
                .local_addr()
                .expect("Failed to get server address.");
            let uri = format!("http://127.0.0.1:{port}/", port = server_address.port());
            let params = Params::from_json_values_test(vec![json!(uri.clone())]);
            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            let db_connection = db_pool.connection_test().await.unwrap();
            info!("Creating execution");
            let stopwatch = std::time::Instant::now();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id: execution_id.clone(),
                    ffqn: HTTP_GET_SUCCESSFUL_ACTIVITY,
                    params,
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    component_id: ComponentId::dummy_activity(),
                    scheduled_by: None,
                })
                .await
                .unwrap();

            let server = MockServer::builder().listener(listener).start().await;
            Mock::given(method("GET"))
                .and(path("/"))
                .respond_with(ResponseTemplate::new(STATUS).set_body_string(""))
                .expect(1)
                .mount(&server)
                .await;

            assert_eq!(
                1,
                exec_task
                    .tick_test(sim_clock.now(), RunId::generate())
                    .await
                    .wait_for_tasks()
                    .await
                    .len()
            );
            let exec_log = db_connection.get(&execution_id).await.unwrap();
            let stopwatch = stopwatch.elapsed();
            info!("Finished in {stopwatch:?}");
            let (res, http_client_traces) = assert_matches!(
                exec_log.last_event().event.clone(),
                ExecutionRequest::Finished { result, http_client_traces: Some(http_client_traces) }
                => (result, http_client_traces));
            let res =
                assert_matches!(res, SupportedFunctionReturnValue::ExecutionError(err) => err);
            let reason = assert_matches!(
                res,
                FinishedExecutionError {
                    kind: ExecutionFailureKind::Uncategorized,
                    reason: Some(reason), // activity trap
                    detail: _
                } => reason
            );
            assert!(reason.starts_with("activity trap"), "{reason}");

            assert_eq!(1, http_client_traces.len());
            let http_client_trace = http_client_traces.into_iter().next().unwrap();
            let (method, uri_actual) = assert_matches!(
                http_client_trace,
                HttpClientTrace {
                    req: RequestTrace {
                        method,
                        sent_at: _,
                        uri
                    },
                    resp: Some(ResponseTrace {
                        status: Ok(STATUS),
                        finished_at: _
                    })
                }
                => (method, uri)
            );
            assert_eq!("GET", method);
            assert_eq!(uri, *uri_actual);
            drop(db_connection);
            drop(exec_task);
            db_close.close().await;
        }

        #[rstest::rstest(
            succeed_eventually => [false, true],
        )]
        #[tokio::test]
        async fn http_get_retry_on_fallible_err(succeed_eventually: bool) {
            use std::ops::Deref;
            use wiremock::{
                Mock, MockServer, ResponseTemplate,
                matchers::{method, path},
            };
            const BODY: &str = "ok";
            const RETRY_EXP_BACKOFF: Duration = Duration::from_millis(10);
            test_utils::set_up();
            let sim_clock = SimClock::default();
            let (_guard, db_pool, db_close) = Database::Memory.set_up().await;
            let engine =
                Engines::get_activity_engine_test(EngineConfig::on_demand_testing()).unwrap();
            let (worker, _) = new_activity_worker(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
                engine,
                sim_clock.clone(),
                TokioSleep,
            );
            let retry_config = ComponentRetryConfig {
                max_retries: Some(1),
                retry_exp_backoff: RETRY_EXP_BACKOFF,
            };
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: Duration::ZERO,
                component_id: ComponentId::dummy_activity(),
                task_limiter: None,
                executor_id: ExecutorId::generate(),
                retry_config,
                locking_strategy: LockingStrategy::default(),
            };
            let ffqns = Arc::from([HTTP_GET_SUCCESSFUL_ACTIVITY]);
            let exec_task = ExecTask::new_test(
                worker,
                exec_config,
                sim_clock.clone(),
                db_pool.clone(),
                ffqns,
            );

            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let server_address = listener
                .local_addr()
                .expect("Failed to get server address.");
            let uri = format!("http://127.0.0.1:{port}/", port = server_address.port());
            let params = Params::from_json_values_test(vec![json!(uri.clone())]);
            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            let db_connection = db_pool.connection_test().await.unwrap();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id: execution_id.clone(),
                    ffqn: HTTP_GET_SUCCESSFUL_ACTIVITY,
                    params,
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    component_id: ComponentId::dummy_activity(),
                    scheduled_by: None,
                })
                .await
                .unwrap();

            let server = MockServer::builder().listener(listener).start().await;
            Mock::given(method("GET"))
                .and(path("/"))
                .respond_with(ResponseTemplate::new(500).set_body_string(BODY))
                .expect(1)
                .mount(&server)
                .await;
            debug!("started mock server on {}", server.address());

            {
                // Expect error result to be interpreted as an temporary failure
                assert_eq!(
                    1,
                    exec_task
                        .tick_test(sim_clock.now(), RunId::generate())
                        .await
                        .wait_for_tasks()
                        .await
                        .len()
                );
                let exec_log = db_connection.get(&execution_id).await.unwrap();

                let (reason, detail, found_expires_at, http_client_traces) = assert_matches!(
                    &exec_log.last_event().event,
                    ExecutionRequest::TemporarilyFailed {
                        backoff_expires_at,
                        reason,
                        detail: Some(detail),
                        http_client_traces: Some(http_client_traces)
                    }
                    => (reason, detail, *backoff_expires_at, http_client_traces)
                );
                assert_eq!(sim_clock.now() + RETRY_EXP_BACKOFF, found_expires_at);
                assert_eq!("activity returned error", reason.deref());
                assert!(
                    detail.contains("wrong status code: 500"),
                    "Unexpected {detail}"
                );

                assert_eq!(1, http_client_traces.len());
                let http_client_trace = http_client_traces.iter().next().unwrap();
                let (method, uri_actual) = assert_matches!(
                    http_client_trace,
                    HttpClientTrace {
                        req: RequestTrace {
                            method,
                            sent_at: _,
                            uri
                        },
                        resp: Some(ResponseTrace {
                            status: Ok(500),
                            finished_at: _
                        })
                    }
                    => (method, uri)
                );
                assert_eq!("GET", method);
                assert_eq!(uri, *uri_actual);
                server.verify().await;
            }
            // Noop until the timeout expires
            assert_eq!(
                0,
                exec_task
                    .tick_test(sim_clock.now(), RunId::generate())
                    .await
                    .wait_for_tasks()
                    .await
                    .len()
            );
            sim_clock.move_time_forward(RETRY_EXP_BACKOFF);
            server.reset().await;
            if succeed_eventually {
                // Reconfigure the server
                Mock::given(method("GET"))
                    .and(path("/"))
                    .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
                    .expect(1)
                    .mount(&server)
                    .await;
                debug!("Reconfigured the server");
            } // otherwise return 404

            assert_eq!(
                1,
                exec_task
                    .tick_test(sim_clock.now(), RunId::generate())
                    .await
                    .wait_for_tasks()
                    .await
                    .len()
            );
            let exec_log = db_connection.get(&execution_id).await.unwrap();
            let res = assert_matches!(exec_log.last_event().event.clone(), ExecutionRequest::Finished { result, .. } => result);
            let wast_val_with_type = if succeed_eventually {
                let wast_val_with_type = assert_matches!(res, SupportedFunctionReturnValue::Ok{ok: Some(wast_val_with_type)} => wast_val_with_type);
                let val = assert_matches!(&wast_val_with_type.value, WastVal::String(val) => val);
                assert_eq!(BODY, val.deref());
                wast_val_with_type
            } else {
                let wast_val_with_type = assert_matches!(res, SupportedFunctionReturnValue::Err{err: Some(wast_val_with_type)} => wast_val_with_type);
                let val = assert_matches!(&wast_val_with_type.value, WastVal::String(val) => val);
                assert_eq!("wrong status code: 404", val.deref());
                wast_val_with_type
            };
            // check types
            assert_matches!(wast_val_with_type.r#type, TypeWrapper::String); // in both cases
            drop(db_connection);
            drop(exec_task);
            db_close.close().await;
        }

        #[tokio::test]
        async fn preopened_dir_sanity() {
            test_utils::set_up();
            let sim_clock = SimClock::default();
            let (_guard, db_pool, db_close) = Database::Memory.set_up().await;
            let db_connection = db_pool.connection().await.unwrap();
            let parent_preopen_tempdir = tempfile::tempdir().unwrap();
            let parent_preopen_dir = Arc::from(parent_preopen_tempdir.path());
            let retry_config = ComponentRetryConfig {
                max_retries: Some(1), // should fail in first try
                retry_exp_backoff: Duration::ZERO,
            };
            let exec = new_activity_with_config(
                db_pool.clone(),
                test_programs_dir_activity_builder::TEST_PROGRAMS_DIR_ACTIVITY,
                sim_clock.clone(),
                TokioSleep,
                move |component_id| ActivityConfig {
                    component_id,
                    forward_stdout: None,
                    forward_stderr: None,
                    env_vars: Arc::default(),
                    retry_on_err: true, // needed
                    directories_config: Some(ActivityDirectoriesConfig {
                        parent_preopen_dir,
                        reuse_on_retry: true, // relies on continuing in the same folder
                        process_provider: None,
                    }),
                    fuel: None,
                },
                retry_config,
            );
            // Create an execution.
            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id: execution_id.clone(),
                    ffqn: FunctionFqn::new_static_tuple(
                        test_programs_dir_activity_builder::exports::testing::dir::dir::IO,
                    ),
                    params: Params::empty(),
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    component_id: ComponentId::dummy_activity(),
                    scheduled_by: None,
                })
                .await
                .unwrap();
            let run_id = RunId::generate();
            let executed = exec.tick_test_await(sim_clock.now(), run_id).await;
            assert_eq!(vec![execution_id.clone()], executed);
            // First execution should have failed
            let pending_state = db_connection
                .get_pending_state(&execution_id)
                .await
                .unwrap()
                .pending_state;
            let (scheduled_at, found_run_id) = assert_matches!(pending_state,
                PendingState::PendingAt {
                    scheduled_at,
                    last_lock: Some(LockedBy { executor_id: _, run_id }),
                }
            => (scheduled_at, run_id));
            // retry_exp_backoff is 0
            assert_eq!(sim_clock.now(), scheduled_at);
            assert_eq!(run_id, found_run_id);

            let executed = exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await;
            assert_eq!(vec![execution_id.clone()], executed);

            // Check the result.
            let res = db_connection
                .wait_for_finished_result(&execution_id, None)
                .await
                .unwrap();
            assert_matches!(res, SupportedFunctionReturnValue::Ok { .. });

            db_close.close().await;
        }

        #[rstest::rstest(
            ffqn => [FunctionFqn::new_static_tuple(
                    test_programs_process_activity_builder::exports::testing::process::process::TOUCH,
                ), FunctionFqn::new_static_tuple(
                    test_programs_process_activity_builder::exports::testing::process::process::KILL,
                ),
                FunctionFqn::new_static_tuple(
                    test_programs_process_activity_builder::exports::testing::process::process::STDIO,
                )],
        )]
        #[tokio::test]
        async fn process_sanity(ffqn: FunctionFqn) {
            test_utils::set_up();
            let sim_clock = SimClock::default();
            let (_guard, db_pool, db_close) = Database::Memory.set_up().await;
            let db_connection = db_pool.connection().await.unwrap();
            let parent_preopen_tempdir = tempfile::tempdir().unwrap();
            let parent_preopen_dir = Arc::from(parent_preopen_tempdir.path());
            let exec = new_activity_with_config(
                db_pool.clone(),
                test_programs_process_activity_builder::TEST_PROGRAMS_PROCESS_ACTIVITY,
                sim_clock.clone(),
                TokioSleep,
                move |component_id| ActivityConfig {
                    component_id,
                    forward_stdout: Some(StdOutputConfig::Stderr),
                    forward_stderr: Some(StdOutputConfig::Stderr),
                    env_vars: Arc::default(),
                    retry_on_err: true,
                    directories_config: Some(ActivityDirectoriesConfig {
                        parent_preopen_dir,
                        reuse_on_retry: false,
                        process_provider: Some(ProcessProvider::Native),
                    }),
                    fuel: None,
                },
                ComponentRetryConfig::ZERO,
            );
            // Create an execution.
            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id: execution_id.clone(),
                    ffqn,
                    params: Params::empty(),
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    component_id: ComponentId::dummy_activity(),
                    scheduled_by: None,
                })
                .await
                .unwrap();
            let executed = exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await;
            assert_eq!(vec![execution_id.clone()], executed);
            // Check the result.
            let res = db_connection
                .wait_for_finished_result(&execution_id, None)
                .await
                .unwrap();
            assert_matches!(res, SupportedFunctionReturnValue::Ok { .. });
            db_close.close().await;
        }

        #[cfg(unix)]
        async fn is_process_running(pid: u32) -> bool {
            let output = tokio::process::Command::new("ps")
                .arg("a")
                .output()
                .await
                .unwrap();
            let stdout = String::from_utf8(output.stdout).unwrap();
            stdout.lines().any(|line| {
                line.split_whitespace()
                    .next()
                    .is_some_and(|field| field == pid.to_string())
            })
        }

        #[cfg(unix)]
        #[tokio::test]
        async fn process_group_cleanup() {
            test_utils::set_up();
            let sim_clock = SimClock::default();
            let (_guard, db_pool, db_close) = Database::Memory.set_up().await;
            let db_connection = db_pool.connection().await.unwrap();
            let parent_preopen_tempdir = tempfile::tempdir().unwrap();
            let parent_preopen_dir = Arc::from(parent_preopen_tempdir.path());
            let exec = new_activity_with_config(
                db_pool.clone(),
                test_programs_process_activity_builder::TEST_PROGRAMS_PROCESS_ACTIVITY,
                sim_clock.clone(),
                TokioSleep,
                move |component_id| ActivityConfig {
                    component_id,
                    forward_stdout: None,
                    forward_stderr: None,
                    env_vars: Arc::from([EnvVar {
                        key: "PATH".to_string(),
                        val: std::env::var("PATH").unwrap(),
                    }]),
                    retry_on_err: true,
                    directories_config: Some(ActivityDirectoriesConfig {
                        parent_preopen_dir,
                        reuse_on_retry: false,
                        process_provider: Some(ProcessProvider::Native),
                    }),
                    fuel: None,
                },
                ComponentRetryConfig::ZERO,
            );
            // Create an execution.
            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id: execution_id.clone(),
                    ffqn: FunctionFqn::new_static_tuple(
                        test_programs_process_activity_builder::exports::testing::process::process::EXEC_SLEEP,
                    ),
                    params: Params::empty(),
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    component_id: ComponentId::dummy_activity(),
                    scheduled_by: None,
                })
                .await
                .unwrap();
            let executed = exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await;
            assert_eq!(vec![execution_id.clone()], executed);
            // Check the result.
            let res = db_connection
                .wait_for_finished_result(&execution_id, None)
                .await
                .unwrap();
            let sleep_pid = assert_matches!(res,
                SupportedFunctionReturnValue::Ok{ok: Some(WastValWithType {value,
                    r#type: _})} => value);
            let sleep_pid = assert_matches!(sleep_pid, WastVal::U32(val) => val);
            debug!("Sleep pid: {sleep_pid}");

            // Test that the process was killed
            let mut attempt = 0;
            while is_process_running(sleep_pid).await {
                assert!(attempt < 5, "failed after 5 attemtps");
                attempt += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            db_close.close().await;
        }

        #[rstest::rstest(
            param => [
                r#"{"image": "foo", "a": false, "b":false}"#,
                r#"{"b": false, "a":false, "image": "foo"}"#,
                ])]
        #[tokio::test]
        async fn record_field_ordering(param: &str) {
            test_utils::set_up();
            let sim_clock = SimClock::default();
            let (_guard, db_pool, db_close) = Database::Memory.set_up().await;
            let db_connection = db_pool.connection().await.unwrap();
            let exec = new_activity_with_config(
                db_pool.clone(),
                test_programs_serde_activity_builder::TEST_PROGRAMS_SERDE_ACTIVITY,
                sim_clock.clone(),
                TokioSleep,
                move |component_id| ActivityConfig {
                    component_id,
                    forward_stdout: Some(StdOutputConfig::Stderr),
                    forward_stderr: Some(StdOutputConfig::Stderr),
                    env_vars: Arc::default(),
                    retry_on_err: false,
                    directories_config: None,
                    fuel: None,
                },
                ComponentRetryConfig::ZERO,
            );
            // Create an execution.
            let ffqn = FunctionFqn::new_static_tuple(
                test_programs_serde_activity_builder::exports::testing::serde::serde::REC,
            );
            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id: execution_id.clone(),
                    ffqn,
                    params: Params::from_json_values_test(vec![
                        serde_json::from_str(param).unwrap(),
                    ]),
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    component_id: ComponentId::dummy_activity(),
                    scheduled_by: None,
                })
                .await
                .unwrap();
            let executed = exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await;
            assert_eq!(vec![execution_id.clone()], executed);
            // Check the result.
            let res = db_connection
                .wait_for_finished_result(&execution_id, None)
                .await
                .unwrap();
            let record =
                assert_matches!(res, SupportedFunctionReturnValue::Ok{ok: record} => record);
            assert_debug_snapshot!(record);
            db_close.close().await;
        }

        #[tokio::test]
        async fn variant_with_optional_none() {
            test_utils::set_up();
            let sim_clock = SimClock::default();
            let (_guard, db_pool, db_close) = Database::Memory.set_up().await;
            let db_connection = db_pool.connection().await.unwrap();
            let exec = new_activity_with_config(
                db_pool.clone(),
                test_programs_serde_activity_builder::TEST_PROGRAMS_SERDE_ACTIVITY,
                sim_clock.clone(),
                TokioSleep,
                move |component_id| ActivityConfig {
                    component_id,
                    forward_stdout: Some(StdOutputConfig::Stderr),
                    forward_stderr: Some(StdOutputConfig::Stderr),
                    env_vars: Arc::default(),
                    retry_on_err: false,
                    directories_config: None,
                    fuel: None,
                },
                ComponentRetryConfig::ZERO,
            );
            // Create an execution.
            let ffqn = FunctionFqn::new_static_tuple(
                test_programs_serde_activity_builder::exports::testing::serde::serde::VAR,
            );
            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id: execution_id.clone(),
                    ffqn,
                    params: Params::from_json_values_test(vec![json!({"var1":null})]),
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    component_id: ComponentId::dummy_activity(),
                    scheduled_by: None,
                })
                .await
                .unwrap();
            let executed = exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await;
            assert_eq!(vec![execution_id.clone()], executed);
            // Check the result.
            let res = db_connection
                .wait_for_finished_result(&execution_id, None)
                .await
                .unwrap();
            let variant =
                assert_matches!(res, SupportedFunctionReturnValue::Ok{ok: variant} => variant);
            assert_debug_snapshot!(variant);
            db_close.close().await;
        }
    }
}
