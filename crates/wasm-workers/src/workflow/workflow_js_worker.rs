//! JS workflow worker that wraps a `WorkflowWorker` running the Boa WASM component.
//!
//! The Boa component exports `obelisk-workflow:workflow-js-runtime/execute.run(fn-name, js-code, params-json) -> result<result<string, string>, js-runtime-error>`.
//! This wrapper translates the user's typed interface `func(params) -> result<T, E>`
//! into calls to the Boa component, deserializing the JSON-encoded ok string as the configured type.

use super::workflow_worker::{BacktraceCapture, WorkflowWorker, WorkflowWorkerCompiled};
use crate::activity::cancel_registry::CancelRegistry;
use crate::component_logger::LogStrageConfig;
use crate::workflow::deadline_tracker::DeadlineTrackerFactory;
use crate::workflow::replay_advance::{AdvanceError, ReplayAdvanceable, ReplayResponse};
use crate::workflow::replay_advance::{AdvanceResponse, ReplayError};
use crate::workflow::replay_db_proxy::InternalCapturedWrite;
use async_trait::async_trait;
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::http_client_trace::HttpClientTrace;
use concepts::storage::{BacktraceInfo, CapturedDbWrite, DbPool, Version};
use concepts::{
    ComponentType, ExecutionFailureKind, ExecutionId, FinishedExecutionFailure, FunctionFqn,
    FunctionMetadata, FunctionRegistry, IfcFqnName, PackageIfcFns, ParameterType, Params,
    ResultParsingError, ResultParsingErrorFromVal, ReturnTypeExtendable,
    SupportedFunctionReturnValue,
};
use executor::worker::{
    FatalError, RunFinished, Worker, WorkerContext, WorkerError, WorkerResult, WorkerResultOk,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tracing::{debug, info};
use utils::wasm_tools::WasmComponent;
use val_json::type_wrapper::{TypeKey, TypeWrapper, indexmap::IndexMap};
use val_json::wast_val::{WastVal, WastValWithType};

/// Compiled JS workflow. Holds the compiled Boa WASM component + JS source + user FFQN.
pub struct WorkflowJsWorkerCompiled {
    inner: WorkflowWorkerCompiled,
    js_entry_path: String,
    js_files: BTreeMap<String, String>,
    user_params: Vec<ParameterType>,
    user_return_type: ReturnTypeExtendable,
    /// User interface parsed from synthesized WIT — provides exports, extensions, and WIT text.
    user_wasm_component: WasmComponent,
}

impl WorkflowJsWorkerCompiled {
    pub fn new(
        inner: WorkflowWorkerCompiled,
        js_source: String,
        js_file_name: String,
        user_ffqn: &FunctionFqn,
        user_params: Vec<ParameterType>,
        user_return_type: ReturnTypeExtendable,
    ) -> Result<Self, utils::wasm_tools::DecodeError> {
        Self::new_graph(
            inner,
            js_file_name.clone(),
            BTreeMap::from([(js_file_name, js_source)]),
            user_ffqn,
            user_params,
            user_return_type,
        )
    }

    pub fn new_graph(
        inner: WorkflowWorkerCompiled,
        js_entry_path: String,
        js_files: BTreeMap<String, String>,
        user_ffqn: &FunctionFqn,
        user_params: Vec<ParameterType>,
        user_return_type: ReturnTypeExtendable,
    ) -> Result<Self, utils::wasm_tools::DecodeError> {
        let user_wasm_component = WasmComponent::new_from_fn_signature(
            user_ffqn,
            &user_params,
            &user_return_type,
            ComponentType::Workflow,
            "js-workflow",
        )?;
        Ok(Self::new_graph_with_wasm_component(
            inner,
            js_entry_path,
            js_files,
            user_params,
            user_return_type,
            user_wasm_component,
        ))
    }

    #[must_use]
    pub fn new_graph_with_wasm_component(
        inner: WorkflowWorkerCompiled,
        js_entry_path: String,
        js_files: BTreeMap<String, String>,
        user_params: Vec<ParameterType>,
        user_return_type: ReturnTypeExtendable,
        user_wasm_component: WasmComponent,
    ) -> Self {
        Self {
            inner,
            js_entry_path,
            js_files,
            user_params,
            user_return_type,
            user_wasm_component,
        }
    }

    #[must_use]
    pub fn exported_functions_ext(&self) -> &[FunctionMetadata] {
        self.user_wasm_component.exported_functions(true)
    }

    #[must_use]
    pub fn exports_hierarchy_ext(&self) -> &[PackageIfcFns] {
        self.user_wasm_component.exports_hierarchy_ext()
    }

    #[must_use]
    pub fn imported_functions(&self) -> &[FunctionMetadata] {
        self.inner.imported_functions()
    }

    /// Return WIT text describing the user interface including extension packages.
    /// Returns `None` if WIT generation fails (should not happen for valid configs).
    #[must_use]
    pub fn wit(&self) -> String {
        self.user_wasm_component.wit()
    }

    pub fn link(
        self,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> Result<WorkflowJsWorkerLinked, crate::WasmFileError> {
        // Resolve JS imports against the function registry before linking.
        // This validates named imports and resolves namespace imports (`import *`).
        // Parse errors in JS source are caught here early rather than at runtime.
        let mut resolved_imports = HashMap::new();
        for source in self.js_files.values() {
            let imports = resolve_js_imports(source, fn_registry.as_ref())
                .map_err(|e| crate::WasmFileError::linking_error("JS import resolution", e))?;
            for (specifier, functions) in imports {
                resolved_imports.entry(specifier).or_insert(functions);
            }
        }

        let linked = self.inner.link(fn_registry)?;
        Ok(WorkflowJsWorkerLinked {
            inner: linked,
            js_entry_path: self.js_entry_path,
            js_files: self.js_files,
            user_params: self.user_params,
            user_return_type: self.user_return_type,
            user_exports_noext: self.user_wasm_component.exported_functions(false).to_vec(),
            resolved_imports,
        })
    }
}

pub struct WorkflowJsWorkerLinked {
    inner: super::workflow_worker::WorkflowWorkerLinked,
    js_entry_path: String,
    js_files: BTreeMap<String, String>,
    user_params: Vec<ParameterType>,
    user_return_type: ReturnTypeExtendable,
    user_exports_noext: Vec<FunctionMetadata>,
    /// Resolved imports: interface FQN → imported functions.
    resolved_imports: HashMap<IfcFqnName, Vec<NamedFnImport>>,
}

impl WorkflowJsWorkerLinked {
    pub fn into_worker(
        self,
        deployment_id: DeploymentId,
        db_pool: Arc<dyn DbPool>,
        deadline_factory: Arc<dyn DeadlineTrackerFactory>,
        cancel_registry: CancelRegistry,
        logs_storage_config: Option<LogStrageConfig>,
    ) -> WorkflowJsWorker {
        let inner = self.inner.into_worker(
            deployment_id,
            db_pool,
            deadline_factory,
            cancel_registry,
            logs_storage_config,
        );
        WorkflowJsWorker {
            inner,
            js_entry_path: self.js_entry_path,
            js_files: self.js_files,
            user_params: self.user_params,
            user_return_type: self.user_return_type,
            user_exports_noext: self.user_exports_noext,
            resolved_imports: self.resolved_imports,
        }
    }
}

pub struct WorkflowJsWorker {
    inner: WorkflowWorker,
    js_entry_path: String,
    js_files: BTreeMap<String, String>,
    user_params: Vec<ParameterType>,
    user_return_type: ReturnTypeExtendable,
    user_exports_noext: Vec<FunctionMetadata>,
    /// Resolved imports: interface FQN → imported functions.
    resolved_imports: HashMap<IfcFqnName, Vec<NamedFnImport>>,
}

use crate::js_imports::{NamedFnImport, resolve_js_imports};

impl WorkflowJsWorker {
    pub async fn capture_backtraces(
        &self,
        execution_id: ExecutionId,
    ) -> Result<Vec<BacktraceInfo>, ReplayError> {
        assert!(
            self.inner.deadline_factory.is_for_replay(),
            "capture_backtraces() requires DeadlineTrackerFactoryForReplay"
        );
        let db_conn = self
            .inner
            .db_pool
            .connection()
            .await
            .map_err(concepts::storage::DbErrorWrite::from)?;
        let log = db_conn
            .get(&execution_id)
            .await
            .map_err(concepts::storage::DbErrorWrite::from)?;
        let (ffqn, params) = Self::boa_invocation(
            log.params(),
            self.js_entry_path.clone(),
            &self.js_files,
            &self.resolved_imports,
            true,
        );
        let (writes, backtraces, _fatal_error, _db_conn) = self
            .inner
            .capture_replay_writes_from_log(
                execution_id,
                log,
                ffqn,
                params,
                db_conn,
                BacktraceCapture::Full,
            )
            .await?;
        Ok(WorkflowWorker::collect_write_backtraces(writes, backtraces))
    }

    /// See [`WorkflowWorker::persist_backtraces`].
    pub async fn persist_backtraces(
        &self,
        execution_id: ExecutionId,
    ) -> Result<usize, ReplayError> {
        let captured = self.capture_backtraces(execution_id.clone()).await?;
        let db_conn = self
            .inner
            .db_pool
            .connection()
            .await
            .map_err(concepts::storage::DbErrorWrite::from)?;
        let next_version = db_conn
            .get(&execution_id)
            .await
            .map_err(concepts::storage::DbErrorWrite::from)?
            .next_version;
        Ok(WorkflowWorker::trim_and_persist_backtraces(
            db_conn.as_ref(),
            &execution_id,
            &next_version,
            captured,
        )
        .await?)
    }

    fn boa_invocation(
        params: &Params,
        js_entry_path: String,
        js_files: &BTreeMap<String, String>,
        resolved_imports: &HashMap<IfcFqnName, Vec<NamedFnImport>>,
        backtrace_enabled: bool,
    ) -> (FunctionFqn, Params) {
        let json_params = params
            .as_json_values()
            .expect("params come from database, not wasmtime");
        let params_json_list: Vec<serde_json::Value> = json_params
            .iter()
            .map(|v| {
                serde_json::Value::String(
                    serde_json::to_string(v).expect("serde_json::Value must be serializable"),
                )
            })
            .collect();

        // Serialize resolved imports as list<resolved-interface-imports>, where
        // each entry is a record { ifc-fqn, functions: list<named-fn-import> }.
        let imports_json: Vec<serde_json::Value> = resolved_imports
            .iter()
            .map(|(ifc_fqn, funcs)| {
                let funcs_json: Vec<serde_json::Value> = funcs
                    .iter()
                    .map(|NamedFnImport { js_name, wit_name }| {
                        serde_json::json!({
                            "js_name": js_name,
                            "wit_name": wit_name,
                        })
                    })
                    .collect();
                serde_json::json!({
                    "ifc_fqn": ifc_fqn.to_string(),
                    "functions": funcs_json,
                })
            })
            .collect();

        let files_json = js_files
            .iter()
            .map(|(path, source)| {
                serde_json::Value::Array(vec![
                    serde_json::Value::String(path.clone()),
                    serde_json::Value::String(source.clone()),
                ])
            })
            .collect();

        let ffqn =
            FunctionFqn::new_static_tuple(("obelisk-workflow:workflow-js-runtime/execute", "run"));
        let boa_params: Arc<[serde_json::Value]> = Arc::from([
            serde_json::Value::String(js_entry_path),
            serde_json::Value::Array(files_json),
            serde_json::Value::Array(params_json_list),
            serde_json::Value::Bool(backtrace_enabled),
            serde_json::Value::Array(imports_json),
        ]);
        let named_fn_import_ty = TypeWrapper::Record(IndexMap::from([
            (TypeKey::new_kebab("js-name"), TypeWrapper::String),
            (TypeKey::new_kebab("wit-name"), TypeWrapper::String),
        ]));
        let resolved_interface_imports_ty = TypeWrapper::Record(IndexMap::from([
            (TypeKey::new_kebab("ifc-fqn"), TypeWrapper::String),
            (
                TypeKey::new_kebab("functions"),
                TypeWrapper::List(Box::new(named_fn_import_ty)),
            ),
        ]));
        let params = Params::from_json_values(
            boa_params,
            [
                &TypeWrapper::String,
                &TypeWrapper::List(Box::new(TypeWrapper::Tuple(Box::new([
                    TypeWrapper::String,
                    TypeWrapper::String,
                ])))),
                &TypeWrapper::List(Box::new(TypeWrapper::String)),
                &TypeWrapper::Bool,
                &TypeWrapper::List(Box::new(resolved_interface_imports_ty)),
            ]
            .into_iter(),
        )
        .expect("types checked at compile time");
        (ffqn, params)
    }
}

#[async_trait]
impl Worker for WorkflowJsWorker {
    fn exported_functions_noext(&self) -> &[FunctionMetadata] {
        &self.user_exports_noext
    }

    async fn run(&self, mut ctx: WorkerContext) -> WorkerResult {
        assert_eq!(
            self.user_params.len(),
            ctx.params
                .as_json_values()
                .expect("params come from database, not wasmtime")
                .len(),
            "type checked in Params::from_json_values"
        );
        (ctx.ffqn, ctx.params) = Self::boa_invocation(
            &ctx.params,
            self.js_entry_path.clone(),
            &self.js_files,
            &self.resolved_imports,
            false, // backtrace is disabled for regular run
        );

        let inner_worker_ok = self.inner.run(ctx).await?;
        debug!("Workflow worker returned {inner_worker_ok:?}");

        match inner_worker_ok {
            WorkerResultOk::DbUpdatedByWorkerOrWatcher => {
                Ok(WorkerResultOk::DbUpdatedByWorkerOrWatcher)
            }
            WorkerResultOk::RunFinished(RunFinished {
                retval,
                version,
                http_client_traces,
            }) => transform_to_outer_result(
                retval,
                version,
                http_client_traces,
                &self.user_return_type,
            )
            .map(WorkerResultOk::RunFinished)
            .map_err(|(err, version)| WorkerError::FatalError(err, version)),
        }
    }
}

/// Transform `result<result<string, string>, js-runtime-error>` returned by `workflow-js-runtime`
/// to user specified `user_return_type`.
fn transform_to_outer_result(
    retval: SupportedFunctionReturnValue,
    version: Version,
    http_client_traces: Option<Vec<HttpClientTrace>>,
    user_return_type: &ReturnTypeExtendable,
) -> Result<RunFinished, (FatalError, Version)> {
    match retval {
        SupportedFunctionReturnValue::Ok(Some(WastValWithType {
            r#type:
                TypeWrapper::Result {
                    ok: Some(ok_type),
                    err: Some(err_type),
                },
            value: WastVal::Result(Ok(Some(ok_val))),
        })) => {
            assert!(*ok_type == TypeWrapper::String && *err_type == TypeWrapper::String);
            let WastVal::String(ok_val) = *ok_val else {
                unreachable!("ok type is String, so value must be WastVal::String")
            };
            let Ok(ok_val) = serde_json::from_str(&ok_val) else {
                unreachable!("workflow-js-runtime always sends JSON-encoded string")
            };
            let retval = crate::js_worker_utils::map_ok_variant_fatal(
                Some(ok_val),
                user_return_type,
                version.clone(),
            )?;
            Ok(RunFinished {
                retval,
                version,
                http_client_traces,
            })
        }

        SupportedFunctionReturnValue::Ok(Some(WastValWithType {
            r#type:
                TypeWrapper::Result {
                    ok: Some(ok_type),
                    err: Some(err_type),
                },
            value: WastVal::Result(Err(Some(err_val))),
        })) => {
            assert!(*ok_type == TypeWrapper::String && *err_type == TypeWrapper::String);
            let WastVal::String(err_val) = *err_val else {
                unreachable!("err type is String, so value must be WastVal::String")
            };
            let Ok(err_val) = serde_json::from_str(&err_val) else {
                unreachable!("workflow-js-runtime always sends JSON-encoded string")
            };
            let retval = crate::js_worker_utils::map_err_variant_fatal(
                Some(err_val),
                user_return_type,
                version.clone(),
            )?;
            Ok(RunFinished {
                retval,
                version,
                http_client_traces,
            })
        }

        SupportedFunctionReturnValue::Err(Some(js_runtime_err)) => {
            // Map JsRuntimeError variants to appropriate WorkerError
            let WastVal::Variant(variant_name, payload) = &js_runtime_err.value else {
                unreachable!("expected Variant for js-runtime-error")
            };
            let name = variant_name.as_snake_str();
            match name {
                "wrong_return_type" | "wrong_thrown_type" => {
                    let reason = if let Some(payload) = payload
                        && let WastVal::String(s) = payload.as_ref()
                    {
                        s.clone()
                    } else {
                        unreachable!("both variants have string payload")
                    };

                    Err((
                        FatalError::ResultParsingError(
                            ResultParsingError::ResultParsingErrorFromVal(
                                ResultParsingErrorFromVal::TypeCheckError(reason),
                            ),
                        ),
                        version,
                    ))
                }
                "cannot_instantiate" | "unresolved_import" => {
                    let reason = if let Some(payload) = payload
                        && let WastVal::String(s) = payload.as_ref()
                    {
                        s.clone()
                    } else {
                        unreachable!("runtime error carries a string payload")
                    };
                    Err((
                        FatalError::CannotInstantiate {
                            reason,
                            detail: None,
                        },
                        version,
                    ))
                }
                "entry_not_found" => Err((
                    FatalError::CannotInstantiate {
                        reason: "JavaScript entry module was not found".to_string(),
                        detail: None,
                    },
                    version,
                )),
                "execution_failed" => {
                    // This variant is returned when a workflow function fails,
                    // e.g., when joinNext returns an error from a child execution.
                    // We propagate this as an ExecutionFailure.
                    Ok(RunFinished {
                        retval: SupportedFunctionReturnValue::ExecutionFailure(
                            FinishedExecutionFailure {
                                kind: ExecutionFailureKind::Uncategorized,
                                reason: Some("js-runtime execution-failed".to_string()),
                                detail: None,
                            },
                        ),
                        version,
                        http_client_traces,
                    })
                }
                _ => unreachable!("unexpected js-runtime-error variant: {name}"),
            }
        }

        retval @ SupportedFunctionReturnValue::ExecutionFailure(_) => Ok(RunFinished {
            retval,
            version,
            http_client_traces,
        }),

        other => unreachable!("unexpected SupportedFunctionReturnValue: {other:?}"),
    }
}

fn transform_to_append_finished(
    retval: SupportedFunctionReturnValue,
    version: &Version,
    user_return_type: &ReturnTypeExtendable,
) -> (SupportedFunctionReturnValue, Option<FatalError>) {
    let (retval, version_obtained, fatal_error) =
        match transform_to_outer_result(retval, version.clone(), None, user_return_type) {
            Ok(RunFinished {
                retval, version, ..
            }) => (retval, version, None),
            Err((fatal_error, version)) => {
                let retval = SupportedFunctionReturnValue::ExecutionFailure(
                    FinishedExecutionFailure::from(&fatal_error),
                );
                (retval, version, Some(fatal_error))
            }
        };
    assert_eq!(*version, version_obtained);
    (retval, fatal_error)
}

impl WorkflowJsWorker {
    /// Replay a JS workflow execution for debugging/verification.
    ///
    /// This function recreates the workflow execution from the database log,
    /// transforming the context to call the workflow-js-runtime just like the
    /// regular `run` method does.
    pub async fn replay(
        &self,
        execution_id: ExecutionId,
        backtrace_capture: BacktraceCapture,
    ) -> Result<ReplayResponse, ReplayError> {
        assert!(
            self.inner.deadline_factory.is_for_replay(),
            "replay() requires DeadlineTrackerFactoryForReplay"
        );
        let db_conn = self
            .inner
            .db_pool
            .connection()
            .await
            .map_err(concepts::storage::DbErrorWrite::from)?;
        let log = db_conn
            .get(&execution_id)
            .await
            .map_err(concepts::storage::DbErrorWrite::from)?;
        let already_finished_result = log.as_finished_result();
        let (ffqn, params) = Self::boa_invocation(
            log.params(),
            self.js_entry_path.clone(),
            &self.js_files,
            &self.resolved_imports,
            backtrace_capture != BacktraceCapture::Disabled,
        );

        let (captured_writes, _backtraces, mut fatal_error, _db_conn) = self
            .inner
            .capture_replay_writes_from_log(
                execution_id,
                log,
                ffqn,
                params,
                db_conn,
                backtrace_capture,
            )
            .await?;
        // Drop replay-only metadata, unwrapping user retval or fatal error.
        let captured_writes: Vec<_> = captured_writes
            .into_iter()
            .map(|internal_write| {
                let write = internal_write.write;
                match write {
                    CapturedDbWrite::AppendFinished {
                        execution_id,
                        version,
                        current_time,
                        retval, // workflow-js-runtime WASM result
                        parent,
                    } => {
                        let (retval, fatal_error_from_wit) =
                            transform_to_append_finished(retval, &version, &self.user_return_type);
                        if fatal_error_from_wit.is_some() {
                            // TODO: can both fatal errors be present?
                            fatal_error = fatal_error_from_wit;
                        }
                        CapturedDbWrite::AppendFinished {
                            execution_id,
                            version,
                            current_time,
                            retval,
                            parent,
                        }
                    }
                    _ => write,
                }
            })
            .collect();

        WorkflowWorker::transform_replay_to_response(
            captured_writes,
            fatal_error,
            already_finished_result,
        )
    }

    /// Advance a paused JS workflow by one interrupt boundary.
    pub async fn advance(
        &self,
        execution_id: ExecutionId,
        requested: ReplayAdvanceable,
        backtrace_capture: BacktraceCapture,
    ) -> Result<AdvanceResponse, AdvanceError> {
        assert!(
            self.inner.deadline_factory.is_for_replay(),
            "advance() requires DeadlineTrackerFactoryForReplay"
        );
        info!("Advance to requested {requested:?}");
        let db_conn = self
            .inner
            .db_pool
            .connection()
            .await
            .map_err(concepts::storage::DbErrorWrite::from)?;
        let log = db_conn
            .get(&execution_id)
            .await
            .map_err(concepts::storage::DbErrorWrite::from)?;
        if requested.captured_writes.is_empty() {
            return Err(AdvanceError::NoWrites);
        }
        if let Some(expected_version) = requested.starting_version()
            && log.next_version != *expected_version
        {
            return Err(AdvanceError::VersionMismatch {
                expected: log.next_version,
            });
        }

        let old_version = log.next_version.clone();
        let (ffqn, params) = Self::boa_invocation(
            log.params(),
            self.js_entry_path.clone(),
            &self.js_files,
            &self.resolved_imports,
            backtrace_capture != BacktraceCapture::Disabled,
        );
        let log_forwarder_sender = self
            .inner
            .logs_storage_config
            .as_ref()
            .map(|config| &config.log_sender);
        let (mut fresh_replay, _backtraces, _fatal_error, db_conn) = self
            .inner
            .capture_replay_writes_from_log(
                execution_id,
                log,
                ffqn,
                params,
                db_conn,
                backtrace_capture,
            )
            .await
            .map_err(AdvanceError::from)?;
        if let Some(InternalCapturedWrite {
            write:
                CapturedDbWrite::AppendFinished {
                    retval, version, ..
                },
            ..
        }) = fresh_replay.last_mut()
        {
            let (retval_transformed, _fatal_error_from_wit) =
                transform_to_append_finished(retval.clone(), version, &self.user_return_type);
            *retval = retval_transformed;
        }
        Ok(WorkflowWorker::advance_from_log(
            db_conn.as_ref(),
            &self.inner.cancel_registry,
            log_forwarder_sender,
            requested,
            fresh_replay,
            old_version,
        )
        .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RunnableComponent;
    use crate::activity::activity_worker::test::compile_activity;
    use crate::activity::activity_worker::tests::new_activity_fibo;
    use crate::cancellation_driver;
    use crate::engines::{EngineConfig, Engines};
    use crate::testing_fn_registry::TestingFnRegistry;
    use crate::workflow::deadline_tracker::{
        DeadlineTrackerFactory, DeadlineTrackerFactoryTokio, deadline_tracker_factory_test,
    };
    use crate::workflow::workflow_worker::tests::write_stub_response;
    use crate::workflow::workflow_worker::{
        JoinNextBlockingStrategy, WorkflowConfig, WorkflowConfigMode,
    };
    use assert_matches::assert_matches;
    use chrono::DateTime;
    use concepts::component_id::{COMPONENT_DIGEST_DUMMY, ComponentDigest, Digest};
    use concepts::prefixed_ulid::{DEPLOYMENT_ID_DUMMY, DelayId, DeploymentId, ExecutorId, RunId};
    use concepts::storage::{
        CapturedDbWrite, ComponentUpgradeOutcome, ComponentUpgradeReason, CreateRequest,
        DbConnectionTest, DbPool, DbPoolCloseable, ExecutionRequest, HistoryEvent, JoinSetRequest,
        JoinSetResponse, Locked, LogEntry, LogInfoAppendRow, LogLevel, PendingState,
        PendingStateFinished, PendingStateFinishedError, PendingStateFinishedResultKind,
        PendingStatePendingAt, Version,
    };
    use concepts::time::{ClockFn, TokioSleep};
    use concepts::{
        ComponentRetryConfig, ComponentType, ExecutionId, ExecutionMetadata, StrVariant,
        TypeWrapperTopLevel,
    };
    use db_tests::Database;
    use executor::executor::{ExecConfig, ExecTask, LockingStrategy};
    use executor::worker::{WorkerContext, WorkerError, WorkerResultOk};
    use executor::{expired_timers_watcher, worker::Worker};
    use insta::assert_json_snapshot;
    use rstest::rstest;
    use serde_json::json;
    use sha2::{Digest as ShaDigest, Sha256};
    use std::str::FromStr as _;
    use std::time::Duration;
    use test_db_macro::expand_enum_database;
    use test_utils::sim_clock::SimClock;
    use test_utils::{ExecutionLogSanitized, redact_component_digest};
    use tokio::sync::mpsc;
    use tracing::{info, info_span};
    use val_json::wast_val::{ValKey, WastVal};
    use wasmtime::Engine;

    type ExecTaskAndClose = (ExecTask, tokio::sync::watch::Sender<bool>);

    const FIBO_10_OUTPUT: u64 = 55;

    fn drain_forwarded_log_messages(
        receiver: &mut mpsc::Receiver<LogInfoAppendRow>,
    ) -> Vec<String> {
        let mut messages = Vec::new();
        loop {
            match receiver.try_recv() {
                Ok(LogInfoAppendRow {
                    log_entry: LogEntry::Log { message, .. },
                    ..
                }) => messages.push(message),
                Ok(_) => {}
                Err(
                    tokio::sync::mpsc::error::TryRecvError::Empty
                    | tokio::sync::mpsc::error::TryRecvError::Disconnected,
                ) => break,
            }
        }
        messages
    }

    fn default_return_type() -> ReturnTypeExtendable {
        ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: Some(Box::new(TypeWrapper::String)),
                err: Some(Box::new(TypeWrapper::String)),
            },
            wit_type: StrVariant::Static("result<string, string>"),
        }
    }

    fn default_js_params() -> Vec<ParameterType> {
        vec![ParameterType {
            type_wrapper: TypeWrapper::List(Box::new(TypeWrapper::String)),
            name: StrVariant::Static("params"),
            wit_type: StrVariant::Static("list<string>"),
        }]
    }

    fn workflow_js_component_digest(
        js_source: &str,
        user_ffqn: &FunctionFqn,
        params: &[ParameterType],
        return_type: &ReturnTypeExtendable,
    ) -> ComponentDigest {
        let mut hasher = Sha256::new();
        hasher.update(b"workflow_js:");
        hasher.update(js_source.as_bytes());
        hasher.update(user_ffqn.to_string().as_bytes());
        for param in params {
            hasher.update(param.wit_type.as_ref().as_bytes());
        }
        hasher.update(return_type.wit_type.as_bytes());
        ComponentDigest(Digest(hasher.finalize().into()))
    }

    /// Build a [`WorkflowJsWorker`] configured for replay/advance.
    #[allow(clippy::too_many_arguments)]
    fn build_js_replay_worker(
        deployment_id: concepts::prefixed_ulid::DeploymentId,
        component_id: concepts::ComponentId,
        runnable_component: &RunnableComponent,
        workflow_engine: Arc<wasmtime::Engine>,
        fn_registry: Arc<dyn FunctionRegistry>,
        db_pool: Arc<dyn DbPool>,
        logs_storage_config: Option<LogStrageConfig>,
        clock_fn: Box<dyn concepts::time::ClockFn>,
        js_source: String,
        user_return_type: ReturnTypeExtendable,
        max_replay_captured_writes: Option<usize>,
    ) -> WorkflowJsWorker {
        use crate::workflow::deadline_tracker::DeadlineTrackerFactoryForReplay;
        let config = WorkflowConfig {
            component_id,
            stub_wasi: true,
            fuel: None,
            mode: WorkflowConfigMode::Replay {
                // `None` in tests means effectively unbounded.
                max_replay_captured_writes: max_replay_captured_writes.unwrap_or(usize::MAX),
            },
        };
        let compiled = WorkflowWorkerCompiled::new_with_config(
            runnable_component.clone(),
            config,
            workflow_engine,
            clock_fn,
        )
        .unwrap();
        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "test-fn");
        let js_compiled = WorkflowJsWorkerCompiled::new(
            compiled,
            js_source,
            "index.js".to_string(),
            &user_ffqn,
            default_js_params(),
            user_return_type,
        )
        .unwrap();
        let linked = js_compiled.link(fn_registry).unwrap();
        linked.into_worker(
            deployment_id,
            db_pool,
            Arc::new(DeadlineTrackerFactoryForReplay {}),
            CancelRegistry::new(),
            logs_storage_config,
        )
    }

    async fn new_js_workflow_worker_with_return_type(
        js_source: &str,
        user_ffqn: &FunctionFqn,
        return_type: ReturnTypeExtendable,
    ) -> (
        Arc<dyn Worker>,
        db_tests::DbGuard,
        db_tests::DbPoolCloseableWrapper,
    ) {
        let engine = Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let cancel_registry = CancelRegistry::new();
        let clock_fn: Box<dyn ClockFn> = SimClock::epoch().clone_box();

        let component_id = concepts::ComponentId::new(
            ComponentType::Workflow,
            StrVariant::Static("test_js_workflow"),
            COMPONENT_DIGEST_DUMMY,
        )
        .unwrap();

        // Compile the Boa WASM component
        let wasm_path = workflow_js_runtime_builder::WORKFLOW_JS_RUNTIME;
        let runnable_component =
            RunnableComponent::new(wasm_path, &engine, component_id.component_type).unwrap();

        let config = WorkflowConfig {
            component_id: component_id.clone(),
            stub_wasi: false,
            fuel: None,
            mode: WorkflowConfigMode::Real {
                join_next_blocking_strategy: JoinNextBlockingStrategy::Interrupt,
                lock_extension: Some(Duration::from_secs(1)),
                max_events_per_run: usize::MAX,
                response_refresh_interval: usize::MAX,
            },
        };

        let compiled = WorkflowWorkerCompiled::new_with_config(
            runnable_component.clone(),
            config,
            engine,
            clock_fn.clone_box(),
        )
        .unwrap();

        let js_compiled = WorkflowJsWorkerCompiled::new(
            compiled,
            js_source.to_string(),
            "index.js".to_string(),
            user_ffqn,
            default_js_params(),
            return_type,
        )
        .unwrap();

        let fn_registry: Arc<dyn FunctionRegistry> =
            TestingFnRegistry::new_from_components(Vec::new());
        let linked = js_compiled.link(fn_registry).unwrap();

        let (guard, db_pool, db_close) = db_tests::Database::Sqlite.set_up().await;
        let deadline_factory = Arc::new(DeadlineTrackerFactoryTokio::new(Duration::ZERO, clock_fn));

        (
            Arc::new(linked.into_worker(
                DEPLOYMENT_ID_DUMMY,
                db_pool,
                deadline_factory,
                cancel_registry,
                None,
            )),
            guard,
            db_close,
        )
    }

    async fn new_js_workflow_worker(
        js_source: &str,
        user_ffqn: &FunctionFqn,
    ) -> (
        Arc<dyn Worker>,
        db_tests::DbGuard,
        db_tests::DbPoolCloseableWrapper,
    ) {
        new_js_workflow_worker_with_return_type(js_source, user_ffqn, default_return_type()).await
    }

    /// Like `new_js_workflow_worker` but returns the Result from `link()` for testing error cases.
    fn try_link_js_workflow_worker(
        js_source: &str,
        user_ffqn: &FunctionFqn,
    ) -> Result<WorkflowJsWorkerLinked, crate::WasmFileError> {
        let engine = Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let clock_fn: Box<dyn ClockFn> = SimClock::epoch().clone_box();

        let component_id = concepts::ComponentId::new(
            ComponentType::Workflow,
            StrVariant::Static("test_js_workflow"),
            COMPONENT_DIGEST_DUMMY,
        )
        .unwrap();

        let wasm_path = workflow_js_runtime_builder::WORKFLOW_JS_RUNTIME;
        let runnable_component =
            RunnableComponent::new(wasm_path, &engine, component_id.component_type).unwrap();

        let config = WorkflowConfig {
            component_id,
            stub_wasi: false,
            fuel: None,
            mode: WorkflowConfigMode::Real {
                join_next_blocking_strategy: JoinNextBlockingStrategy::Interrupt,
                lock_extension: None,
                max_events_per_run: usize::MAX,
                response_refresh_interval: usize::MAX,
            },
        };

        let compiled =
            WorkflowWorkerCompiled::new_with_config(runnable_component, config, engine, clock_fn)
                .unwrap();

        let js_compiled = WorkflowJsWorkerCompiled::new(
            compiled,
            js_source.to_string(),
            "index.js".to_string(),
            user_ffqn,
            default_js_params(),
            default_return_type(),
        )
        .unwrap();

        let fn_registry: Arc<dyn FunctionRegistry> =
            TestingFnRegistry::new_from_components(Vec::new());
        js_compiled.link(fn_registry)
    }

    fn make_worker_context(ffqn: FunctionFqn, params: &[String]) -> WorkerContext {
        // The user function signature is: func(params: list<string>) -> result<string, string>
        // So we wrap the params in a list
        let params_json: Vec<serde_json::Value> = vec![json!(params)];
        let component_id = concepts::ComponentId::new(
            ComponentType::Workflow,
            StrVariant::Static("test_js_workflow"),
            COMPONENT_DIGEST_DUMMY,
        )
        .unwrap();
        WorkerContext {
            execution_id: ExecutionId::generate(),
            metadata: ExecutionMetadata::empty(),
            component_digest: component_id.component_digest.clone(),
            ffqn,
            params: Params::from_json_values_test(params_json),
            event_history: Vec::new(),
            responses: Vec::new(),
            parent: None,
            version: Version::new(0),
            can_be_retried: true,
            worker_span: info_span!("js_workflow_test"),
            locked_event: Locked {
                component_id,
                executor_id: ExecutorId::generate(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                run_id: RunId::generate(),
                lock_expires_at: chrono::DateTime::UNIX_EPOCH + chrono::Duration::seconds(60),
                retry_config: ComponentRetryConfig::WORKFLOW,
            },
            execution_interrupt_watcher: tokio::sync::watch::channel(false).1,
        }
    }

    fn extract_string(val: &WastVal) -> String {
        match val {
            WastVal::String(s) => s.clone(),
            other => panic!("expected string, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn workflow_js_simple_return() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "hello");
        let js_source = r#"
            export default function hello() {
                return "hello world";
            }
        "#;

        let (worker, _guard, _db_close) = new_js_workflow_worker(js_source, &ffqn).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished(RunFinished { retval, .. }) => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "hello world");
    }

    #[tokio::test]
    async fn workflow_js_async_fn() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "hello");
        let js_source = r#"
            export default async function hello() {
                return "hello world";
            }
        "#;

        let (worker, _guard, _db_close) = new_js_workflow_worker(js_source, &ffqn).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished(RunFinished { retval, .. }) => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "hello world");
    }

    #[tokio::test]
    async fn workflow_js_async_fn_drains_nested_awaits() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "hello");
        let js_source = r"
            async function increment(value) {
                return await Promise.resolve(value + 1);
            }

            export default async function hello() {
                let value = 0;
                for (let i = 0; i < 128; i += 1) {
                    value = await increment(value);
                }
                return String(value);
            }
        ";

        let (worker, _guard, _db_close) = new_js_workflow_worker(js_source, &ffqn).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished(RunFinished { retval, .. }) => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "128");
    }

    #[tokio::test]
    async fn workflow_js_with_params() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "greet");
        let js_source = r#"
            export default function greet(params) {
                let name = params[0];
                let greeting = params[1];
                return greeting + ", " + name + "!";
            }
        "#;

        let (worker, _guard, _db_close) = new_js_workflow_worker(js_source, &ffqn).await;
        let ctx = make_worker_context(ffqn, &["World".to_string(), "Hello".to_string()]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished(RunFinished { retval, .. }) => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let ok_val = output.expect("should have ok value");
        assert_eq!(extract_string(&ok_val.value), "Hello, World!");
    }

    #[tokio::test]
    async fn workflow_js_with_throw_string() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "fail");
        let js_source = r#"
            export default function fail() {
                throw "something went wrong";
            }
        "#;

        let (worker, _guard, _db_close) = new_js_workflow_worker(js_source, &ffqn).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished(RunFinished { retval, .. }) => retval);
        // For result<string, string>, a throw becomes Err
        let err_val = assert_matches!(retval, SupportedFunctionReturnValue::Err(err) => err);
        let err_val = err_val.expect("should have err value");
        assert_eq!(extract_string(&err_val.value), "something went wrong");
    }

    /// A schedule object with more than one of the mutually-exclusive keys is
    /// rejected (rather than silently picking one). The JS catches the thrown
    /// `TypeError` and returns its message so we can assert on it.
    #[tokio::test]
    async fn workflow_js_schedule_rejects_multiple_keys() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "fail");
        let js_source = r"
            export default function fail() {
                try {
                    obelisk.sleep({ seconds: 5, minutes: 3 });
                    return 'no-throw';
                } catch (e) {
                    return String(e.message ?? e);
                }
            }
        ";

        let (worker, _guard, _db_close) = new_js_workflow_worker(js_source, &ffqn).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished(RunFinished { retval, .. }) => retval);
        let output = assert_matches!(retval, SupportedFunctionReturnValue::Ok(ok) => ok);
        let msg = extract_string(&output.expect("should have ok value").value);
        assert!(
            msg.contains("multiple keys") && msg.contains("seconds") && msg.contains("minutes"),
            "expected a multiple-keys rejection naming the conflicting keys, got: {msg}"
        );
    }

    #[tokio::test]
    async fn workflow_js_syntax_error_should_fail_to_link() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "broken");
        let js_source = r"
            export default function broken( {
                return 'this has a syntax error';
            }
        ";

        match try_link_js_workflow_worker(js_source, &ffqn) {
            Err(e) => {
                let msg = e.to_string();
                assert!(msg.contains("parse error"), "error: {msg}");
            }
            Ok(_) => panic!("linking should fail for JS with syntax errors"),
        }
    }

    #[tokio::test]
    async fn workflow_js_no_default_export() {
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "missing");
        let js_source = r"
            export function some_other_function() {
                return 'hello';
            }
        ";

        let (worker, _guard, _db_close) = new_js_workflow_worker(js_source, &ffqn).await;
        let ctx = make_worker_context(ffqn, &[]);

        let err = worker.run(ctx).await.unwrap_err();
        assert_matches!(
            err,
            WorkerError::FatalError(
                FatalError::CannotInstantiate { reason, detail: _ },
                _version,
            ) => {
                assert!(reason.contains("no default export"), "reason: {reason}");
            }
        );
    }

    // ==================== Workflow function tests ====================

    const TICK_SLEEP: Duration = Duration::from_millis(1);

    fn compile_js_workflow_worker(
        js_source: &str,
        user_ffqn: &FunctionFqn,
        db_pool: Arc<dyn DbPool>,
        clock_fn: Box<dyn ClockFn>,
        fn_registry: Arc<dyn FunctionRegistry>,
        workflow_engine: Arc<Engine>,
    ) -> (WorkflowJsWorker, concepts::ComponentId, RunnableComponent) {
        compile_js_workflow_worker_with_deployment_id(
            js_source,
            user_ffqn,
            db_pool,
            clock_fn.clone_box().as_ref(),
            fn_registry,
            workflow_engine,
            DEPLOYMENT_ID_DUMMY,
            JoinNextBlockingStrategy::Interrupt,
            Arc::new(DeadlineTrackerFactoryTokio::new(Duration::ZERO, clock_fn)),
        )
    }

    #[expect(clippy::too_many_arguments)]
    fn compile_js_workflow_worker_with_deployment_id(
        js_source: &str,
        user_ffqn: &FunctionFqn,
        db_pool: Arc<dyn DbPool>,
        clock_fn: &dyn ClockFn,
        fn_registry: Arc<dyn FunctionRegistry>,
        workflow_engine: Arc<Engine>,
        deployment_id: DeploymentId,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        deadline_factory: Arc<dyn DeadlineTrackerFactory>,
    ) -> (WorkflowJsWorker, concepts::ComponentId, RunnableComponent) {
        compile_js_workflow_worker_with_deployment_id_and_return_type(
            js_source,
            user_ffqn,
            db_pool,
            clock_fn,
            fn_registry,
            workflow_engine,
            deployment_id,
            join_next_blocking_strategy,
            deadline_factory,
            default_return_type(),
            usize::MAX,
            usize::MAX,
        )
    }

    #[expect(clippy::too_many_arguments)]
    fn compile_js_workflow_worker_with_deployment_id_and_return_type(
        js_source: &str,
        user_ffqn: &FunctionFqn,
        db_pool: Arc<dyn DbPool>,
        clock_fn: &dyn ClockFn,
        fn_registry: Arc<dyn FunctionRegistry>,
        workflow_engine: Arc<Engine>,
        deployment_id: DeploymentId,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        deadline_factory: Arc<dyn DeadlineTrackerFactory>,
        return_type: ReturnTypeExtendable,
        max_events_per_run: usize,
        response_refresh_interval: usize,
    ) -> (WorkflowJsWorker, concepts::ComponentId, RunnableComponent) {
        let wasm_path = workflow_js_runtime_builder::WORKFLOW_JS_RUNTIME;
        let params = default_js_params();
        let component_id = concepts::ComponentId::new(
            ComponentType::Workflow,
            StrVariant::Static("test_js_workflow"),
            workflow_js_component_digest(js_source, user_ffqn, &params, &return_type),
        )
        .unwrap();

        let runnable_component =
            RunnableComponent::new(wasm_path, &workflow_engine, component_id.component_type)
                .unwrap();

        let config = WorkflowConfig {
            component_id: component_id.clone(),
            stub_wasi: false,
            fuel: None,
            mode: WorkflowConfigMode::Real {
                join_next_blocking_strategy,
                lock_extension: None,
                max_events_per_run,
                response_refresh_interval,
            },
        };

        let compiled = WorkflowWorkerCompiled::new_with_config(
            runnable_component.clone(),
            config,
            workflow_engine,
            clock_fn.clone_box(),
        )
        .unwrap();

        let js_compiled = WorkflowJsWorkerCompiled::new(
            compiled,
            js_source.to_string(),
            "index.js".to_string(),
            user_ffqn,
            params,
            return_type,
        )
        .unwrap();

        let linked = js_compiled.link(fn_registry).unwrap();

        (
            linked.into_worker(
                deployment_id,
                db_pool,
                deadline_factory,
                CancelRegistry::new(),
                None,
            ),
            component_id,
            runnable_component,
        )
    }

    fn new_js_workflow_exec_task(
        worker: WorkflowJsWorker,
        clock_fn: Box<dyn ClockFn>,
        db_pool: Arc<dyn DbPool>,
    ) -> ExecTaskAndClose {
        new_js_workflow_exec_task_with_locking_strategy(
            worker,
            clock_fn,
            db_pool,
            LockingStrategy::ByComponentDigest,
        )
    }

    fn new_js_workflow_exec_task_with_locking_strategy(
        worker: WorkflowJsWorker,
        clock_fn: Box<dyn ClockFn>,
        db_pool: Arc<dyn DbPool>,
        locking_strategy: LockingStrategy,
    ) -> ExecTaskAndClose {
        new_js_workflow_exec_task_with_locking_strategy_and_executor_id(
            worker,
            clock_fn,
            db_pool,
            locking_strategy,
            ExecutorId::generate(),
        )
    }

    fn new_js_workflow_exec_task_with_locking_strategy_and_executor_id(
        worker: WorkflowJsWorker,
        clock_fn: Box<dyn ClockFn>,
        db_pool: Arc<dyn DbPool>,
        locking_strategy: LockingStrategy,
        executor_id: ExecutorId,
    ) -> ExecTaskAndClose {
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(3),
            tick_sleep: TICK_SLEEP,
            component_id: worker.inner.config.component_id.clone(),
            task_limiter_global: None,
            task_limiter_local: None,
            executor_id,
            retry_config: ComponentRetryConfig::WORKFLOW,
            locking_strategy,
        };
        ExecTask::new_all_ffqns_test(Arc::new(worker), exec_config, clock_fn, db_pool)
    }

    fn new_js_workflow_exec_task_with_interrupt_watcher(
        worker: WorkflowJsWorker,
        clock_fn: Box<dyn ClockFn>,
        db_pool: Arc<dyn DbPool>,
        close_watcher: tokio::sync::watch::Receiver<bool>,
    ) -> ExecTask {
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(3),
            tick_sleep: TICK_SLEEP,
            component_id: worker.inner.config.component_id.clone(),
            task_limiter_global: None,
            task_limiter_local: None,
            executor_id: ExecutorId::generate(),
            retry_config: ComponentRetryConfig::WORKFLOW,
            locking_strategy: LockingStrategy::ByComponentDigest,
        };
        ExecTask::new_all_ffqns_test_with_interrupt_watcher(
            Arc::new(worker),
            exec_config,
            clock_fn,
            db_pool,
            close_watcher,
        )
    }

    /// Test: joinNextTry successfully finds a delay response after it expires
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_join_next_try_found(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_delay(params) {
            const js = obelisk.createJoinSet();
            const delayId = js.submitDelay({ milliseconds: 10 });
            obelisk.sleep({ milliseconds: 20 });
            const response = js.joinNextTry(); // First joinNextTry must succeed
            let afterErrorCode = null;
            try {
                js.joinNextTry(); // Second joinNextTry must fail
                throw 'unreachable';
            } catch (e) {
                afterErrorCode = e.code ?? null;
            }
            return JSON.stringify({
                responseIsNull: response === null,
                afterErrorCode
            });
        }";

        let harness =
            JsWorkflowTestHarness::with_no_activities(db_pool, js_source, "test-delay").await;
        harness.tick().await; // blocks on sleep
        harness.advance_time(Duration::from_millis(30)).await;
        harness.tick().await; // completes

        let result = harness.get_result_json().await;
        assert_eq!(json!(true), result["responseIsNull"]);
        assert_eq!(
            json!("OBELISK_JOIN_SET_EXHAUSTED"),
            result["afterErrorCode"]
        );
        drop(harness);
        db_close.close().await;
    }

    /// Test: JS workflow exercises all workflow-support APIs
    /// - createJoinSet (with and without name)
    /// - joinSet.submit (calls fibo activity)
    /// - joinSet.joinNext
    /// - joinSet.joinNextTry
    /// - obelisk.getResult
    /// - obelisk.randomU64, randomU64Inclusive, randomString
    /// - joinSet.submitDelay
    /// - joinSet.close
    /// - console logging
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_all_apis(
        database: Database,
        #[values(false, true)] activity_should_win: bool,
        #[values(false, true)] explicit_close: bool,
    ) {
        let (_guard, db_pool, db_close) = database.set_up().await;
        Box::pin(workflow_js_all_apis_inner(
            db_pool.clone(),
            activity_should_win,
            explicit_close,
        ))
        .await;
        db_close.close().await;
    }

    async fn workflow_js_all_apis_inner(
        db_pool: Arc<dyn DbPool>,
        activity_should_win: bool,
        explicit_close: bool,
    ) {
        test_utils::set_up();
        let sim_clock = SimClock::epoch();

        // JS code that exercises all workflow-support APIs
        let js_source = r"
        export default function test_all_apis(params) {
            console.log('Starting comprehensive API test');
            const explicit_close = params[0] === 'true';

            /* Test random functions */
            const rand1 = obelisk.randomU64(0, 10);
            const rand2 = obelisk.randomU64Inclusive(1, 10);
            const randStr = obelisk.randomString(5, 10);
            console.debug('Random values:', Number(rand1), Number(rand2), randStr);

            /* Test createJoinSet (unnamed) */
            const js1 = obelisk.createJoinSet();
            console.log('Created unnamed join set:', js1.id());

            /* Test createJoinSet (named) */
            const js2 = obelisk.createJoinSet({ name: 'my-named-set' });
            console.log('Created named join set:', js2.id());

            /* Test joinNextTry on empty join set - should throw JoinSetExhaustedError */
            let tryEmptyErrorCode = null;
            try {
                js2.joinNextTry();
                throw 'unreachable';
            } catch (e) {
                tryEmptyErrorCode = e.code ?? null;
            }
            console.log('joinNextTry on empty error code:', tryEmptyErrorCode);

            /* Submit fibo(10) activity call */
            const fiboFfqn = 'testing:fibo/fibo.fibo';
            const execId = js1.submit(fiboFfqn, [10]);
            console.log('Submitted fibo(10), execId:', execId);

            /* Submit a delay */
            const delayId = js1.submitDelay({ milliseconds: 100 });
            console.log('Submitted delay, delayId:', delayId);

            /* Test joinNextTry before any response is ready - should return undefined */
            const tryPending = js1.joinNextTry();
            console.log('joinNextTry pending:', JSON.stringify(tryPending));

            /* Join next - should get fibo result first if the activity wins */
            const response1 = js1.joinNext();
            console.log('joinNext response 1:', JSON.stringify(response1), 'lastId:', js1.lastId);

            let fiboResult = response1;
            const response1WasExecution = js1.lastId === execId;
            const loser = response1WasExecution ? delayId : execId;
            if (explicit_close) {
                js1.close();
                js2.close();
            }
            console.log('all done');
            return JSON.stringify({
                rand1InRange: rand1 >= 0n && rand1 < 10n,
                rand2InRange: rand2 >= 1n && rand2 <= 10n,
                randStrLenOk: randStr.length >= 5 && randStr.length < 10,
                fiboResult: fiboResult,
                response1WasExecution,
                loser,
                joinNextTryEmptyErrorCode: tryEmptyErrorCode,
                joinNextTryPendingIsUndefined: tryPending === undefined
            });
        }";

        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "test-all-apis");

        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
        ]);

        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        );

        let (workflow_exec, _workflow_close_tx) =
            new_js_workflow_exec_task(worker, sim_clock.clone_box(), db_pool.clone());
        let cancel_registry = CancelRegistry::new();

        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        let params = Params::from_json_values_test(vec![json!(vec![if explicit_close {
            "true"
        } else {
            "false"
        }])]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params,
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id,
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        info!("Step 1: Run workflow until blocked by join set (waiting for activity)");
        assert_eq!(
            1,
            workflow_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await
                .len()
        );

        assert_matches!(
            db_connection
                .get_pending_state(&execution_id)
                .await
                .unwrap()
                .pending_state,
            PendingState::BlockedByJoinSet(..)
        );

        if activity_should_win {
            info!("Step 2: Run activity to complete the fibo(10) child execution");
            let (activity_exec, _activity_close_tx) = new_activity_fibo(
                db_pool.clone(),
                sim_clock.clone_box(),
                TokioSleep,
                LockingStrategy::ByComponentDigest,
            )
            .await;
            let executed_activities = activity_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await;
            assert_eq!(1, executed_activities.len());
        } else {
            info!("Step 2: Move time forward to complete the delay");
            sim_clock.move_time_forward(Duration::from_millis(200));
            {
                let timer =
                    expired_timers_watcher::tick_test(db_connection.as_ref(), sim_clock.now())
                        .await
                        .unwrap();
                assert_eq!(1, timer.expired_async_timers);
            }
        }

        info!("Step 3: Resume workflow - should process the winner");
        assert_eq!(
            1,
            workflow_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await
                .len()
        );

        info!("Step 4: Finish cancelled loser and resume workflow if still pending");
        cancellation_driver::tick_test(db_connection.as_ref(), &cancel_registry, sim_clock.now())
            .await;
        assert!(
            workflow_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await
                .len()
                <= 1
        );

        let res = db_connection
            .get_finished_result(&execution_id)
            .await
            .unwrap();
        info!("Got result: {res:?}");

        // Verify results
        let ok_val = assert_matches!(res, SupportedFunctionReturnValue::Ok(Some(val)) => val);
        let json_str = assert_matches!(&ok_val.value, WastVal::String(s) => s);
        let result: serde_json::Value = serde_json::from_str(json_str).unwrap();

        assert_eq!(
            json!(true),
            result["rand1InRange"],
            "rand1 should be in range [0, 100)"
        );
        assert_eq!(
            json!(true),
            result["rand2InRange"],
            "rand2 should be in range [1, 10]"
        );
        assert_eq!(
            json!(true),
            result["randStrLenOk"],
            "random string length should be in range [5, 10)"
        );
        assert_eq!(
            json!("OBELISK_JOIN_SET_EXHAUSTED"),
            result["joinNextTryEmptyErrorCode"],
            "joinNextTry on empty join set should throw JoinSetExhaustedError"
        );
        assert_eq!(
            json!(true),
            result["joinNextTryPendingIsUndefined"],
            "joinNextTry before response ready should return undefined"
        );
        if activity_should_win {
            assert_eq!(
                json!(true),
                result["response1WasExecution"],
                "first response should be execution"
            );
            assert_eq!(
                json!(FIBO_10_OUTPUT),
                result["fiboResult"],
                "fibo(10) should be 55"
            );
        } else {
            assert_eq!(
                json!(false),
                result["response1WasExecution"],
                "first response should be delay"
            );
        }
        // check that the loser is cancelled.
        if activity_should_win {
            let delay_id = DelayId::from_str(result["loser"].as_str().unwrap()).unwrap();
            let responses = db_connection.get(&execution_id).await.unwrap().responses;
            let resp = responses
                .iter()
                .find_map(|resp| {
                    if let JoinSetResponse::DelayFinished {
                        delay_id: found,
                        result,
                    } = &resp.event.event.event
                        && *found == delay_id
                    {
                        Some(result)
                    } else {
                        None
                    }
                })
                .unwrap();
            assert!(resp.is_err());
        } else {
            let child_id = ExecutionId::from_str(result["loser"].as_str().unwrap()).unwrap();
            let state = db_connection
                .get_pending_state(&child_id)
                .await
                .unwrap()
                .pending_state;
            assert_matches!(
                state,
                PendingState::Finished(PendingStateFinished {
                    result_kind: PendingStateFinishedResultKind::Err(
                        PendingStateFinishedError::ExecutionFailure(
                            ExecutionFailureKind::Cancelled
                        )
                    ),
                    ..
                })
            );
        }

        let (log_sender, mut log_storage_recv) = mpsc::channel(100);
        let replay_worker = build_js_replay_worker(
            DeploymentId::generate(),
            workflow_exec.config.component_id.clone(),
            &runnable_component,
            workflow_engine,
            fn_registry,
            db_pool.clone(),
            Some(LogStrageConfig {
                min_level: concepts::storage::LogLevel::Debug,
                log_sender,
            }),
            sim_clock.clone_box(),
            js_source.to_string(),
            default_return_type(),
            None, // max_replay_captured_writes
        );
        replay_worker
            .replay(execution_id, BacktraceCapture::Disabled)
            .await
            .unwrap();
        // Drop the worker so the log_sender is closed; otherwise `recv_many` blocks indefinitely.
        drop(replay_worker);
        let mut buffer = Vec::new();
        let received = log_storage_recv.recv_many(&mut buffer, 100).await;
        assert_eq!(0, received, "expected no new messages, got {buffer:?}");
    }

    // ==================== JS Workflow test harness ====================

    /// What activities to register for the test.
    enum TestActivities {
        None,
        Stub,
        ChildErrorProjections,
    }

    /// Helper for running JS workflow tests with reduced boilerplate.
    struct JsWorkflowTestHarness {
        workflow_exec: ExecTask,
        #[expect(dead_code)]
        workflow_close_tx: tokio::sync::watch::Sender<bool>,
        execution_id: ExecutionId,
        db_connection: Box<dyn DbConnectionTest>,
        sim_clock: SimClock,
        cancel_registry: CancelRegistry,
    }

    impl JsWorkflowTestHarness {
        /// Create harness with stub activity registered, interrupt blocking strategy.
        async fn with_stub_activity(
            db_pool: Arc<dyn DbPool>,
            js_source: &str,
            fn_name: &'static str,
        ) -> Self {
            Self::new(
                db_pool,
                js_source,
                fn_name,
                TestActivities::Stub,
                JoinNextBlockingStrategy::Interrupt,
            )
            .await
        }

        /// Create harness with stub activity registered and a given blocking strategy.
        async fn with_stub_activity_strategy(
            db_pool: Arc<dyn DbPool>,
            js_source: &str,
            fn_name: &'static str,
            join_next_blocking_strategy: JoinNextBlockingStrategy,
        ) -> Self {
            Self::new(
                db_pool,
                js_source,
                fn_name,
                TestActivities::Stub,
                join_next_blocking_strategy,
            )
            .await
        }

        /// Create harness with no activities registered, interrupt blocking strategy.
        async fn with_no_activities(
            db_pool: Arc<dyn DbPool>,
            js_source: &str,
            fn_name: &'static str,
        ) -> Self {
            Self::new(
                db_pool,
                js_source,
                fn_name,
                TestActivities::None,
                JoinNextBlockingStrategy::Interrupt,
            )
            .await
        }

        async fn new(
            db_pool: Arc<dyn DbPool>,
            js_source: &str,
            fn_name: &'static str,
            activities: TestActivities,
            join_next_blocking_strategy: JoinNextBlockingStrategy,
        ) -> Self {
            Self::new_with_return_type(
                db_pool,
                js_source,
                fn_name,
                activities,
                join_next_blocking_strategy,
                default_return_type(),
            )
            .await
        }

        async fn new_with_return_type(
            db_pool: Arc<dyn DbPool>,
            js_source: &str,
            fn_name: &'static str,
            activities: TestActivities,
            join_next_blocking_strategy: JoinNextBlockingStrategy,
            return_type: ReturnTypeExtendable,
        ) -> Self {
            use crate::activity::activity_worker::test::compile_activity_stub;

            let sim_clock = SimClock::epoch();
            let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", fn_name);

            let components = match activities {
                TestActivities::None => vec![],
                TestActivities::Stub => vec![
                    compile_activity_stub(
                        test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY,
                    )
                    .await,
                ],
                TestActivities::ChildErrorProjections => vec![
                    compile_activity(
                        test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                    )
                    .await,
                    compile_activity(
                        test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
                    )
                    .await,
                    compile_activity(
                        test_programs_serde_activity_builder::TEST_PROGRAMS_SERDE_ACTIVITY,
                    )
                    .await,
                ],
            };
            let fn_registry: Arc<dyn FunctionRegistry> =
                TestingFnRegistry::new_from_components(components);

            // `Await` needs the SimClock deadline tracker for determinism; `Interrupt` never calls `track`.
            let deadline_factory: Arc<dyn DeadlineTrackerFactory> =
                match join_next_blocking_strategy {
                    JoinNextBlockingStrategy::Await { .. } => {
                        deadline_tracker_factory_test(&sim_clock)
                    }
                    JoinNextBlockingStrategy::Interrupt => Arc::new(
                        DeadlineTrackerFactoryTokio::new(Duration::ZERO, sim_clock.clone_box()),
                    ),
                };

            let workflow_engine =
                Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
            let (worker, component_id, _runnable_component) =
                compile_js_workflow_worker_with_deployment_id_and_return_type(
                    js_source,
                    &user_ffqn,
                    db_pool.clone(),
                    sim_clock.clone_box().as_ref(),
                    fn_registry,
                    workflow_engine,
                    DEPLOYMENT_ID_DUMMY,
                    join_next_blocking_strategy,
                    deadline_factory,
                    return_type,
                    usize::MAX,
                    usize::MAX,
                );

            let (workflow_exec, workflow_close_tx) =
                new_js_workflow_exec_task(worker, sim_clock.clone_box(), db_pool.clone());

            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            let db_connection = db_pool.connection_test().await.unwrap();

            let params = Params::from_json_values_test(vec![json!(Vec::<String>::new())]);
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id: execution_id.clone(),
                    ffqn: user_ffqn,
                    params,
                    parent: None,
                    metadata: ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    component_id,
                    deployment_id: DEPLOYMENT_ID_DUMMY,
                    scheduled_by: None,
                    paused: false,
                })
                .await
                .unwrap();

            Self {
                workflow_exec,
                workflow_close_tx,
                execution_id,
                db_connection,
                sim_clock,
                cancel_registry: CancelRegistry::new(),
            }
        }

        async fn tick(&self) {
            self.workflow_exec
                .tick_test_await(self.sim_clock.now(), RunId::generate())
                .await;
            cancellation_driver::tick_test(
                self.db_connection.as_ref(),
                &self.cancel_registry,
                self.sim_clock.now(),
            )
            .await;
        }

        /// Move time forward and process expired timers.
        async fn advance_time(&self, duration: Duration) {
            self.sim_clock.move_time_forward(duration);
            expired_timers_watcher::tick_test(self.db_connection.as_ref(), self.sim_clock.now())
                .await
                .unwrap();
        }

        async fn get_result_json(&self) -> serde_json::Value {
            let res = self
                .db_connection
                .get_finished_result(&self.execution_id)
                .await
                .unwrap();
            let ok_val = assert_matches!(res, SupportedFunctionReturnValue::Ok(Some(val)) => val);
            let json_str = assert_matches!(&ok_val.value, WastVal::String(s) => s);
            serde_json::from_str(json_str).unwrap()
        }
    }

    // ==================== Workflow tests ====================

    #[derive(Clone, Copy, Debug)]
    enum ChildErrorAwaitStyle {
        DirectImport,
        JoinNext,
        JoinNextTry,
    }

    impl ChildErrorAwaitStyle {
        fn js_source(self, projection: ChildErrProjection) -> String {
            let (direct_import, direct_call) = projection.direct_import();
            let (import, setup, await_child) = match self {
                Self::DirectImport => (direct_import, String::new(), direct_call.to_string()),
                Self::JoinNext => (
                    "",
                    format!(
                        "const js = obelisk.createJoinSet();\njs.submit('{}', {});",
                        projection.target_ffqn(),
                        projection.params_json()
                    ),
                    "js.joinNext();".to_string(),
                ),
                Self::JoinNextTry => (
                    "",
                    format!(
                        "const js = obelisk.createJoinSet();\njs.submit('{}', {});\nobelisk.sleep({{ milliseconds: 1 }});",
                        projection.target_ffqn(),
                        projection.params_json()
                    ),
                    "js.joinNextTry();\nthrow 'expected ChildError';".to_string(),
                ),
            };
            let value_assertion = projection.value_assertion();
            format!(
                r"
                {import}
                export default function test_child_error(_params) {{
                    {setup}
                    try {{
                        {await_child}
                    }} catch (e) {{
                        if (!(e instanceof obelisk.ChildError)) {{
                            throw `expected ChildError, got: ${{e}}`;
                        }}
                        {value_assertion}
                        if (e.failureKind !== 'uncategorized') {{
                            throw `unexpected failure kind: ${{e.failureKind}}`;
                        }}
                        throw e;
                    }}
                }}
                "
            )
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum ChildErrProjection {
        Unit,
        String,
        ExecutionFailedVariant,
    }

    impl ChildErrProjection {
        fn direct_import(self) -> (&'static str, &'static str) {
            match self {
                Self::Unit => ("import { fibo } from 'testing:fibo/fibo';", "fibo(1);"),
                Self::String => (
                    "import { get } from 'testing:http/http-get';",
                    "get('http://unused');",
                ),
                Self::ExecutionFailedVariant => {
                    ("import { trap } from 'testing:serde/serde';", "trap();")
                }
            }
        }

        fn target_ffqn(self) -> &'static str {
            match self {
                Self::Unit => "testing:fibo/fibo.fibo",
                Self::String => "testing:http/http-get.get",
                Self::ExecutionFailedVariant => "testing:serde/serde.trap",
            }
        }

        fn params_json(self) -> &'static str {
            match self {
                Self::Unit => "[1]",
                Self::String => "['http://unused']",
                Self::ExecutionFailedVariant => "[]",
            }
        }

        fn value_assertion(self) -> &'static str {
            match self {
                Self::Unit => {
                    "if (e.value !== undefined) { throw `unexpected child err value: ${e.value}`; }"
                }
                Self::String | Self::ExecutionFailedVariant => {
                    "if (e.value !== 'execution_failed') { throw `unexpected child err value: ${e.value}`; }"
                }
            }
        }

        fn parent_return_type(self) -> ReturnTypeExtendable {
            let err = match self {
                Self::Unit => None,
                Self::String => Some(Box::new(TypeWrapper::String)),
                Self::ExecutionFailedVariant => {
                    Some(Box::new(TypeWrapper::Variant(IndexMap::from([
                        (TypeKey::new_kebab("foo"), None),
                        (TypeKey::new_kebab("execution-failed"), None),
                    ]))))
                }
            };
            let wit_type = match self {
                Self::Unit => StrVariant::Static("result<string>"),
                Self::String => StrVariant::Static("result<string, string>"),
                Self::ExecutionFailedVariant => {
                    StrVariant::Static("result<string, variant { foo, execution-failed }>")
                }
            };
            ReturnTypeExtendable {
                type_wrapper_tl: TypeWrapperTopLevel {
                    ok: Some(Box::new(TypeWrapper::String)),
                    err,
                },
                wit_type,
            }
        }
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_rethrows_child_platform_failure(
        database: Database,
        #[values(
            ChildErrorAwaitStyle::DirectImport,
            ChildErrorAwaitStyle::JoinNext,
            ChildErrorAwaitStyle::JoinNextTry
        )]
        await_style: ChildErrorAwaitStyle,
        #[values(
            ChildErrProjection::Unit,
            ChildErrProjection::String,
            ChildErrProjection::ExecutionFailedVariant
        )]
        projection: ChildErrProjection,
    ) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;
        let js_source = await_style.js_source(projection);
        let harness = JsWorkflowTestHarness::new_with_return_type(
            db_pool,
            &js_source,
            "test-child-error",
            TestActivities::ChildErrorProjections,
            JoinNextBlockingStrategy::Interrupt,
            projection.parent_return_type(),
        )
        .await;

        harness.tick().await;
        let log = harness
            .db_connection
            .get(&harness.execution_id)
            .await
            .unwrap();
        let child_ffqn = projection.target_ffqn().parse::<FunctionFqn>().unwrap();
        let child_execution_id = log
            .events
            .iter()
            .find_map(|event| match &event.event {
                ExecutionRequest::HistoryEvent {
                    event:
                        HistoryEvent::JoinSetRequest {
                            request:
                                JoinSetRequest::ChildExecutionRequest {
                                    child_execution_id,
                                    target_ffqn,
                                    ..
                                },
                            ..
                        },
                } if target_ffqn == &child_ffqn => Some(child_execution_id.clone()),
                _ => None,
            })
            .expect("workflow should submit the child activity");
        write_stub_response(
            harness.db_connection.as_ref(),
            harness.sim_clock.now(),
            child_execution_id,
            SupportedFunctionReturnValue::ExecutionFailure(FinishedExecutionFailure {
                kind: ExecutionFailureKind::Uncategorized,
                reason: Some("injected platform failure".to_string()),
                detail: None,
            }),
        )
        .await;

        if matches!(await_style, ChildErrorAwaitStyle::JoinNextTry) {
            harness.advance_time(Duration::from_millis(1)).await;
        }
        harness.tick().await;

        let result = harness
            .db_connection
            .get_finished_result(&harness.execution_id)
            .await
            .unwrap();
        match projection {
            ChildErrProjection::Unit => {
                assert_matches!(result, SupportedFunctionReturnValue::Err(None));
            }
            ChildErrProjection::String => {
                let err =
                    assert_matches!(result, SupportedFunctionReturnValue::Err(Some(err)) => err);
                assert_eq!(WastVal::String("execution_failed".into()), err.value);
            }
            ChildErrProjection::ExecutionFailedVariant => {
                let err =
                    assert_matches!(result, SupportedFunctionReturnValue::Err(Some(err)) => err);
                assert_eq!(
                    WastVal::Variant(ValKey::from_kebab("execution-failed"), None),
                    err.value
                );
            }
        }

        drop(harness);
        db_close.close().await;
    }

    /// Test: JS workflow uses `obelisk.stub()` to stub an `activity_stub` execution.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_stub(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_stub(params) {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.foo', ['test-param']);
            obelisk.stub(execId, {'ok': 'stubbed-result-42'});
            const result = js.joinNext();
            return JSON.stringify({
                lastId: js.lastId,
                result: result
            });
        }";

        let harness =
            JsWorkflowTestHarness::with_stub_activity(db_pool, js_source, "test-stub").await;
        harness.tick().await; // submit, stub, block on joinNext
        harness.tick().await; // resume, complete

        let result = harness.get_result_json().await;
        assert!(
            result["lastId"].as_str().unwrap().starts_with("E_"),
            "lastId should be a child execution id, got {result}"
        );
        assert_eq!(json!("stubbed-result-42"), result["result"]);
        drop(harness);
        db_close.close().await;
    }

    /// The public docs' "self-fulfilled stub events" pattern under `Await`: submit
    /// a stub child, fulfil it via `obelisk.stub`, then consume it either by an
    /// explicit `joinNext` or by draining it through `joinSet.close()`. The response
    /// is durable before the consume, so one tick with no sim-time advance must
    /// reach `Finished` rather than fall back to lock-expiry recovery.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_self_fulfilled_stub_await_is_fast(
        database: Database,
        #[values(0, 10)] non_blocking_event_batching: u32,
        #[values(false, true)] drain_via_close: bool,
    ) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        // The consumers differ only in how the fulfilled stub is drained; the JS
        // validates the joinNext response inline, so `Finished(Ok)` covers both.
        let consume = if drain_via_close {
            "events.close();"
        } else {
            "const published = events.joinNext();
             if (events.lastId !== executionId || published !== valueJson) {
                 throw new Error('unexpected event response: ' + events.lastId);
             }
             events.close();"
        };
        let js_source = format!(
            r"
        export default function session(params) {{
            const events = obelisk.createJoinSet({{ name: 'events' }});
            const valueJson = JSON.stringify({{ stdout: 'hello\n', exit_code: 0 }});
            const executionId = events.submit('testing:stub-activity/activity.foo', ['command-1']);
            obelisk.stub(executionId, {{ ok: valueJson }});
            {consume}
            return JSON.stringify({{ done: true }});
        }}"
        );

        let harness = JsWorkflowTestHarness::with_stub_activity_strategy(
            db_pool,
            &js_source,
            "session",
            JoinNextBlockingStrategy::Await {
                non_blocking_event_batching,
                subscription_interruption: None,
            },
        )
        .await;

        harness.tick().await;

        let pending_state = harness
            .db_connection
            .get_pending_state(&harness.execution_id)
            .await
            .unwrap()
            .pending_state;
        assert_matches!(
            pending_state,
            PendingState::Finished(PendingStateFinished {
                result_kind: PendingStateFinishedResultKind::Ok,
                ..
            }),
            "one Await tick must finish the workflow, got {pending_state:?}"
        );

        let result = harness.get_result_json().await;
        assert_eq!(json!(true), result["done"]);
        drop(harness);
        db_close.close().await;
    }

    /// Test: Stub with error response (`result<string>` has no error type, so err is null).
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_stub_with_error(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_stub_err(params) {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.foo', ['test-param']);
            obelisk.stub(execId, {'err': null}); // result<string> has no error type
            let threw = false;
            let isChildErr = false;
            let isError = false;
            let valueIsUndefined = null;
            let cancelled = null;
            let failureKind = 'unset';
            let childId = null;
            try {
                js.joinNext();
            } catch (e) {
                threw = true;
                isChildErr = e instanceof obelisk.ChildError;
                isError = e instanceof Error;
                valueIsUndefined = e.value === undefined;
                cancelled = e.cancelled;
                failureKind = e.failureKind ?? null;
                childId = e.childId;
            }
            return JSON.stringify({
                lastId: js.lastId,
                threw,
                isChildErr,
                isError,
                valueIsUndefined,
                cancelled,
                failureKind,
                childId,
            });
        }";

        let harness =
            JsWorkflowTestHarness::with_stub_activity(db_pool, js_source, "test-stub-err").await;
        harness.tick().await;
        harness.tick().await;

        let result = harness.get_result_json().await;
        assert!(
            result["lastId"].as_str().unwrap().starts_with("E_"),
            "lastId should be set before throwing child err, got {result}"
        );
        assert_eq!(json!(true), result["threw"]);
        // A unit err (result<string> has no err type) throws a ChildError
        // (also an Error) whose `.value` is undefined; it is a business err, not a
        // platform failure, so `.cancelled` is false and `.failureKind` is absent.
        assert_eq!(json!(true), result["isChildErr"]);
        assert_eq!(json!(true), result["isError"]);
        assert_eq!(json!(true), result["valueIsUndefined"]);
        assert_eq!(json!(false), result["cancelled"]);
        assert_eq!(json!(null), result["failureKind"]);
        assert_eq!(result["lastId"], result["childId"]);
        drop(harness);
        db_close.close().await;
    }

    /// Test: Stub non-existent execution returns error.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_stub_execution_not_found(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_stub_not_found(params) {
            try {
                obelisk.stub('E_00000000000000000000000000.n:fake_1', {'ok': 'x'});
                return JSON.stringify({ error: 'expected-error-but-got-none' });
            } catch (e) {
                return JSON.stringify({ errorType: 'stub-error', errorMessage: e.message });
            }
        }";

        let harness =
            JsWorkflowTestHarness::with_stub_activity(db_pool, js_source, "test-stub-not-found")
                .await;
        harness.tick().await;

        let result = harness.get_result_json().await;
        assert_eq!(json!("stub-error"), result["errorType"]);
        let error_msg = result["errorMessage"].as_str().unwrap();
        assert!(
            error_msg.contains("NotFound") || error_msg.contains("ExecutionNotFound"),
            "Expected 'NotFound' in error message, got: {error_msg}"
        );
        drop(harness);
        db_close.close().await;
    }

    /// Test: Stubbing the same value twice succeeds (idempotent).
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_stub_same_value_twice(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_stub_twice(params) {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.foo', ['test-param']);
            obelisk.stub(execId, {'ok': 'same-value'});
            obelisk.stub(execId, {'ok': 'same-value'}); // same value - should succeed
            const result = js.joinNext();
            return JSON.stringify({ lastId: js.lastId, result: result });
        }";

        let harness =
            JsWorkflowTestHarness::with_stub_activity(db_pool, js_source, "test-stub-twice").await;
        harness.tick().await;
        harness.tick().await;

        let result = harness.get_result_json().await;
        assert!(
            result["lastId"].as_str().unwrap().starts_with("E_"),
            "lastId should be a child execution id, got {result}"
        );
        assert_eq!(json!("same-value"), result["result"]);
        drop(harness);
        db_close.close().await;
    }

    /// Test: Stub `noret` function (returns `result` with no ok/err payloads).
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_stub_noret(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_stub_noret(params) {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.noret', []);
            obelisk.stub(execId, {'ok': null}); // result has no payload
            const result = js.joinNext();
            return JSON.stringify({ lastId: js.lastId, result: result });
        }";

        let harness =
            JsWorkflowTestHarness::with_stub_activity(db_pool, js_source, "test-stub-noret").await;
        harness.tick().await;
        harness.tick().await;

        let result = harness.get_result_json().await;
        assert!(
            result["lastId"].as_str().unwrap().starts_with("E_"),
            "lastId should be a child execution id, got {result}"
        );
        assert_eq!(json!(null), result["result"]);
        drop(harness);
        db_close.close().await;
    }

    /// Test: Stub conflict - second stub with different value must fail.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_stub_conflict(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_stub_conflict(params) {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.foo', ['test-param']);
            obelisk.stub(execId, {'ok': 'first-value'});
            try {
                obelisk.stub(execId, {'ok': 'different-value'}); // must fail
                return JSON.stringify({ error: 'expected-conflict-but-stub-succeeded' });
            } catch (e) {
                const result = js.joinNext();
                return JSON.stringify({
                    conflictDetected: true,
                    errorMessage: e.message,
                    lastId: js.lastId,
                    result: result
                });
            }
        }";

        let harness =
            JsWorkflowTestHarness::with_stub_activity(db_pool, js_source, "test-stub-conflict")
                .await;
        harness.tick().await;
        harness.tick().await;

        let result = harness.get_result_json().await;
        assert_eq!(
            json!(true),
            result["conflictDetected"],
            "Expected conflict, got: {result}"
        );
        let error_msg = result["errorMessage"].as_str().unwrap();
        assert!(
            error_msg.contains("Conflict"),
            "Expected 'Conflict' in error, got: {error_msg}"
        );
        assert!(
            result["lastId"].as_str().unwrap().starts_with("E_"),
            "lastId should be a child execution id, got {result}"
        );
        assert_eq!(json!("first-value"), result["result"]);
        drop(harness);
        db_close.close().await;
    }

    /// Test: `obelisk.executionIdGenerate()` and `obelisk.schedule()` to schedule a top-level execution.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_schedule(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        // scheduleAt is optional - omitting it defaults to 'now'
        let js_source = r"
        export default function test_schedule(params) {
            const execId = obelisk.executionIdGenerate();
            obelisk.schedule(execId, 'testing:stub-activity/activity.foo', ['scheduled-param']);
            return JSON.stringify({ scheduledExecutionId: execId });
        }";

        let harness =
            JsWorkflowTestHarness::with_stub_activity(db_pool.clone(), js_source, "test-schedule")
                .await;
        harness.tick().await;

        let result = harness.get_result_json().await;
        let scheduled_exec_id_str = result["scheduledExecutionId"]
            .as_str()
            .expect("scheduledExecutionId should be a string");

        // Verify the scheduled execution was created in the database
        let scheduled_exec_id =
            ExecutionId::from_str(scheduled_exec_id_str).expect("should parse execution ID");
        let db_connection = db_pool.connection_test().await.unwrap();
        let create_request = db_connection
            .get_create_request(&scheduled_exec_id)
            .await
            .expect("scheduled execution should exist");

        assert_eq!(
            create_request.ffqn,
            FunctionFqn::new_static("testing:stub-activity/activity", "foo")
        );
        drop(harness);
        drop(db_connection);
        db_close.close().await;
    }

    #[tokio::test]
    async fn void_result_return_null() {
        // `result` (no ok, no err): `return null` → Ok(None)
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "do-work");
        let js_source = r"
            export default function do_work() {
                return null;
            }
        ";

        let return_type = ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: None,
                err: None,
            },
            wit_type: StrVariant::Static("result"),
        };

        let (worker, _guard, _db_close) =
            new_js_workflow_worker_with_return_type(js_source, &ffqn, return_type).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished(RunFinished { retval, .. }) => retval);
        assert_matches!(retval, SupportedFunctionReturnValue::Ok(None));
    }

    #[tokio::test]
    async fn void_result_throw_null() {
        // `result` (no ok, no err): `throw null` → Err(None)
        test_utils::set_up();
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "do-work");
        let js_source = r"
            export default function do_work() {
                throw null;
            }
        ";

        let return_type = ReturnTypeExtendable {
            type_wrapper_tl: TypeWrapperTopLevel {
                ok: None,
                err: None,
            },
            wit_type: StrVariant::Static("result"),
        };

        let (worker, _guard, _db_close) =
            new_js_workflow_worker_with_return_type(js_source, &ffqn, return_type).await;
        let ctx = make_worker_context(ffqn, &[]);

        let result = worker.run(ctx).await.expect("worker should succeed");
        let retval = assert_matches!(result, WorkerResultOk::RunFinished(RunFinished { retval, .. }) => retval);
        assert_matches!(retval, SupportedFunctionReturnValue::Err(None));
    }

    /// When the executor signals close while a JS workflow worker is running,
    /// the worker must write an `Unlocked` event and exit.
    ///
    /// Test steps:
    /// 1. Spawn the busy workflow worker task via `tick_test` (returns immediately
    ///    with a handle to the in-progress worker task).
    /// 2. Send the executor-close signal.
    /// 3. Join the worker task.
    /// 4. Assert the execution log contains an `Unlocked` event.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn execution_interrupt_writes_unlocked_event(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
            export default function busy(params) {
                for (let i = 0; i < 30; i++) {
                    obelisk.sleep({ milliseconds: 300 });
                }
                return 'done';
            }
        ";
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "busy");

        let (close_sender, close_receiver) = tokio::sync::watch::channel(false);

        let sim_clock = SimClock::epoch();
        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (worker, component_id, _) = compile_js_workflow_worker(
            js_source,
            &ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry,
            workflow_engine,
        );

        let exec_task = new_js_workflow_exec_task_with_interrupt_watcher(
            worker,
            sim_clock.clone_box(),
            db_pool.clone(),
            close_receiver,
        );

        // Register the execution so the DB is ready for the Unlocked append.
        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id,
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        // Spawn the busy worker task. tick_test returns as soon as the task is
        // spawned — the worker itself runs concurrently.
        let progress = exec_task
            .tick_test(sim_clock.now(), RunId::generate())
            .await;

        // Signal the executor to close. The worker bails on the next apply()
        // call (i.e. at the very first obelisk.sleep) and writes Unlocked.
        close_sender.send(true).unwrap();

        // Join the worker task.
        progress.wait_for_tasks().await;

        // Verify the Unlocked event is present in the execution log.
        let log = db_connection.get(&execution_id).await.unwrap();
        let has_unlocked = log
            .events
            .iter()
            .any(|e| matches!(e.event, ExecutionRequest::Unlocked(_)));
        assert!(
            has_unlocked,
            "expected Unlocked event in execution log, got: {:?}",
            log.events
        );
        drop(db_connection);
        db_close.close().await;
    }

    /// When a pause or cancel RPC signals a locally-running JS workflow via
    /// `signal_workflow_interrupt` (`InterruptKind::PauseOrCancel`), the worker must
    /// stop promptly without waiting for the lock deadline and append nothing
    /// (`WorkerResultOk::DbUpdatedByWorkerOrWatcher`): the durable Paused/Cancelling
    /// event is written out of band by the endpoint, so unlike the executor-close
    /// signal the worker must not append an `Unlocked`.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn signal_workflow_interrupt_interrupts_running_workflow(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
            export default function busy(params) {
                for (let i = 0; i < 30; i++) {
                    obelisk.sleep({ milliseconds: 300 });
                }
                return 'done';
            }
        ";
        let ffqn = FunctionFqn::new_static("test:pkg/ifc", "busy");

        // Never-set close watcher: the interrupt can only come from `signal_workflow_interrupt`.
        let (_close_sender, close_receiver) = tokio::sync::watch::channel(false);

        let sim_clock = SimClock::epoch();
        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (worker, component_id, _) = compile_js_workflow_worker(
            js_source,
            &ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry,
            workflow_engine,
        );
        // Share the worker's registry so the test can drive `signal_workflow_interrupt`.
        let cancel_registry = worker.inner.cancel_registry.clone();

        let exec_task = new_js_workflow_exec_task_with_interrupt_watcher(
            worker,
            sim_clock.clone_box(),
            db_pool.clone(),
            close_receiver,
        );

        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id,
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        let progress = exec_task
            .tick_test(sim_clock.now(), RunId::generate())
            .await;

        db_pool
            .external_api_conn()
            .await
            .unwrap()
            .pause_execution(&execution_id, sim_clock.now())
            .await
            .unwrap();

        // `signal_workflow_interrupt` is a no-op until `run()` registers the execution,
        // so retry until the worker (blocked in the first `obelisk.sleep`) observes it
        // and returns; the loop is aborted once the worker task exits.
        let signaller = tokio::spawn({
            let cancel_registry = cancel_registry.clone();
            let execution_id = execution_id.clone();
            async move {
                loop {
                    cancel_registry.signal_workflow_interrupt(&execution_id);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        });
        progress.wait_for_tasks().await;
        signaller.abort();

        // Pausing the locked workflow appends one `Unlocked`; the worker must not
        // append another one when it observes the local interrupt.
        let log = db_connection.get(&execution_id).await.unwrap();
        let unlocked_count = log
            .events
            .iter()
            .filter(|e| matches!(e.event, ExecutionRequest::Unlocked(_)))
            .count();
        assert_eq!(unlocked_count, 1, "unexpected events: {:?}", log.events);
        assert!(
            log.pending_state.is_paused(),
            "execution must remain paused, got: {:?}",
            log.pending_state
        );
        drop(db_connection);
        db_close.close().await;
    }

    /// Test: `Math.random()` returns a value in [0, 1) and is deterministic (replay
    /// produces identical output because random values are replayed from the event log).
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_math_random(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_math_random(params) {
            const r1 = Math.random();
            const r2 = Math.random();
            const r3 = Math.random();
            return JSON.stringify({
                r1, r2, r3,
                allInRange: r1 >= 0 && r1 < 1 && r2 >= 0 && r2 < 1 && r3 >= 0 && r3 < 1,
                notAllZero: r1 !== 0 || r2 !== 0 || r3 !== 0
            });
        }";

        let harness = JsWorkflowTestHarness::with_no_activities(
            db_pool.clone(),
            js_source,
            "test-math-random",
        )
        .await;
        harness.tick().await;

        let result = harness.get_result_json().await;
        assert_eq!(
            json!(true),
            result["allInRange"],
            "all values must be in [0, 1): {result}"
        );
        assert_eq!(
            json!(true),
            result["notAllZero"],
            "values should not all be zero: {result}"
        );

        // Execution log must contain Persist events — one per Math.random() call
        let log = db_pool
            .connection_test()
            .await
            .unwrap()
            .get(&harness.execution_id)
            .await
            .unwrap();
        assert!(
            log.events.iter().any(|e| matches!(
                e.event,
                ExecutionRequest::HistoryEvent {
                    event: HistoryEvent::Persist { .. }
                }
            )),
            "expected at least one Persist event for Math.random()"
        );
        drop(harness);
        db_close.close().await;
    }

    /// Test: `Date.now()` returns the current simulated clock time.
    /// - `advance_time(42ms)` → clock=42ms (no timers yet)
    /// - `tick()` → workflow creates `sleep_bt(Now)` with `expires_at=42ms`, yields
    /// - `advance_time(ZERO)` → fires the timer (42ms ≤ 42ms)
    /// - `tick()` → workflow resumes, `sleep_bt` returns 42ms, `Date.now()` = 42ms
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_date_now(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_date_now(params) {
            const now = Date.now();
            return JSON.stringify({ now });
        }";

        let harness =
            JsWorkflowTestHarness::with_no_activities(db_pool.clone(), js_source, "test-date-now")
                .await;
        // Put the clock at 42 ms before the workflow first runs so that the
        // sleep_bt(Now) call schedules its wakeup at t=42 ms.
        harness.advance_time(Duration::from_millis(42)).await;
        harness.tick().await; // workflow yields at sleep_bt(Now), expires_at=42ms
        harness.advance_time(Duration::ZERO).await; // fire the timer (42ms ≤ 42ms)
        harness.tick().await; // workflow resumes, sleep returns 42ms

        let result = harness.get_result_json().await;
        assert_eq!(
            json!(42),
            result["now"],
            "Date.now() should return the simulated clock time (42ms): {result}"
        );

        // Execution log must contain a DelayRequest event — Date.now() uses
        // the internal sleep_bt(Now) which creates a JoinSetRequest::DelayRequest
        let db_conn = db_pool.connection_test().await.unwrap();
        let log = db_conn.get(&harness.execution_id).await.unwrap();
        assert!(
            log.events.iter().any(|e| matches!(
                e.event,
                ExecutionRequest::HistoryEvent {
                    event: HistoryEvent::JoinSetRequest {
                        request: JoinSetRequest::DelayRequest { .. },
                        ..
                    }
                }
            )),
            "expected a JoinSetRequest::DelayRequest event for Date.now()"
        );
        drop(harness);
        drop(db_conn);
        db_close.close().await;
    }

    /// Test: `new Date()` resolves to the same deterministic Obelisk clock as
    /// `Date.now()` (both go through `ObeliskClock::system_time_millis`). In 0.40.0
    /// the workflow runtime wired a `FixedClock(0)`, so `new Date()` returned epoch 0
    /// while `Date.now()` returned the real clock; this asserts they now agree.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_new_date(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_new_date(params) {
            const now = new Date().getTime();
            return JSON.stringify({ now });
        }";

        let harness =
            JsWorkflowTestHarness::with_no_activities(db_pool.clone(), js_source, "test-new-date")
                .await;
        harness.advance_time(Duration::from_millis(42)).await;
        harness.tick().await; // workflow yields at sleep_bt(Now), expires_at=42ms
        harness.advance_time(Duration::ZERO).await; // fire the timer (42ms ≤ 42ms)
        harness.tick().await; // workflow resumes, clock read returns 42ms

        let result = harness.get_result_json().await;
        assert_eq!(
            json!(42),
            result["now"],
            "new Date() should return the simulated clock time (42ms): {result}"
        );

        // Same as Date.now(): the clock read goes through sleep_bt(Now), which
        // creates a JoinSetRequest::DelayRequest event.
        let db_conn = db_pool.connection_test().await.unwrap();
        let log = db_conn.get(&harness.execution_id).await.unwrap();
        assert!(
            log.events.iter().any(|e| matches!(
                e.event,
                ExecutionRequest::HistoryEvent {
                    event: HistoryEvent::JoinSetRequest {
                        request: JoinSetRequest::DelayRequest { .. },
                        ..
                    }
                }
            )),
            "expected a JoinSetRequest::DelayRequest event for new Date()"
        );
        drop(harness);
        drop(db_conn);
        db_close.close().await;
    }

    /// Test: `obelisk.sleep()` returns a `Date` object representing the wake-up time.
    /// - `advance_time(42ms)` → clock=42ms
    /// - `tick()` → workflow calls `sleep({ milliseconds: 100 })`, yields at `expires_at=142ms`
    /// - `advance_time(100ms)` → fires the timer (142ms ≤ 142ms)
    /// - `tick()` → workflow resumes; sleep returns a Date whose `.getTime()` == 142
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_sleep_returns_date(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_sleep_date(params) {
            const wakeUp = obelisk.sleep({ milliseconds: 100 });
            return JSON.stringify({ ms: wakeUp.getTime() });
        }";

        let harness = JsWorkflowTestHarness::with_no_activities(
            db_pool.clone(),
            js_source,
            "test-sleep-date",
        )
        .await;
        harness.advance_time(Duration::from_millis(42)).await;
        harness.tick().await; // workflow yields at sleep, expires_at=142ms
        harness.advance_time(Duration::from_millis(100)).await; // fire the timer
        harness.tick().await; // workflow resumes, sleep returns Date(142ms)

        let result = harness.get_result_json().await;
        assert_eq!(
            json!(142),
            result["ms"],
            "sleep() should return a Date whose getTime() equals the wake-up ms: {result}"
        );
        drop(harness);
        db_close.close().await;
    }

    /// Test: `obelisk.sleep` accepts a JS `Date` as an absolute wake-up time
    /// (symmetric with the `Date` it returns). `new Date(142)` is 142ms since epoch,
    /// so with the clock at 42ms the sleep must resolve at the absolute 142ms.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_sleep_accepts_date(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function test_sleep_date_arg(params) {
            const wakeUp = obelisk.sleep(new Date(142));
            return JSON.stringify({ ms: wakeUp.getTime() });
        }";

        let harness = JsWorkflowTestHarness::with_no_activities(
            db_pool.clone(),
            js_source,
            "test-sleep-date-arg",
        )
        .await;
        harness.advance_time(Duration::from_millis(42)).await;
        harness.tick().await; // workflow yields at sleep, expires_at=142ms absolute
        harness.advance_time(Duration::from_millis(100)).await; // clock=142ms, fire the timer
        harness.tick().await; // workflow resumes

        let result = harness.get_result_json().await;
        assert_eq!(
            json!(142),
            result["ms"],
            "sleep(new Date(142)) should wake at the absolute 142ms: {result}"
        );
        drop(harness);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_auto_locking_upgrades_modified_sleeping_workflow(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;
        let sim_clock = SimClock::epoch();
        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "test-auto-locking");
        let original_js_source = r"
        export default function test_auto_locking(params) {
            obelisk.sleep({ milliseconds: 10 });
            obelisk.sleep({ milliseconds: 10 });
            return 'ok';
        }";
        let first_upgrade_js_source = r"
        export default function test_auto_locking(params) {
            obelisk.sleep({ milliseconds: 10 });
            obelisk.sleep({ milliseconds: 10 });
            return 'first-upgrade';
        }";
        let second_upgrade_js_source = r"
        export default function test_auto_locking(params) {
            obelisk.sleep({ milliseconds: 10 });
            obelisk.sleep({ milliseconds: 10 });
            return 'second-upgrade';
        }";

        let fn_registry: Arc<dyn FunctionRegistry> =
            TestingFnRegistry::new_from_components(Vec::new());
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (original_worker, original_component_id, _original_runnable) =
            compile_js_workflow_worker(
                original_js_source,
                &user_ffqn,
                db_pool.clone(),
                sim_clock.clone_box(),
                fn_registry.clone(),
                workflow_engine.clone(),
            );
        let (first_upgrade_worker, first_upgrade_component_id, _first_upgrade_runnable) =
            compile_js_workflow_worker(
                first_upgrade_js_source,
                &user_ffqn,
                db_pool.clone(),
                sim_clock.clone_box(),
                fn_registry.clone(),
                workflow_engine.clone(),
            );
        let (second_upgrade_worker, second_upgrade_component_id, _second_upgrade_runnable) =
            compile_js_workflow_worker(
                second_upgrade_js_source,
                &user_ffqn,
                db_pool.clone(),
                sim_clock.clone_box(),
                fn_registry,
                workflow_engine,
            );
        assert_ne!(
            original_component_id.component_digest,
            first_upgrade_component_id.component_digest
        );
        assert_ne!(
            first_upgrade_component_id.component_digest,
            second_upgrade_component_id.component_digest
        );

        let (original_exec, _original_close_tx) =
            new_js_workflow_exec_task_with_locking_strategy_and_executor_id(
                original_worker,
                sim_clock.clone_box(),
                db_pool.clone(),
                LockingStrategy::Auto,
                ExecutorId::from_parts(0, 9004),
            );
        let (first_upgrade_exec, _first_upgrade_close_tx) =
            new_js_workflow_exec_task_with_locking_strategy_and_executor_id(
                first_upgrade_worker,
                sim_clock.clone_box(),
                db_pool.clone(),
                LockingStrategy::Auto,
                ExecutorId::from_parts(0, 9005),
            );
        let (second_upgrade_exec, _second_upgrade_close_tx) =
            new_js_workflow_exec_task_with_locking_strategy_and_executor_id(
                second_upgrade_worker,
                sim_clock.clone_box(),
                db_pool.clone(),
                LockingStrategy::Auto,
                ExecutorId::from_parts(0, 9006),
            );

        let execution_id = ExecutionId::from_parts(0, 9004);
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: original_component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        assert_eq!(
            1,
            original_exec
                .tick_test_await(sim_clock.now(), RunId::from_parts(0, 9004))
                .await
                .len()
        );
        let first_pending_state = db_connection
            .get_pending_state(&execution_id)
            .await
            .unwrap();
        assert_eq!(
            original_component_id.component_digest,
            first_pending_state.component_digest
        );
        assert_matches!(
            first_pending_state.pending_state,
            PendingState::BlockedByJoinSet(..)
        );
        let original_log = db_connection.get(&execution_id).await.unwrap();
        assert_eq!(5, original_log.events.len());
        assert_matches!(
            &original_log.events[0].event,
            ExecutionRequest::Created { .. }
        );
        assert_matches!(&original_log.events[1].event, ExecutionRequest::Locked(_));
        assert_matches!(
            &original_log.events[2].event,
            ExecutionRequest::HistoryEvent {
                event: HistoryEvent::JoinSetCreate { .. }
            }
        );
        assert_matches!(
            &original_log.events[3].event,
            ExecutionRequest::HistoryEvent {
                event: HistoryEvent::JoinSetRequest {
                    request: JoinSetRequest::DelayRequest { .. },
                    ..
                }
            }
        );
        assert_matches!(
            &original_log.events[4].event,
            ExecutionRequest::HistoryEvent {
                event: HistoryEvent::JoinNext { .. }
            }
        );

        sim_clock.move_time_forward(Duration::from_millis(10));
        assert_eq!(
            1,
            expired_timers_watcher::tick_test(db_connection.as_ref(), sim_clock.now())
                .await
                .unwrap()
                .expired_async_timers
        );

        assert_eq!(
            1,
            first_upgrade_exec
                .tick_test_await(sim_clock.now(), RunId::from_parts(0, 9005))
                .await
                .len()
        );
        let upgraded_pending_state = db_connection
            .get_pending_state(&execution_id)
            .await
            .unwrap();
        assert_eq!(
            first_upgrade_component_id.component_digest,
            upgraded_pending_state.component_digest
        );
        assert_matches!(
            upgraded_pending_state.pending_state,
            PendingState::BlockedByJoinSet(..)
        );
        let first_upgrade_log = db_connection.get(&execution_id).await.unwrap();
        assert_eq!(10, first_upgrade_log.events.len());
        assert!(
            first_upgrade_log
                .events
                .iter()
                .all(|event| !matches!(event.event, ExecutionRequest::Unlocked(_))),
            "blocked auto-upgrade must not append Unlocked: {:?}",
            first_upgrade_log.events
        );
        assert_matches!(
            &first_upgrade_log.events[5].event,
            ExecutionRequest::Locked(_)
        );
        assert_matches!(
            &first_upgrade_log.events[6].event,
            ExecutionRequest::ComponentUpgradeFinished {
                component_digest,
                outcome: ComponentUpgradeOutcome::Success {
                    reason: ComponentUpgradeReason::Auto,
                },
                ..
            } => {
                assert_eq!(&first_upgrade_component_id.component_digest, component_digest);
            }
        );
        assert_matches!(
            &first_upgrade_log.events[7].event,
            ExecutionRequest::HistoryEvent {
                event: HistoryEvent::JoinSetCreate { .. }
            }
        );
        assert_matches!(
            &first_upgrade_log.events[8].event,
            ExecutionRequest::HistoryEvent {
                event: HistoryEvent::JoinSetRequest {
                    request: JoinSetRequest::DelayRequest { .. },
                    ..
                }
            }
        );
        assert_matches!(
            &first_upgrade_log.events[9].event,
            ExecutionRequest::HistoryEvent {
                event: HistoryEvent::JoinNext { .. }
            }
        );
        sim_clock.move_time_forward(Duration::from_millis(10));
        assert_eq!(
            1,
            expired_timers_watcher::tick_test(db_connection.as_ref(), sim_clock.now())
                .await
                .unwrap()
                .expired_async_timers
        );
        assert_eq!(
            1,
            second_upgrade_exec
                .tick_test_await(sim_clock.now(), RunId::from_parts(0, 9006))
                .await
                .len()
        );
        let second_upgrade_log = db_connection.get(&execution_id).await.unwrap();
        assert_eq!(13, second_upgrade_log.events.len());
        assert!(
            second_upgrade_log
                .events
                .iter()
                .all(|event| !matches!(event.event, ExecutionRequest::Unlocked(_))),
            "finished auto-upgrade must not append Unlocked: {:?}",
            second_upgrade_log.events
        );
        assert_matches!(
            &second_upgrade_log.events[10].event,
            ExecutionRequest::Locked(_)
        );
        assert_matches!(
            &second_upgrade_log.events[11].event,
            ExecutionRequest::ComponentUpgradeFinished {
                component_digest,
                outcome: ComponentUpgradeOutcome::Success {
                    reason: ComponentUpgradeReason::Auto,
                },
                ..
            } => {
                assert_eq!(&second_upgrade_component_id.component_digest, component_digest);
            }
        );
        assert_matches!(
            &second_upgrade_log.events[12].event,
            ExecutionRequest::Finished { .. }
        );

        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_auto_locking_upgrade_failure_records_failed_outcome(database: Database) {
        use crate::activity::activity_worker::test::compile_activity_stub;

        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;
        let sim_clock = SimClock::epoch();
        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "test-auto-locking-failure");
        let original_deployment_id = DeploymentId::from_parts(0, 9009);
        let upgrade_deployment_id = DeploymentId::from_parts(0, 9010);
        let original_js_source = r"
        export default function test_auto_locking_failure(params) {
            obelisk.sleep({ milliseconds: 10 });
            return 'old';
        }";
        let failing_upgrade_js_source = r"
        export default function test_auto_locking_failure(params) {
            const js = obelisk.createJoinSet();
            js.submit('testing:stub-activity/activity.foo', ['test-input']);
            return 'new';
        }";

        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY)
                .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (original_worker, original_component_id, _original_runnable) =
            compile_js_workflow_worker_with_deployment_id(
                original_js_source,
                &user_ffqn,
                db_pool.clone(),
                sim_clock.clone_box().as_ref(),
                fn_registry.clone(),
                workflow_engine.clone(),
                original_deployment_id,
                JoinNextBlockingStrategy::Interrupt,
                Arc::new(DeadlineTrackerFactoryTokio::new(
                    Duration::ZERO,
                    sim_clock.clone_box(),
                )),
            );
        let (upgrade_worker, upgrade_component_id, _upgrade_runnable) =
            compile_js_workflow_worker_with_deployment_id(
                failing_upgrade_js_source,
                &user_ffqn,
                db_pool.clone(),
                sim_clock.clone_box().as_ref(),
                fn_registry,
                workflow_engine,
                upgrade_deployment_id,
                JoinNextBlockingStrategy::Interrupt,
                Arc::new(DeadlineTrackerFactoryTokio::new(
                    Duration::ZERO,
                    sim_clock.clone_box(),
                )),
            );
        assert_ne!(
            original_component_id.component_digest,
            upgrade_component_id.component_digest
        );

        let (original_exec, _original_close_tx) =
            new_js_workflow_exec_task_with_locking_strategy_and_executor_id(
                original_worker,
                sim_clock.clone_box(),
                db_pool.clone(),
                LockingStrategy::Auto,
                ExecutorId::from_parts(0, 9009),
            );
        let (upgrade_exec, _upgrade_close_tx) =
            new_js_workflow_exec_task_with_locking_strategy_and_executor_id(
                upgrade_worker,
                sim_clock.clone_box(),
                db_pool.clone(),
                LockingStrategy::Auto,
                ExecutorId::from_parts(0, 9010),
            );

        let execution_id = ExecutionId::from_parts(0, 9010);
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: original_component_id.clone(),
                deployment_id: original_deployment_id,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        assert_eq!(
            1,
            original_exec
                .tick_test_await(sim_clock.now(), RunId::from_parts(0, 9009))
                .await
                .len()
        );
        sim_clock.move_time_forward(Duration::from_millis(10));
        assert_eq!(
            1,
            expired_timers_watcher::tick_test(db_connection.as_ref(), sim_clock.now())
                .await
                .unwrap()
                .expired_async_timers
        );

        assert_eq!(
            1,
            upgrade_exec
                .tick_test_await(sim_clock.now(), RunId::from_parts(0, 9010))
                .await
                .len()
        );

        let log = db_connection.get(&execution_id).await.unwrap();
        assert_eq!(original_deployment_id, log.deployment_id);
        assert_eq!(original_component_id.component_digest, log.component_digest);
        assert_eq!(8, log.events.len());
        assert_matches!(&log.events[0].event, ExecutionRequest::Created { .. });
        assert_matches!(&log.events[1].event, ExecutionRequest::Locked(_));
        assert_matches!(&log.events[5].event, ExecutionRequest::Locked(_));
        assert_matches!(
            &log.events[6].event,
            ExecutionRequest::ComponentUpgradeFinished {
                component_digest,
                outcome: ComponentUpgradeOutcome::Failed { reason },
                ..
            } => {
                assert_eq!(&upgrade_component_id.component_digest, component_digest);
                assert!(
                    !reason.as_ref().is_empty(),
                    "unexpected failure reason: {reason}"
                );
            }
        );
        assert_matches!(
            &log.events[7].event,
            ExecutionRequest::Unlocked(unlocked) => {
                assert_eq!("auto-upgrade failed", unlocked.reason.as_ref());
            }
        );
        assert_matches!(log.pending_state, PendingState::PendingAt(..));

        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_auto_locking_upgrade_through_stub_write_appends_unlocked(
        database: Database,
    ) {
        use crate::activity::activity_worker::test::compile_activity_stub;

        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;
        let sim_clock = SimClock::epoch();
        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "test-auto-locking-stub");
        let original_js_source = r"
        export default function test_auto_locking_stub(params) {
            return 'old';
        }";
        let stub_upgrade_js_source = r"
        export default function test_auto_locking_stub(params) {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.foo', ['test-input']);
            obelisk.stub(execId, { 'ok': 'stubbed-by-upgrade' });
            return js.joinNext();
        }";

        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY)
                .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (_original_worker, original_component_id, _original_runnable) =
            compile_js_workflow_worker(
                original_js_source,
                &user_ffqn,
                db_pool.clone(),
                sim_clock.clone_box(),
                fn_registry.clone(),
                workflow_engine.clone(),
            );
        let (upgrade_worker, upgrade_component_id, _upgrade_runnable) = compile_js_workflow_worker(
            stub_upgrade_js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry,
            workflow_engine,
        );
        assert_ne!(
            original_component_id.component_digest,
            upgrade_component_id.component_digest
        );

        let (upgrade_exec, _upgrade_close_tx) =
            new_js_workflow_exec_task_with_locking_strategy_and_executor_id(
                upgrade_worker,
                sim_clock.clone_box(),
                db_pool.clone(),
                LockingStrategy::Auto,
                ExecutorId::from_parts(0, 9011),
            );

        let execution_id = ExecutionId::from_parts(0, 9011);
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: original_component_id,
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        assert_eq!(
            1,
            upgrade_exec
                .tick_test_await(sim_clock.now(), RunId::from_parts(0, 9011))
                .await
                .len()
        );

        let pending_state = db_connection
            .get_pending_state(&execution_id)
            .await
            .unwrap();
        assert_eq!(
            upgrade_component_id.component_digest,
            pending_state.component_digest
        );
        assert_matches!(pending_state.pending_state, PendingState::PendingAt(..));

        let log = db_connection.get(&execution_id).await.unwrap();
        assert_eq!(6, log.events.len());
        assert_eq!(
            1,
            log.responses.len(),
            "stub write should append a response"
        );
        assert_matches!(&log.events[0].event, ExecutionRequest::Created { .. });
        assert_matches!(&log.events[1].event, ExecutionRequest::Locked(_));
        assert_matches!(
            &log.events[2].event,
            ExecutionRequest::ComponentUpgradeFinished {
                component_digest,
                outcome: ComponentUpgradeOutcome::Success {
                    reason: ComponentUpgradeReason::Auto,
                },
                ..
            } => {
                assert_eq!(&upgrade_component_id.component_digest, component_digest);
            }
        );
        assert_matches!(
            &log.events[3].event,
            ExecutionRequest::HistoryEvent {
                event: HistoryEvent::JoinSetCreate { .. }
            }
        );
        assert_matches!(
            &log.events[4].event,
            ExecutionRequest::HistoryEvent {
                event: HistoryEvent::JoinSetRequest {
                    request: JoinSetRequest::ChildExecutionRequest { .. },
                    ..
                }
            }
        );
        assert_matches!(
            &log.events[5].event,
            ExecutionRequest::Unlocked(unlocked) => {
                assert_eq!("auto-upgrade succeeded", unlocked.reason.as_ref());
            }
        );
        assert_matches!(
            &log.responses[0].event.event.event,
            JoinSetResponse::ChildExecutionFinished { result, .. } => {
                let ok = assert_matches!(result, SupportedFunctionReturnValue::Ok(Some(ok)) => ok);
                assert_eq!(WastVal::String("stubbed-by-upgrade".into()), ok.value);
            }
        );

        drop(db_connection);
        db_close.close().await;
    }

    /// Test: replay of a JS workflow that creates a join set and returns.
    /// Replays at three stages:
    /// 1. Just after creation (no events in DB) — preview returns `JoinSetCreate` + finished result.
    /// 2. After `JoinSetCreate` event is manually inserted but execution is not finished — preview returns finished result.
    /// 3. After the execution is fully done — replay returns finished result, no `next_events`.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_replay_create_join_set(database: Database) {
        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r#"
        export default function test_replay(params) {
            const js = obelisk.createJoinSet();
            return "done-" + js.id();
        }"#;

        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "test-replay");
        let sim_clock = SimClock::epoch();

        let fn_registry: Arc<dyn FunctionRegistry> =
            TestingFnRegistry::new_from_components(Vec::new());
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        );

        let (workflow_exec, _workflow_close_tx) =
            new_js_workflow_exec_task(worker, sim_clock.clone_box(), db_pool.clone());

        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        let params = Params::from_json_values_test(vec![json!(Vec::<String>::new())]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params,
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        // --- Stage 1: Replay on just-created execution (no events in DB) ---
        let (log_sender, _log_recv) = mpsc::channel(100);
        let replay_worker = build_js_replay_worker(
            DeploymentId::generate(),
            component_id.clone(),
            &runnable_component,
            workflow_engine.clone(),
            fn_registry.clone(),
            db_pool.clone(),
            Some(LogStrageConfig {
                min_level: concepts::storage::LogLevel::Debug,
                log_sender: log_sender.clone(),
            }),
            sim_clock.clone_box(),
            js_source.to_string(),
            default_return_type(),
            None, // max_replay_captured_writes
        );
        let replay = replay_worker
            .replay(execution_id.clone(), BacktraceCapture::Disabled)
            .await
            .unwrap();

        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        info!("Stage 1 replay: {replay:?}");
        assert!(
            replay.get_return_value().is_some(),
            "workflow should complete during preview (non-blocking events only)"
        );
        let next_events = replay.history_events();
        assert!(
            !next_events.is_empty(),
            "preview should contain at least the JoinSetCreate event"
        );
        assert_matches!(&next_events[0], HistoryEvent::JoinSetCreate { .. });

        // --- Stage 2: Manually insert JoinSetCreate event, execution not finished ---
        // Extract the JoinSetCreate event from the replay preview and insert it.
        use concepts::storage::AppendRequest;
        let join_set_create_event = next_events[0].clone();
        db_connection
            .append_batch(
                sim_clock.now(),
                vec![AppendRequest {
                    created_at: sim_clock.now(),
                    event: ExecutionRequest::HistoryEvent {
                        event: join_set_create_event.clone(),
                    },
                }],
                execution_id.clone(),
                Version::new(1),
            )
            .await
            .unwrap();

        let replay2 = replay_worker
            .replay(execution_id.clone(), BacktraceCapture::Disabled)
            .await
            .unwrap();

        let replay2 = assert_matches!(replay2, ReplayResponse::Advanceable(replay2) => replay2);
        info!("Stage 2 replay: {replay2:?}");
        assert!(
            replay2.get_return_value().is_some(),
            "workflow should complete even with partial event log (JoinSetCreate already present)"
        );
        // The JoinSetCreate is already in the log, so it should not appear as a next_event.
        assert!(
            !replay2
                .history_events()
                .iter()
                .any(|e| matches!(e, HistoryEvent::JoinSetCreate { .. })),
            "JoinSetCreate should not appear in next_events since it's already in the log"
        );

        // --- Stage 3: Execute the workflow fully, then replay ---
        // Tick the execution (which already has JoinSetCreate in the log) to completion.
        assert_eq!(
            1,
            workflow_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await
                .len()
        );

        let res = db_connection
            .get_finished_result(&execution_id)
            .await
            .unwrap();
        let ok_val = assert_matches!(&res, SupportedFunctionReturnValue::Ok(Some(val)) => val);
        let result_str = assert_matches!(&ok_val.value, WastVal::String(s) => s);
        assert!(
            result_str.starts_with("done-"),
            "unexpected result: {result_str}"
        );

        let replay3 = replay_worker
            .replay(execution_id.clone(), BacktraceCapture::Disabled)
            .await
            .unwrap();

        let result = assert_matches!(replay3, ReplayResponse::Finished { result } => result);
        info!("Stage 3 replay result: {result:?}");
        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_replay_call_stub(database: Database) {
        use crate::activity::activity_worker::test::compile_activity_stub;

        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function call_stub() {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.foo', ['test-input']);
            obelisk.stub(execId, { 'ok': 'hello' });
            return js.joinNext();
        }";

        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "call-stub");
        let sim_clock = SimClock::epoch();

        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY)
                .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        );

        let (workflow_exec, _workflow_close_tx) =
            new_js_workflow_exec_task(worker, sim_clock.clone_box(), db_pool.clone());

        let execution_id = ExecutionId::from_parts(0, 0);
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();

        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        // replay on just-created execution
        let (log_sender, _log_recv) = mpsc::channel(100);
        let replay_worker = build_js_replay_worker(
            DeploymentId::generate(),
            component_id.clone(),
            &runnable_component,
            workflow_engine.clone(),
            fn_registry.clone(),
            db_pool.clone(),
            Some(LogStrageConfig {
                min_level: concepts::storage::LogLevel::Debug,
                log_sender: log_sender.clone(),
            }),
            sim_clock.clone_box(),
            js_source.to_string(),
            default_return_type(),
            None, // max_replay_captured_writes
        );
        let replay = replay_worker
            .replay(execution_id.clone(), BacktraceCapture::Disabled)
            .await
            .unwrap();

        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        // With FlushedCache interruption, replay stops after the first cache flush.
        // The first flush covers JoinSetCreate and child execution submit.
        assert_eq!(2, replay.history_events().len());
        insta::with_settings!({
            prepend_module_to_snapshot => false},
            {
                assert_json_snapshot!(replay.history_events());
            }
        );

        // Tick - should end with JoinNext
        // Tick the execution (which already has JoinSetCreate in the log) to completion.
        assert_eq!(
            1,
            workflow_exec
                .tick_test_await(sim_clock.now(), RunId::generate())
                .await
                .len()
        );
        // Replay should return the return value
        let replay = replay_worker
            .replay(execution_id.clone(), BacktraceCapture::Disabled)
            .await
            .unwrap();

        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        let retval = replay
            .get_return_value()
            .expect("retval should be computed");
        let retval = assert_matches!(retval, SupportedFunctionReturnValue::Ok(Some(WastValWithType{ r#type: _, value })) => value);
        assert_eq!(WastVal::String("hello".into()), *retval);
        drop(db_connection);
        db_close.close().await;
    }

    /// A workflow that submits a stub whose response is never injected, then polls it forever with
    /// `joinNextTry()`. During replay the poll never blocks, so replay would collect captured writes
    /// indefinitely. `max_replay_captured_writes` bounds a single pass: replay returns the first N
    /// writes as an advanceable prefix (instead of looping forever or erroring), and advancing them
    /// then replaying again resumes from the persisted tip, stepping the workflow forward N at a time.
    #[tokio::test]
    async fn workflow_js_replay_join_next_try_loop_bounds_captured_writes() {
        use crate::activity::activity_worker::test::compile_activity_stub;

        const MAX: usize = 5;

        test_utils::set_up();
        let (_guard, db_pool, db_close) = Database::Sqlite.set_up().await;

        let js_source = r"
        export default function loop_forever() {
            const js = obelisk.createJoinSet();
            js.submit('testing:stub-activity/activity.foo', ['test-input']);
            for (;;) {
                const result = js.joinNextTry();
                if (result !== undefined) {
                    throw 'stub result was injected unexpectedly';
                }
            }
        }";

        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "loop-forever");
        let sim_clock = SimClock::epoch();

        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY)
                .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (_worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        );

        let execution_id = ExecutionId::from_parts(0, 0);
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let replay_worker = build_js_replay_worker(
            DeploymentId::generate(),
            component_id,
            &runnable_component,
            workflow_engine,
            fn_registry,
            db_pool,
            None,
            sim_clock.clone_box(),
            js_source.to_string(),
            default_return_type(),
            Some(MAX),
        );

        // Replay bounds the pass to MAX captured writes and returns them as an advanceable prefix
        // rather than looping forever.
        let replay = replay_worker
            .replay(execution_id.clone(), BacktraceCapture::Disabled)
            .await
            .unwrap();
        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        assert_eq!(replay.captured_writes.len(), MAX);

        // Advancing the prefix persists it; a fresh replay resumes past the tip and yields the next
        // bounded batch, so the never-terminating workflow keeps making progress without erroring.
        replay_worker
            .advance(execution_id.clone(), replay, BacktraceCapture::Disabled)
            .await
            .unwrap();
        let replay2 = replay_worker
            .replay(execution_id, BacktraceCapture::Disabled)
            .await
            .unwrap();
        let replay2 = assert_matches!(replay2, ReplayResponse::Advanceable(replay2) => replay2);
        assert_eq!(replay2.captured_writes.len(), MAX);

        drop(db_connection);
        db_close.close().await;
    }

    #[tokio::test]
    async fn workflow_js_real_run_event_limit_yields_and_unlocks() {
        use crate::activity::activity_worker::test::compile_activity_stub;

        const MAX: usize = 5;

        test_utils::set_up();
        let (_guard, db_pool, db_close) = Database::Sqlite.set_up().await;
        let js_source = r"
        export default function loop_forever() {
            const js = obelisk.createJoinSet();
            js.submit('testing:stub-activity/activity.foo', ['test-input']);
            for (;;) {
                js.joinNextTry();
            }
        }";
        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "loop-forever");
        let sim_clock = SimClock::epoch();
        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY)
                .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (worker, component_id, _) =
            compile_js_workflow_worker_with_deployment_id_and_return_type(
                js_source,
                &user_ffqn,
                db_pool.clone(),
                &sim_clock,
                fn_registry,
                workflow_engine,
                DEPLOYMENT_ID_DUMMY,
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: 10,
                    subscription_interruption: None,
                },
                deadline_tracker_factory_test(&sim_clock),
                default_return_type(),
                MAX,
                usize::MAX,
            );
        let (workflow_exec, _close_tx) =
            new_js_workflow_exec_task(worker, sim_clock.clone_box(), db_pool.clone());
        let execution_id = ExecutionId::from_parts(0, 42);
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id,
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        for expected_history_events in [MAX, MAX * 2] {
            assert_eq!(
                1,
                workflow_exec
                    .tick_test_await(sim_clock.now(), RunId::generate())
                    .await
                    .len()
            );
            let log = db_connection.get(&execution_id).await.unwrap();
            assert_eq!(expected_history_events, log.event_history().count());
            assert_matches!(
                &log.events.last().unwrap().event,
                ExecutionRequest::Unlocked(unlocked)
                    if unlocked.reason.as_ref() == "workflow event limit reached"
            );
            assert_matches!(log.pending_state, PendingState::PendingAt(..));
        }

        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn workflow_js_hot_run_refreshes_responses(database: Database) {
        use crate::activity::activity_worker::test::compile_activity_stub;

        const MAX_EVENTS_PER_RUN: usize = 10_000;
        const RESPONSE_REFRESH_INTERVAL: usize = 4;

        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;
        let js_source = r"
        export default function poll_until_ready() {
            const js = obelisk.createJoinSet();
            js.submit('testing:stub-activity/activity.foo', ['test-input']);
            for (;;) {
                if (js.joinNextTry() !== undefined) {
                    return 'refreshed';
                }
            }
        }";
        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "poll-until-ready");
        let sim_clock = SimClock::epoch();
        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY)
                .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (worker, component_id, _) =
            compile_js_workflow_worker_with_deployment_id_and_return_type(
                js_source,
                &user_ffqn,
                db_pool.clone(),
                &sim_clock,
                fn_registry,
                workflow_engine,
                DEPLOYMENT_ID_DUMMY,
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: 100,
                    subscription_interruption: None,
                },
                deadline_tracker_factory_test(&sim_clock),
                default_return_type(),
                MAX_EVENTS_PER_RUN,
                RESPONSE_REFRESH_INTERVAL,
            );
        let (workflow_exec, _close_tx) =
            new_js_workflow_exec_task(worker, sim_clock.clone_box(), db_pool.clone());
        let execution_id = ExecutionId::from_parts(0, 43);
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id,
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();

        let progress = workflow_exec
            .tick_test(sim_clock.now(), RunId::generate())
            .await;
        let stub_execution_id = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let log = db_connection.get(&execution_id).await.unwrap();
                if let Some(child_execution_id) = log.events.iter().find_map(|event| {
                    if let ExecutionRequest::HistoryEvent {
                        event:
                            HistoryEvent::JoinSetRequest {
                                request:
                                    JoinSetRequest::ChildExecutionRequest {
                                        child_execution_id, ..
                                    },
                                ..
                            },
                    } = &event.event
                    {
                        Some(child_execution_id.clone())
                    } else {
                        None
                    }
                }) {
                    break child_execution_id;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("response refresh must flush the submitted child");
        write_stub_response(
            db_connection.as_ref(),
            sim_clock.now(),
            stub_execution_id,
            SupportedFunctionReturnValue::Ok(None),
        )
        .await;

        tokio::time::timeout(Duration::from_secs(5), progress.wait_for_tasks())
            .await
            .expect("hot workflow must observe the refreshed response");
        let log = db_connection.get(&execution_id).await.unwrap();
        assert!(
            log.pending_state.is_finished(),
            "state: {:?}",
            log.pending_state
        );
        assert!(
            log.events
                .iter()
                .all(|event| !matches!(event.event, ExecutionRequest::Unlocked(_))),
            "the workflow should finish within one run: {:?}",
            log.events
        );

        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[case::full(None, "full", 0)]
    #[case::trimmed_to_1(Some(1), "trimmed_to_1", 1)]
    #[tokio::test]
    async fn workflow_js_advance_call_stub(
        database: Database,
        #[case] trim_to: Option<usize>,
        #[case] snapshot_suffix: &str,
        #[case] execution_idx: u16,
    ) {
        use crate::activity::activity_worker::test::compile_activity_stub;

        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function call_stub() {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.foo', ['test-input']);
            obelisk.stub(execId, { 'ok': 'hello' });
            return js.joinNext();
        }";

        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "call-stub");
        let sim_clock = SimClock::epoch();

        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY)
                .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (_worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        );

        let db_connection = db_pool.connection_test().await.unwrap();

        let (log_sender, _log_recv) = mpsc::channel(100);
        let logs_storage_config = Some(LogStrageConfig {
            min_level: concepts::storage::LogLevel::Debug,
            log_sender: log_sender.clone(),
        });

        let deployment_id = DeploymentId::from_parts(0, 0);
        let create_paused_execution = |execution_id: ExecutionId| CreateRequest {
            created_at: sim_clock.now(),
            execution_id,
            ffqn: user_ffqn.clone(),
            params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
            parent: None,
            metadata: ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            component_id: component_id.clone(),
            deployment_id: DEPLOYMENT_ID_DUMMY,
            scheduled_by: None,
            paused: true,
        };

        let execution_id = ExecutionId::from_parts(0, execution_idx.into());
        db_connection
            .create(create_paused_execution(execution_id.clone()))
            .await
            .unwrap();
        let replay_worker = Arc::new(build_js_replay_worker(
            deployment_id,
            component_id,
            &runnable_component,
            workflow_engine,
            fn_registry,
            db_pool.clone(),
            logs_storage_config,
            sim_clock.clone_box(),
            js_source.to_string(),
            default_return_type(),
            None, // max_replay_captured_writes
        ));
        let result = workflow_js_step_execution_until_finished(
            &*db_connection,
            WorkflowJsAdvanceHarness {
                db_pool,
                execution_id,
                sim_clock,
                idle_action: None,
                replay_worker,
            },
            snapshot_suffix,
            16,
            trim_to,
        )
        .await;

        assert_matches!(
            result,
            SupportedFunctionReturnValue::Ok(Some(WastValWithType { value: WastVal::String(s), .. }))
                if s == "hello"
        );

        drop(db_connection);
        db_close.close().await;
    }

    /// The stub call site backtrace must be keyed to the parent's future `HistoryEvent::Stub`
    /// version, not the stub child's `1..2` domain, so `persist_execution_backtraces` skips it while
    /// the event is unpersisted instead of storing it against the parent.
    #[expand_enum_database]
    #[rstest]
    #[tokio::test]
    async fn capture_backtraces_keys_stub_to_future_history_event(database: Database) {
        use crate::activity::activity_worker::test::compile_activity_stub;

        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function call_stub() {
            const js = obelisk.createJoinSet();
            const execId = js.submit('testing:stub-activity/activity.foo', ['test-input']);
            obelisk.stub(execId, { 'ok': 'hello' });
            return js.joinNext();
        }";

        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "call-stub");
        let sim_clock = SimClock::epoch();

        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity_stub(test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY)
                .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (_worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        );

        let db_connection = db_pool.connection_test().await.unwrap();
        let execution_id = ExecutionId::from_parts(0, 0);
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: user_ffqn.clone(),
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let (log_sender, _log_recv) = mpsc::channel(100);
        let replay_worker = build_js_replay_worker(
            DeploymentId::from_parts(0, 0),
            component_id,
            &runnable_component,
            workflow_engine,
            fn_registry,
            db_pool.clone(),
            Some(LogStrageConfig {
                min_level: concepts::storage::LogLevel::Debug,
                log_sender,
            }),
            sim_clock.clone_box(),
            js_source.to_string(),
            default_return_type(),
            None, // max_replay_captured_writes
        );

        // The paused workflow has not executed, so every backtrace the replay captures belongs to a
        // future log entry (at or after `next_version`), including the stub's display backtrace.
        let next_version = db_connection.get(&execution_id).await.unwrap().next_version;
        let captured = replay_worker
            .capture_backtraces(execution_id.clone())
            .await
            .unwrap();
        assert!(!captured.is_empty(), "replay must capture backtraces");
        assert!(
            captured
                .iter()
                .all(|bt| bt.version_min_including >= next_version),
            "paused workflow: every captured backtrace targets a future log entry",
        );

        // Persisting must trim those future backtraces. The stub used to key its backtrace to the
        // child's `1..2` range, which persist would treat as already-written and store against the
        // parent; the fix keys it to the future `HistoryEvent::Stub`, so nothing is persisted here.
        let persisted = replay_worker
            .persist_backtraces(execution_id.clone())
            .await
            .unwrap();
        assert_eq!(
            persisted, 0,
            "future backtraces must be trimmed, not persisted"
        );

        drop(db_connection);
        db_close.close().await;
    }

    #[expand_enum_database]
    #[rstest]
    #[case::full(None, "submit_cancel_full", 10)]
    #[case::trimmed_to_1(Some(1), "submit_cancel_trimmed_to_1", 11)]
    #[tokio::test]
    async fn workflow_js_advance_submit_without_await_cancels_child_activity(
        database: Database,
        #[case] trim_to: Option<usize>,
        #[case] test_name: &str,
        #[case] execution_idx: u16,
    ) {
        use concepts::ExecutionFailureKind;

        test_utils::set_up();
        let (_guard, db_pool, db_close) = database.set_up().await;

        let js_source = r"
        export default function submit_and_return() {
            const js = obelisk.createJoinSet();
            js.submit('testing:fibo/fibo.fibo', [10]);
            return 'done';
        }";

        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "submit-and-return");
        let sim_clock = SimClock::epoch();

        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();

        let (_worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        );

        let db_connection = db_pool.connection_test().await.unwrap();

        let deployment_id = DeploymentId::from_parts(0, 0);
        let execution_id = ExecutionId::from_parts(0, execution_idx.into());
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: user_ffqn.clone(),
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let replay_worker = Arc::new(build_js_replay_worker(
            deployment_id,
            component_id,
            &runnable_component,
            workflow_engine,
            fn_registry,
            db_pool.clone(),
            None,
            sim_clock.clone_box(),
            js_source.to_string(),
            default_return_type(),
            None, // max_replay_captured_writes
        ));
        let result = workflow_js_step_execution_until_finished(
            &*db_connection,
            WorkflowJsAdvanceHarness {
                db_pool: db_pool.clone(),
                execution_id: execution_id.clone(),
                sim_clock,
                idle_action: None,
                replay_worker,
            },
            test_name,
            16,
            trim_to,
        )
        .await;

        assert_matches!(
            result,
            SupportedFunctionReturnValue::Ok(Some(WastValWithType { value: WastVal::String(s), .. }))
                if s == "done"
        );

        let log = db_connection.get(&execution_id).await.unwrap();
        assert_eq!(1, log.responses.len(), "join set should be auto-closed");
        let result = assert_matches!(
            &log.responses[0].event.event.event,
            JoinSetResponse::ChildExecutionFinished { result, .. } => result
        );
        let err =
            assert_matches!(result, SupportedFunctionReturnValue::ExecutionFailure(err) => err);
        assert_matches!(err.kind, ExecutionFailureKind::Cancelled);

        drop(db_connection);
        db_close.close().await;
    }

    #[tokio::test]
    async fn advance_forwards_captured_application_logs() {
        test_utils::set_up();

        let js_source = r"
        export default function () {
            console.info('before sleep');
            obelisk.sleep({ milliseconds: 10 });
            return 'done';
        }";
        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "log-then-sleep");
        let sim_clock = SimClock::epoch();
        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (_worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        );

        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let (log_sender, mut log_recv) = mpsc::channel(16);
        let logs_storage_config = Some(LogStrageConfig {
            min_level: LogLevel::Debug,
            log_sender,
        });

        let replay_worker = build_js_replay_worker(
            DeploymentId::generate(),
            component_id,
            &runnable_component,
            workflow_engine,
            fn_registry,
            db_pool,
            logs_storage_config,
            sim_clock.clone_box(),
            js_source.to_string(),
            default_return_type(),
            None, // max_replay_captured_writes
        );
        let replay = replay_worker
            .replay(execution_id.clone(), BacktraceCapture::Disabled)
            .await
            .unwrap();
        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        assert_matches!(
            log_recv.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );

        replay_worker
            .advance(execution_id, replay, BacktraceCapture::Disabled)
            .await
            .unwrap();

        assert_matches!(
            log_recv.try_recv(),
            Ok(LogInfoAppendRow {
                log_entry: LogEntry::Log {
                    level: LogLevel::Info,
                    message,
                    ..
                },
                ..
            }) if message == "before sleep"
        );
        assert_matches!(
            log_recv.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty
                | tokio::sync::mpsc::error::TryRecvError::Disconnected)
        );
    }

    #[tokio::test]
    async fn advance_trimmed_writes_forward_only_prefix_logs() {
        test_utils::set_up();

        let js_source = r"
        export default function () {
            console.info('before create');
            const js = obelisk.createJoinSet();
            console.info('before delay');
            js.submitDelay({ milliseconds: 10 });
            console.info('before join');
            js.joinNext();
            return 'done';
        }";
        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "trimmed-log-prefix");
        let sim_clock = SimClock::epoch();
        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (_worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        );

        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection_test().await.unwrap();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: user_ffqn,
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let (log_sender, mut log_recv) = mpsc::channel(16);
        let logs_storage_config = Some(LogStrageConfig {
            min_level: LogLevel::Debug,
            log_sender,
        });

        let replay_worker = build_js_replay_worker(
            DeploymentId::generate(),
            component_id,
            &runnable_component,
            workflow_engine,
            fn_registry,
            db_pool,
            logs_storage_config,
            sim_clock.clone_box(),
            js_source.to_string(),
            default_return_type(),
            None, // max_replay_captured_writes
        );
        let mut forwarded = Vec::new();
        for expected_len in [3_usize, 2, 1] {
            let replay = replay_worker
                .replay(execution_id.clone(), BacktraceCapture::Disabled)
                .await
                .unwrap();
            let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
            assert_eq!(replay.captured_writes.len(), expected_len);

            replay_worker
                .advance(
                    execution_id.clone(),
                    replay.truncate_to(1),
                    BacktraceCapture::Disabled,
                )
                .await
                .unwrap();

            forwarded.extend(drain_forwarded_log_messages(&mut log_recv));
        }

        assert_eq!(
            forwarded,
            vec!["before create", "before delay", "before join"]
        );
        let replay = replay_worker
            .replay(execution_id, BacktraceCapture::Disabled)
            .await
            .unwrap();
        assert_matches!(replay, ReplayResponse::Blocked);
    }

    struct WorkflowJsAdvanceHarness {
        db_pool: Arc<dyn DbPool>,
        execution_id: ExecutionId,
        sim_clock: SimClock,
        idle_action: Option<WorkflowJsAdvanceIdleAction>,
        replay_worker: Arc<WorkflowJsWorker>,
    }

    #[allow(dead_code)]
    #[derive(Clone, Copy)]
    enum WorkflowJsAdvanceIdleAction {
        TickFiboActivity,
    }

    async fn workflow_js_step_execution_until_finished(
        db_connection: &dyn DbConnectionTest,
        harness: WorkflowJsAdvanceHarness,
        test_name: &str,
        max_steps: usize,
        trim_to: Option<usize>,
    ) -> SupportedFunctionReturnValue {
        let mut steps = 0;
        let mut saw_trimmed_preview = false;
        let cancel_registry = CancelRegistry::new();
        loop {
            harness
                .sim_clock
                .move_time_forward(Duration::from_millis(100));
            cancellation_driver::tick_test(
                db_connection,
                &cancel_registry,
                harness.sim_clock.now(),
            )
            .await;
            let replay = harness
                .replay_worker
                .replay(harness.execution_id.clone(), BacktraceCapture::Disabled)
                .await
                .unwrap();
            let replay = match replay {
                ReplayResponse::Advanceable(replay) => replay,
                ReplayResponse::Finished {
                    result: finished_result,
                } => {
                    assert!(
                        steps > 0,
                        "step-through harness must execute at least one replay+advance round",
                    );
                    if trim_to.is_some() {
                        assert!(
                            saw_trimmed_preview,
                            "test must exercise trimmed replay writes"
                        );
                    }
                    return finished_result;
                }
                ReplayResponse::Blocked => match harness.idle_action {
                    Some(WorkflowJsAdvanceIdleAction::TickFiboActivity) => {
                        let (activity_exec, _activity_close_tx) = new_activity_fibo(
                            harness.db_pool.clone(),
                            harness.sim_clock.clone_box(),
                            TokioSleep,
                            LockingStrategy::ByComponentDigest,
                        )
                        .await;
                        activity_exec
                            .tick_test_await(harness.sim_clock.now(), RunId::generate())
                            .await;
                        continue;
                    }
                    None => panic!(
                        "replay must be advanceable or finished while stepping paused JS workflow"
                    ),
                },
            };

            steps += 1;
            insta::with_settings!({
                snapshot_suffix => format!("{test_name}_replay_{steps}"),
                prepend_module_to_snapshot => false},
                {
                    assert_json_snapshot!(
                        redact_component_digest(serde_json::to_value(&replay).unwrap())
                    );
                }
            );

            let requested = match trim_to {
                Some(trim_to) => replay.truncate_to(trim_to),
                None => replay.clone(),
            };
            saw_trimmed_preview |= requested.captured_writes.len() < replay.captured_writes.len();
            harness
                .sim_clock
                .move_time_forward(Duration::from_millis(100));

            let advance = harness
                .replay_worker
                .advance(
                    harness.execution_id.clone(),
                    requested.clone(),
                    BacktraceCapture::Disabled,
                )
                .await
                .unwrap();
            assert_eq!(advance.finished, requested.get_return_value().cloned());

            insta::with_settings!({
                snapshot_suffix => format!("{test_name}_advance_{steps}"),
                prepend_module_to_snapshot => false},
                {
                    assert_json_snapshot!(
                        json!({
                            "finished": advance.finished.is_some(),
                            "requested_captured_writes_len": requested.captured_writes.len(),
                            "replayed_captured_writes_len": replay.captured_writes.len(),
                        })
                    );
                }
            );

            let log = db_connection.get(&harness.execution_id).await.unwrap();
            insta::with_settings!({
                snapshot_suffix => format!("{test_name}_log_{steps}"),
                prepend_module_to_snapshot => false},
                {
                    assert_json_snapshot!(ExecutionLogSanitized::from(log));
                }
            );

            if let Ok(finished_result) = db_connection
                .get_finished_result(&harness.execution_id)
                .await
            {
                assert!(
                    steps > 0,
                    "step-through harness must execute at least one replay+advance round",
                );
                if trim_to.is_some() {
                    assert!(
                        saw_trimmed_preview,
                        "test must exercise trimmed replay writes",
                    );
                }
                return finished_result;
            }

            assert!(
                steps < max_steps,
                "execution did not finish after {steps} replay+advance steps",
            );
        }
    }

    #[tokio::test]
    async fn advance_paused_js_workflow_can_pause_new_child_execution() {
        test_utils::set_up();

        let js_source = "
            export default function (_params) {
                const js = obelisk.createJoinSet();
                js.submit('testing:fibo/fibo.fibo', [10]);
                js.joinNext();
                return 'done';
            }
        ";
        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "pause-child");
        let sim_clock = SimClock::epoch();
        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY)
                .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (_worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        );
        let db_connection = db_pool.connection_test().await.unwrap();
        let execution_id = ExecutionId::from_parts(0, 9002);
        let created_at = sim_clock.now();

        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: user_ffqn.clone(),
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let deployment_id = DeploymentId::from_parts(0, 0);
        sim_clock.move_time_forward(Duration::from_millis(100));
        let replay_worker = build_js_replay_worker(
            deployment_id,
            component_id,
            &runnable_component,
            workflow_engine,
            fn_registry,
            db_pool.clone(),
            None,
            sim_clock.clone_box(),
            js_source.to_string(),
            default_return_type(),
            None, // max_replay_captured_writes
        );
        let replay = replay_worker
            .replay(execution_id.clone(), BacktraceCapture::Disabled)
            .await
            .unwrap();

        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        let mut requested = replay.clone();
        let replayed_child_created_at = requested
            .captured_writes
            .iter()
            .find_map(|write| match write {
                CapturedDbWrite::AppendBatchCreateNewExecution { child_req, .. } => {
                    child_req.first().map(|child| child.created_at)
                }
                _ => None,
            })
            .expect("replay should create a child execution");
        assert_eq!(replayed_child_created_at, sim_clock.now());
        let child_execution_id = requested
            .captured_writes
            .iter_mut()
            .find_map(|write| match write {
                CapturedDbWrite::AppendBatchCreateNewExecution {
                    current_time,
                    batch,
                    child_req,
                    ..
                } => {
                    *current_time = DateTime::UNIX_EPOCH; // modifying time in the advance request should have no effect.
                    for req in batch.iter_mut() {
                        req.created_at = DateTime::UNIX_EPOCH;
                    }
                    for child in child_req.iter_mut() {
                        child.paused = true;
                        child.created_at = DateTime::UNIX_EPOCH;
                        child.scheduled_at = DateTime::UNIX_EPOCH;
                    }
                    child_req.first().map(|child| child.execution_id.clone())
                }
                _ => None,
            })
            .expect("replay should create a child execution");
        sim_clock.move_time_forward(Duration::from_millis(100));

        let advance = replay_worker
            .advance(execution_id, requested, BacktraceCapture::Disabled)
            .await
            .unwrap();

        assert_eq!(advance.finished, None);
        assert_matches!(
            db_connection
                .get_pending_state(&child_execution_id)
                .await
                .unwrap()
                .pending_state,
            PendingState::Paused(_)
        );
        let create_event = db_connection
            .get_execution_event(&child_execution_id, &Version::new(0))
            .await
            .unwrap();
        // Check that the current system time is used.
        assert_eq!(create_event.created_at, sim_clock.now());
        let ExecutionRequest::Created { scheduled_at, .. } = create_event.event else {
            panic!("child execution log must start with Created");
        };
        assert_eq!(scheduled_at, sim_clock.now());
    }

    #[tokio::test]
    async fn advance_paused_js_workflow_uses_server_time_for_relative_schedule() {
        test_utils::set_up();

        let js_source = r"
            export default function (_params) {
                const execId = obelisk.executionIdGenerate();
                obelisk.schedule(
                    execId,
                    'testing:stub-activity/activity.foo',
                    ['scheduled-param'],
                    { minutes: 5 },
                );
                return execId;
            }
        ";
        let user_ffqn = FunctionFqn::new_static("test:pkg/ifc", "schedule-relative");
        let sim_clock = SimClock::epoch();
        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
        let fn_registry: Arc<dyn FunctionRegistry> = TestingFnRegistry::new_from_components(vec![
            crate::activity::activity_worker::test::compile_activity_stub(
                test_programs_stub_activity_builder::TEST_PROGRAMS_STUB_ACTIVITY,
            )
            .await,
        ]);
        let workflow_engine =
            Engines::get_workflow_engine_test(EngineConfig::on_demand_testing()).unwrap();
        let (_worker, component_id, runnable_component) = compile_js_workflow_worker(
            js_source,
            &user_ffqn,
            db_pool.clone(),
            sim_clock.clone_box(),
            fn_registry.clone(),
            workflow_engine.clone(),
        );
        let db_connection = db_pool.connection_test().await.unwrap();
        let execution_id = ExecutionId::from_parts(0, 9003);
        let created_at = sim_clock.now();

        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: user_ffqn.clone(),
                params: Params::from_json_values_test(vec![json!(Vec::<String>::new())]),
                parent: None,
                metadata: ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: true,
            })
            .await
            .unwrap();

        let deployment_id = DeploymentId::from_parts(0, 0);
        sim_clock.move_time_forward(Duration::from_millis(100));
        let replay_worker = build_js_replay_worker(
            deployment_id,
            component_id,
            &runnable_component,
            workflow_engine,
            fn_registry,
            db_pool.clone(),
            None,
            sim_clock.clone_box(),
            js_source.to_string(),
            default_return_type(),
            None, // max_replay_captured_writes
        );
        let replay = replay_worker
            .replay(execution_id.clone(), BacktraceCapture::Disabled)
            .await
            .unwrap();

        let replay = assert_matches!(replay, ReplayResponse::Advanceable(replay) => replay);
        let mut requested = replay.clone();
        let replayed_child_scheduled_at = requested
            .captured_writes
            .iter()
            .find_map(|write| match write {
                CapturedDbWrite::AppendBatchCreateNewExecution { child_req, .. } => {
                    child_req.first().map(|child| child.scheduled_at)
                }
                _ => None,
            })
            .expect("replay should create a scheduled execution");
        assert_eq!(
            replayed_child_scheduled_at,
            sim_clock.now() + chrono::TimeDelta::minutes(5)
        );
        let scheduled_execution_id = requested
            .captured_writes
            .iter_mut()
            .find_map(|write| match write {
                CapturedDbWrite::AppendBatchCreateNewExecution {
                    current_time,
                    batch,
                    child_req,
                    ..
                } => {
                    *current_time = DateTime::UNIX_EPOCH; // modifying time in the advance request should have no effect.
                    for req in batch.iter_mut() {
                        req.created_at = DateTime::UNIX_EPOCH;
                    }
                    for child in child_req.iter_mut() {
                        child.created_at = DateTime::UNIX_EPOCH;
                        child.scheduled_at = DateTime::UNIX_EPOCH;
                    }
                    child_req.first().map(|child| child.execution_id.clone())
                }
                _ => None,
            })
            .expect("replay should create a scheduled execution");
        sim_clock.move_time_forward(Duration::from_millis(100));

        let advance = replay_worker
            .advance(execution_id, requested, BacktraceCapture::Disabled)
            .await
            .unwrap();

        advance
            .finished
            .expect("main execution should have finished");
        let expected_scheduled_at = sim_clock.now() + chrono::TimeDelta::minutes(5);
        assert_matches!(
            db_connection
                .get_pending_state(&scheduled_execution_id)
                .await
                .unwrap()
                .pending_state,
            PendingState::PendingAt(PendingStatePendingAt { scheduled_at, .. }) if scheduled_at == expected_scheduled_at
        );
        let create_event = db_connection
            .get_execution_event(&scheduled_execution_id, &Version::new(0))
            .await
            .unwrap();
        assert_eq!(create_event.created_at, sim_clock.now());
        let ExecutionRequest::Created { scheduled_at, .. } = create_event.event else {
            panic!("scheduled execution log must start with Created");
        };
        assert_eq!(scheduled_at, expected_scheduled_at);
    }
}
