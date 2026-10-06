// Each entry defines both the typed client method and the server dispatch.
#[doc(hidden)]
#[macro_export]
macro_rules! rpc_methods {
    ($callback:ident) => {
$callback! { DbExecutor, db_exec_conn;
    fn lock_pending_by_ffqns(
        batch_size: (u32),
        pending_at_or_sooner: (DateTime<Utc>),
        ffqns: (Arc<[FunctionFqn]>),
        created_at: (DateTime<Utc>),
        component_id: (ComponentId),
        deployment_id: (DeploymentId),
        executor_id: (ExecutorId),
        lock_expires_at: (DateTime<Utc>),
        run_id: (RunId),
        retry_config: (ComponentRetryConfig),
    ) -> LockPendingResponse, DbErrorWrite;
    fn lock_pending_by_ffqns_auto(
        batch_size: (u32),
        pending_at_or_sooner: (DateTime<Utc>),
        ffqns: (Arc<[FunctionFqn]>),
        created_at: (DateTime<Utc>),
        component_id: (ComponentId),
        deployment_id: (DeploymentId),
        executor_id: (ExecutorId),
        lock_expires_at: (DateTime<Utc>),
        run_id: (RunId),
        retry_config: (ComponentRetryConfig),
    ) -> LockPendingResponse, DbErrorWrite;
    fn lock_pending_by_component_digest(
        batch_size: (u32),
        pending_at_or_sooner: (DateTime<Utc>),
        component_id: (&ComponentId),
        deployment_id: (DeploymentId),
        created_at: (DateTime<Utc>),
        executor_id: (ExecutorId),
        lock_expires_at: (DateTime<Utc>),
        run_id: (RunId),
        retry_config: (ComponentRetryConfig),
    ) -> LockPendingResponse, DbErrorWrite;
    #[cfg(feature = "test")]
    fn lock_one(
        created_at: (DateTime<Utc>),
        component_id: (ComponentId),
        deployment_id: (DeploymentId),
        execution_id: (&ExecutionId),
        run_id: (RunId),
        version: (Version),
        executor_id: (ExecutorId),
        lock_expires_at: (DateTime<Utc>),
        retry_config: (ComponentRetryConfig),
    ) -> LockedExecution, DbErrorWrite;
    fn append(
        execution_id: (ExecutionId),
        version: (Version),
        req: (AppendRequest),
    ) -> AppendResponse, DbErrorWrite;
    fn append_batch_respond_to_parent(
        events: (AppendEventsToExecution),
        response: (AppendResponseToExecution),
        current_time: (DateTime<Utc>),
    ) -> AppendBatchResponse, DbErrorWrite;
    fn append_cancel_workflow_requested(
        execution_id: (&ExecutionId),
        cancelled_at: (DateTime<Utc>),
    ) -> CancelOutcome, DbErrorWrite;
    fn get_last_execution_event(execution_id: (&ExecutionId)) -> ExecutionEvent, DbErrorRead;
    fn append_activity_cancellation_requested(
        execution_id: (&ExecutionId),
        cancelled_at: (DateTime<Utc>),
    ) -> CancelOutcome, DbErrorWrite;
    @extras {
        async fn wait_for_pending_by_ffqn(
            &self,
            pending_at_or_sooner: DateTime<Utc>,
            ffqns: Arc<[FunctionFqn]>,
            current_digest: Option<ComponentDigest>,
            mut timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
        ) {
            tokio::select! {
                result = self.call::<()>("Notifications", "pending_ffqn", (pending_at_or_sooner, ffqns, current_digest)) => {
                    if let Err(err) = result { tracing::warn!(?err, "HTTP pending subscription failed"); tokio::select! { () = tokio::time::sleep(Duration::from_secs(1)) => {}, () = &mut timeout_fut => {} } }
                }
                () = &mut timeout_fut => {},
            }
        }
        async fn wait_for_pending_by_component_digest(
            &self,
            pending_at_or_sooner: DateTime<Utc>,
            component_digest: &ComponentDigest,
            mut timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
        ) {
            tokio::select! {
                result = self.call::<()>("Notifications", "pending_digest", (pending_at_or_sooner, component_digest)) => {
                    if let Err(err) = result { tracing::warn!(?err, "HTTP pending subscription failed"); tokio::select! { () = tokio::time::sleep(Duration::from_secs(1)) => {}, () = &mut timeout_fut => {} } }
                }
                () = &mut timeout_fut => {},
            }
        }

    }
}

$callback! { DbConnection, connection;
    fn get(execution_id: (&ExecutionId)) -> ExecutionLog, DbErrorRead;
    fn get_cancelling(batch_size: (u32)) -> Vec<ExecutionId>, DbErrorRead;
    fn append_delay_response(
        created_at: (DateTime<Utc>),
        execution_id: (ExecutionId),
        join_set_id: (JoinSetId),
        delay_id: (DelayId),
        outcome: (Result<(), ()>),
    ) -> AppendDelayResponseOutcome, DbErrorWrite;
    fn append_batch(
        current_time: (DateTime<Utc>),
        batch: (Vec<AppendRequest>),
        execution_id: (ExecutionId),
        version: (Version),
    ) -> AppendBatchResponse, DbErrorWrite;
    fn append_batch_with_delay_response(
        current_time: (DateTime<Utc>),
        batch: (Vec<AppendRequest>),
        execution_id: (ExecutionId),
        version: (Version),
        join_set_id: (JoinSetId),
        delay_id: (DelayId),
    ) -> AppendBatchResponse, DbErrorWrite;
    fn append_batch_create_new_execution(
        current_time: (DateTime<Utc>),
        batch: (Vec<AppendRequest>),
        execution_id: (ExecutionId),
        version: (Version),
        child_req: (Vec<CreateRequest>),
        backtraces: (Vec<BacktraceInfo>),
    ) -> AppendBatchResponse, DbErrorWrite;
    fn get_execution_event(
        execution_id: (&ExecutionId),
        version: (&Version),
    ) -> ExecutionEvent, DbErrorRead;
    fn upsert_stub_response(
        execution_id: (ExecutionIdDerived),
        version: (Version),
        req: (AppendRequest),
        response: (AppendResponseToExecution),
        current_time: (DateTime<Utc>),
    ) -> (), DbErrorStubResponse;
    fn get_pending_state(execution_id: (&ExecutionId)) -> ExecutionWithState, DbErrorRead;
    fn get_expired_timers(at: (DateTime<Utc>)) -> Vec<ExpiredTimer>, DbErrorGeneric;
    fn create(req: (CreateRequest)) -> AppendResponse, DbErrorWrite;
    fn append_backtrace(append: (BacktraceInfo)) -> (), DbErrorWrite;
    fn append_backtrace_batch(batch: (Vec<BacktraceInfo>)) -> usize, DbErrorWrite;
    fn append_log(row: (LogInfoAppendRow)) -> (), DbErrorWrite;
    fn append_log_batch(batch: (&[LogInfoAppendRow])) -> (), DbErrorWrite;
    @extras {
        async fn subscribe_to_next_responses(
            &self,
            execution_id: &ExecutionId,
            last_response: ResponseCursor,
            mut subscription_end_fut: Pin<Box<dyn Future<Output = ResponseSubscriptionEnd> + Send>>,
        ) -> Result<Vec<ResponseWithCursor>, SubscribeToResponsesError> {
            let immediate = self
                .call(
                    "Notifications",
                    "responses",
                    (execution_id, last_response, 0_u64),
                )
                .await
                .map_err(SubscribeToResponsesError::from);
            if !matches!(
                immediate,
                Err(SubscribeToResponsesError::SubscriptionEnded(
                    ResponseSubscriptionEnd::PollIntervalElapsed
                ))
            ) {
                return immediate;
            }
            tokio::select! {
                result = self.call("Notifications", "responses", (execution_id, last_response, LONG_POLL_MILLIS)) => result.map_err(SubscribeToResponsesError::from),
                reason = &mut subscription_end_fut => Err(SubscribeToResponsesError::SubscriptionEnded(reason)),
            }
        }
        async fn wait_for_finished_result(
            &self,
            execution_id: &ExecutionId,
            timeout_fut: Option<Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>>,
        ) -> Result<SupportedFunctionReturnValue, DbErrorReadWithTimeout> {
            let immediate = self
                .call("Notifications", "finished", (execution_id, 0_u64))
                .await
                .map_err(DbErrorReadWithTimeout::from);
            if !matches!(
                immediate,
                Err(DbErrorReadWithTimeout::Timeout(TimeoutOutcome::Timeout))
            ) {
                return immediate;
            }
            let mut timeout_fut = timeout_fut.unwrap_or_else(|| Box::pin(std::future::pending()));
            loop {
                let result = tokio::select! {
                    result = self.call("Notifications", "finished", (execution_id, LONG_POLL_MILLIS)) => result.map_err(DbErrorReadWithTimeout::from),
                    reason = &mut timeout_fut => return Err(DbErrorReadWithTimeout::Timeout(reason)),
                };
                if !matches!(
                    result,
                    Err(DbErrorReadWithTimeout::Timeout(TimeoutOutcome::Timeout))
                ) {
                    return result;
                }
            }
        }

    }
}

$callback! { DbExternalApi, external_api_conn;
    fn get_backtrace(
        execution_id: (&ExecutionId),
        filter: (BacktraceFilter),
    ) -> BacktraceInfo, DbErrorRead;
    fn upsert_source_mapping(
        component_digest: (&ComponentDigest),
        frame_key: (&str),
        is_suffix: (bool),
        digest: (&ContentDigest),
    ) -> (), DbErrorWrite;
    fn resolve_source_digest(
        component_digest: (&ComponentDigest),
        file: (&str),
    ) -> Option<ContentDigest>, DbErrorRead;
    fn upsert_component_metadata(records: (Vec<ComponentMetadataRecord>)) -> (), DbErrorWrite;
    fn insert_deployment_components(
        deployment_id: (DeploymentId),
        records: (Vec<DeploymentComponentRecord>),
    ) -> (), DbErrorWrite;
    fn list_deployment_components(
        deployment_id: (DeploymentId),
    ) -> Vec<DeploymentComponentDetail>, DbErrorRead;
    fn get_deployment_component_wit(
        deployment_id: (DeploymentId),
        component_digest: (&ComponentDigest),
    ) -> Option<String>, DbErrorRead;
    fn list_executions(
        filter: (ListExecutionsFilter),
        pagination: (ExecutionListPagination),
    ) -> Vec<ExecutionWithState>, DbErrorGeneric;
    fn list_execution_events(
        execution_id: (&ExecutionId),
        pagination: (Pagination<VersionType>),
        include_backtrace_id: (bool),
    ) -> ListExecutionEventsResponse, DbErrorRead;
    fn get_execution_event_bounds_batch(
        execution_ids: (&[ExecutionId]),
    ) -> Vec<ExecutionEventBounds>, DbErrorRead;
    fn list_responses_filtered(
        execution_id: (&ExecutionId),
        pagination: (Pagination<u32>),
        join_set: (Option<&JoinSetId>),
    ) -> ListResponsesResponse, DbErrorRead;
    fn list_execution_events_responses(
        execution_id: (&ExecutionId),
        req_since: (&Version),
        req_max_length: (NonZeroU16),
        req_include_backtrace_id: (bool),
        resp_pagination: (Pagination<VersionType>),
    ) -> ExecutionWithStateRequestsResponses, DbErrorRead;
    fn upgrade_execution_component(
        execution_id: (&ExecutionId),
        old: (&ComponentDigest),
        new: (&ComponentDigest),
        reason: (ComponentUpgradeReason),
    ) -> (), DbErrorWrite;
    fn list_logs(
        execution_id: (&ExecutionId),
        show_derived: (bool),
        filter: (LogFilter),
        pagination: (Pagination<LogCursor>),
    ) -> ListLogsResponse, DbErrorRead;
    fn list_deployment_states(
        current_time: (DateTime<Utc>),
        pagination: (Pagination<Option<DeploymentId>>),
        include_deployment_toml: (bool),
        execution_counts: (DeploymentExecutionCounts),
    ) -> Vec<DeploymentState>, DbErrorRead;
    fn insert_deployment_with_components(
        record: (DeploymentRecord),
        component_metadata: (Vec<ComponentMetadataRecord>),
        deployment_components: (Vec<DeploymentComponentRecord>),
        deployment_component_files: (Vec<DeploymentComponentFileRecord>),
    ) -> (), DbErrorWrite;
    fn missing_digests(deployment_id: (DeploymentId)) -> Vec<ContentDigest>, DbErrorRead;
    fn list_deployment_files(
        deployment_id: (DeploymentId),
    ) -> Vec<DeploymentFileRecord>, DbErrorRead;
    fn activate_deployment(
        deployment_id: (DeploymentId),
        now: (DateTime<Utc>),
        app_config_digest: (Option<&str>),
    ) -> (), DbErrorWrite;
    fn enqueue_deployment(deployment_id: (DeploymentId)) -> EnqueueOutcome, DbErrorWrite;
    fn get_deployment(deployment_id: (DeploymentId)) -> Option<DeploymentRecord>, DbErrorRead;
    #[cfg(feature = "test")]
    fn get_active_deployment() -> Option<DeploymentRecord>, DbErrorRead;
    fn get_current_deployment() -> Option<DeploymentRecord>, DbErrorRead;
    fn list_deployments(
        pagination: (Pagination<Option<DeploymentId>>),
    ) -> Vec<DeploymentRecord>, DbErrorRead;
    fn pause_execution(
        execution_id: (&ExecutionId),
        paused_at: (DateTime<Utc>),
    ) -> AppendResponse, DbErrorWrite;
    fn unpause_execution(
        execution_id: (&ExecutionId),
        unpaused_at: (DateTime<Utc>),
    ) -> AppendResponse, DbErrorWrite;
    fn pause_delay(delay_id: (&DelayId)) -> (), DbErrorWrite;
    fn unpause_delay(delay_id: (&DelayId)) -> (), DbErrorWrite;
    @extras {
    }
}

$callback! { DbAdmin, admin_conn;
    fn append_system_event(event: (SystemEvent)) -> (), DbErrorWrite;
    fn append_system_event_with_cas(event: (SystemEvent), content: (Vec<u8>)) -> (), DbErrorWrite;
    fn list_system_events(filter: (SystemEventFilter)) -> Vec<SystemEvent>, DbErrorRead;
    fn find_http_policy_event_ids(
        deployment_id: (DeploymentId),
        component: (&str),
        component_policy_hash: (&str),
        server_policy_hash: (&str),
    ) -> Option<HttpPolicyEventIds>, DbErrorRead;
    fn get_storage_status() -> StorageStatus, DbErrorRead;
    fn retain_system_events(
        created_before: (DateTime<Utc>),
        limit: (u32),
        dry_run: (bool),
    ) -> SystemEventRetentionResult, DbErrorWrite;
    fn delete_execution_tree(
        execution_id: (&ExecutionId),
        force_non_terminal: (bool),
    ) -> DeleteExecutionTreeResult, DbErrorWrite;
    fn retain_executions(
        retention: (RetentionPolicy),
        batch_size: (u32),
        force_non_terminal: (bool),
        dry_run: (bool),
    ) -> CleanupResult, DbErrorWrite;
    fn delete_deployment(
        deployment_id: (DeploymentId),
        delete_executions: (bool),
        force_non_terminal: (bool),
    ) -> DeleteDeploymentResult, DbErrorWrite;
    fn retain_deployments(
        retention: (RetentionPolicy),
        batch_size: (u32),
        delete_executions: (bool),
        force_non_terminal: (bool),
        dry_run: (bool),
    ) -> CleanupResult, DbErrorWrite;
    fn gc_executions(batch_size: (u32)) -> ExecutionGcResult, DbErrorWrite;
    @extras {
    }
}

$callback! { CasGc, cas_gc_conn;
    fn gc_cas(dry_run: (bool), batch_size: (u32)) -> CasGcResult, DbErrorWrite;
    @extras {
    }
}

#[cfg(feature = "test")]
$callback! { DbConnectionTest, connection_test;
    #[cfg(feature = "test")]
    fn append_response(
        created_at: (DateTime<Utc>),
        execution_id: (ExecutionId),
        response_event: (JoinSetResponseEvent),
    ) -> (), DbErrorWrite;
    @extras {
    }
}
    };
}
