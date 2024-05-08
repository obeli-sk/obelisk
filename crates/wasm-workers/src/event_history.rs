use crate::workflow_ctx::FunctionError;
use crate::workflow_worker::JoinNextBlockingStrategy;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{DelayId, JoinSetId};
use concepts::storage::{
    AppendRequest, CreateRequest, DbConnection, DbError, DbPool, ExecutionEventInner,
    JoinSetResponse, Version,
};
use concepts::storage::{HistoryEvent, JoinSetRequest};
use concepts::{ExecutionId, StrVariant};
use concepts::{FunctionFqn, Params, SupportedFunctionResult};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, trace};
use utils::time::ClockFn;
use val_json::type_wrapper::TypeWrapper;
use val_json::wast_val::{WastVal, WastValWithType};

const DB_POLL_SLEEP: Duration = Duration::from_millis(50);

pub(crate) struct EventHistory {
    pub(crate) execution_id: ExecutionId,
    pub(crate) events: Vec<HistoryEvent>,
    pub(crate) events_idx: usize,
    pub(crate) join_next_blocking_strategy: JoinNextBlockingStrategy,
    pub(crate) execution_deadline: DateTime<Utc>,
    pub(crate) child_retry_exp_backoff: Duration,
    pub(crate) child_max_retries: u32,
}

impl EventHistory {
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn replay_or_interrupt<DB: DbConnection>(
        &mut self,
        event_call: EventCall,
        db_pool: &impl DbPool<DB>,
        version: &mut Version,
        clock_fn: &impl ClockFn,
    ) -> Result<SupportedFunctionResult, FunctionError> {
        #[derive(derive_more::Display)]
        enum PollVariant {
            JoinNextChild(JoinSetId),
            JoinNextDelay(JoinSetId),
        }
        impl PollVariant {
            fn join_set_id(&self) -> JoinSetId {
                match self {
                    PollVariant::JoinNextChild(join_set_id)
                    | PollVariant::JoinNextDelay(join_set_id) => *join_set_id,
                }
            }
            fn as_command(&self) -> EventHistoryCommand {
                match self {
                    PollVariant::JoinNextChild(join_set_id) => EventHistoryCommand::JoinNextChild {
                        join_set_id: *join_set_id,
                    },
                    PollVariant::JoinNextDelay(join_set_id) => EventHistoryCommand::JoinNextDelay {
                        join_set_id: *join_set_id,
                    },
                }
            }
        }
        if let Some(accept_resp) = self.find_matching_atomic(&event_call)? {
            return Ok(accept_resp);
        }
        // not found in the history, persisting the request
        let created_at = clock_fn();
        let poll_variant = match &event_call {
            EventCall::BlockingChildExecutionRequest { join_set_id, .. }
            | EventCall::BlockingChildJoinNext { join_set_id } => {
                Some(PollVariant::JoinNextChild(*join_set_id))
            }
            EventCall::BlockingDelayRequest { join_set_id, .. } => {
                Some(PollVariant::JoinNextDelay(*join_set_id))
            }
            _ => None,
        };

        let db_connection = db_pool.connection();
        let lock_expires_at =
            if self.join_next_blocking_strategy == JoinNextBlockingStrategy::Interrupt {
                created_at
            } else {
                self.execution_deadline
            };
        let poll_variant = match poll_variant {
            None => {
                let cloned_non_blocking = event_call.clone();
                let mut history_events = event_call
                    .append_to_db(
                        &db_connection,
                        created_at,
                        lock_expires_at,
                        self.execution_id,
                        version,
                        self.child_retry_exp_backoff,
                        self.child_max_retries,
                    )
                    .await?;
                self.events.append(&mut history_events);
                return Ok(self
                    .find_matching_atomic(&cloned_non_blocking)?
                    .expect("non-blocking EventCall must return some response"));
            }
            Some(poll_variant) => {
                let commands = event_call.commands();
                let mut history_events = event_call
                    .append_to_db(
                        &db_connection,
                        created_at,
                        lock_expires_at,
                        self.execution_id,
                        version,
                        self.child_retry_exp_backoff,
                        self.child_max_retries,
                    )
                    .await?;
                self.events.append(&mut history_events);
                // move the index forward by n - 1, the last [`JoinSetResponse`] is found yet.
                let expected_idx = self.events_idx + commands.len() - 1;
                for command in commands.into_iter().peekable() {
                    self.find_matching_command(command)?;
                }
                assert_eq!(expected_idx, self.events_idx);
                poll_variant
            }
        };

        if self.join_next_blocking_strategy == JoinNextBlockingStrategy::Await {
            debug!(join_set_id = %poll_variant.join_set_id(),  "Waiting for {poll_variant}");
            while clock_fn() < self.execution_deadline {
                if self.get_update(&db_connection).await?.is_some() {
                    if let Some(accept_resp) =
                        self.find_matching_command(poll_variant.as_command())?
                    {
                        debug!(join_set_id = %poll_variant.join_set_id(), "Got result");
                        return Ok(accept_resp);
                    }
                }
                tokio::time::sleep(DB_POLL_SLEEP).await;
            }
        }
        match poll_variant {
            PollVariant::JoinNextChild(_) => Err(FunctionError::ChildExecutionRequest),
            PollVariant::JoinNextDelay(_) => Err(FunctionError::DelayRequest),
        }
    }

    fn find_matching_atomic(
        &mut self,
        event_call: &EventCall,
    ) -> Result<Option<SupportedFunctionResult>, FunctionError> {
        let commands = event_call.commands();
        assert!(!commands.is_empty());
        let mut last = None;
        for (idx, command) in commands.into_iter().enumerate() {
            last = self.find_matching_command(command)?;
            if last.is_none() {
                assert_eq!(
                    idx, 0,
                    "EventCall must be processed in an all or nothing fashion"
                );
                return Ok(None);
            }
        }
        Ok(Some(last.unwrap()))
    }

    #[allow(clippy::too_many_lines)]
    fn find_matching_command(
        &mut self,
        command: EventHistoryCommand,
    ) -> Result<Option<SupportedFunctionResult>, FunctionError> {
        let Some(found) = self.events.get(self.events_idx) else {
            return Ok(None);
        };
        trace!("Finding match for {command:?}, {found:?}",);
        match (command, found) {
            (
                EventHistoryCommand::CreateJoinSet { join_set_id },
                HistoryEvent::JoinSet {
                    join_set_id: found_join_set_id,
                },
            ) if join_set_id == *found_join_set_id => {
                trace!(%join_set_id, "Matched JoinSet");
                self.events_idx += 1;
                // if this is a [`EventCall::CreateJoinSet`] , return join set id
                Ok(Some(SupportedFunctionResult::Infallible(WastValWithType {
                    r#type: TypeWrapper::String,
                    value: WastVal::String(join_set_id.to_string()),
                })))
            }
            (
                EventHistoryCommand::ChildExecutionRequest {
                    join_set_id,
                    child_execution_id: execution_id,
                    ..
                },
                HistoryEvent::JoinSetRequest {
                    join_set_id: found_join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                },
            ) if join_set_id == *found_join_set_id && execution_id == *child_execution_id => {
                trace!(%child_execution_id,
                %join_set_id, "Matched JoinSetRequest::ChildExecutionRequest");
                self.events_idx += 1;
                // if this is a [`EventCall::StartAsync`] , return execution id
                Ok(Some(SupportedFunctionResult::Infallible(WastValWithType {
                    r#type: TypeWrapper::String,
                    value: WastVal::String(execution_id.to_string()),
                })))
            }
            (
                EventHistoryCommand::DelayRequest {
                    join_set_id,
                    delay_id,
                },
                HistoryEvent::JoinSetRequest {
                    join_set_id: found_join_set_id,
                    request:
                        JoinSetRequest::DelayRequest {
                            delay_id: found_delay_id,
                            expires_at: _,
                        },
                },
            ) if join_set_id == *found_join_set_id && delay_id == *found_delay_id => {
                trace!(%delay_id,
                %join_set_id, "Matched JoinSetRequest::DelayRequest");
                self.events_idx += 1;
                // return delay id
                Ok(Some(SupportedFunctionResult::Infallible(WastValWithType {
                    r#type: TypeWrapper::String,
                    value: WastVal::String(delay_id.to_string()),
                })))
            }
            (
                EventHistoryCommand::JoinNextChild { join_set_id },
                HistoryEvent::JoinNext {
                    join_set_id: found_join_set_id,
                    ..
                },
            ) if join_set_id == *found_join_set_id => {
                trace!(
                %join_set_id, "Peeked at JoinNext - Child");
                // TODO: Add support for conditions on JoinNext, skip unrelated responses
                match self.events.get(self.events_idx + 1) {
                    Some(HistoryEvent::JoinSetResponse {
                        response:
                            JoinSetResponse::ChildExecutionFinished {
                                child_execution_id,
                                result,
                            },
                        join_set_id: following_join_set_id,
                    }) if join_set_id == *following_join_set_id => {
                        self.events_idx += 2;
                        match result {
                            Ok(result) => Ok(Some(result.clone())),
                            Err(err) => {
                                error!(%child_execution_id,
                                    %join_set_id,
                                    "Child execution finished with an execution error, failing the parent");
                                Err(FunctionError::ChildExecutionError(err.clone()))
                            }
                        }
                    }
                    _ => Ok(None), // no progress, still at JoinNext
                }
            }
            (
                EventHistoryCommand::JoinNextDelay { join_set_id },
                HistoryEvent::JoinNext {
                    join_set_id: found_join_set_id,
                    ..
                },
            ) if join_set_id == *found_join_set_id => {
                trace!(
                    %join_set_id, "Peeked at JoinNext - Delay");
                match self.events.get(self.events_idx + 1) {
                    Some(HistoryEvent::JoinSetResponse {
                        response:
                            JoinSetResponse::DelayFinished {
                                delay_id: _, // Currently only a single blocking delay is supported, no need to match the id
                            },
                        join_set_id: following_join_set_id,
                    }) if join_set_id == *following_join_set_id => {
                        self.events_idx += 2;
                        Ok(Some(SupportedFunctionResult::None))
                    }
                    _ => Ok(None), // no progress, still at JoinNext
                }
            }
            (command, found) => Err(FunctionError::NonDeterminismDetected(StrVariant::Arc(
                Arc::from(format!(
                    "unexpected command {command:?} not matching {found:?} at index {}",
                    self.events_idx
                )),
            ))),
        }
    }

    async fn get_update<DB: DbConnection>(
        &mut self,
        db_connection: &DB,
    ) -> Result<Option<()>, DbError> {
        let event_history = db_connection
            .get(self.execution_id)
            .await?
            .event_history()
            .collect::<Vec<_>>();
        if event_history.len() > self.events.len() {
            self.events = event_history;
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum EventCall {
    CreateJoinSet {
        join_set_id: JoinSetId,
    },
    StartAsync {
        ffqn: FunctionFqn,
        join_set_id: JoinSetId,
        child_execution_id: ExecutionId,
        params: Params,
    },
    BlockingChildJoinNext {
        join_set_id: JoinSetId,
    },
    /// combines [`Self::CreateJoinSet`] [`Self::StartAsync`] [`Self::BlockingChildJoinNext`]
    BlockingChildExecutionRequest {
        ffqn: FunctionFqn,
        join_set_id: JoinSetId,
        child_execution_id: ExecutionId,
        params: Params,
    },
    BlockingDelayRequest {
        join_set_id: JoinSetId,
        delay_id: DelayId,
        expires_at_if_new: DateTime<Utc>,
    },
}

#[derive(Debug)]
// TODO: Rename to EventHistoryKey
enum EventHistoryCommand {
    CreateJoinSet {
        join_set_id: JoinSetId,
    },
    ChildExecutionRequest {
        join_set_id: JoinSetId,
        child_execution_id: ExecutionId,
    },
    DelayRequest {
        join_set_id: JoinSetId,
        delay_id: DelayId,
    },
    JoinNextChild {
        join_set_id: JoinSetId,
        // TODO: Add wakeup conditions: ffqn (used by function_await), execution id
    },
    JoinNextDelay {
        join_set_id: JoinSetId,
    },
    // TODO: JoinNextAny
}

impl EventCall {
    fn commands(&self) -> Vec<EventHistoryCommand> {
        match self {
            EventCall::CreateJoinSet { join_set_id } => {
                vec![EventHistoryCommand::CreateJoinSet {
                    join_set_id: *join_set_id,
                }]
            }
            EventCall::StartAsync {
                join_set_id,
                child_execution_id,
                ..
            } => vec![EventHistoryCommand::ChildExecutionRequest {
                join_set_id: *join_set_id,
                child_execution_id: *child_execution_id,
            }],
            EventCall::BlockingChildJoinNext { join_set_id } => {
                vec![EventHistoryCommand::JoinNextChild {
                    join_set_id: *join_set_id,
                }]
            }
            EventCall::BlockingChildExecutionRequest {
                join_set_id,
                child_execution_id,
                ..
            } => vec![
                EventHistoryCommand::CreateJoinSet {
                    join_set_id: *join_set_id,
                },
                EventHistoryCommand::ChildExecutionRequest {
                    join_set_id: *join_set_id,
                    child_execution_id: *child_execution_id,
                },
                EventHistoryCommand::JoinNextChild {
                    join_set_id: *join_set_id,
                },
            ],
            EventCall::BlockingDelayRequest {
                join_set_id,
                delay_id,
                ..
            } => vec![
                EventHistoryCommand::CreateJoinSet {
                    join_set_id: *join_set_id,
                },
                EventHistoryCommand::DelayRequest {
                    join_set_id: *join_set_id,
                    delay_id: *delay_id,
                },
                EventHistoryCommand::JoinNextDelay {
                    join_set_id: *join_set_id,
                },
            ],
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn append_to_db(
        self,
        db_connection: &impl DbConnection,
        created_at: DateTime<Utc>,
        lock_expires_at: DateTime<Utc>,
        execution_id: ExecutionId,
        version: &mut Version,
        child_retry_exp_backoff: Duration,
        child_max_retries: u32,
    ) -> Result<Vec<HistoryEvent>, DbError> {
        match self {
            EventCall::CreateJoinSet { join_set_id } => {
                let mut history_events = Vec::with_capacity(1);
                let event = HistoryEvent::JoinSet { join_set_id };
                history_events.push(event.clone());
                let join_set = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%join_set_id, "Creating new JoinNext");
                *version = db_connection
                    .append(execution_id, Some(version.clone()), join_set)
                    .await?;
                Ok(history_events)
            }
            EventCall::StartAsync {
                ffqn,
                join_set_id,
                child_execution_id,
                params,
            } => {
                let mut history_events = Vec::with_capacity(1);
                let event = HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                };
                history_events.push(event.clone());
                let child_exec_req = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%child_execution_id, %join_set_id, "Scheduling child execution");
                let child = CreateRequest {
                    created_at,
                    execution_id: child_execution_id,
                    ffqn,
                    params,
                    parent: Some((execution_id, join_set_id)),
                    scheduled_at: None,
                    retry_exp_backoff: child_retry_exp_backoff,
                    max_retries: child_max_retries,
                };

                *version = db_connection
                    .append_batch_create_child(
                        vec![child_exec_req],
                        execution_id,
                        version.clone(),
                        child,
                    )
                    .await?;
                Ok(history_events)
            }
            EventCall::BlockingChildJoinNext { join_set_id } => {
                let mut history_events = Vec::with_capacity(1);
                let event = HistoryEvent::JoinNext {
                    join_set_id,
                    lock_expires_at,
                };
                history_events.push(event.clone());
                let join_next = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%join_set_id, "Blocking on JoinNext");
                *version = db_connection
                    .append(execution_id, Some(version.clone()), join_next)
                    .await?;
                Ok(history_events)
            }
            EventCall::BlockingChildExecutionRequest {
                ffqn,
                join_set_id,
                child_execution_id,
                params,
            } => {
                let mut history_events = Vec::with_capacity(3);
                let event = HistoryEvent::JoinSet { join_set_id };
                history_events.push(event.clone());
                let join_set = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                let event = HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                };
                history_events.push(event.clone());
                let child_exec_req = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                let event = HistoryEvent::JoinNext {
                    join_set_id,
                    lock_expires_at,
                };
                history_events.push(event.clone());
                let join_next = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%child_execution_id, %join_set_id, "Blocking on child execution");
                let child = CreateRequest {
                    created_at,
                    execution_id: child_execution_id,
                    ffqn: ffqn.clone(),
                    params: params.clone(),
                    parent: Some((execution_id, join_set_id)),
                    scheduled_at: None,
                    retry_exp_backoff: child_retry_exp_backoff,
                    max_retries: child_max_retries,
                };
                *version = db_connection
                    .append_batch_create_child(
                        vec![join_set, child_exec_req, join_next],
                        execution_id,
                        version.clone(),
                        child,
                    )
                    .await?;
                Ok(history_events)
            }
            EventCall::BlockingDelayRequest {
                join_set_id,
                delay_id,
                expires_at_if_new,
            } => {
                let mut history_events = Vec::with_capacity(3);
                let event = HistoryEvent::JoinSet { join_set_id };
                history_events.push(event.clone());
                let join_set = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                let event = HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::DelayRequest {
                        delay_id,
                        expires_at: expires_at_if_new,
                    },
                };
                history_events.push(event.clone());
                let delay_req = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                let event = HistoryEvent::JoinNext {
                    join_set_id,
                    lock_expires_at,
                };
                history_events.push(event.clone());
                let join_next = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%delay_id, %join_set_id, "Blocking on delay request");
                *version = db_connection
                    .append_batch(
                        vec![join_set, delay_req, join_next],
                        execution_id,
                        version.clone(),
                    )
                    .await?;
                Ok(history_events)
            }
        }
    }
}
