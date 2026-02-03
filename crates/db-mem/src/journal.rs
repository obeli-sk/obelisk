use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::{
    CreateRequest, DbErrorWrite, DbErrorWriteNonRetriable, ExecutionEvent, ExecutionRequest,
    HistoryEvent, JoinSetRequest, JoinSetResponse, JoinSetResponseEvent, JoinSetResponseEventOuter,
    Locked, LockedBy, PendingStateBlockedByJoinSet, PendingStateFinished,
    PendingStateFinishedResultKind, PendingStateLocked, PendingStatePendingAt, ResponseCursor,
    ResponseWithCursor, VersionType,
};
use concepts::storage::{ExecutionLog, PendingState, Version};
use concepts::{ComponentId, JoinSetId};
use concepts::{ExecutionId, ExecutionMetadata};
use concepts::{FunctionFqn, Params};
use std::cmp::max;
use std::panic::Location;
use tokio::sync::oneshot;
use tracing_error::SpanTrace;

#[derive(Debug)]
pub(crate) struct ExecutionJournal {
    pub(crate) execution_id: ExecutionId,
    pub(crate) pending_state: PendingState, // updated on every state change
    pub(crate) component_id: ComponentId,   // updated on every Locked event
    pub(crate) deployment_id: DeploymentId, // updated on every Locked event
    pub(crate) execution_events: Vec<ExecutionEvent>,
    pub(crate) responses: Vec<ResponseWithCursor>, // response cursor is its index.
    pub(crate) response_subscriber: Option<oneshot::Sender<ResponseWithCursor>>,
}

impl ExecutionJournal {
    #[must_use]
    pub fn new(req: CreateRequest) -> Self {
        let pending_state = PendingState::PendingAt(PendingStatePendingAt {
            scheduled_at: req.scheduled_at,
            last_lock: None,
        });
        let execution_id = req.execution_id.clone();
        let component_id = req.component_id.clone();
        let deployment_id = req.deployment_id;

        let created_at = req.created_at;
        let event = ExecutionEvent {
            event: ExecutionRequest::from(req),
            created_at,
            backtrace_id: None,
            version: Version(0),
        };
        Self {
            execution_id,
            pending_state,
            execution_events: vec![event],
            responses: Vec::default(),
            response_subscriber: None,
            component_id,
            deployment_id,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.execution_events.len()
    }

    #[expect(dead_code)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.execution_events.is_empty()
    }

    #[must_use]
    pub fn ffqn(&self) -> &FunctionFqn {
        match self.execution_events.first().unwrap() {
            ExecutionEvent {
                event: ExecutionRequest::Created { ffqn, .. },
                ..
            } => ffqn,
            _ => panic!("first event must be `Created`"),
        }
    }

    #[must_use]
    pub(super) fn component_id_last(&self) -> &ComponentId {
        if let Some(last_lock) = self.find_last_lock() {
            &last_lock.component_id
        } else {
            let ExecutionEvent {
                event: ExecutionRequest::Created { component_id, .. },
                ..
            } = self.execution_events.first().unwrap()
            else {
                unreachable!("first event must be `Created`")
            };
            component_id
        }
    }

    #[must_use]
    pub fn version(&self) -> Version {
        Version(
            VersionType::try_from(self.execution_events.len()).unwrap()
                - VersionType::from(self.pending_state.is_finished()), // if is_finished then -1 as it does not grow anymore
        )
    }

    #[must_use]
    pub fn execution_id(&self) -> &ExecutionId {
        &self.execution_id
    }

    #[must_use]
    pub fn metadata(&self) -> &ExecutionMetadata {
        match self.execution_events.first().unwrap() {
            ExecutionEvent {
                event: ExecutionRequest::Created { metadata, .. },
                ..
            } => metadata,
            _ => panic!("first event must be `Created`"),
        }
    }

    pub(crate) fn append(
        &mut self,
        created_at: DateTime<Utc>,
        event: ExecutionRequest,
        appending_version: Version,
    ) -> Result<Version, DbErrorWrite> {
        assert_eq!(self.version(), appending_version);
        if self.pending_state.is_finished() {
            return Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::IllegalState {
                    reason: "already finished".into(),
                    context: SpanTrace::capture(),
                    source: None,
                    loc: Location::caller(),
                },
            ));
        }

        if let ExecutionRequest::Locked(Locked {
            executor_id,
            lock_expires_at,
            run_id,
            component_id: _,
            deployment_id: _,
            retry_config: _,
        }) = &event
        {
            self.pending_state.can_append_lock(
                created_at,
                *executor_id,
                *run_id,
                *lock_expires_at,
            )?;
        }

        // Make sure delay id is unique
        if let ExecutionRequest::HistoryEvent {
            event:
                HistoryEvent::JoinSetRequest {
                    request: JoinSetRequest::DelayRequest { delay_id, .. },
                    ..
                },
        } = &event
            && self.execution_events.iter().any(|event| {
                matches!(&event.event, ExecutionRequest::HistoryEvent {
                        event:
                            HistoryEvent::JoinSetRequest {
                                request:
                                    JoinSetRequest::DelayRequest {
                                        delay_id: found_id, ..
                                    },
                                ..
                            },
                    } if delay_id == found_id)
            })
        {
            return Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::Conflict,
            ));
        }

        self.execution_events.push(ExecutionEvent {
            created_at,
            event,
            backtrace_id: None,
            version: appending_version,
        });
        // update the state
        self.update_pending_state();
        Ok(self.version())
    }

    pub fn append_response(
        &mut self,
        created_at: DateTime<Utc>,
        event: JoinSetResponseEvent,
    ) -> Result<(), DbErrorWrite> {
        let event = JoinSetResponseEventOuter { created_at, event };
        {
            // Check child id uniqueness
            if let JoinSetResponseEvent {
                event:
                    JoinSetResponse::ChildExecutionFinished {
                        child_execution_id, ..
                    },
                ..
            } = &event.event
                && self.responses.iter().any(|event| {
                    matches!(&event.event.event, JoinSetResponseEvent {
                        event:
                            JoinSetResponse::ChildExecutionFinished {
                                child_execution_id: found_id,
                                ..
                            },
                        ..
                    } if child_execution_id == found_id)
                })
            {
                return Err(DbErrorWrite::NonRetriable(
                    DbErrorWriteNonRetriable::Conflict,
                ));
            }
        }
        {
            // Check delay id uniqueness
            if let JoinSetResponseEvent {
                event: JoinSetResponse::DelayFinished { delay_id, .. },
                ..
            } = &event.event
                && self.responses.iter().any(|event| {
                    matches!(&event.event.event, JoinSetResponseEvent {
                        event:
                            JoinSetResponse::DelayFinished {
                                delay_id: found_id, ..
                            },
                        ..
                    } if delay_id == found_id)
                })
            {
                return Err(DbErrorWrite::NonRetriable(
                    DbErrorWriteNonRetriable::Conflict,
                ));
            }
        }

        let event = ResponseWithCursor {
            event,
            cursor: ResponseCursor(
                u32::try_from(self.responses.len()).expect("too many responses"),
            ),
        };
        self.responses.push(event.clone());
        // update the state
        self.update_pending_state();
        if let Some(subscriber) = self.response_subscriber.take() {
            let _ = subscriber.send(event);
        }
        Ok(())
    }

    pub(crate) fn find_last_lock(&self) -> Option<&Locked> {
        self.execution_events
            .iter()
            .rev()
            .find_map(|event| match &event.event {
                ExecutionRequest::Locked(locked) => Some(locked),
                _ => None,
            })
    }

    fn get_create_request(&self) -> CreateRequest {
        let execution_event = self.execution_events.first().expect("must not be empty");
        let ExecutionRequest::Created {
            ffqn,
            params,
            parent,
            scheduled_at,
            component_id,
            deployment_id,
            metadata,
            scheduled_by,
        } = execution_event.event.clone()
        else {
            unreachable!("must start with Created event")
        };
        CreateRequest {
            created_at: execution_event.created_at,
            execution_id: self.execution_id.clone(),
            ffqn,
            params,
            parent,
            scheduled_at,
            component_id,
            deployment_id,
            metadata,
            scheduled_by,
        }
    }

    fn update_pending_state(&mut self) {
        let mut unpause_encountered = false;

        let pending_state = self
            .execution_events
            .iter()
            .enumerate()
            .rev()
            .find_map(|(idx, event)| match &event.event {
                ExecutionRequest::Created { scheduled_at, .. } => {
                    Some(PendingState::PendingAt(PendingStatePendingAt {
                        scheduled_at: *scheduled_at,
                        last_lock: None,
                    }))
                }

                ExecutionRequest::Finished { result, .. } => {
                    assert_eq!(self.execution_events.len() - 1, idx);
                    Some(PendingState::Finished {
                        finished: PendingStateFinished {
                            version: VersionType::try_from(idx).expect("version limit reached"),
                            finished_at: event.created_at,
                            result_kind: PendingStateFinishedResultKind::from(result),
                        },
                    })
                }

                ExecutionRequest::Locked(Locked {
                    executor_id,
                    lock_expires_at,
                    run_id,
                    component_id: _,
                    deployment_id: _,
                    retry_config: _,
                }) => Some(PendingState::Locked(PendingStateLocked {
                    locked_by: LockedBy {
                        executor_id: *executor_id,
                        run_id: *run_id,
                    },
                    lock_expires_at: *lock_expires_at,
                })),

                ExecutionRequest::TemporarilyFailed {
                    backoff_expires_at: expires_at,
                    ..
                }
                | ExecutionRequest::TemporarilyTimedOut {
                    backoff_expires_at: expires_at,
                    ..
                }
                | ExecutionRequest::Unlocked {
                    backoff_expires_at: expires_at,
                    ..
                } => Some(PendingState::PendingAt(PendingStatePendingAt {
                    scheduled_at: *expires_at,
                    last_lock: self.find_last_lock().map(LockedBy::from),
                })),

                ExecutionRequest::HistoryEvent {
                    event:
                        HistoryEvent::JoinNext {
                            join_set_id: expected_join_set_id,
                            run_expires_at: lock_expires_at,
                            closing,
                            requested_ffqn: _,
                        },
                    ..
                } => {
                    let join_next_count = self
                        .event_history()
                        .filter(|(event, _version)| {
                            matches!(
                                event,
                                HistoryEvent::JoinNext {
                                    join_set_id,
                                    ..
                                } if join_set_id == expected_join_set_id
                            )
                        })
                        .count();
                    assert!(join_next_count > 0);
                    // Did the response arrive?
                    let resp = self
                        .responses
                        .iter()
                        .filter_map(|event| match &event.event {
                            JoinSetResponseEventOuter {
                                event: JoinSetResponseEvent { join_set_id, .. },
                                created_at,
                            } if expected_join_set_id == join_set_id => Some(created_at),
                            _ => None,
                        })
                        .nth(join_next_count - 1);
                    if let Some(nth_created_at) = resp {
                        // Original executor has a chance to continue, but after expiry any executor can pick up the execution.
                        let scheduled_at = max(*lock_expires_at, *nth_created_at);
                        Some(PendingState::PendingAt(PendingStatePendingAt {
                            scheduled_at,
                            last_lock: self.find_last_lock().map(LockedBy::from),
                        }))
                    } else {
                        // Still waiting for response
                        Some(PendingState::BlockedByJoinSet(
                            PendingStateBlockedByJoinSet {
                                join_set_id: expected_join_set_id.clone(),
                                lock_expires_at: *lock_expires_at,
                                closing: *closing,
                            },
                        ))
                    }
                }
                ExecutionRequest::Unpaused => {
                    assert!(!unpause_encountered);
                    unpause_encountered = true;
                    None // Treat the unpause as skipping the corresponding pause
                }
                ExecutionRequest::Paused => {
                    if unpause_encountered {
                        // This pause was effectively cancelled by a later unpause, keep looking for last event affecting the pending state
                        unpause_encountered = false;
                        None
                    } else {
                        // No unpauses were found with bigger version
                        unpause_encountered = false; // For sanity check below.
                        Some(PendingState::Paused)
                    }
                }
                // No pending state change for following events:
                ExecutionRequest::HistoryEvent {
                    event:
                        HistoryEvent::JoinSetCreate { .. }
                        | HistoryEvent::JoinSetRequest {
                            // Adding a request does not change pending state.
                            request:
                                JoinSetRequest::DelayRequest { .. }
                                | JoinSetRequest::ChildExecutionRequest { .. },
                            ..
                        }
                        | HistoryEvent::Persist { .. }
                        | HistoryEvent::Schedule { .. }
                        | HistoryEvent::Stub { .. }
                        | HistoryEvent::JoinNextTooMany { .. },
                } => None,
            })
            .expect("journal must begin with Created event");

        assert!(!unpause_encountered, "unpause must be preceeded with pause");
        self.pending_state = pending_state;
        self.component_id = self
            .find_last_lock()
            .map(|locked| locked.component_id.clone())
            .unwrap_or_else(|| self.get_create_request().component_id);
        self.deployment_id = self
            .find_last_lock()
            .map(|locked| locked.deployment_id)
            .unwrap_or_else(|| self.get_create_request().deployment_id);
    }

    pub fn event_history(&self) -> impl Iterator<Item = (HistoryEvent, Version)> + '_ {
        self.execution_events.iter().filter_map(|event| {
            if let ExecutionRequest::HistoryEvent { event: eh, .. } = &event.event {
                Some((eh.clone(), event.version.clone()))
            } else {
                None
            }
        })
    }

    #[must_use]
    pub fn temporary_event_count(&self) -> u32 {
        u32::try_from(
            self.execution_events
                .iter()
                .filter(|event| event.event.is_temporary_event())
                .count(),
        )
        .unwrap()
    }

    #[must_use]
    pub fn params(&self) -> Params {
        self.get_create_request().params
    }

    #[must_use]
    pub fn parent(&self) -> Option<(ExecutionId, JoinSetId)> {
        self.get_create_request().parent.clone()
    }

    #[must_use]
    pub fn as_execution_log(&self) -> ExecutionLog {
        ExecutionLog {
            execution_id: self.execution_id.clone(),
            events: self.execution_events.clone(),
            next_version: self.version(),
            pending_state: self.pending_state.clone(),
            responses: self.responses.clone(),
            component_digest: self.component_id.input_digest.clone(),
            component_type: self.component_id.component_type,
            deployment_id: self.deployment_id,
        }
    }

    pub fn truncate_and_update_pending_state(&mut self, len: usize) {
        self.execution_events.truncate(len);
        self.update_pending_state();
    }
}
