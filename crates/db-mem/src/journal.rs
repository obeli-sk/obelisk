use assert_matches::assert_matches;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::JoinSetId;
use concepts::storage::{
    CreateRequest, ExecutionEvent, ExecutionEventInner, HistoryEvent, JoinSetResponseEvent,
    JoinSetResponseEventOuter,
};
use concepts::storage::{ExecutionLog, PendingState, SpecificError, Version};
use concepts::{ExecutionId, ExecutionMetadata};
use concepts::{FunctionFqn, Params, StrVariant};
use std::cmp::max;
use std::{collections::VecDeque, time::Duration};
use tokio::sync::oneshot;
use val_json::type_wrapper::TypeWrapper;

#[derive(Debug)]
pub(crate) struct ExecutionJournal {
    pub(crate) execution_id: ExecutionId,
    pub(crate) pending_state: PendingState,
    pub(crate) execution_events: VecDeque<ExecutionEvent>,
    pub(crate) responses: Vec<JoinSetResponseEventOuter>,
    pub(crate) response_subscriber: Option<oneshot::Sender<JoinSetResponseEventOuter>>,
}

impl ExecutionJournal {
    #[must_use]
    pub fn new(req: CreateRequest) -> Self {
        let pending_state = PendingState::PendingAt {
            scheduled_at: req.scheduled_at,
        };
        let execution_id = req.execution_id;
        let created_at = req.created_at;
        let event = ExecutionEvent {
            event: ExecutionEventInner::from(req),
            created_at,
        };
        Self {
            execution_id,
            pending_state,
            execution_events: VecDeque::from([event]),
            responses: Vec::default(),
            response_subscriber: None,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.execution_events.len()
    }

    #[allow(dead_code)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.execution_events.is_empty()
    }

    #[must_use]
    pub fn ffqn(&self) -> &FunctionFqn {
        match self.execution_events.front().unwrap() {
            ExecutionEvent {
                event: ExecutionEventInner::Created { ffqn, .. },
                ..
            } => ffqn,
            _ => panic!("first event must be `Created`"),
        }
    }

    #[must_use]
    pub fn version(&self) -> Version {
        Version(self.execution_events.len())
    }

    #[must_use]
    pub fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    #[must_use]
    pub fn metadata(&self) -> &ExecutionMetadata {
        match self.execution_events.front().unwrap() {
            ExecutionEvent {
                event: ExecutionEventInner::Created { metadata, .. },
                ..
            } => metadata,
            _ => panic!("first event must be `Created`"),
        }
    }

    pub fn append(
        &mut self,
        created_at: DateTime<Utc>,
        event: ExecutionEventInner,
    ) -> Result<Version, SpecificError> {
        if self.pending_state == PendingState::Finished {
            return Err(SpecificError::ValidationFailed(StrVariant::Static(
                "already finished",
            )));
        }

        if let ExecutionEventInner::Locked {
            executor_id,
            lock_expires_at,
            run_id,
        } = &event
        {
            self.pending_state.can_append_lock(
                created_at,
                *executor_id,
                *run_id,
                *lock_expires_at,
            )?;
        }

        self.execution_events
            .push_back(ExecutionEvent { created_at, event });
        // update the state
        self.update_pending_state();
        Ok(self.version())
    }

    pub fn append_response(&mut self, created_at: DateTime<Utc>, event: JoinSetResponseEvent) {
        let event = JoinSetResponseEventOuter { created_at, event };
        self.responses.push(event.clone());
        // update the state
        self.update_pending_state();
        if let Some(subscriber) = self.response_subscriber.take() {
            let _ = subscriber.send(event);
        }
    }

    fn update_pending_state(&mut self) {
        self.pending_state = self.calculate_pending_state();
    }

    fn calculate_pending_state(&self) -> PendingState {
        self.execution_events
            .iter()
            .rev()
            .find_map(|event| match &event.event {
                ExecutionEventInner::Created { scheduled_at, .. } => {
                    Some(PendingState::PendingAt {
                        scheduled_at: *scheduled_at,
                    })
                }

                ExecutionEventInner::Unlocked => Some(PendingState::PendingAt {
                    scheduled_at: event.created_at,
                }),

                ExecutionEventInner::Finished { .. } => Some(PendingState::Finished),

                ExecutionEventInner::Locked {
                    executor_id,
                    lock_expires_at,
                    run_id,
                } => Some(PendingState::Locked {
                    executor_id: *executor_id,
                    lock_expires_at: *lock_expires_at,
                    run_id: *run_id,
                }),

                ExecutionEventInner::IntermittentFailure { expires_at, .. }
                | ExecutionEventInner::IntermittentTimeout { expires_at, .. } => {
                    Some(PendingState::PendingAt {
                        scheduled_at: *expires_at,
                    })
                }

                ExecutionEventInner::HistoryEvent {
                    event:
                        HistoryEvent::JoinNext {
                            join_set_id: expected_join_set_id,
                            lock_expires_at,
                        },
                    ..
                } => {
                    let join_next_count = self
                        .event_history()
                        .filter(|e| {
                            matches!(
                                e,
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
                        .filter_map(|event| match event {
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
                        Some(PendingState::PendingAt { scheduled_at })
                    } else {
                        Some(PendingState::BlockedByJoinSet {
                            join_set_id: *expected_join_set_id,
                            lock_expires_at: *lock_expires_at,
                        })
                    }
                }
                ExecutionEventInner::HistoryEvent { .. } => None, // previous event
            })
            .expect("journal must begin with Created event")
    }

    pub fn event_history(&self) -> impl Iterator<Item = HistoryEvent> + '_ {
        self.execution_events.iter().filter_map(|event| {
            if let ExecutionEventInner::HistoryEvent { event: eh, .. } = &event.event {
                Some(eh.clone())
            } else {
                None
            }
        })
    }

    #[must_use]
    pub fn retry_exp_backoff(&self) -> Duration {
        assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { retry_exp_backoff, .. },
                ..
            }) => *retry_exp_backoff)
    }

    #[must_use]
    pub fn max_retries(&self) -> u32 {
        assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { max_retries, .. },
                ..
            }) => *max_retries)
    }

    #[must_use]
    pub fn intermittent_event_count(&self) -> u32 {
        u32::try_from(
            self.execution_events
                .iter()
                .filter(|event| event.event.is_intermittent_event())
                .count(),
        )
        .unwrap()
    }

    #[must_use]
    pub fn params(&self) -> Params {
        assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { params, .. },
                ..
            }) => params.clone())
    }

    #[must_use]
    pub fn parent(&self) -> Option<(ExecutionId, JoinSetId)> {
        assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { parent, .. },
                ..
            }) => *parent)
    }

    #[must_use]
    pub fn return_type(&self) -> Option<&TypeWrapper> {
        assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
            event: ExecutionEventInner::Created { return_type, .. },
            ..
        }) => return_type.as_ref())
    }

    #[must_use]
    pub fn as_execution_log(&self) -> ExecutionLog {
        ExecutionLog {
            execution_id: self.execution_id,
            events: self.execution_events.iter().cloned().collect(),
            version: self.version(),
            pending_state: self.pending_state,
            responses: self.responses.clone(),
        }
    }

    pub fn truncate_and_update_pending_state(&mut self, len: usize) {
        self.execution_events.truncate(len);
        self.update_pending_state();
    }
}
