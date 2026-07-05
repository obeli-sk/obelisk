use concepts::prefixed_ulid::{DelayId, ExecutionIdDerived};
use concepts::storage::{HistoryEvent, JoinNextTryOutcome, JoinSetRequest, JoinSetResponse};
use concepts::{ComponentType, FunctionFqn, JoinSetId};
use indexmap::IndexMap;
use std::collections::HashMap;

/// Simplified version of [`JoinSetResponse`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinSetResponseId {
    ChildExecutionId(ExecutionIdDerived),
    DelayId(DelayId),
}

impl From<&JoinSetResponse> for JoinSetResponseId {
    fn from(value: &JoinSetResponse) -> Self {
        match value {
            JoinSetResponse::ChildExecutionFinished {
                child_execution_id, ..
            } => Self::ChildExecutionId(child_execution_id.clone()),
            JoinSetResponse::DelayFinished { delay_id, .. } => Self::DelayId(delay_id.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinSetMember {
    Child {
        component_type: ComponentType,
        target_ffqn: FunctionFqn,
    },
    Delay,
}

impl JoinSetMember {
    #[must_use]
    pub fn is_activity(&self) -> bool {
        matches!(
            self,
            Self::Child {
                component_type,
                target_ffqn: _
            } if component_type.is_activity()
        )
    }

    #[must_use]
    pub fn is_cancellable_workflow(&self) -> bool {
        matches!(
            self,
            Self::Child {
                component_type: ComponentType::Workflow,
                target_ffqn
            } if target_ffqn.is_cancellable()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JoinSetOpenTracker {
    open_join_sets: IndexMap<JoinSetId, IndexMap<JoinSetResponseId, JoinSetMember>>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum JoinSetOpenTrackerError {
    #[error("join set already exists: `{0}`")]
    JoinSetAlreadyExists(JoinSetId),
    #[error("join set is not open: `{0}`")]
    JoinSetNotOpen(JoinSetId),
    #[error("response is not present in join set `{join_set_id}`: `{response_id:?}`")]
    ResponseNotPresent {
        join_set_id: JoinSetId,
        response_id: JoinSetResponseId,
    },
}

impl JoinSetOpenTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Reconstruct the open join sets from a persisted execution log, for the
    /// cancellation driver (which cannot replay WASM). Walks the history events,
    /// pairing each consuming `JoinNext` with the next response of its join set in
    /// cursor order. `child_component_type` classifies each child request
    /// (activity vs workflow) from the log. Members left in the result are the
    /// still-unawaited ones; those without a response are still running.
    pub fn reconstruct(
        history: impl IntoIterator<Item = HistoryEvent>,
        responses: impl IntoIterator<Item = (JoinSetId, JoinSetResponseId)>,
        mut child_component_type: impl FnMut(&ExecutionIdDerived) -> ComponentType,
    ) -> Result<JoinSetOpenTracker, JoinSetOpenTrackerError> {
        // Responses per join set in cursor order, plus how many were consumed.
        let mut per_join_set: HashMap<JoinSetId, (Vec<JoinSetResponseId>, usize)> = HashMap::new();
        for (join_set_id, response_id) in responses {
            per_join_set
                .entry(join_set_id)
                .or_default()
                .0
                .push(response_id);
        }
        let mut tracker = JoinSetOpenTracker::new();
        for event in history {
            match event {
                HistoryEvent::JoinSetCreate { join_set_id } => {
                    tracker.create_join_set(join_set_id)?;
                }
                HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request:
                        JoinSetRequest::ChildExecutionRequest {
                            child_execution_id,
                            target_ffqn,
                            result: Ok(()),
                            ..
                        },
                } => {
                    let component_type = child_component_type(&child_execution_id);
                    tracker.insert_child(
                        &join_set_id,
                        child_execution_id,
                        component_type,
                        target_ffqn,
                    )?;
                }
                HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::DelayRequest { delay_id, .. },
                } => tracker.insert_delay(&join_set_id, delay_id)?,
                // A worker close appends one closing `JoinNext` per member; the first
                // drops the whole set, the rest are no-ops.
                HistoryEvent::JoinNext {
                    join_set_id,
                    closing: true,
                    ..
                } => {
                    let _ = tracker.close_join_set(&join_set_id);
                }
                HistoryEvent::JoinNext {
                    join_set_id,
                    closing: false,
                    ..
                }
                | HistoryEvent::JoinNextTry {
                    join_set_id,
                    outcome: JoinNextTryOutcome::Found,
                } => {
                    if let Some((responses, cursor)) = per_join_set.get_mut(&join_set_id)
                        && let Some(response_id) = responses.get(*cursor)
                    {
                        tracker.remove_response(&join_set_id, response_id)?;
                        *cursor += 1;
                    }
                }
                _ => {}
            }
        }
        Ok(tracker)
    }

    #[must_use]
    pub fn open_join_sets(
        &self,
    ) -> &IndexMap<JoinSetId, IndexMap<JoinSetResponseId, JoinSetMember>> {
        &self.open_join_sets
    }

    pub fn create_join_set(
        &mut self,
        join_set_id: JoinSetId,
    ) -> Result<(), JoinSetOpenTrackerError> {
        let prev = self
            .open_join_sets
            .insert(join_set_id.clone(), IndexMap::new());
        if prev.is_some() {
            Err(JoinSetOpenTrackerError::JoinSetAlreadyExists(join_set_id))
        } else {
            Ok(())
        }
    }

    pub fn close_join_set(
        &mut self,
        join_set_id: &JoinSetId,
    ) -> Result<IndexMap<JoinSetResponseId, JoinSetMember>, JoinSetOpenTrackerError> {
        self.open_join_sets
            .shift_remove(join_set_id)
            .ok_or_else(|| JoinSetOpenTrackerError::JoinSetNotOpen(join_set_id.clone()))
    }

    pub fn insert_child(
        &mut self,
        join_set_id: &JoinSetId,
        child_execution_id: ExecutionIdDerived,
        component_type: ComponentType,
        target_ffqn: FunctionFqn,
    ) -> Result<(), JoinSetOpenTrackerError> {
        self.insert_member(
            join_set_id,
            JoinSetResponseId::ChildExecutionId(child_execution_id),
            JoinSetMember::Child {
                component_type,
                target_ffqn,
            },
        )
    }

    pub fn insert_delay(
        &mut self,
        join_set_id: &JoinSetId,
        delay_id: DelayId,
    ) -> Result<(), JoinSetOpenTrackerError> {
        self.insert_member(
            join_set_id,
            JoinSetResponseId::DelayId(delay_id),
            JoinSetMember::Delay,
        )
    }

    pub fn remove_response(
        &mut self,
        join_set_id: &JoinSetId,
        response_id: &JoinSetResponseId,
    ) -> Result<JoinSetMember, JoinSetOpenTrackerError> {
        self.open_join_sets
            .get_mut(join_set_id)
            .ok_or_else(|| JoinSetOpenTrackerError::JoinSetNotOpen(join_set_id.clone()))?
            .shift_remove(response_id)
            .ok_or_else(|| JoinSetOpenTrackerError::ResponseNotPresent {
                join_set_id: join_set_id.clone(),
                response_id: response_id.clone(),
            })
    }

    #[cfg(test)]
    fn apply_history_event(
        &mut self,
        event: &concepts::storage::HistoryEvent,
        child_component_type: Option<ComponentType>,
        consumed_response: Option<&JoinSetResponse>,
    ) -> Result<(), JoinSetOpenTrackerError> {
        use concepts::storage::{HistoryEvent, JoinSetRequest};
        match event {
            HistoryEvent::JoinSetCreate { join_set_id } => {
                self.create_join_set(join_set_id.clone())?;
            }
            HistoryEvent::JoinSetRequest {
                join_set_id,
                request:
                    JoinSetRequest::ChildExecutionRequest {
                        child_execution_id,
                        target_ffqn,
                        result,
                        ..
                    },
            } if result.is_ok() => {
                let component_type = child_component_type
                    .expect("child_component_type must be provided for successful child requests");
                self.insert_child(
                    join_set_id,
                    child_execution_id.clone(),
                    component_type,
                    target_ffqn.clone(),
                )?;
            }
            HistoryEvent::JoinSetRequest {
                join_set_id,
                request: JoinSetRequest::DelayRequest { delay_id, .. },
            } => {
                self.insert_delay(join_set_id, delay_id.clone())?;
            }
            HistoryEvent::JoinNext {
                join_set_id,
                closing,
                ..
            } => {
                if *closing {
                    self.close_join_set(join_set_id)?;
                } else if let Some(response) = consumed_response {
                    let response_id = JoinSetResponseId::from(response);
                    self.remove_response(join_set_id, &response_id)?;
                }
            }
            HistoryEvent::JoinNextTry { join_set_id, .. } => {
                if let Some(response) = consumed_response {
                    let response_id = JoinSetResponseId::from(response);
                    self.remove_response(join_set_id, &response_id)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn insert_member(
        &mut self,
        join_set_id: &JoinSetId,
        response_id: JoinSetResponseId,
        member: JoinSetMember,
    ) -> Result<(), JoinSetOpenTrackerError> {
        self.open_join_sets
            .get_mut(join_set_id)
            .ok_or_else(|| JoinSetOpenTrackerError::JoinSetNotOpen(join_set_id.clone()))?
            .insert(response_id, member);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use concepts::prefixed_ulid::DelayId;
    use concepts::storage::{HistoryEvent, JoinSetRequest};
    use concepts::{ExecutionId, JoinSetKind, Params, StrVariant};

    fn join_set_id() -> JoinSetId {
        JoinSetId::new(JoinSetKind::Named, StrVariant::from("test".to_string())).unwrap()
    }

    fn child_id(join_set_id: &JoinSetId, n: u128) -> concepts::prefixed_ulid::ExecutionIdDerived {
        ExecutionId::from_parts(1, n).next_level(join_set_id)
    }

    fn delay_id(join_set_id: &JoinSetId, n: u64) -> DelayId {
        DelayId::new_with_index(&ExecutionId::from_parts(1, u128::from(n)), join_set_id, n)
    }

    #[test]
    fn tracker_tracks_open_join_set_members() {
        let join_set_id = join_set_id();
        let child_id = child_id(&join_set_id, 1);
        let delay_id = delay_id(&join_set_id, 2);
        let ffqn = FunctionFqn::new_static("testing:integration/workflow-add", "add-workflow");
        let mut tracker = JoinSetOpenTracker::new();

        tracker
            .apply_history_event(
                &HistoryEvent::JoinSetCreate {
                    join_set_id: join_set_id.clone(),
                },
                None,
                None,
            )
            .unwrap();
        tracker
            .apply_history_event(
                &HistoryEvent::JoinSetRequest {
                    join_set_id: join_set_id.clone(),
                    request: JoinSetRequest::ChildExecutionRequest {
                        child_execution_id: child_id.clone(),
                        target_ffqn: ffqn.clone(),
                        params: Params::empty(),
                        result: Ok(()),
                    },
                },
                Some(ComponentType::Workflow),
                None,
            )
            .unwrap();
        tracker
            .apply_history_event(
                &HistoryEvent::JoinSetRequest {
                    join_set_id: join_set_id.clone(),
                    request: JoinSetRequest::DelayRequest {
                        delay_id: delay_id.clone(),
                        expires_at: chrono::DateTime::UNIX_EPOCH,
                        schedule_at: concepts::storage::HistoryEventScheduleAt::Now,
                        paused: false,
                    },
                },
                None,
                None,
            )
            .unwrap();

        let members = tracker.open_join_sets().get(&join_set_id).unwrap();
        assert_eq!(members.len(), 2);
        assert_eq!(
            members.get(&JoinSetResponseId::ChildExecutionId(child_id)),
            Some(&JoinSetMember::Child {
                component_type: ComponentType::Workflow,
                target_ffqn: ffqn,
            })
        );
        assert_eq!(
            members.get(&JoinSetResponseId::DelayId(delay_id)),
            Some(&JoinSetMember::Delay)
        );
    }

    #[test]
    fn tracker_removes_consumed_response_id() {
        let join_set_id = join_set_id();
        let child_id = child_id(&join_set_id, 3);
        let mut tracker = JoinSetOpenTracker::new();
        tracker.create_join_set(join_set_id.clone()).unwrap();
        tracker
            .insert_child(
                &join_set_id,
                child_id.clone(),
                ComponentType::Activity,
                FunctionFqn::new_static("testing:integration/sleep", "sleep"),
            )
            .unwrap();

        tracker
            .apply_history_event(
                &HistoryEvent::JoinNext {
                    join_set_id: join_set_id.clone(),
                    run_expires_at: chrono::DateTime::UNIX_EPOCH,
                    requested_ffqn: Some(FunctionFqn::new_static(
                        "testing:integration/other",
                        "other",
                    )),
                    closing: false,
                },
                None,
                Some(&JoinSetResponse::ChildExecutionFinished {
                    child_execution_id: child_id.clone(),
                    finished_version: concepts::storage::Version::new(2),
                    result: concepts::SUPPORTED_RETURN_VALUE_OK_EMPTY,
                }),
            )
            .unwrap();

        assert!(
            tracker
                .open_join_sets()
                .get(&join_set_id)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn reconstruct_keeps_unresponded_members_open() {
        let join_set_id = join_set_id();
        let awaited = child_id(&join_set_id, 1);
        let running = child_id(&join_set_id, 2);
        let ffqn = FunctionFqn::new_static("testing:integration/workflow", "child");
        let history = vec![
            HistoryEvent::JoinSetCreate {
                join_set_id: join_set_id.clone(),
            },
            HistoryEvent::JoinSetRequest {
                join_set_id: join_set_id.clone(),
                request: JoinSetRequest::ChildExecutionRequest {
                    child_execution_id: awaited.clone(),
                    target_ffqn: ffqn.clone(),
                    params: Params::empty(),
                    result: Ok(()),
                },
            },
            HistoryEvent::JoinSetRequest {
                join_set_id: join_set_id.clone(),
                request: JoinSetRequest::ChildExecutionRequest {
                    child_execution_id: running.clone(),
                    target_ffqn: ffqn.clone(),
                    params: Params::empty(),
                    result: Ok(()),
                },
            },
            HistoryEvent::JoinNext {
                join_set_id: join_set_id.clone(),
                run_expires_at: chrono::DateTime::UNIX_EPOCH,
                requested_ffqn: None,
                closing: false,
            },
        ];
        // Only the awaited child has landed a response.
        let responses = vec![(
            join_set_id.clone(),
            JoinSetResponseId::ChildExecutionId(awaited),
        )];

        let tracker =
            JoinSetOpenTracker::reconstruct(history, responses, |_| ComponentType::Workflow)
                .unwrap();

        let members = tracker.open_join_sets().get(&join_set_id).unwrap();
        assert_eq!(
            members.len(),
            1,
            "the awaited child is consumed, running stays"
        );
        assert!(members.contains_key(&JoinSetResponseId::ChildExecutionId(running)));
    }

    #[test]
    fn closing_join_next_removes_the_join_set() {
        let join_set_id = join_set_id();
        let mut tracker = JoinSetOpenTracker::new();
        tracker.create_join_set(join_set_id.clone()).unwrap();
        tracker
            .insert_delay(&join_set_id, delay_id(&join_set_id, 4))
            .unwrap();

        tracker
            .apply_history_event(
                &HistoryEvent::JoinNext {
                    join_set_id: join_set_id.clone(),
                    run_expires_at: chrono::DateTime::UNIX_EPOCH,
                    requested_ffqn: None,
                    closing: true,
                },
                None,
                None,
            )
            .unwrap();

        assert!(!tracker.open_join_sets().contains_key(&join_set_id));
    }

    /// Differential property test: `reconstruct` (the driver's log reconstruction) must agree
    /// with the worker's incremental `JoinSetOpenTracker` maintenance for every valid
    /// execution log. The worker consumes each join set's responses in arrival
    /// (cursor) order — a plain `JoinNext` removes the returned response, an
    /// await-next `FunctionMismatch` removes the *arriving* id, both of which are
    /// the next unconsumed arrival — and a close drops the whole set. This test
    /// generates randomized-but-valid scripts, computes the expected open set with a
    /// worker-faithful FIFO model, and asserts `reconstruct` matches, guarding the
    /// two trackers against drift (the P8 "fiddly part": `JoinNext`<->response pairing,
    /// closing `JoinNext`s, per-join-set isolation).
    #[test]
    fn reconstruct_matches_worker_fifo_model() {
        // Self-contained deterministic PRNG (db-common has no `rand` dev-dep).
        struct Lcg(u64);
        impl Lcg {
            fn next_u64(&mut self) -> u64 {
                self.0 = self
                    .0
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                self.0 >> 33
            }
            fn below(&mut self, n: usize) -> usize {
                usize::try_from(self.next_u64() % n as u64).expect("modulo n fits in usize")
            }
            fn coin(&mut self) -> bool {
                self.next_u64() & 1 == 0
            }
        }

        let cancellable = FunctionFqn::new_static("testing:it/wf", "child-cancellable");
        let plain = FunctionFqn::new_static("testing:it/wf", "child");

        for seed in 0..2000u64 {
            let mut rng = Lcg(seed.wrapping_mul(2_654_435_761).wrapping_add(1));
            let mut uid: u64 = 0; // globally-unique id source across join sets

            let mut history: Vec<HistoryEvent> = Vec::new();
            let mut responses: Vec<(JoinSetId, JoinSetResponseId)> = Vec::new();
            // Worker-faithful expected result and the child classification the closure returns.
            let mut expected: IndexMap<JoinSetId, IndexMap<JoinSetResponseId, JoinSetMember>> =
                IndexMap::new();
            let mut child_types: HashMap<ExecutionIdDerived, ComponentType> = HashMap::new();

            let num_sets = 1 + rng.below(3);
            for s in 0..num_sets {
                let js =
                    JoinSetId::new(JoinSetKind::Named, StrVariant::from(format!("js{s}"))).unwrap();
                history.push(HistoryEvent::JoinSetCreate {
                    join_set_id: js.clone(),
                });

                // Members, in insertion order, with their response id + member value.
                let num_members = rng.below(5);
                let mut members: Vec<(JoinSetResponseId, JoinSetMember)> =
                    Vec::with_capacity(num_members);
                for _ in 0..num_members {
                    uid += 1;
                    if rng.below(3) == 0 {
                        // Delay member.
                        let did = delay_id(&js, uid);
                        history.push(HistoryEvent::JoinSetRequest {
                            join_set_id: js.clone(),
                            request: JoinSetRequest::DelayRequest {
                                delay_id: did.clone(),
                                expires_at: chrono::DateTime::UNIX_EPOCH,
                                schedule_at: concepts::storage::HistoryEventScheduleAt::Now,
                                paused: false,
                            },
                        });
                        members.push((JoinSetResponseId::DelayId(did), JoinSetMember::Delay));
                    } else {
                        // Child member (activity, plain workflow, or cancellable workflow).
                        let cid = child_id(&js, u128::from(uid));
                        let (component_type, ffqn) = match rng.below(3) {
                            0 => (ComponentType::Activity, plain.clone()),
                            1 => (ComponentType::Workflow, plain.clone()),
                            _ => (ComponentType::Workflow, cancellable.clone()),
                        };
                        child_types.insert(cid.clone(), component_type);
                        history.push(HistoryEvent::JoinSetRequest {
                            join_set_id: js.clone(),
                            request: JoinSetRequest::ChildExecutionRequest {
                                child_execution_id: cid.clone(),
                                target_ffqn: ffqn.clone(),
                                params: Params::empty(),
                                result: Ok(()),
                            },
                        });
                        members.push((
                            JoinSetResponseId::ChildExecutionId(cid),
                            JoinSetMember::Child {
                                component_type,
                                target_ffqn: ffqn,
                            },
                        ));
                    }
                }

                // Responders: a random subset in a random arrival order (partial
                // Fisher-Yates over member indices, then truncate).
                let mut order: Vec<usize> = (0..members.len()).collect();
                for i in 0..order.len() {
                    let j = i + rng.below(order.len() - i);
                    order.swap(i, j);
                }
                let responder_count = if members.is_empty() {
                    0
                } else {
                    rng.below(members.len() + 1)
                };
                let arrivals: Vec<usize> = order.into_iter().take(responder_count).collect();
                for &idx in &arrivals {
                    responses.push((js.clone(), members[idx].0.clone()));
                }

                // Consuming JoinNexts: may exceed arrivals (excess = blocking no-ops).
                let consume = rng.below(members.len() + 2);
                for _ in 0..consume {
                    history.push(HistoryEvent::JoinNext {
                        join_set_id: js.clone(),
                        run_expires_at: chrono::DateTime::UNIX_EPOCH,
                        // Vary requested_ffqn to also cover the await-next log shape.
                        requested_ffqn: if rng.coin() {
                            Some(plain.clone())
                        } else {
                            None
                        },
                        closing: false,
                    });
                }

                let will_close = rng.coin();

                // Worker-faithful expected open set for this join set.
                if will_close {
                    // Close drops the whole set (emit one closing JoinNext; the driver
                    // tracker treats the first as the drop and any extra as no-ops).
                    history.push(HistoryEvent::JoinNext {
                        join_set_id: js.clone(),
                        run_expires_at: chrono::DateTime::UNIX_EPOCH,
                        requested_ffqn: None,
                        closing: true,
                    });
                    // Absent from `expected`.
                } else {
                    // Consumed = first min(consume, arrivals) arrivals, FIFO.
                    let consumed_count = consume.min(arrivals.len());
                    let consumed: std::collections::HashSet<usize> =
                        arrivals.iter().copied().take(consumed_count).collect();
                    let mut open = IndexMap::new();
                    for (i, (rid, member)) in members.iter().enumerate() {
                        if !consumed.contains(&i) {
                            open.insert(rid.clone(), member.clone());
                        }
                    }
                    expected.insert(js.clone(), open);
                }
            }

            let tracker = JoinSetOpenTracker::reconstruct(history, responses, |cid| {
                child_types
                    .get(cid)
                    .copied()
                    .expect("every child id was classified")
            })
            .unwrap_or_else(|err| panic!("seed {seed}: reconstruct failed: {err}"));

            assert_eq!(
                tracker.open_join_sets(),
                &expected,
                "seed {seed}: reconstruct diverged from the worker FIFO model"
            );
        }
    }
}
