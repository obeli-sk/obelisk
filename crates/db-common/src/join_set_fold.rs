use concepts::prefixed_ulid::{DelayId, ExecutionIdDerived};
use concepts::storage::{HistoryEvent, JoinSetRequest, JoinSetResponse};
use concepts::{ComponentType, FunctionFqn, JoinSetId};
use indexmap::IndexMap;

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
pub struct JoinSetFold {
    open_join_sets: IndexMap<JoinSetId, IndexMap<JoinSetResponseId, JoinSetMember>>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum JoinSetFoldError {
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

impl JoinSetFold {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn open_join_sets(
        &self,
    ) -> &IndexMap<JoinSetId, IndexMap<JoinSetResponseId, JoinSetMember>> {
        &self.open_join_sets
    }

    pub fn create_join_set(&mut self, join_set_id: JoinSetId) -> Result<(), JoinSetFoldError> {
        let prev = self
            .open_join_sets
            .insert(join_set_id.clone(), IndexMap::new());
        if prev.is_some() {
            Err(JoinSetFoldError::JoinSetAlreadyExists(join_set_id))
        } else {
            Ok(())
        }
    }

    pub fn close_join_set(
        &mut self,
        join_set_id: &JoinSetId,
    ) -> Result<IndexMap<JoinSetResponseId, JoinSetMember>, JoinSetFoldError> {
        self.open_join_sets
            .shift_remove(join_set_id)
            .ok_or_else(|| JoinSetFoldError::JoinSetNotOpen(join_set_id.clone()))
    }

    pub fn insert_child(
        &mut self,
        join_set_id: &JoinSetId,
        child_execution_id: ExecutionIdDerived,
        component_type: ComponentType,
        target_ffqn: FunctionFqn,
    ) -> Result<(), JoinSetFoldError> {
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
    ) -> Result<(), JoinSetFoldError> {
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
    ) -> Result<JoinSetMember, JoinSetFoldError> {
        self.open_join_sets
            .get_mut(join_set_id)
            .ok_or_else(|| JoinSetFoldError::JoinSetNotOpen(join_set_id.clone()))?
            .shift_remove(response_id)
            .ok_or_else(|| JoinSetFoldError::ResponseNotPresent {
                join_set_id: join_set_id.clone(),
                response_id: response_id.clone(),
            })
    }

    pub fn apply_history_event(
        &mut self,
        event: &HistoryEvent,
        child_component_type: Option<ComponentType>,
        consumed_response: Option<&JoinSetResponse>,
    ) -> Result<(), JoinSetFoldError> {
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
    ) -> Result<(), JoinSetFoldError> {
        self.open_join_sets
            .get_mut(join_set_id)
            .ok_or_else(|| JoinSetFoldError::JoinSetNotOpen(join_set_id.clone()))?
            .insert(response_id, member);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use concepts::prefixed_ulid::DelayId;
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
    fn fold_tracks_open_join_set_members() {
        let join_set_id = join_set_id();
        let child_id = child_id(&join_set_id, 1);
        let delay_id = delay_id(&join_set_id, 2);
        let ffqn = FunctionFqn::new_static("testing:integration/workflow-add", "add-workflow");
        let mut fold = JoinSetFold::new();

        fold.apply_history_event(
            &HistoryEvent::JoinSetCreate {
                join_set_id: join_set_id.clone(),
            },
            None,
            None,
        )
        .unwrap();
        fold.apply_history_event(
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
        fold.apply_history_event(
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

        let members = fold.open_join_sets().get(&join_set_id).unwrap();
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
    fn fold_removes_consumed_response_id() {
        let join_set_id = join_set_id();
        let child_id = child_id(&join_set_id, 3);
        let mut fold = JoinSetFold::new();
        fold.create_join_set(join_set_id.clone()).unwrap();
        fold.insert_child(
            &join_set_id,
            child_id.clone(),
            ComponentType::Activity,
            FunctionFqn::new_static("testing:integration/sleep", "sleep"),
        )
        .unwrap();

        fold.apply_history_event(
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

        assert!(fold.open_join_sets().get(&join_set_id).unwrap().is_empty());
    }

    #[test]
    fn closing_join_next_removes_the_join_set() {
        let join_set_id = join_set_id();
        let mut fold = JoinSetFold::new();
        fold.create_join_set(join_set_id.clone()).unwrap();
        fold.insert_delay(&join_set_id, delay_id(&join_set_id, 4))
            .unwrap();

        fold.apply_history_event(
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

        assert!(!fold.open_join_sets().contains_key(&join_set_id));
    }
}
