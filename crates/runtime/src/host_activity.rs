use crate::{
    activity::ActivityRequest,
    event_history::{CurrentEventHistory, Event, EventKind, HostActivitySync},
    ActivityResponse, FunctionFqnStr, SupportedFunctionResult,
};
use assert_matches::assert_matches;
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;
use wasmtime::component::{Linker, Val};

// generate Host trait
wasmtime::component::bindgen!({
    path: "../../wit/workflow-engine/",
    async: true,
    interfaces: "import my-org:workflow-engine/host-activities;",
});

pub(crate) struct HostImports {
    pub(crate) current_event_history: CurrentEventHistory,
}

impl HostImports {
    pub(crate) fn add_to_linker(linker: &mut Linker<Self>) -> Result<(), anyhow::Error> {
        my_org::workflow_engine::host_activities::add_to_linker(
            linker,
            |state: &mut HostImports| state,
        )
    }
}
pub(crate) const HOST_ACTIVITY_PACKAGE: &str = "my-org:workflow-engine/host-activities";

pub(crate) const HOST_ACTIVITY_SLEEP_FQN: FunctionFqnStr<'static> =
    FunctionFqnStr::new(HOST_ACTIVITY_PACKAGE, "sleep");

// When calling host functions, create events and continue or interrupt the execution.
#[async_trait::async_trait]
impl my_org::workflow_engine::host_activities::Host for HostImports {
    async fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        let event = Event {
            request: ActivityRequest {
                workflow_id: self.current_event_history.workflow_id.clone(),
                fqn: HOST_ACTIVITY_SLEEP_FQN.to_owned(),
                params: Arc::new(vec![Val::U64(millis)]),
            },
            kind: EventKind::ActivityAsync,
        };
        let replay_result = self
            .current_event_history
            .replay_handle_interrupt(event)
            .await?;
        assert!(replay_result.is_empty());
        Ok(())
    }

    async fn noop(&mut self) -> wasmtime::Result<()> {
        const FQN: FunctionFqnStr = FunctionFqnStr::new(HOST_ACTIVITY_PACKAGE, "noop");
        let event = Event {
            request: ActivityRequest {
                workflow_id: self.current_event_history.workflow_id.clone(),
                fqn: FQN.to_owned(),
                params: Arc::new(vec![]),
            },
            kind: EventKind::HostActivitySync(HostActivitySync::Noop),
        };
        let replay_result = self
            .current_event_history
            .replay_handle_interrupt(event)
            .await?;
        assert!(replay_result.is_empty());
        Ok(())
    }
}

pub(crate) async fn execute_host_activity(
    request: ActivityRequest,
    resp_tx: oneshot::Sender<ActivityResponse>,
) {
    if request.fqn == HOST_ACTIVITY_SLEEP_FQN {
        // sleep implementation
        assert_eq!(request.params.len(), 1);
        let duration = request.params.first().unwrap();
        let duration = *assert_matches!(duration, wasmtime::component::Val::U64(v) => v);
        tokio::time::sleep(Duration::from_millis(duration)).await;
        let _ = resp_tx.send(Ok(SupportedFunctionResult::None));
    } else {
        panic!("cannot execute host activity `{}`", request.fqn);
    }
}
