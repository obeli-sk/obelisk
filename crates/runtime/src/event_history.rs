use wasmtime::component::Linker;

use crate::activity::Activities;
use std::{fmt::Debug, sync::Arc, time::Duration};

// generate Host trait
wasmtime::component::bindgen!({
    path: "../../wit/workflow-engine/",
    async: true,
    interfaces: "import my-org:workflow-engine/host-activities;",
});

type SupportedActivityResult = Option<Result<String, String>>;

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

// When calling host functions, create events and use `exit_early_or_replay` to continue or break the execution.
#[async_trait::async_trait]
impl my_org::workflow_engine::host_activities::Host for HostImports {
    async fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        let event = Event::HostActivity(HostActivity::HostActivityAsync(HostActivityAsync::Sleep(
            Duration::from_millis(millis),
        )));
        let replay_result = self.current_event_history.handle_or_interrupt(event)?;
        assert!(replay_result.is_none());
        Ok(())
    }

    async fn noop(&mut self) -> wasmtime::Result<()> {
        let event = Event::HostActivity(HostActivity::HostActivitySync(HostActivitySync::Noop));
        let replay_result = self.current_event_history.handle_or_interrupt(event)?;
        assert!(replay_result.is_none());
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Event {
    HostActivity(HostActivity),
    WasmActivity(WasmActivity),
}

impl Event {
    pub(crate) async fn handle(
        &self,
        activities: Arc<Activities>,
    ) -> Result<SupportedActivityResult, anyhow::Error> {
        match self {
            Self::HostActivity(host_activity) => Ok(host_activity.handle().await),
            Self::WasmActivity(wasm_activity) => wasm_activity.handle(activities).await,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum HostActivity {
    HostActivityAsync(HostActivityAsync),
    HostActivitySync(HostActivitySync),
}

impl HostActivity {
    async fn handle(&self) -> SupportedActivityResult {
        match self {
            Self::HostActivityAsync(host_activity_async) => host_activity_async.handle().await,
            Self::HostActivitySync(host_activity_sync) => host_activity_sync.handle(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum HostActivityAsync {
    Sleep(Duration),
}
impl HostActivityAsync {
    async fn handle(&self) -> SupportedActivityResult {
        match self {
            Self::Sleep(duration) => {
                tokio::time::sleep(*duration).await;
                None
            }
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum HostActivitySync {
    Noop,
}
impl HostActivitySync {
    fn handle(&self) -> SupportedActivityResult {
        match self {
            Self::Noop => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WasmActivity {
    pub(crate) ifc_fqn: Arc<String>,
    pub(crate) function_name: Arc<String>,
}

impl WasmActivity {
    async fn handle(
        &self,
        activities: Arc<Activities>,
    ) -> Result<SupportedActivityResult, anyhow::Error> {
        println!(
            "Running activity {ifc_fqn}.{function_name}",
            ifc_fqn = self.ifc_fqn,
            function_name = self.function_name
        );
        let res = activities
            .run(self.ifc_fqn.as_str(), self.function_name.as_str())
            .await?;
        Ok(Some(res))
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum HostFunctionError {
    #[error("non deterministic execution: {0}")]
    NonDeterminismDetected(String),
    #[error("handle: {0:?}")]
    Interrupt(Event),
}

// Holds the wasmtime error in order to avoid cloning the event
#[derive(Clone)]
pub(crate) enum EventWrapper {
    FromErr(Arc<anyhow::Error>),
    HostActivitySync(Event),
}
impl EventWrapper {
    pub(crate) fn new_from_err(err: anyhow::Error) -> Self {
        Self::FromErr(Arc::new(err))
    }
    pub(crate) fn new_from_host_activity_sync(host_activity_sync: HostActivitySync) -> Self {
        Self::HostActivitySync(Event::HostActivity(HostActivity::HostActivitySync(
            host_activity_sync,
        )))
    }
}

impl AsRef<Event> for EventWrapper {
    fn as_ref(&self) -> &Event {
        match self {
            Self::FromErr(err) => match err
                .source()
                .expect("source must be present")
                .downcast_ref::<HostFunctionError>()
                .expect("source must be HostFunctionError")
            {
                HostFunctionError::Interrupt(event) => event,
                other => panic!("HostFunctionError::Handle expected, got {other:?}"),
            },
            Self::HostActivitySync(event) => event,
        }
    }
}
impl Debug for EventWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

pub(crate) struct CurrentEventHistory {
    event_history: Vec<(EventWrapper, SupportedActivityResult)>,
    idx: usize,
    pub(crate) new_sync_events: Vec<(HostActivitySync, SupportedActivityResult)>,
}

impl CurrentEventHistory {
    pub(crate) fn new(event_history: &EventHistory) -> Self {
        Self {
            event_history: event_history.copy_vec(),
            idx: 0,
            new_sync_events: Vec::new(),
        }
    }

    pub(crate) fn handle_or_interrupt_wasm_activity(
        &mut self,
        wasm_activity: WasmActivity,
    ) -> Result<SupportedActivityResult, HostFunctionError> {
        self.handle_or_interrupt(Event::WasmActivity(wasm_activity))
    }

    fn handle_or_interrupt(
        &mut self,
        event: Event,
    ) -> Result<SupportedActivityResult, HostFunctionError> {
        match (
            event,
            self.event_history
                .get(self.idx)
                .map(|(event, res)| (event.as_ref(), res.as_ref())),
        ) {
            // Continue running on HostActivitySync
            (Event::HostActivity(HostActivity::HostActivitySync(host_activity)), None) => {
                // handle the event and continue
                let res = host_activity.handle();
                self.new_sync_events.push((host_activity, res.clone()));
                Ok(res)
            }
            (event, None) => {
                // new event needs to be handled by the runtime
                // println!("Handling {event:?}");
                Err(HostFunctionError::Interrupt(event))
            }
            (event, Some((current, res))) if *current == event => {
                //println!("Replaying {current:?}");
                self.idx += 1;
                Ok(res.map(Clone::clone))
            }
            (event, Some(other)) => Err(HostFunctionError::NonDeterminismDetected(format!(
                "Expected {event:?}, got {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct EventHistory(pub(crate) Vec<(EventWrapper, SupportedActivityResult)>);
impl EventHistory {
    pub(crate) fn persist_start(&mut self, _key: EventWrapper) {
        // TODO
    }

    pub(crate) fn persist_end(&mut self, key: EventWrapper, val: SupportedActivityResult) {
        self.0.push((key, val))
    }

    fn copy_vec(&self) -> Vec<(EventWrapper, SupportedActivityResult)> {
        self.0.clone()
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}
