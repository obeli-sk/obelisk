use crate::activity::Activities;
use std::{fmt::Debug, sync::Arc, time::Duration};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Event {
    HostActivity(HostActivity),
    WasmActivity(WasmActivity),
}

impl Event {
    async fn handle(
        &self,
        activities: Arc<Activities>,
    ) -> Result<Option<Result<String, String>>, anyhow::Error> {
        match self {
            Self::HostActivity(host_activity) => host_activity.handle().await,
            Self::WasmActivity(wasm_activity) => wasm_activity.handle(activities).await,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum HostActivity {
    Sleep(Duration),
    Noop,
}

impl HostActivity {
    async fn handle(&self) -> Result<Option<Result<String, String>>, anyhow::Error> {
        match self {
            Self::Sleep(duration) => {
                tokio::time::sleep(*duration).await;
                Ok(None)
            }
            Self::Noop => Ok(None),
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
    ) -> Result<Option<Result<String, String>>, anyhow::Error> {
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
    Handle(Event),
}

// Holds the wasmtime error in order to avoid cloning the event
#[derive(Clone)]
pub(crate) struct EventWrapper(pub(crate) Arc<anyhow::Error>);
impl AsRef<Event> for EventWrapper {
    fn as_ref(&self) -> &Event {
        match self
            .0
            .source()
            .expect("source must be present")
            .downcast_ref::<HostFunctionError>()
            .expect("source must be HostFunctionError")
        {
            HostFunctionError::Handle(event) => event,
            other => panic!("HostFunctionError::Handle expected, got {other:?}"),
        }
    }
}
impl Debug for EventWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl EventWrapper {
    pub(crate) async fn handle(
        self,
        event_history: &mut EventHistory,
        activities: Arc<Activities>,
    ) -> Result<Option<Result<String, String>>, anyhow::Error> {
        let event = self.as_ref();
        //event_history.persist_start(self.clone());
        let res = event.handle(activities).await?;
        event_history.persist_end(self, res.clone());
        Ok(res)
    }
}

pub(crate) struct CurrentEventHistory {
    event_history: Vec<(EventWrapper, Option<Result<String, String>>)>,
    idx: usize,
}

impl CurrentEventHistory {
    pub(crate) fn new(event_history: &EventHistory) -> Self {
        Self {
            event_history: event_history.copy_vec(),
            idx: 0,
        }
    }

    pub(crate) fn exit_early_or_replay(
        &mut self,
        event: Event,
    ) -> Result<Option<Result<String, String>>, HostFunctionError> {
        match self
            .event_history
            .get(self.idx)
            .map(|(event, res)| (event.as_ref(), res.as_ref()))
        {
            None => {
                // new event needs to be handled by the runtime
                // println!("Handling {event:?}");
                Err(HostFunctionError::Handle(event))
            }
            Some((current, res)) if *current == event => {
                //println!("Replaying {current:?}");
                self.idx += 1;
                Ok(res.map(Clone::clone))
            }
            Some(other) => Err(HostFunctionError::NonDeterminismDetected(format!(
                "Expected {event:?}, got {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct EventHistory(Vec<(EventWrapper, Option<Result<String, String>>)>);
impl EventHistory {
    fn persist_start(&mut self, _key: EventWrapper) {
        // TODO
    }

    fn persist_end(&mut self, key: EventWrapper, val: Option<Result<String, String>>) {
        self.0.push((key, val))
    }

    fn copy_vec(&self) -> Vec<(EventWrapper, Option<Result<String, String>>)> {
        self.0.clone()
    }
}
