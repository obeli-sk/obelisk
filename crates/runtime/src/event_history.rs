use crate::{activity::Activities, workflow::AsyncActivityBehavior, FunctionFqn};
use std::{fmt::Debug, sync::Arc, time::Duration};
use tracing::{debug, error, trace};
use wasmtime::component::{Linker, Val};

// generate Host trait
wasmtime::component::bindgen!({
    path: "../../wit/workflow-engine/",
    async: true,
    interfaces: "import my-org:workflow-engine/host-activities;",
});

#[derive(Clone, Debug, PartialEq)]
pub enum SupportedFunctionResult {
    None,
    Single(Val),
}

impl SupportedFunctionResult {
    pub fn new(mut vec: Vec<Val>) -> Self {
        if vec.is_empty() {
            Self::None
        } else if vec.len() == 1 {
            Self::Single(vec.pop().unwrap())
        } else {
            unimplemented!("multi-value return types are not supported")
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Single(_) => 1,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Self::None)
    }
}

impl IntoIterator for SupportedFunctionResult {
    type Item = Val;
    type IntoIter = std::option::IntoIter<Val>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::None => None.into_iter(),
            Self::Single(item) => Some(item).into_iter(),
        }
    }
}

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

// When calling host functions, create events and continue or interrupt the execution.
#[async_trait::async_trait]
impl my_org::workflow_engine::host_activities::Host for HostImports {
    async fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        const FQN: FunctionFqn<'static> =
            FunctionFqn::new("my-org:workflow-engine/host-activities", "sleep");
        let event = Event {
            fqn: Arc::new(FQN),
            params: Arc::new(vec![Val::U64(millis)]),
            kind: EventKind::ActivityAsync(ActivityAsync::HostActivityAsync(
                HostActivityAsync::Sleep(Duration::from_millis(millis)),
            )),
        };
        let replay_result = self
            .current_event_history
            .replay_handle_interrupt(event)
            .await?;
        assert!(replay_result.is_empty());
        Ok(())
    }

    async fn noop(&mut self) -> wasmtime::Result<()> {
        const FQN: FunctionFqn<'static> =
            FunctionFqn::new("my-org:workflow-engine/host-activities", "noop");
        let event = Event {
            fqn: Arc::new(FQN),
            params: Arc::new(vec![]),
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

#[derive(Clone, Debug, PartialEq)]

pub(crate) struct Event {
    pub(crate) fqn: Arc<FunctionFqn<'static>>,
    pub(crate) params: Arc<Vec<Val>>,
    pub(crate) kind: EventKind,
}

impl Event {
    pub fn new_from_interrupt(
        fqn: Arc<FunctionFqn<'static>>,
        params: Arc<Vec<Val>>,
        activity_async: ActivityAsync,
    ) -> Self {
        Self {
            fqn,
            params,
            kind: EventKind::ActivityAsync(activity_async),
        }
    }
    pub fn new_from_wasm_activity(fqn: Arc<FunctionFqn<'static>>, params: Arc<Vec<Val>>) -> Self {
        Self {
            fqn,
            params,
            kind: EventKind::ActivityAsync(ActivityAsync::WasmActivity),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum EventKind {
    HostActivitySync(HostActivitySync),
    ActivityAsync(ActivityAsync),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ActivityAsync {
    WasmActivity,
    HostActivityAsync(HostActivityAsync),
}

impl ActivityAsync {
    pub(crate) async fn handle(
        &self,
        fqn: &FunctionFqn<'static>,
        params: &[Val],
        activities: &Activities,
    ) -> Result<SupportedFunctionResult, anyhow::Error> {
        match self {
            Self::WasmActivity => activities.run(fqn, params).await,
            Self::HostActivityAsync(h) => h.handle().await,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum HostActivityAsync {
    Sleep(Duration),
}
impl HostActivityAsync {
    async fn handle(&self) -> Result<SupportedFunctionResult, wasmtime::Error> {
        match self {
            Self::Sleep(duration) => {
                tokio::time::sleep(*duration).await;
                Ok(SupportedFunctionResult::None)
            }
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum HostActivitySync {
    Noop,
}
impl HostActivitySync {
    fn handle(&self) -> Result<SupportedFunctionResult, HostActivitySyncError> {
        match self {
            Self::Noop => Ok(SupportedFunctionResult::None),
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("host activity failed: {source:?}`")]
struct HostActivitySyncError {
    source: anyhow::Error,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum HostFunctionError {
    #[error("non deterministic execution: `{0}`")]
    NonDeterminismDetected(String),
    #[error("interrupt: `{fqn}`, `{activity_async:?}`")]
    Interrupt {
        fqn: Arc<FunctionFqn<'static>>,
        params: Arc<Vec<Val>>,
        activity_async: ActivityAsync,
    },
    #[error("activity `{activity_fqn}` failed: `{source}`")]
    ActivityFailed {
        activity_fqn: Arc<FunctionFqn<'static>>,
        source: anyhow::Error,
    },
}

pub(crate) struct CurrentEventHistory {
    activities: Arc<Activities>,
    pub(crate) event_history: EventHistory,
    idx: usize,
    async_activity_behavior: AsyncActivityBehavior,
}

impl CurrentEventHistory {
    pub(crate) fn new(
        event_history: EventHistory,
        activities: Arc<Activities>,
        async_activity_behavior: AsyncActivityBehavior,
    ) -> Self {
        Self {
            activities,
            event_history,
            idx: 0,
            async_activity_behavior,
        }
    }

    pub(crate) fn persist_start(&mut self, fqn: &FunctionFqn<'_>, params: &[Val]) {
        self.event_history.persist_start(fqn, params)
    }

    pub(crate) fn persist_end(
        &mut self,
        fqn: Arc<FunctionFqn<'static>>,
        params: Arc<Vec<Val>>,
        val: SupportedFunctionResult,
    ) {
        self.event_history.persist_end(fqn, params, val);
        self.idx += 1;
    }

    pub(crate) async fn replay_handle_interrupt(
        &mut self,
        event: Event,
    ) -> Result<SupportedFunctionResult, HostFunctionError> {
        let found = self.event_history.0.get(self.idx);
        let found_matches = matches!(found,  Some((found_fqn, found_params, _replay_result))
            if event.fqn == *found_fqn && event.params == *found_params);
        trace!(
            "replay_handle_interrupt {fqn}, found: {found_matches}, idx: {idx}, history.len: {len}",
            fqn = event.fqn,
            idx = self.idx,
            len = self.event_history.len()
        );
        match (event, found_matches, found) {
            // Continue running on HostActivitySync
            (
                Event {
                    fqn,
                    params,
                    kind: EventKind::HostActivitySync(host_activity),
                },
                _,
                None,
            ) => {
                debug!("Running {host_activity:?}");
                self.persist_start(&fqn, &params);
                let res =
                    host_activity
                        .handle()
                        .map_err(|err| HostFunctionError::ActivityFailed {
                            activity_fqn: fqn.clone(),
                            source: err.source,
                        })?;
                // TODO: persist activity failure
                self.persist_end(fqn.clone(), params.clone(), res.clone());
                Ok(res)
            }
            // Replay if found
            (event, true, Some((_, _, replay_result))) => {
                debug!("Replaying [{idx}] {fqn}", idx = self.idx, fqn = event.fqn);
                self.idx += 1;
                Ok(replay_result.clone())
            }
            // New event needs to be handled by the runtime, interrupt or execute it.
            (
                Event {
                    fqn,
                    params,
                    kind: EventKind::ActivityAsync(activity_async),
                },
                _,
                None,
            ) => match self.async_activity_behavior {
                AsyncActivityBehavior::Restart => {
                    debug!("Interrupting {fqn}");
                    Err(HostFunctionError::Interrupt {
                        fqn,
                        params,
                        activity_async,
                    })
                }
                AsyncActivityBehavior::KeepWaiting => {
                    debug!("Executing {fqn}");
                    self.persist_start(&fqn, &params);
                    let res = activity_async
                        .handle(&fqn, &params, &self.activities)
                        .await
                        .map_err(|source| HostFunctionError::ActivityFailed {
                            activity_fqn: fqn.clone(),
                            source,
                        })?;
                    // TODO: persist activity failure
                    self.persist_end(fqn.clone(), params.clone(), res.clone());
                    Ok(res)
                }
            },
            // Non determinism
            (event, false, Some(found)) => Err(HostFunctionError::NonDeterminismDetected(format!(
                "Expected {found:?}, got {event:?}"
            ))),
        }
    }
}

#[derive(Debug, Default)]
pub struct EventHistory(
    Vec<(
        Arc<FunctionFqn<'static>>,
        Arc<Vec<Val>>,
        SupportedFunctionResult,
    )>,
);
impl EventHistory {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn persist_start(&mut self, _fqn: &FunctionFqn<'_>, _params: &[Val]) {
        // TODO
    }

    fn persist_end(
        &mut self,
        fqn: Arc<FunctionFqn<'static>>,
        params: Arc<Vec<Val>>,
        val: SupportedFunctionResult,
    ) {
        self.0.push((fqn, params, val));
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
