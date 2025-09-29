use super::data::{BusyIntervalStatus, TraceData};
use crate::{
    app::{AppState, Route},
    components::{
        execution_detail::utils::{compute_join_next_to_response, event_to_detail},
        execution_header::{ExecutionHeader, ExecutionLink},
        trace::{
            data::{BusyInterval, TraceDataChild, TraceDataRoot},
            execution_trace::ExecutionTrace,
        },
    },
    grpc::{
        execution_id::{EXECUTION_ID_INFIX, ExecutionIdExt as _},
        ffqn::FunctionFqn,
        grpc_client::{
            self, ComponentType, ExecutionEvent, ExecutionId, JoinSetId, JoinSetResponseEvent,
            ResponseWithCursor, ResultDetail,
            execution_event::{
                self, Finished, TemporarilyFailed, TemporarilyTimedOut,
                history_event::{JoinSetRequest, join_set_request},
            },
            http_client_trace, join_set_response_event, result_detail,
        },
    },
};
use assert_matches::assert_matches;
use chrono::{DateTime, Utc};
use gloo::timers::future::TimeoutFuture;
use hashbrown::HashMap;
use log::{debug, trace};
use std::{ops::Deref as _, rc::Rc};
use yew::prelude::*;
use yew_router::prelude::Link;

#[derive(Properties, PartialEq)]
pub struct TraceViewProps {
    pub execution_id: grpc_client::ExecutionId,
}

pub const PAGE: u32 = 500;
pub const SLEEP_MILLIS: u32 = 500;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Cursors {
    version_from: u32,
    responses_cursor_from: u32,
}

#[derive(Clone, Copy, PartialEq)]
enum ExecutionFetchState {
    Requested(Cursors),
    Pending,
    Finished,
}

enum TraceviewStateAction {
    AddExecutionId(ExecutionId),
    HttpTracesSwitch {
        execution_id: ExecutionId,
        show: bool,
    },
    SetPending(ExecutionId),
    SavePage {
        execution_id: ExecutionId,
        new_events: Vec<ExecutionEvent>,
        new_responses: Vec<ResponseWithCursor>,
        is_finished: bool,
    },
    RequestNextPage {
        execution_id: ExecutionId,
        cursors: Cursors,
    },
}

#[derive(Default, Clone, PartialEq)]
struct TraceViewState {
    execution_ids_to_fetch_state: HashMap<ExecutionId, ExecutionFetchState>,
    events: HashMap<ExecutionId, Vec<ExecutionEvent>>,
    responses: HashMap<ExecutionId, HashMap<JoinSetId, Vec<JoinSetResponseEvent>>>,
    execution_ids_to_show_http_traces: HashMap<ExecutionId, bool>,
}
impl Reducible for TraceViewState {
    type Action = TraceviewStateAction;

    fn reduce(self: Rc<Self>, action: Self::Action) -> Rc<Self> {
        match action {
            TraceviewStateAction::AddExecutionId(execution_id) => {
                if !self
                    .execution_ids_to_fetch_state
                    .contains_key(&execution_id)
                {
                    let mut this = self.as_ref().clone();
                    this.execution_ids_to_fetch_state.insert(
                        execution_id,
                        ExecutionFetchState::Requested(Cursors::default()),
                    );
                    Rc::from(this)
                } else {
                    self
                }
            }
            TraceviewStateAction::SetPending(execution_id) => {
                let mut this = self.as_ref().clone();
                this.execution_ids_to_fetch_state
                    .insert(execution_id, ExecutionFetchState::Pending);
                Rc::from(this)
            }
            TraceviewStateAction::RequestNextPage {
                execution_id,
                cursors,
            } => {
                let mut this = self.as_ref().clone();
                this.execution_ids_to_fetch_state
                    .insert(execution_id, ExecutionFetchState::Requested(cursors));
                Rc::from(this)
            }
            TraceviewStateAction::SavePage {
                execution_id,
                new_events,
                new_responses,
                is_finished: finished,
            } => {
                let mut this = self.as_ref().clone();

                this.events
                    .entry(execution_id.clone())
                    .or_default()
                    .extend(new_events);

                let join_set_to_resps = this.responses.entry(execution_id.clone()).or_default();
                for response in new_responses {
                    let response = response
                        .event
                        .expect("`event` is sent in `ResponseWithCursor`");
                    let join_set_id = response
                        .join_set_id
                        .clone()
                        .expect("`join_set_id` is sent in `JoinSetResponseEvent`");
                    let execution_responses = join_set_to_resps.entry(join_set_id).or_default();
                    execution_responses.push(response);
                }
                let new_state = if finished {
                    ExecutionFetchState::Finished
                } else {
                    ExecutionFetchState::Pending
                };
                // Will be followed by ExecutionFetchState::RequestNextPage
                this.execution_ids_to_fetch_state
                    .insert(execution_id, new_state);
                Rc::from(this)
            }
            TraceviewStateAction::HttpTracesSwitch { execution_id, show } => {
                let mut this = self.as_ref().clone();
                this.execution_ids_to_show_http_traces
                    .insert(execution_id, show);
                Rc::from(this)
            }
        }
    }
}

#[function_component(TraceView)]
pub fn trace_view(TraceViewProps { execution_id }: &TraceViewProps) -> Html {
    let trace_view_state = use_reducer_eq(TraceViewState::default);
    // Fill the current execution id
    use_effect_with(execution_id.clone(), {
        let trace_view_state = trace_view_state.clone();
        move |execution_id| {
            trace_view_state.dispatch(TraceviewStateAction::AddExecutionId(execution_id.clone()));
        }
    });

    use_effect_with(trace_view_state.clone(), on_state_change);

    let trace_view = trace_view_state.deref();

    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");

    let root_trace = {
        let events_map = &trace_view.events;
        let responses_map = &trace_view.responses;
        compute_root_trace(
            execution_id,
            events_map,
            responses_map,
            &trace_view_state,
            &app_state,
        )
    };

    let execution_log = {
        let events = &trace_view.events;
        let dummy_events = Vec::new();
        let events = events.get(execution_id).unwrap_or(&dummy_events);
        let dummy_response_map = HashMap::new();
        let responses = &trace_view.responses;
        let responses = responses.get(execution_id).unwrap_or(&dummy_response_map);
        let join_next_version_to_response = compute_join_next_to_response(events, responses);
        events
            .iter()
            .filter(|event| {
                let event_inner = event.event.as_ref().expect("event is sent by the server");
                !matches!(
                    event_inner,
                    execution_event::Event::Locked(..)
                        | execution_event::Event::Unlocked(..)
                        | execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
                            event: Some(execution_event::history_event::Event::JoinSetCreated(_))
                        })
                )
            })
            .map(|event| {
                event_to_detail(
                    execution_id,
                    event,
                    &join_next_version_to_response,
                    ExecutionLink::Trace,
                    false,
                )
            })
            .collect::<Vec<_>>()
    };

    html! {<>
        <ExecutionHeader execution_id={execution_id.clone()} link={ExecutionLink::Trace} />

        <div class="trace-layout-container">
            <div class="trace-view">
                if let Some(root_trace) = root_trace {
                    <ExecutionTrace
                        root_scheduled_at={root_trace.scheduled_at}
                        root_last_event_at={root_trace.last_event_at}
                        data={TraceData::Root(root_trace)}
                    />
                } else {
                    {"Loading..."}
                }
            </div>
            <div class="trace-detail">
                {execution_log}
            </div>
        </div>

    </>}
}

fn on_state_change(trace_view_state: &UseReducerHandle<TraceViewState>) {
    trace!("Triggered use_effects");
    for (execution_id, cursors) in trace_view_state
        .execution_ids_to_fetch_state
        .iter()
        .filter_map(|(id, state)| match state {
            ExecutionFetchState::Requested(cursors) => Some((id, *cursors)),
            ExecutionFetchState::Pending | ExecutionFetchState::Finished => None,
        })
    {
        trace_view_state.dispatch(TraceviewStateAction::SetPending(execution_id.clone()));
        let execution_id = execution_id.clone();
        let trace_view_state = trace_view_state.clone();
        wasm_bindgen_futures::spawn_local(async move {
            trace!("list_execution_events {cursors:?}");
            let base_url = "/api";
            let mut execution_client =
                grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                    tonic_web_wasm_client::Client::new(base_url.to_string()),
                );
            let new_events_and_responses = execution_client
                .list_execution_events_and_responses(
                    grpc_client::ListExecutionEventsAndResponsesRequest {
                        execution_id: Some(execution_id.clone()),
                        version_from: cursors.version_from,
                        events_length: PAGE,
                        responses_cursor_from: cursors.responses_cursor_from,
                        responses_length: PAGE,
                        responses_including_cursor: cursors.responses_cursor_from == 0,
                        include_backtrace_id: true,
                    },
                )
                .await
                .unwrap()
                .into_inner();
            debug!(
                "{execution_id} Got {} events, {} responses",
                new_events_and_responses.events.len(),
                new_events_and_responses.responses.len()
            );

            let last_event = new_events_and_responses.events.last();
            let is_finished = matches!(
                last_event.and_then(|e| e.event.as_ref()),
                Some(execution_event::Event::Finished(_))
            );
            let cursors = Cursors {
                version_from: last_event
                    .map(|e| e.version + 1)
                    .unwrap_or(cursors.version_from),
                responses_cursor_from: new_events_and_responses
                    .responses
                    .last()
                    .map(|resp| resp.cursor)
                    .unwrap_or(cursors.responses_cursor_from),
            };
            trace_view_state.dispatch(TraceviewStateAction::SavePage {
                execution_id: execution_id.clone(),
                new_events: new_events_and_responses.events,
                new_responses: new_events_and_responses.responses,
                is_finished,
            });
            if !is_finished {
                TimeoutFuture::new(SLEEP_MILLIS).await;
                trace_view_state.dispatch(TraceviewStateAction::RequestNextPage {
                    execution_id,
                    cursors,
                });
            };
        });
    }
}

fn compute_root_trace(
    execution_id: &ExecutionId,
    events_map: &HashMap<ExecutionId, Vec<ExecutionEvent>>,
    responses_map: &HashMap<ExecutionId, HashMap<JoinSetId, Vec<JoinSetResponseEvent>>>,
    trace_view_state: &UseReducerHandle<TraceViewState>,
    app_state: &AppState,
) -> Option<TraceDataRoot> {
    let events = match events_map.get(execution_id) {
        Some(events) if !events.is_empty() => events,
        _ => return None,
    };

    let last_event = events.last().expect("not found is sent as an error");
    let is_finished = matches!(last_event.event, Some(execution_event::Event::Finished(_)));
    let responses = responses_map.get(execution_id);
    let mut last_event_at = compute_last_event_at(last_event, is_finished, responses);

    let create_event = events
        .first()
        .expect("not found is sent as an error")
        .event
        .as_ref()
        .expect("`event` is sent by the server");
    let create_event = assert_matches!(
        create_event,
        grpc_client::execution_event::Event::Created(created) => created
    );
    let execution_scheduled_at = DateTime::from(
        create_event
            .scheduled_at
            .expect("`scheduled_at` is sent by the server"),
    );

    let ffqn = FunctionFqn::from(create_event);
    let component_type = app_state
        .ffqns_to_details
        .get(&ffqn)
        .map(|(_, c)| c.as_type());
    let maybe_stub_link = if events.len() == 1 // stub execution can only contain Created and Finished events
        && component_type == Some(ComponentType::ActivityStub)
    {
        Some(html! {
            <Link<Route> to={Route::ExecutionStubResult { ffqn: ffqn.clone(), execution_id: execution_id.clone() }}>{"Submit stub response"}</Link<Route>>
        })
    } else {
        None
    };

    let child_ids_to_results = compute_child_execution_id_to_child_execution_finished(responses);

    let mut loadable_child_ids = vec![];

    let http_traces_enabled = trace_view_state
        .deref()
        .execution_ids_to_show_http_traces
        .get(execution_id)
        .cloned()
        .unwrap_or_default();

    let children = events
            .iter()
            .filter_map(|event| {
                let event_created_at = DateTime::from(event.created_at.expect("event.created_at is sent"));
                let event_inner = event.event.as_ref().expect("event is sent by the server");
                match event_inner {
                    // Add HTTP Client traces
                    execution_event::Event::TemporarilyFailed(TemporarilyFailed {
                        http_client_traces,
                        ..
                    })
                    | execution_event::Event::TemporarilyTimedOut(TemporarilyTimedOut{
                        http_client_traces,
                        ..
                    })
                    | execution_event::Event::Finished(Finished {
                        http_client_traces, ..
                    }) => {
                        if http_traces_enabled {
                            let children: Vec<_> = http_client_traces
                                .iter()
                                .map(|trace| {
                                    let name = format!(
                                        "{method} {uri}",
                                        method = trace.method,
                                        uri = trace.uri,
                                    );
                                    let status = match trace.result {
                                        Some(http_client_trace::Result::Status(status_code)) => BusyIntervalStatus::HttpTraceFinished(status_code),
                                        Some(http_client_trace::Result::Error(_)) => BusyIntervalStatus::HttpTraceError,
                                        None => BusyIntervalStatus::HttpTraceNotResponded,
                                    };
                                    TraceData::Child(TraceDataChild {
                                        name: name.to_html(),
                                        title: name,
                                        busy: vec![BusyInterval {
                                            started_at: DateTime::from(trace.sent_at.expect("sent_at is sent")),
                                            finished_at: Some(trace.finished_at.map(DateTime::from).unwrap_or(event_created_at)),
                                            title: None,
                                            status,
                                        }],
                                        children: match &trace.result {
                                            Some(http_client_trace::Result::Status(status_code)) => {
                                                let name = format!("Status code: {status_code}");
                                                vec![
                                                    TraceData::Child(TraceDataChild {
                                                        name: name.to_html(),
                                                        title: name,
                                                        busy: vec![],
                                                        children: vec![],
                                                        load_button: None,
                                                    })
                                                ]
                                            },
                                            Some(http_client_trace::Result::Error(error)) => {
                                                let name = format!("Failed: `{error}`");
                                                vec![
                                                    TraceData::Child(TraceDataChild {
                                                        name: name.to_html(),
                                                        title: name,
                                                        busy: vec![],
                                                        children: vec![],
                                                        load_button: None,
                                                    })
                                                ]
                                            },
                                            None => {
                                                vec![]
                                            }
                                        },
                                        load_button: None,
                                    })
                                })
                                .collect();
                            Some(children)
                        } else {
                            None
                        }
                    }
                    // Add child executions
                    execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
                        event:
                            Some(execution_event::history_event::Event::JoinSetRequest(
                                JoinSetRequest {
                                    join_set_request: Some(join_set_request::JoinSetRequest::ChildExecutionRequest(
                                        join_set_request::ChildExecutionRequest{child_execution_id: Some(child_execution_id)})),
                                    ..
                                },
                            )),
                    }) => {
                        let name = if let Some(suffix) =  child_execution_id.id.strip_prefix(&format!("{execution_id}{EXECUTION_ID_INFIX}")) {
                            suffix.to_string()
                        } else {
                            child_execution_id.to_string()
                        };

                        if !trace_view_state.deref().execution_ids_to_fetch_state.contains_key(child_execution_id) {
                            loadable_child_ids.push(child_execution_id.clone());
                        }

                        if let Some(child_root) = compute_root_trace(
                            child_execution_id,
                            events_map,
                            responses_map,
                            trace_view_state,
                            app_state
                        ) {
                            last_event_at = last_event_at.max(child_root.last_event_at);
                            Some(vec![TraceData::Root(child_root)])
                        } else {
                            let started_at = DateTime::from(event.created_at.expect("event.created_at must be sent"));
                            let (status, finished_at, interval_title) =
                                if let Some((result_detail_value, finished_at)) = child_ids_to_results.get(child_execution_id) {
                                    let status = BusyIntervalStatus::from(result_detail_value);
                                    let duration = (*finished_at - started_at).to_std().expect("started_at must be <= finished_at");
                                    (status, Some(*finished_at), format!("{status} in {duration:?}"))
                                } else {
                                    let status = BusyIntervalStatus::ExecutionUnfinished;
                                    (status, None, status.to_string())
                                };
                            Some(vec![
                                TraceData::Child(TraceDataChild {
                                    name: html!{<>
                                        <Link<Route> to={Route::ExecutionTrace { execution_id: child_execution_id.clone() }}>
                                            {name}
                                        </Link<Route>>
                                    </>},
                                    title: child_execution_id.to_string(),
                                    busy: vec![BusyInterval {
                                        started_at,
                                        finished_at,
                                        title: Some(interval_title),
                                        status
                                    }],
                                    children: Vec::new(),
                                    load_button: None,
                                })
                            ])
                        }
                    },
                    _ => None,
                }
            })
            .flatten()
            .collect();
    let last_event_at = last_event_at; // drop mut

    let mut current_locked_at: Option<(DateTime<Utc>, DateTime<Utc>)> = None;
    let mut busy = vec![BusyInterval {
        started_at: execution_scheduled_at,
        finished_at: Some(last_event_at),
        title: None,
        status: BusyIntervalStatus::ExecutionSinceScheduled,
    }];
    for event in events {
        let event_inner = event.event.as_ref().unwrap();
        match event_inner {
            execution_event::Event::Locked(locked) => {
                if let Some((locked_at, lock_expires_at)) = current_locked_at.take() {
                    // if the created_at..expires_at includes the current lock's created_at, we are extending the lock
                    let duration = (lock_expires_at - locked_at)
                        .to_std()
                        .expect("locked_at must be <= expires_at");
                    busy.push(BusyInterval {
                        started_at: locked_at,
                        finished_at: Some(lock_expires_at),
                        title: Some(format!("Locked for {duration:?}")),
                        status: BusyIntervalStatus::ExecutionLocked,
                    });
                }
                let locked_at =
                    DateTime::from(event.created_at.expect("event.created_at is always sent"));
                let expires_at = DateTime::from(
                    locked
                        .lock_expires_at
                        .expect("Locked.lock_expires_at is sent"),
                );
                current_locked_at = Some((locked_at, expires_at));
            }
            execution_event::Event::TemporarilyFailed(..)
            | execution_event::Event::Unlocked(..)
            | execution_event::Event::TemporarilyTimedOut(..)
            | execution_event::Event::Finished(..) => {
                let started_at = current_locked_at
                    .take()
                    .map(|(locked_at, _)| locked_at)
                    .unwrap_or(execution_scheduled_at); // webhooks have no locks
                let finished_at =
                    DateTime::from(event.created_at.expect("event.created_at is always sent"));
                let duration = (finished_at - started_at)
                    .to_std()
                    .expect("started_at must be <= finished_at");
                let status = match event_inner {
                    execution_event::Event::TemporarilyFailed(..) => {
                        BusyIntervalStatus::ExecutionErrorTemporary
                    }
                    execution_event::Event::Unlocked(..) => BusyIntervalStatus::ExecutionLocked,
                    execution_event::Event::TemporarilyTimedOut(..) => {
                        BusyIntervalStatus::ExecutionTimeoutTemporary
                    }
                    execution_event::Event::Finished(Finished {
                        result_detail:
                            Some(ResultDetail {
                                value: Some(result_detail_value),
                            }),
                        ..
                    }) => BusyIntervalStatus::from(result_detail_value),
                    _ => unreachable!("unexpected {event_inner:?}"),
                };
                let title = format!("{status} in {duration:?}");
                busy.push(BusyInterval {
                    started_at,
                    finished_at: Some(finished_at),
                    title: Some(title),
                    status,
                });
            }
            _ => {}
        }
    }
    // If there is locked without unlocked, add the unfinished interval.
    // Ignore the lock_expires_at as it might be in the future or beyond the last seen event.
    if let Some((locked_at, _lock_expires_at)) = current_locked_at {
        busy.push(BusyInterval {
            started_at: locked_at,
            finished_at: None,
            title: Some("unfinished".to_string()),
            status: BusyIntervalStatus::ExecutionUnfinished,
        });
    }

    let ffqn = {
        let first = events
            .first()
            .expect("checked that events is not empty")
            .event
            .as_ref()
            .expect("event.event is sent");
        let fn_name = assert_matches!(first,
            execution_event::Event::Created(execution_event::Created{function_name: Some(fn_name), ..}) => fn_name);
        FunctionFqn::from(fn_name.clone())
    };

    let http_traces_button = if matches!(
        component_type,
        Some(ComponentType::ActivityWasm | ComponentType::WebhookEndpoint)
    ) {
        let show = !http_traces_enabled;
        let onclick = Callback::from({
            let trace_view_state = trace_view_state.clone();
            let execution_id = execution_id.clone();
            move |_| {
                trace_view_state.dispatch(TraceviewStateAction::HttpTracesSwitch {
                    execution_id: execution_id.clone(),
                    show,
                });
            }
        });
        Some(html! {
            <a {onclick} >
                if show {
                    {"+"}
                } else {
                    {"-"}
                }
            </a>
        })
    } else {
        None
    };

    let load_button = if !loadable_child_ids.is_empty() {
        let onclick = Callback::from({
            let trace_view_state = trace_view_state.clone();
            move |_| {
                for child_execution_id in &loadable_child_ids {
                    debug!("Adding {child_execution_id}");
                    trace_view_state.dispatch(TraceviewStateAction::AddExecutionId(
                        child_execution_id.clone(),
                    ));
                }
            }
        });
        Some(html! {
            <button {onclick} >{"Load"} </button>
        })
    } else {
        None
    };

    let name = html! {
        <>
            {execution_id.render_execution_parts(true, ExecutionLink::Trace)}
            {" "}{&ffqn.ifc_fqn.ifc_name}{"."}{&ffqn.function_name}
            {" "}
            {maybe_stub_link}
        </>
    };
    Some(TraceDataRoot {
        name,
        title: format!("{execution_id} {ffqn}"),
        scheduled_at: execution_scheduled_at,
        last_event_at,
        busy,
        children,
        load_button,
        expand_collapse: http_traces_button,
    })
}

fn compute_child_execution_id_to_child_execution_finished(
    responses: Option<&HashMap<JoinSetId, Vec<JoinSetResponseEvent>>>,
) -> HashMap<ExecutionId, (result_detail::Value, DateTime<Utc>)> {
    responses
        .into_iter()
        .flat_map(|map| {
            map.values().flatten().filter_map(|resp| {
                if let JoinSetResponseEvent {
                    response:
                        Some(join_set_response_event::Response::ChildExecutionFinished(
                            join_set_response_event::ChildExecutionFinished {
                                child_execution_id: Some(child_execution_id),
                                result_detail:
                                    Some(ResultDetail {
                                        value: Some(result_detail_value),
                                    }),
                            },
                        )),
                    ..
                } = resp
                {
                    let created_at =
                        DateTime::from(resp.created_at.expect("response.created_at is sent"));
                    Some((
                        child_execution_id.clone(),
                        (result_detail_value.clone(), created_at),
                    ))
                } else {
                    None
                }
            })
        })
        .collect()
}

fn compute_last_event_at(
    last_event: &ExecutionEvent,
    is_finished: bool,
    responses: Option<&HashMap<JoinSetId, Vec<JoinSetResponseEvent>>>,
) -> DateTime<Utc> {
    let candidate = DateTime::from(last_event.created_at.expect("event.created_at is sent"));

    match responses {
        Some(responses) if !is_finished => responses
            .values()
            .filter_map(|vec| vec.last())
            .map(|e| DateTime::from(e.created_at.expect("event.created_at is sent")))
            .chain(std::iter::once(candidate))
            .max()
            .expect("chained with last_event so cannot be empty"),
        _ => candidate,
    }
}
