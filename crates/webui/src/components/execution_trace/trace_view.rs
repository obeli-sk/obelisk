use super::data::{BusyIntervalStatus, TraceData};
use crate::{
    app::Route,
    components::execution_trace::{
        data::{BusyInterval, TraceDataChild, TraceDataRoot},
        execution_step::ExecutionStep,
    },
    grpc::{
        execution_id::EXECUTION_ID_INFIX,
        grpc_client::{
            self,
            execution_event::{
                self,
                history_event::{join_set_request, JoinSetRequest},
                Finished, TemporarilyFailed, TemporarilyTimedOut,
            },
            http_client_trace, join_set_response_event, result_detail, ExecutionEvent, ExecutionId,
            JoinSetId, JoinSetResponseEvent, ResultDetail,
        },
    },
};
use assert_matches::assert_matches;
use chrono::{DateTime, Utc};
use gloo::timers::future::TimeoutFuture;
use hashbrown::{HashMap, HashSet};
use log::{debug, trace};
use std::{
    ops::Deref as _,
    sync::{Arc, RwLock},
};
use yew::prelude::*;
use yew_router::prelude::Link;

#[derive(Properties, PartialEq)]
pub struct TraceViewProps {
    pub execution_id: grpc_client::ExecutionId,
}

const PAGE: u32 = 100;

type ExecutionIdsStateType = UseStateHandle<HashSet<ExecutionId>>;
type IsFetchingStateType = UseStateHandle<Arc<RwLock<HashMap<ExecutionId, bool>>>>;
type EventsStateType = UseStateHandle<Arc<RwLock<HashMap<ExecutionId, Vec<ExecutionEvent>>>>>;
type ResponsesStateType = UseStateHandle<
    Arc<
        RwLock<
            HashMap<
                ExecutionId,
                (
                    HashMap<JoinSetId, Vec<JoinSetResponseEvent>>,
                    u32, /* Cursor */
                ),
            >,
        >,
    >,
>;

#[function_component(TraceView)]
pub fn trace_view(TraceViewProps { execution_id }: &TraceViewProps) -> Html {
    let execution_ids_state: ExecutionIdsStateType = use_state(|| {
        let mut set = HashSet::new();
        set.insert(execution_id.clone());
        set
    });
    let events_state: EventsStateType = use_state(Default::default);
    let responses_state: ResponsesStateType = use_state(Default::default);
    let is_fetching_state: IsFetchingStateType = use_state(Default::default); // Track if we're currently fetching
    let effect_hook = EffectHook {
        is_fetching_state: is_fetching_state.clone(),
        events_state: events_state.clone(),
        responses_state: responses_state.clone(),
    };

    use_effect_with(
        (
            execution_ids_state.deref().clone(), // Retrigger on adding child executions
            is_fetching_state.deref().read().unwrap().clone(), // Retrigger on change, used for fetching the next page.
        ),
        move |(execution_ids, current_is_fetching_map)| {
            effect_hook.call(execution_ids, current_is_fetching_map)
        },
    );

    let root_trace = {
        let events_map = events_state.deref().read().unwrap(); // Keep the read lock, there is no blocking while rendering the traces.
        let responses_map = responses_state.deref().read().unwrap(); // Keep the read lock, there is no blocking while rendering the traces.
        compute_root_trace(
            execution_id,
            &events_map,
            &responses_map,
            &execution_ids_state,
            None,
        )
    };

    if let Some(root_trace) = root_trace {
        html! {
            <div class="trace-view">
                <ExecutionStep
                    root_scheduled_at={root_trace.scheduled_at}
                    root_last_event_at={root_trace.last_event_at}
                    data={TraceData::Root(root_trace)}
                />
            </div>
        }
    } else {
        html! {
            "Loading..."
        }
    }
}

struct EffectHook {
    is_fetching_state: IsFetchingStateType, // triggered, read by the hook. Modified asynchronously.
    events_state: EventsStateType,          // Modified asynchronously.
    responses_state: ResponsesStateType,    // Modified asynchronously.
}
impl EffectHook {
    // Fetch ListExecutionEventsAndResponses, populate events_state,responses_state based on execution_ids
    fn call(
        &self,
        execution_ids: &HashSet<ExecutionId>,
        current_is_fetching_map: &HashMap<ExecutionId, bool>,
    ) {
        trace!("Triggered fetch callback: {execution_ids:?}");
        for execution_id in execution_ids {
            if current_is_fetching_map
                .get(execution_id)
                .cloned()
                .unwrap_or_default()
            {
                trace!("Skipping {execution_id}");
                continue;
            }
            trace!("Setting is_fetching_state=true for {execution_id}");
            let is_fetching_rwlock = self.is_fetching_state.deref().clone();
            {
                let mut lock = is_fetching_rwlock.write().unwrap();
                lock.insert(execution_id.clone(), true);
            }
            self.is_fetching_state.set(is_fetching_rwlock); // this will trigger the outer `use_effect` but will be short-circuted.
            {
                let execution_id = execution_id.clone();
                let events_state = self.events_state.clone();
                let responses_state = self.responses_state.clone();
                let is_fetching_state = self.is_fetching_state.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    let events_rwlock = events_state.deref();
                    let mut events = events_rwlock
                        .read()
                        .unwrap()
                        .get(&execution_id)
                        .cloned()
                        .unwrap_or_default();
                    let version_from = events.last().map(|e| e.version + 1).unwrap_or_default();
                    let responses_rwlock = responses_state.deref();
                    let (mut responses, responses_cursor_from) = responses_rwlock
                        .read()
                        .unwrap()
                        .get(&execution_id)
                        .cloned()
                        .unwrap_or_default();
                    trace!("list_execution_events {execution_id} {version_from}");
                    let base_url = "/api";
                    let mut execution_client =
                        grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                            tonic_web_wasm_client::Client::new(base_url.to_string()),
                        );
                    let new_events_and_responses = execution_client
                        .list_execution_events_and_responses(
                            grpc_client::ListExecutionEventsAndResponsesRequest {
                                execution_id: Some(execution_id.clone()),
                                version_from,
                                events_length: PAGE,
                                responses_cursor_from,
                                responses_length: PAGE,
                                responses_including_cursor: responses_cursor_from == 0,
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
                    events.extend(new_events_and_responses.events);
                    let last_event = events.last().expect("not found is sent as an error");
                    let is_finished =
                        matches!(last_event.event, Some(execution_event::Event::Finished(_)));

                    {
                        // Persist `events_state`
                        let events_rwlock = events_state.deref().clone();
                        {
                            let mut lock = events_rwlock.write().unwrap();
                            lock.insert(execution_id.clone(), events);
                        }
                        events_state.set(events_rwlock);
                    }

                    {
                        // Persist `responses_state`
                        let responses_cursor_from = new_events_and_responses
                            .responses
                            .last()
                            .map(|r| r.cursor)
                            .unwrap_or(responses_cursor_from);
                        for response in new_events_and_responses.responses {
                            let response = response
                                .event
                                .expect("`event` is sent in `ResponseWithCursor`");
                            let join_set_id = response
                                .join_set_id
                                .clone()
                                .expect("`join_set_id` is sent in `JoinSetResponseEvent`");
                            let execution_responses = responses.entry(join_set_id).or_default();
                            execution_responses.push(response);
                        }
                        let responses_rwlock = responses_state.deref().clone();
                        {
                            let mut lock = responses_rwlock.write().unwrap();
                            lock.insert(execution_id.clone(), (responses, responses_cursor_from));
                        }
                        responses_state.set(responses_rwlock);
                    }

                    if is_finished {
                        debug!("{execution_id} Execution Finished");
                        // Keep is_fetching_state as true, the use_effect_with will not be triggered again.
                    } else {
                        trace!("{execution_id} Timeout: start");
                        TimeoutFuture::new(1000).await;
                        trace!("{execution_id} Timeout: Triggering refetch");
                        let is_fetching_rwlock = is_fetching_state.deref().clone();
                        {
                            let mut lock = is_fetching_rwlock.write().unwrap();
                            lock.insert(execution_id.clone(), false);
                        }
                        is_fetching_state.set(is_fetching_rwlock); // Trigger use_effect_with again.
                    }
                });
            }
        }
    }
}

fn compute_root_trace(
    execution_id: &ExecutionId,
    events_map: &HashMap<ExecutionId, Vec<ExecutionEvent>>,
    responses_map: &HashMap<ExecutionId, (HashMap<JoinSetId, Vec<JoinSetResponseEvent>>, u32)>,
    execution_ids_state: &UseStateHandle<HashSet<ExecutionId>>,
    hide_parent: Option<&ExecutionId>,
) -> Option<TraceDataRoot> {
    let events = match events_map.get(execution_id) {
        Some(events) if !events.is_empty() => events,
        _ => return None,
    };

    let last_event = events.last().expect("not found is sent as an error");
    let is_finished = matches!(last_event.event, Some(execution_event::Event::Finished(_)));
    let responses = responses_map.get(execution_id).map(|m| &m.0);
    let last_event_at = compute_last_event_at(last_event, is_finished, responses);

    let execution_scheduled_at = {
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
        DateTime::from(
            create_event
                .scheduled_at
                .expect("`scheduled_at` is sent by the server"),
        )
    };

    let child_ids_to_results = compute_child_execution_id_to_child_execution_finished(responses);

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
                        let children: Vec<_> = http_client_traces
                            .iter()
                            .map(|trace| {
                                let name = format!(
                                    "{result} {method} {uri}",
                                    method = trace.method,
                                    uri = trace.uri,
                                    result = match &trace.result {
                                        Some(
                                            http_client_trace::Result::Status(
                                                status,
                                            ),
                                        ) => format!("status {status}"),
                                        Some(
                                            http_client_trace::Result::Error(err),
                                        ) => format!("error: {err}"),
                                        None => "unfinished".to_string(),
                                    }
                                );
                                let status = match trace.result {
                                    Some(http_client_trace::Result::Status(_)) => BusyIntervalStatus::HttpTraceFinished,
                                    Some(http_client_trace::Result::Error(_)) => BusyIntervalStatus::HttpTraceError,
                                    None => BusyIntervalStatus::HttpTraceUnfinished,
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
                                    children: vec![],
                                })
                            })
                            .collect();
                        Some(children)
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

                        let load_button = if !execution_ids_state.deref().contains(child_execution_id) {
                                let onclick =
                                Callback::from({
                                    let execution_ids_state = execution_ids_state.clone();
                                    let child_execution_id = child_execution_id.clone();
                                    move |_| {
                                        debug!("Adding {child_execution_id}");
                                        let mut execution_ids = execution_ids_state.deref().clone();
                                        execution_ids.insert(child_execution_id.clone());
                                        execution_ids_state.set(execution_ids);
                                    }
                                });
                            html!{
                                <button {onclick} >{"Load"} </button>
                            }

                        } else {
                            Html::default()
                        };
                        let children = if let Some(child_root) = compute_root_trace(
                            child_execution_id,
                            events_map,
                            responses_map,
                            execution_ids_state,
                            Some(execution_id),
                        ) {
                            vec![TraceData::Root(child_root)]
                        } else {
                            Vec::new()
                        };
                        let started_at = DateTime::from(event.created_at.expect("event.created_at must be sent"));
                        let (status, finished_at, interval_title) = if let Some((result_detail_value, finished_at)) = child_ids_to_results.get(child_execution_id) {
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
                                    {status}{" "}
                                    <Link<Route> to={Route::ExecutionTrace { execution_id: child_execution_id.clone() }}>
                                        {name}
                                    </Link<Route>>
                                    {load_button}
                                </>},
                                title: child_execution_id.to_string(),
                                busy: vec![BusyInterval {
                                    started_at,
                                    finished_at,
                                    title: Some(interval_title),
                                    status
                                }],
                                children,
                            })
                        ])
                    },
                    _ => None,
                }
            })
            .flatten()
            .collect();

    let mut current_locked_at: Option<(DateTime<Utc>, DateTime<Utc>)> = None;
    let mut busy = vec![];
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
                let (locked_at, _) = current_locked_at
                    .take()
                    .expect("must have been locked at this point");
                let finished_at =
                    DateTime::from(event.created_at.expect("event.created_at is always sent"));
                let duration = (finished_at - locked_at)
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
                    started_at: locked_at,
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

    let name = format!(
        "{status} {id}",
        id = if let Some(suffix) = hide_parent.and_then(|parent| execution_id
            .id
            .strip_prefix(&format!("{parent}{EXECUTION_ID_INFIX}")))
        {
            suffix
        } else {
            &execution_id.id
        },
        status = if let Some(status) = busy.iter().last().map(|i| i.status) {
            status.to_string()
        } else {
            "unknown".to_string()
        }
    );
    Some(TraceDataRoot {
        name: name.to_html(),
        title: execution_id.to_string(),
        scheduled_at: execution_scheduled_at,
        last_event_at,
        busy,
        children,
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
