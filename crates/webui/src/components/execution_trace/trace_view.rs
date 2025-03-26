use super::data::TraceData;
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
                Finished, TemporarilyFailed,
            },
            join_set_response_event, ExecutionEvent, ExecutionId, JoinSetId, JoinSetResponseEvent,
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

#[function_component(TraceView)]
pub fn trace_view(TraceViewProps { execution_id }: &TraceViewProps) -> Html {
    // let props_changed_state = use_state(|| execution_id.clone());
    let execution_ids_state: UseStateHandle<HashSet<ExecutionId>> = use_state(|| {
        let mut set = HashSet::new();
        set.insert(execution_id.clone());
        set
    });
    #[expect(clippy::type_complexity)]
    let events_state: UseStateHandle<Arc<RwLock<HashMap<ExecutionId, Vec<ExecutionEvent>>>>> =
        use_state(Default::default);
    #[expect(clippy::type_complexity)]
    let responses_state: UseStateHandle<
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
    > = use_state(Default::default);
    let is_fetching_state: UseStateHandle<Arc<RwLock<HashMap<ExecutionId, bool>>>> =
        use_state(Default::default); // Track if we're currently fetching

    // Fetch ListExecutionEventsAndResponses
    use_effect_with(
        (
            execution_ids_state.deref().clone(), // Retrigger on adding child executions
            is_fetching_state.deref().read().unwrap().clone(), // Retrigger on change, used for fetching the next page.
        ),
        {
            let events_state = events_state.clone();
            let responses_state = responses_state.clone();
            let is_fetching_state = is_fetching_state.clone();
            move |(execution_ids, current_is_fetching_map)| {
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
                    let is_fetching_rwlock = is_fetching_state.deref().clone();
                    {
                        let mut lock = is_fetching_rwlock.write().unwrap();
                        lock.insert(execution_id.clone(), true);
                    }
                    is_fetching_state.set(is_fetching_rwlock); // this will trigger the outer `use_effect` but will be short-circuted.
                    {
                        let execution_id = execution_id.clone();
                        let events_state = events_state.clone();
                        let responses_state = responses_state.clone();
                        let is_fetching_state = is_fetching_state.clone();
                        wasm_bindgen_futures::spawn_local(async move {
                            let events_rwlock = events_state.deref();
                            let mut events = events_rwlock
                                .read()
                                .unwrap()
                                .get(&execution_id)
                                .cloned()
                                .unwrap_or_default();
                            let version_from =
                                events.last().map(|e| e.version + 1).unwrap_or_default();
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
                            let is_finished = matches!(
                                last_event.event,
                                Some(execution_event::Event::Finished(_))
                            );

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
                                    let execution_responses =
                                        responses.entry(join_set_id).or_default();
                                    execution_responses.push(response);
                                }
                                let responses_rwlock = responses_state.deref().clone();
                                {
                                    let mut lock = responses_rwlock.write().unwrap();
                                    lock.insert(
                                        execution_id.clone(),
                                        (responses, responses_cursor_from),
                                    );
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

fn compute_root_trace(
    execution_id: &ExecutionId,
    events_map: &HashMap<ExecutionId, Vec<ExecutionEvent>>,
    responses_map: &HashMap<ExecutionId, (HashMap<JoinSetId, Vec<JoinSetResponseEvent>>, u32)>,
    execution_ids_state: &UseStateHandle<HashSet<ExecutionId>>,
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

    let child_ids_to_responses_creation =
        compute_child_execution_id_to_response_created_at(responses);

    let children = events
            .iter()
            .filter_map(|event| {
                match event.event.as_ref().expect("event is sent by the server") {
                    execution_event::Event::TemporarilyFailed(TemporarilyFailed {
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
                                    "{method} {uri}{result}",
                                    method = trace.method,
                                    uri = trace.uri,
                                    result = match &trace.result {
                                        Some(
                                            grpc_client::http_client_trace::Result::Status(
                                                status,
                                            ),
                                        ) => format!(" ({status})"),
                                        Some(
                                            grpc_client::http_client_trace::Result::Error(err),
                                        ) => format!(" (error: {err})"),
                                        None => String::new(),
                                    }
                                );
                                TraceData::Child(TraceDataChild {
                                    name: name.to_html(),
                                    title: name,
                                    busy: vec![BusyInterval {
                                        started_at: DateTime::from(trace.sent_at.expect("sent_at is sent")),
                                        finished_at: trace.finished_at.map(DateTime::from),
                                        title: None,
                                    }],
                                    children: vec![],
                                })
                            })
                            .collect();
                        Some(children)
                    }
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
                        ) {
                            vec![TraceData::Root(child_root)]
                        } else {
                            Vec::new()
                        };

                        Some(vec![
                            TraceData::Child(TraceDataChild {
                                name: html!{<>
                                    <Link<Route> to={Route::ExecutionTrace { execution_id: child_execution_id.clone() }}>
                                        {name}
                                    </Link<Route>>
                                    {load_button}
                                </>},
                                title: child_execution_id.to_string(),
                                busy: vec![BusyInterval {
                                    started_at: DateTime::from(event.created_at.expect("event.created_at must be sent")),
                                    finished_at: child_ids_to_responses_creation.get(child_execution_id).cloned(),
                                    title: None,
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
        match event.event.as_ref().unwrap() {
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
                let title = match event.event.as_ref().unwrap() {
                    execution_event::Event::TemporarilyFailed(..) => {
                        Some(format!("Temporarily failed after {duration:?}"))
                    }
                    execution_event::Event::Unlocked(..) => {
                        Some(format!("Unlocked after {duration:?}"))
                    }
                    execution_event::Event::TemporarilyTimedOut(..) => {
                        Some(format!("Temporarily timed out after {duration:?}"))
                    }
                    execution_event::Event::Finished(..) => {
                        Some(format!("Finished in {duration:?}"))
                    }
                    _ => unreachable!(),
                };
                busy.push(BusyInterval {
                    started_at: locked_at,
                    finished_at: Some(finished_at),
                    title,
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
            title: Some("pending".to_string()),
        });
    }

    let name = format!(
        "{execution_id} ({})",
        if is_finished { "finished" } else { "pending" }
    );
    Some(TraceDataRoot {
        name: name.to_html(),
        title: name,
        scheduled_at: execution_scheduled_at,
        last_event_at,
        busy,
        children,
    })
}

fn compute_child_execution_id_to_response_created_at(
    responses: Option<&HashMap<JoinSetId, Vec<JoinSetResponseEvent>>>,
) -> HashMap<ExecutionId, DateTime<Utc>> {
    responses
        .into_iter()
        .flat_map(|map| {
            map.values().flatten().filter_map(|resp| {
                if let JoinSetResponseEvent {
                    response:
                        Some(join_set_response_event::Response::ChildExecutionFinished(
                            join_set_response_event::ChildExecutionFinished {
                                child_execution_id: Some(child_execution_id),
                                ..
                            },
                        )),
                    ..
                } = resp
                {
                    Some((
                        child_execution_id.clone(),
                        DateTime::from(resp.created_at.expect("response.created_at is sent")),
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
