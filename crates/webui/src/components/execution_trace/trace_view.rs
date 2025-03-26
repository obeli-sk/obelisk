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
use hashbrown::HashMap;
use log::{debug, trace};
use std::ops::Deref as _;
use yew::prelude::*;
use yew_router::prelude::Link;

#[derive(Properties, PartialEq)]
pub struct TraceViewProps {
    pub execution_id: grpc_client::ExecutionId,
}

const PAGE: u32 = 100;

#[function_component(TraceView)]
pub fn trace_view(TraceViewProps { execution_id }: &TraceViewProps) -> Html {
    let execution_id_state = use_state(|| execution_id.clone());
    let events_state: UseStateHandle<Vec<ExecutionEvent>> = use_state(Vec::new);
    let responses_state: UseStateHandle<(
        HashMap<JoinSetId, Vec<JoinSetResponseEvent>>,
        u32, /* Cursor */
    )> = use_state(|| (HashMap::new(), 0));
    let is_fetching_state = use_state(|| false); // Track if we're currently fetching

    // Cleanup the state on execution_id change.
    use_effect_with(execution_id.clone(), {
        let execution_id_state = execution_id_state.clone();
        let events_state = events_state.clone();
        let responses_state = responses_state.clone();
        let is_fetching_state = is_fetching_state.clone();
        move |execution_id| {
            if *execution_id != *execution_id_state.deref() {
                debug!("Execution ID changed");
                execution_id_state.set(execution_id.clone());
                events_state.set(Default::default());
                responses_state.set(Default::default());
                is_fetching_state.set(false);
            }
        }
    });

    // Fetch ListExecutionEventsAndResponses
    use_effect_with(
        (
            execution_id_state.deref().clone(),
            *is_fetching_state.deref(),
        ),
        {
            let events_state = events_state.clone();
            let responses_state = responses_state.clone();
            move |(execution_id, is_fetching)| {
                if *is_fetching {
                    trace!("Prevented concurrent fetches");
                    return;
                }
                trace!("Setting is_fetching_state=true");
                is_fetching_state.set(true);

                {
                    let execution_id = execution_id.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        let mut events = events_state.deref().clone();
                        let version_from = events.last().map(|e| e.version + 1).unwrap_or_default();
                        let (mut responses, responses_cursor_from) =
                            responses_state.deref().clone();
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
                            "Got {} events, {} responses",
                            new_events_and_responses.events.len(),
                            new_events_and_responses.responses.len()
                        );
                        events.extend(new_events_and_responses.events);
                        let last_event = events.last().expect("not found is sent as an error");
                        let is_finished =
                            matches!(last_event.event, Some(execution_event::Event::Finished(_)));
                        events_state.set(events);
                        {
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
                            responses_state.set((responses, responses_cursor_from));
                        }

                        if is_finished {
                            debug!("Execution Finished");
                            // Keep is_fetching_state as true, the use_effect_with will not be triggered again.
                        } else {
                            trace!("Timeout: start");
                            TimeoutFuture::new(1000).await;
                            trace!("Timeout: Triggering refetch");
                            is_fetching_state.set(false); // Trigger use_effect_with again.
                        }
                    });
                }
            }
        },
    );

    let root_trace = {
        let events = events_state.deref();
        if events.is_empty() {
            None
        } else {
            let last_event = events.last().expect("not found is sent as an error");
            let is_finished = matches!(last_event.event, Some(execution_event::Event::Finished(_)));
            let last_event_at = compute_last_event_at(last_event, is_finished, &responses_state.0);

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
                compute_child_execution_id_to_response_created_at(&responses_state.0);

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
                                    TraceDataChild {
                                        name: name.to_html(),
                                        title: name,
                                        busy: vec![BusyInterval {
                                            started_at: DateTime::from(trace.sent_at.expect("sent_at is sent")),
                                            finished_at: trace.finished_at.map(DateTime::from),
                                            title: None,
                                        }],
                                        children: vec![],
                                    }
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
                            Some(vec![
                                TraceDataChild {
                                    name: html!{
                                        <Link<Route> to={Route::ExecutionTrace { execution_id: child_execution_id.clone() }}>
                                            {name}
                                        </Link<Route>>

                                    },
                                    title: child_execution_id.to_string(),
                                    busy: vec![BusyInterval {
                                        started_at: DateTime::from(event.created_at.expect("event.created_at must be sent")),
                                        finished_at: child_ids_to_responses_creation.get(child_execution_id).cloned(),
                                        title: None,
                                    }],
                                    children: vec![],
                                }
                            ])
                        },
                        _ => None,
                    }
                })
                .flatten()
                .collect();

            let mut current_locked_at = None;
            let mut busy = vec![];
            for event in events {
                match event.event.as_ref().unwrap() {
                    execution_event::Event::Locked(..) => {
                        if current_locked_at.is_none() {
                            current_locked_at = Some(DateTime::from(
                                event.created_at.expect("event.created_at is always sent"),
                            ));
                        } // otherwise we are extending the lock
                    }
                    execution_event::Event::TemporarilyFailed(..)
                    | execution_event::Event::Unlocked(..)
                    | execution_event::Event::TemporarilyTimedOut(..)
                    | execution_event::Event::Finished(..) => {
                        let started_at = current_locked_at
                            .take()
                            .expect("must have been locked at this point");
                        let finished_at = DateTime::from(
                            event.created_at.expect("event.created_at is always sent"),
                        );
                        let duration = (finished_at - started_at)
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
                            started_at,
                            finished_at: Some(finished_at),
                            title,
                        });
                    }
                    _ => {}
                }
            }
            // If there is locked without unlocked, add the unfinished interval
            if let Some(started_at) = current_locked_at {
                busy.push(BusyInterval {
                    started_at,
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

fn compute_child_execution_id_to_response_created_at(
    responses: &HashMap<JoinSetId, Vec<JoinSetResponseEvent>>,
) -> HashMap<ExecutionId, DateTime<Utc>> {
    responses
        .values()
        .flatten()
        .filter_map(|resp| {
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
        .collect()
}

fn compute_last_event_at(
    last_event: &ExecutionEvent,
    is_finished: bool,
    responses: &HashMap<JoinSetId, Vec<JoinSetResponseEvent>>,
) -> DateTime<Utc> {
    let candidate = DateTime::from(last_event.created_at.expect("event.created_at is sent"));
    if !is_finished {
        responses
            .values()
            .filter_map(|vec| vec.last())
            .map(|e| DateTime::from(e.created_at.expect("event.created_at is sent")))
            .chain(std::iter::once(candidate))
            .max()
            .expect("chained with last_event so cannot be empty")
    } else {
        candidate
    }
}
