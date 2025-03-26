use super::data::TraceData;
use crate::{
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
use chrono::DateTime;
use gloo::timers::future::TimeoutFuture;
use hashbrown::HashMap;
use log::{debug, trace};
use std::{ops::Deref as _, time::Duration};
use yew::prelude::*;

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
            let since_scheduled_at = |timestamp: ::prost_wkt_types::Timestamp| {
                (DateTime::from(timestamp) - execution_scheduled_at)
                    .to_std()
                    .expect("must not be negative")
            };
            let total_duration = since_scheduled_at(compute_current_finished_at(
                last_event,
                is_finished,
                &responses_state.0,
            ));

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
                                    let started_at = since_scheduled_at(trace
                                        .sent_at
                                        .expect("HttpClientTrace.sent_at is always sent"));
                                    let finished_at = trace
                                        .finished_at
                                        .as_ref()
                                        .map(|finished_at| since_scheduled_at(*finished_at));
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
                                        name,
                                        started_at,
                                        finished_at: finished_at.unwrap_or(total_duration),
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
                            let finished_at = child_ids_to_responses_creation.get(child_execution_id).map(|created_at| since_scheduled_at(*created_at));
                            let name = if let Some(suffix) =  child_execution_id.id.strip_prefix(&format!("{execution_id}{EXECUTION_ID_INFIX}")) {
                                suffix.to_string()
                            } else {
                                child_execution_id.to_string()
                            };
                            Some(vec![
                                TraceDataChild {
                                    name,
                                    started_at: since_scheduled_at(event.created_at.expect("event.created_at must be sent")),
                                    finished_at: finished_at.unwrap_or(total_duration),
                                    children: vec![],
                                }
                            ])
                        },
                        _ => None,
                    }
                })
                .flatten()
                .collect();

            Some(TraceDataRoot {
                name: format!(
                    "{execution_id} ({})",
                    if is_finished { "finished" } else { "loading" }
                ),
                finished_at: total_duration,
                busy: vec![BusyInterval {
                    started_at: Duration::ZERO,
                    finished_at: total_duration,
                }],
                children,
            })
        }
    };

    if let Some(root_trace) = root_trace {
        html! {
            <div class="trace-view">
                <ExecutionStep
                    total_duration={root_trace.finished_at}
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
) -> HashMap<ExecutionId, prost_wkt_types::Timestamp> {
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
                    resp.created_at.expect("response.created_at is sent"),
                ))
            } else {
                None
            }
        })
        .collect()
}

fn compute_current_finished_at(
    last_event: &ExecutionEvent,
    is_finished: bool,
    responses: &HashMap<JoinSetId, Vec<JoinSetResponseEvent>>,
) -> ::prost_wkt_types::Timestamp {
    let candidate = last_event.created_at.expect("event.created_at is sent");
    if !is_finished {
        responses
            .values()
            .filter_map(|vec| vec.last())
            .map(|e| DateTime::from(e.created_at.expect("event.created_at is sent")))
            .chain(std::iter::once(DateTime::from(candidate)))
            .max()
            .expect("chained with last_event so cannot be empty")
            .into()
    } else {
        candidate
    }
}
