use super::data::TraceData;
use crate::{
    components::execution_trace::{
        data::{TraceDataChild, TraceDataRoot},
        execution_step::ExecutionStep,
    },
    grpc::grpc_client::{
        self,
        execution_event::{
            self,
            history_event::{join_set_request, JoinSetRequest},
            Finished, TemporarilyFailed,
        },
        join_set_response_event, ExecutionEvent, ExecutionId, JoinSetId, JoinSetResponseEvent,
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
    let events_state: UseStateHandle<Vec<grpc_client::ExecutionEvent>> = use_state(Vec::new);
    let responses_state: UseStateHandle<(
        HashMap<JoinSetId, Vec<JoinSetResponseEvent>>,
        u32, /* Cursor */
    )> = use_state(|| (HashMap::new(), 0));
    // let root_trace_state: UseStateHandle<Option<TraceDataRoot>> = use_state(|| None);
    let is_fetching_state = use_state(|| false); // Track if we're currently fetching

    // Cleanup the state on execution_id change.
    use_effect_with(execution_id.clone(), {
        let execution_id_state = execution_id_state.clone();
        let events_state = events_state.clone();
        let responses_state = responses_state.clone();
        // let root_trace_state = root_trace_state.clone();
        let is_fetching_state = is_fetching_state.clone();
        move |execution_id| {
            if *execution_id != *execution_id_state.deref() {
                debug!("Execution ID changed");
                execution_id_state.set(execution_id.clone());
                events_state.set(Default::default());
                responses_state.set(Default::default());
                // root_trace_state.set(None);
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
            // let root_trace_state = root_trace_state.clone();
            move |(execution_id, is_fetching)| {
                if *is_fetching {
                    trace!("Prevented concurrent fetches");
                    return;
                }
                trace!("Setting is_fetching_state=true");
                is_fetching_state.set(true);

                // Request ListExecutionEvents
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

                        // root_trace_state.set(Some(root));
                        events_state.set(events);

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

            let child_ids_to_responses =
                compute_child_execution_id_to_response(events, &responses_state.0);

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
                                        finished_at,
                                        children: vec![],
                                        details: None,
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



                            let finished_at = if let Some(JoinSetResponseEvent{created_at: Some(created_at), ..}) = child_ids_to_responses.get(child_execution_id) {
                                Some(since_scheduled_at(*created_at))
                            } else {
                                None
                            };
                            Some(vec![
                                TraceDataChild {
                                    name: format!("{child_execution_id}"),
                                    started_at: since_scheduled_at(event.created_at.expect("event.created_at must be sent")),
                                    finished_at,
                                    children: vec![],
                                    details: None,
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
                started_at: Duration::ZERO,
                finished_at: since_scheduled_at(
                    last_event.created_at.expect("created_at must be sent"),
                ),
                children,
                details: None,
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

fn compute_child_execution_id_to_response<'a>(
    events: &[ExecutionEvent],
    responses: &'a HashMap<JoinSetId, Vec<JoinSetResponseEvent>>,
) -> HashMap<ExecutionId, &'a JoinSetResponseEvent> {
    let mut map = HashMap::new();
    let mut seen_join_nexts: HashMap<&JoinSetId, usize> = HashMap::new();
    for event in events {
        if let Some(execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
            event: Some(execution_event::history_event::Event::JoinNext(join_next)),
        })) = &event.event
        {
            let join_set_id = join_next
                .join_set_id
                .as_ref()
                .expect("`join_set_id` is sent in `JoinNext`");
            let counter_val = seen_join_nexts.entry(join_set_id).or_default();
            if let Some(
                resp @ JoinSetResponseEvent {
                    response:
                        Some(join_set_response_event::Response::ChildExecutionFinished(
                            join_set_response_event::ChildExecutionFinished {
                                child_execution_id: Some(child_execution_id),
                                ..
                            },
                        )),
                    ..
                },
            ) = responses
                .get(join_set_id)
                .and_then(|responses| responses.get(*counter_val))
            {
                map.insert(child_execution_id.clone(), resp);
            }
            *counter_val += 1;
        }
    }
    map
}
