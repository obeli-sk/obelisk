use crate::app::Route;
use crate::components::execution_detail::created::CreatedEvent;
use crate::components::execution_detail::finished::FinishedEvent;
use crate::components::execution_detail::history::join_next::HistoryJoinNextEvent;
use crate::components::execution_detail::history::join_set_created::HistoryJoinSetCreatedEvent;
use crate::components::execution_detail::history::join_set_request::HistoryJoinSetRequestEvent;
use crate::components::execution_detail::history::persist::HistoryPersistEvent;
use crate::components::execution_detail::history::schedule::HistoryScheduleEvent;
use crate::components::execution_detail::http_trace::HttpTraceEvent;
use crate::components::execution_detail::locked::LockedEvent;
use crate::components::execution_detail::temporarily_failed::TemporarilyFailedEvent;
use crate::components::execution_detail::timed_out::TemporarilyTimedOutEvent;
use crate::components::execution_detail::unlocked::UnlockedEvent;
use crate::components::execution_status::ExecutionStatus;
use crate::grpc::execution_id::{ExecutionIdExt, EXECUTION_ID_INFIX};
use crate::grpc::grpc_client::{
    self, execution_event, ExecutionEvent, JoinSetId, JoinSetResponseEvent,
};
use assert_matches::assert_matches;
use chrono::DateTime;
use gloo::timers::future::TimeoutFuture;
use hashbrown::HashMap;
use log::{debug, trace};
use std::ops::Deref;
use yew::prelude::*;
use yew_router::prelude::Link;

const PAGE: u32 = 100;

#[derive(Properties, PartialEq)]
pub struct ExecutionDetailPageProps {
    pub execution_id: grpc_client::ExecutionId,
}
#[function_component(ExecutionDetailPage)]
pub fn execution_detail_page(
    ExecutionDetailPageProps { execution_id }: &ExecutionDetailPageProps,
) -> Html {
    debug!("<ExecutionDetailPage />");
    let execution_id_state = use_state(|| execution_id.clone());
    let events_state: UseStateHandle<Vec<ExecutionEvent>> = use_state(Vec::new);
    let responses_state: UseStateHandle<(
        HashMap<grpc_client::JoinSetId, Vec<JoinSetResponseEvent>>,
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

    // Fetch ListExecutionEvents
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
                // Request ListExecutionEvents
                {
                    let execution_id = execution_id.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        let mut events = events_state.deref().clone();
                        let version_from = events.last().map(|e| e.version + 1).unwrap_or_default();
                        trace!("list_execution_events {execution_id} {version_from}");
                        let base_url = "/api";
                        let mut execution_client =
                        grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                            tonic_web_wasm_client::Client::new(base_url.to_string()),
                        );
                        let new_events = execution_client
                            .list_execution_events(grpc_client::ListExecutionEventsRequest {
                                execution_id: Some(execution_id.clone()),
                                version_from,
                                length: PAGE,
                            })
                            .await
                            .unwrap()
                            .into_inner()
                            .events;
                        debug!("Got {} events", new_events.len());
                        events.extend(new_events);
                        let last_event = events.last().expect("not found is sent as an error");
                        let is_finished =
                            matches!(last_event.event, Some(execution_event::Event::Finished(_)));
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
                // Request ListResponses
                {
                    let execution_id = execution_id.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        let (mut responses, cursor_from) = responses_state.deref().clone();
                        trace!("list_responses {execution_id} {cursor_from}");
                        let base_url = "/api";
                        let mut execution_client =
                        grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                            tonic_web_wasm_client::Client::new(base_url.to_string()),
                        );
                        let new_responses = execution_client
                            .list_responses(grpc_client::ListResponsesRequest {
                                execution_id: Some(execution_id.clone()),
                                cursor_from,
                                length: PAGE,
                                including_cursor: cursor_from == 0,
                            })
                            .await
                            .unwrap()
                            .into_inner()
                            .responses;
                        debug!("Got {} responses", new_responses.len());
                        let cursor_from = new_responses
                            .last()
                            .map(|r| r.cursor)
                            .unwrap_or(cursor_from);
                        for response in new_responses {
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
                        responses_state.set((responses, cursor_from));
                    });
                }
            }
        },
    );

    let execution_parts = execution_id.as_hierarchy();
    let execution_parts: Vec<_> = execution_parts
        .into_iter()
        .enumerate()
        .map(|(idx, (part, execution_id))| {
            html! {<>
            if idx > 0 {
                {EXECUTION_ID_INFIX}
            }
            <Link<Route> to={Route::ExecutionDetail { execution_id } }>
                {part}
            </Link<Route>>
            </>}
        })
        .collect();

    let events = events_state.deref();
    let join_next_version_to_response =
        compute_join_next_to_response(events, &responses_state.deref().0);

    let details_html = render_execution_details(events, &join_next_version_to_response);

    html! {
        <>
        <h3>{ execution_parts }</h3>
        <ExecutionStatus execution_id={execution_id.clone()} status={None} print_finished_status={true} />
        if !events.is_empty() {
            {details_html}
        } else {
            <p>{"Loading details..."}</p>
        }
    </>}
}

pub fn compute_join_next_to_response<'a>(
    events: &[ExecutionEvent],
    responses: &'a HashMap<JoinSetId, Vec<JoinSetResponseEvent>>,
) -> HashMap<u32 /* version of JoinNext */, &'a JoinSetResponseEvent> {
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
            if let Some(response) = responses
                .get(join_set_id)
                .and_then(|responses| responses.get(*counter_val))
            {
                map.insert(event.version, response);
            }
            *counter_val += 1;
        }
    }
    map
}

fn render_execution_details(
    events: &[ExecutionEvent],
    join_next_version_to_response: &HashMap<u32, &JoinSetResponseEvent>,
) -> Option<Html> {
    if events.is_empty() {
        return None;
    }
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
    let rows: Vec<_> = events
        .iter()
        .map(|event| {
            let detail = match event.event.as_ref().expect("event is sent by the server") {
                execution_event::Event::Created(created) => {
                    html! {
                        <CreatedEvent created={created.clone()} />
                    }
                }
                execution_event::Event::Locked(locked) => html! {
                    <LockedEvent locked={locked.clone()} />
                },
                execution_event::Event::Unlocked(event) => html! {
                    <UnlockedEvent event={event.clone()}/>
                },
                execution_event::Event::TemporarilyFailed(event) => {
                    html! {
                        <>
                            <HttpTraceEvent http_client_traces={event.http_client_traces.clone()} />
                            <TemporarilyFailedEvent event={event.clone()} />
                        </>
                    }
                }
                execution_event::Event::TemporarilyTimedOut(event) => html! {
                    <TemporarilyTimedOutEvent event={*event} />
                },
                execution_event::Event::Finished(event) => {
                    let result_detail = event
                        .result_detail
                        .as_ref()
                        .expect("`result_detail` is sent in the `Finished` message")
                        .clone();
                    html! {<>
                        <HttpTraceEvent http_client_traces={event.http_client_traces.clone()} />
                        <FinishedEvent {result_detail} />
                        </>
                    }
                }
                execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
                    event: Some(execution_event::history_event::Event::Schedule(event)),
                }) => html! {
                    <HistoryScheduleEvent event={event.clone()} />
                },
                execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
                    event: Some(execution_event::history_event::Event::JoinSetCreated(event)),
                }) => html! {
                    <HistoryJoinSetCreatedEvent event={event.clone()} />
                },
                execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
                    event: Some(execution_event::history_event::Event::JoinSetRequest(event)),
                }) => html! {
                    <HistoryJoinSetRequestEvent event={event.clone()} />
                },
                execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
                    event: Some(execution_event::history_event::Event::JoinNext(join_next)),
                }) => {
                    let response = join_next_version_to_response
                        .get(&event.version)
                        .cloned()
                        .cloned();
                    html! {
                        <HistoryJoinNextEvent
                            event={join_next.clone()}
                            {response}
                        />
                    }
                }
                execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
                    event: Some(execution_event::history_event::Event::Persist(event)),
                }) => html! {
                    <HistoryPersistEvent event={event.clone()} />
                },

                other => html! { {format!("unknown variant {other:?}")}},
            };
            let created_at =
                DateTime::from(event.created_at.expect("`created_at` sent by the server"));
            let since_scheduled = (created_at - execution_scheduled_at)
                .to_std()
                .ok()
                .unwrap_or_default();
            html! { <tr>
                <td>{event.version}</td>
                    <td>{created_at.to_string()}</td>
                <td>
                    if !since_scheduled.is_zero() {
                        {format!("{since_scheduled:?}")}
                    }
                </td>
                <td>{detail}</td>
            </tr>}
        })
        .collect();
    Some(html! {
        <div class="table-wrapper">
        <table>
        <thead>
        <tr>
            <th>{"Version"}</th>
            <th>{"Timestamp"}</th>
            <th>{"Since scheduled"}</th>
            <th>{"Detail"}</th>
        </tr>
        </thead>
        <tbody>
        {rows}
        </tbody>
        </table>
        </div>
    })
}
