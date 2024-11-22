use crate::app::Route;
use crate::components::execution_detail::created::CreatedEvent;
use crate::components::execution_detail::finished::FinishedEvent;
use crate::components::execution_detail::history::join_next::HistoryJoinNextEvent;
use crate::components::execution_detail::history::join_set_created::HistoryJoinSetCreatedEvent;
use crate::components::execution_detail::history::join_set_request::HistoryJoinSetRequestEvent;
use crate::components::execution_detail::history::persist::HistoryPersistEvent;
use crate::components::execution_detail::history::schedule::HistoryScheduleEvent;
use crate::components::execution_detail::intermittently_failed::IntermittentlyFailedEvent;
use crate::components::execution_detail::locked::LockedEvent;
use crate::components::execution_detail::timed_out::IntermittentlyTimedOutEvent;
use crate::components::execution_detail::unlocked::UnlockedEvent;
use crate::components::execution_status::ExecutionStatus;
use crate::grpc::execution_id::{ExecutionIdExt, EXECUTION_ID_INFIX};
use crate::grpc::grpc_client::{self, execution_event, ExecutionEvent};
use assert_matches::assert_matches;
use chrono::DateTime;
use hashbrown::HashMap;
use log::debug;
use std::ops::Deref;
use yew::prelude::*;
use yew_router::prelude::Link;

const PAGE: u32 = 20;

#[derive(Properties, PartialEq)]
pub struct ExecutionDetailPageProps {
    pub execution_id: grpc_client::ExecutionId,
}
#[function_component(ExecutionDetailPage)]
pub fn execution_detail_page(
    ExecutionDetailPageProps { execution_id }: &ExecutionDetailPageProps,
) -> Html {
    let execution_id_state = use_state(|| execution_id.clone());
    let events_version_from_state = use_state(|| 0);
    let events_state = use_state(|| Vec::new());
    let last_response_cursor = use_state(|| 0);
    let responses_state: UseStateHandle<HashMap<_, Vec<_>>> = use_state(|| HashMap::new());

    // Cleanup the state on execution_id change.
    use_effect_with(execution_id.clone(), {
        let execution_id_state = execution_id_state.clone();
        let events_version_from_state = events_version_from_state.clone();
        let events_state = events_state.clone();
        let last_response_cursor = last_response_cursor.clone();
        let responses_state = responses_state.clone();
        move |execution_id| {
            if *execution_id != *execution_id_state.deref() {
                debug!("Execution ID changed");
                execution_id_state.set(execution_id.clone());
                events_version_from_state.set(Default::default());
                events_state.set(Default::default());
                last_response_cursor.set(Default::default());
                responses_state.set(Default::default());
            }
        }
    });

    // Fetch ListExecutionEvents
    // TODO: make dependent on timestamp changed by the button, fixes noop when no new events arrived with this version yet.
    use_effect_with(
        (
            execution_id_state.deref().clone(),
            *events_version_from_state.deref(),
        ),
        {
            let events_state = events_state.clone();
            move |(execution_id, version_from)| {
                let version_from = *version_from;
                let mut events = events_state.deref().clone();
                let execution_id = execution_id.clone();
                debug!("list_execution_events {execution_id} {version_from}");
                wasm_bindgen_futures::spawn_local(async move {
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
                    events_state.set(events);
                });
            }
        },
    );

    // Fetch ListResponses
    // The closure is the only writer to `last_response_cursor` except for
    // cleanup. We get the new execution_id after cleanup runs.
    // TODO: make dependent on timestamp changed by the button
    use_effect_with(execution_id_state.deref().clone(), {
        let responses_state = responses_state.clone();
        let cursor_from = *last_response_cursor.deref();

        move |execution_id| {
            let mut responses = responses_state.deref().clone();
            let execution_id = execution_id.clone();
            debug!("list_responses {execution_id} {cursor_from}");
            wasm_bindgen_futures::spawn_local(async move {
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
                        including_cursor: false,
                    })
                    .await
                    .unwrap()
                    .into_inner()
                    .responses;
                debug!("Got {} responses", new_responses.len());
                if let Some(last) = new_responses.last() {
                    last_response_cursor.set(last.cursor);
                }
                for response in new_responses {
                    let response = response
                        .event
                        .expect("`event` is sent in `ResponseWithCursor`");
                    let join_set_id = response
                        .join_set_id
                        .expect("`join_set_id` is sent in `JoinSetResponseEvent`");
                    let response = response
                        .response
                        .expect("`response` is sent in `JoinSetResponseEvent`");
                    let execution_responses = responses.entry(join_set_id).or_default();
                    execution_responses.push(response);
                }
                responses_state.set(responses);
            });
        }
    });

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
    //let join_next_version_to_response = compute_join_next_to_response();

    let details_html = render_execution_details(&events);

    let load_more_callback = Callback::from(move |_| {
        events_version_from_state.set(*events_version_from_state + PAGE);
    });
    let finished = matches!(
        events.last(),
        Some(ExecutionEvent {
            event: Some(execution_event::Event::Finished(_)),
            ..
        })
    );
    html! {
        <>
        <h3>{ execution_parts }</h3>
        <ExecutionStatus execution_id={execution_id.clone()} status={None} print_finished_status={true} />
        if !events.is_empty() {
            {details_html}
            if !finished {
                <button onclick={load_more_callback} >{"Load more"} </button>
            }
        } else {
            <p>{"Loading details..."}</p>
        }
    </>}
}

fn render_execution_details(events: &[ExecutionEvent]) -> Option<Html> {
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
                    <UnlockedEvent event={*event}/>
                },
                execution_event::Event::Failed(event) => html! {
                    <IntermittentlyFailedEvent event={event.clone()} />
                },
                execution_event::Event::TimedOut(event) => html! {
                    <IntermittentlyTimedOutEvent event={*event} />
                },
                execution_event::Event::Finished(event) => html! {
                    <FinishedEvent event={event.clone()} />
                },
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
                    event: Some(execution_event::history_event::Event::JoinNext(event)),
                }) => html! {
                    <HistoryJoinNextEvent event={event.clone()} response={None} />
                },
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
        <table>
        <tr>
            <th>{"Version"}</th>
            <th>{"Timestamp"}</th>
            <th>{"Since scheduled"}</th>
            <th>{"Detail"}</th>
        </tr>
        {rows}
        </table>
    })
}
