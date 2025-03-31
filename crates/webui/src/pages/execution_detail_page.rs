use crate::app::Route;
use crate::components::debugger::debugger_view::EventsAndResponsesState;
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
use crate::grpc::execution_id::ExecutionIdExt;
use crate::grpc::grpc_client::{
    self, execution_event, ExecutionEvent, ExecutionId, JoinSetId, JoinSetResponseEvent,
};
use assert_matches::assert_matches;
use chrono::DateTime;
use hashbrown::HashMap;
use std::ops::Deref;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ExecutionDetailPageProps {
    pub execution_id: grpc_client::ExecutionId,
}
#[function_component(ExecutionDetailPage)]
pub fn execution_detail_page(
    ExecutionDetailPageProps { execution_id }: &ExecutionDetailPageProps,
) -> Html {
    let events_state: UseStateHandle<Vec<ExecutionEvent>> = use_state(Vec::new);
    let responses_state: UseStateHandle<(
        HashMap<grpc_client::JoinSetId, Vec<JoinSetResponseEvent>>,
        u32, /* Cursor */
    )> = use_state(|| (HashMap::new(), 0));
    let is_fetching_state = use_state(|| (execution_id.clone(), 0)); // Retrigger if not on the last page

    let events_and_responses_state = EventsAndResponsesState {
        events_state,
        responses_state,
        is_fetching_state,
    };
    use_effect_with(
        (
            execution_id.clone(),
            events_and_responses_state.is_fetching_state.deref().clone(),
        ),
        {
            let events_and_responses_state = events_and_responses_state.clone();
            move |(execution_id, is_fetching)| {
                events_and_responses_state.hook(execution_id, is_fetching, true)
            }
        },
    );

    let events = events_and_responses_state.events_state.deref();
    log::debug!("rendering ExecutionDetailPage {:?}", events.iter().next());
    let responses = &events_and_responses_state.responses_state.0;
    let join_next_version_to_response = compute_join_next_to_response(events, responses);

    let details_html =
        render_execution_details(execution_id, events, &join_next_version_to_response);

    html! {
        <>
        <h3>{ execution_id.render_execution_parts(false, |execution_id| Route::ExecutionDetail { execution_id }) }</h3>
        <ExecutionStatus execution_id={execution_id.clone()} status={None} print_finished_status={true} />
        if !events.is_empty() {
            {details_html}
        } else {
            <p>{"Loading details..."}</p>
        }
    </>}
}

pub fn compute_join_next_to_response<'a>(
    events: impl IntoIterator<Item = &'a ExecutionEvent>,
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

pub fn event_to_detail(
    execution_id: &ExecutionId,
    event: &ExecutionEvent,
    join_next_version_to_response: &HashMap<u32, &JoinSetResponseEvent>,
) -> Html {
    match event.event.as_ref().expect("event is sent by the server") {
        execution_event::Event::Created(created) => {
            html! {
                <CreatedEvent created={created.clone()} version={event.version} />
            }
        }
        execution_event::Event::Locked(locked) => html! {
            <LockedEvent locked={locked.clone()} version={event.version} />
        },
        execution_event::Event::Unlocked(inner_event) => html! {
            <UnlockedEvent event={inner_event.clone()} version={event.version}/>
        },
        execution_event::Event::TemporarilyFailed(inner_event) => {
            html! {
                <>
                    <HttpTraceEvent http_client_traces={inner_event.http_client_traces.clone()} />
                    <TemporarilyFailedEvent event={inner_event.clone()} version={event.version} />
                </>
            }
        }
        execution_event::Event::TemporarilyTimedOut(inner_event) => html! {<>
            <HttpTraceEvent http_client_traces={inner_event.http_client_traces.clone()} />
            <TemporarilyTimedOutEvent event={inner_event.clone()} version={event.version} />
        </>},
        execution_event::Event::Finished(inner_event) => {
            let result_detail = inner_event
                .result_detail
                .as_ref()
                .expect("`result_detail` is sent in the `Finished` message")
                .clone();
            html! {<>
                <HttpTraceEvent http_client_traces={inner_event.http_client_traces.clone()} />
                <FinishedEvent {result_detail} version={event.version}/>
                </>
            }
        }
        execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
            event: Some(execution_event::history_event::Event::Schedule(inner_event)),
        }) => html! {
            <HistoryScheduleEvent event={inner_event.clone()} version={event.version}/>
        },
        execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
            event: Some(execution_event::history_event::Event::JoinSetCreated(inner_event)),
        }) => html! {
            <HistoryJoinSetCreatedEvent event={inner_event.clone()} version={event.version}/>
        },
        execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
            event: Some(execution_event::history_event::Event::JoinSetRequest(join_set_request)),
        }) => html! {
            <HistoryJoinSetRequestEvent
                event={join_set_request.clone()}
                execution_id={execution_id.clone()}
                backtrace_id={event.backtrace_id}
                version={event.version}
                />
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
                    execution_id={execution_id.clone()}
                    backtrace_id={event.backtrace_id}
                    version={event.version}
                />
            }
        }
        execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
            event: Some(execution_event::history_event::Event::Persist(inner_event)),
        }) => html! {
            <HistoryPersistEvent event={inner_event.clone()} version={event.version}/>
        },

        other => html! { {format!("unknown variant {other:?}")}},
    }
}

fn render_execution_details(
    execution_id: &ExecutionId,
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
            let detail = event_to_detail(execution_id, event, join_next_version_to_response);
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
