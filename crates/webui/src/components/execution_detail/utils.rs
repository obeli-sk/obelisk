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
use crate::components::execution_header::ExecutionLink;
use crate::grpc::grpc_client::{
    execution_event, ExecutionEvent, ExecutionId, JoinSetId, JoinSetResponseEvent,
};
use hashbrown::HashMap;
use yew::prelude::*;

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
    link: ExecutionLink,
    is_selected: bool,
) -> Html {
    match event.event.as_ref().expect("event is sent by the server") {
        execution_event::Event::Created(created) => {
            html! {
                <CreatedEvent created={created.clone()} version={event.version} {link} {is_selected} />
            }
        }
        execution_event::Event::Locked(locked) => html! {
            <LockedEvent locked={locked.clone()} version={event.version} {is_selected} />
        },
        execution_event::Event::Unlocked(inner_event) => html! {
            <UnlockedEvent event={inner_event.clone()} version={event.version} {is_selected} />
        },
        execution_event::Event::TemporarilyFailed(inner_event) => {
            html! {
                <>
                    <HttpTraceEvent http_client_traces={inner_event.http_client_traces.clone()} />
                    <TemporarilyFailedEvent event={inner_event.clone()} version={event.version} {is_selected} />
                </>
            }
        }
        execution_event::Event::TemporarilyTimedOut(inner_event) => html! {<>
            <HttpTraceEvent http_client_traces={inner_event.http_client_traces.clone()} />
            <TemporarilyTimedOutEvent event={inner_event.clone()} version={event.version} {is_selected} />
        </>},
        execution_event::Event::Finished(inner_event) => {
            let result_detail = inner_event
                .result_detail
                .as_ref()
                .expect("`result_detail` is sent in the `Finished` message")
                .clone();
            html! {<>
                <HttpTraceEvent http_client_traces={inner_event.http_client_traces.clone()} />
                <FinishedEvent {result_detail} version={event.version} {link} {is_selected} />
                </>
            }
        }
        execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
            event: Some(execution_event::history_event::Event::Schedule(inner_event)),
        }) => html! {
            <HistoryScheduleEvent event={inner_event.clone()} version={event.version} {link} {is_selected} />
        },
        execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
            event: Some(execution_event::history_event::Event::JoinSetCreated(inner_event)),
        }) => html! {
            <HistoryJoinSetCreatedEvent
                event={inner_event.clone()}
                version={event.version}
                {is_selected}
                execution_id={execution_id.clone()}
                backtrace_id={event.backtrace_id}
                />
        },
        execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
            event: Some(execution_event::history_event::Event::JoinSetRequest(join_set_request)),
        }) => html! {
            <HistoryJoinSetRequestEvent
                event={join_set_request.clone()}
                execution_id={execution_id.clone()}
                backtrace_id={event.backtrace_id}
                version={event.version}
                {link}
                {is_selected}
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
                    {link}
                    {is_selected}
                />
            }
        }
        execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
            event: Some(execution_event::history_event::Event::Persist(inner_event)),
        }) => html! {
            <HistoryPersistEvent event={inner_event.clone()} version={event.version} {is_selected}/>
        },

        other => html! { {format!("unknown variant {other:?}")}},
    }
}
