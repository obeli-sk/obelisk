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
use crate::grpc::grpc_client::{self, execution_event};
use assert_matches::assert_matches;
use chrono::DateTime;
use log::debug;
use std::ops::Deref;
use yew::prelude::*;
use yew_router::prelude::Link;

#[derive(Properties, PartialEq)]
pub struct ExecutionDetailPageProps {
    pub execution_id: grpc_client::ExecutionId,
}
#[function_component(ExecutionDetailPage)]
pub fn execution_detail_page(
    ExecutionDetailPageProps { execution_id }: &ExecutionDetailPageProps,
) -> Html {
    let events_state = use_state(|| None);
    use_effect_with(execution_id.clone(), {
        let events_state = events_state.clone();
        move |execution_id| {
            let execution_id = execution_id.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let base_url = "/api";
                let mut execution_client =
                    grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                        tonic_web_wasm_client::Client::new(base_url.to_string()),
                    );
                let events = execution_client
                    .list_execution_events(grpc_client::ListExecutionEventsRequest {
                        execution_id: Some(execution_id.clone()),
                        version_from: 0,
                        length: 20,
                    })
                    .await
                    .unwrap()
                    .into_inner()
                    .events;
                debug!("Got {} events", events.len());
                events_state.set(Some(events));
            })
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

    let details = if let Some(events) = events_state.deref() {
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
                    execution_event::Event::Failed(event) => html! {
                        <IntermittentlyFailedEvent event={event.clone()} />
                    },
                    execution_event::Event::TimedOut(event) => html! {
                        <IntermittentlyTimedOutEvent event={event.clone()} />
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
                        <HistoryJoinNextEvent event={event.clone()} />
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
        html! {
            <table>
            <tr>
                <th>{"Version"}</th>
                <th>{"Timestamp"}</th>
                <th>{"Since scheduled"}</th>
                <th>{"Detail"}</th>
            </tr>
            {rows}
            </table>
        }
    } else {
        html! {
            <p>{"Loading"}</p>
        }
    };

    html! {
        <>
        <h3>{ execution_parts }</h3>
        <ExecutionStatus execution_id={execution_id.clone()} status={None} print_finished_status={true} />
        {details}
    </>}
}
