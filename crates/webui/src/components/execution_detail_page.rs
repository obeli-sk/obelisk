use crate::app::Route;
use crate::components::debugger::debugger_view::EventsAndResponsesState;
use crate::components::execution_detail::utils::{compute_join_next_to_response, event_to_detail};
use crate::components::execution_header::ExecutionHeader;
use crate::grpc::grpc_client::{self, ExecutionEvent, ExecutionId, JoinSetResponseEvent};
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
        <ExecutionHeader execution_id={execution_id.clone()} route_fn={Callback::from(|execution_id| Route::ExecutionDetail { execution_id })} />

        if !events.is_empty() {
            {details_html}
        } else {
            <p>{"Loading details..."}</p>
        }
    </>}
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
