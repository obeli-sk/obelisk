use crate::app::{AppState, Route};
use crate::components::debugger::debugger_view::EventsAndResponsesState;
use crate::components::execution_detail::utils::{compute_join_next_to_response, event_to_detail};
use crate::components::execution_header::{ExecutionHeader, ExecutionLink};
use crate::grpc::ffqn::FunctionFqn;
use crate::grpc::grpc_client::{
    self, ComponentType, ExecutionEvent, ExecutionId, JoinSetResponseEvent,
};
use assert_matches::assert_matches;
use chrono::DateTime;
use hashbrown::HashMap;
use std::ops::Deref;
use yew::prelude::*;
use yew_router::prelude::Link;

#[derive(Properties, PartialEq)]
pub struct ExecutionLogPageProps {
    pub execution_id: grpc_client::ExecutionId,
}
#[function_component(ExecutionLogPage)]
pub fn execution_log_page(ExecutionLogPageProps { execution_id }: &ExecutionLogPageProps) -> Html {
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

    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");

    let details_html = render_execution_details(
        execution_id,
        events,
        &join_next_version_to_response,
        &app_state,
    );

    html! {
        <>
        <ExecutionHeader execution_id={execution_id.clone()} link={ExecutionLink::Log} />

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
    app_state: &AppState,
) -> Option<Html> {
    if events.is_empty() {
        return None;
    }
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

    let execution_scheduled_at = {
        DateTime::from(
            create_event
                .scheduled_at
                .expect("`scheduled_at` is sent by the server"),
        )
    };

    let ffqn = FunctionFqn::from(create_event);
    let maybe_stub_link = if let Some((_, component_id)) = app_state.ffqns_to_details.get(&ffqn)
        && let Some(found_component) = app_state.components_by_id.get(component_id)
        && found_component.as_type() == ComponentType::ActivityStub
    {
        Some(html! {
            <Link<Route> to={Route::ExecutionStubResult { ffqn: ffqn.clone(), execution_id: execution_id.clone() }}>{"Submit stub response"}</Link<Route>>
        })
    } else {
        None
    };

    let rows: Vec<_> = events
        .iter()
        .map(|event| {
            let detail = event_to_detail(
                execution_id,
                event,
                join_next_version_to_response,
                ExecutionLink::Log,
                false,
            );
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
        {maybe_stub_link}
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
