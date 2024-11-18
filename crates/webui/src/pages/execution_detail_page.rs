use crate::app::Route;
use crate::components::execution_detail::create::CreateEvent;
use crate::components::execution_status::ExecutionStatus;
use crate::grpc::execution_id::{ExecutionIdExt, EXECUTION_ID_INFIX};
use crate::grpc::ffqn::FunctionFqn;
use crate::grpc::grpc_client::execution_event::Created;
use crate::grpc::grpc_client::{self, execution_event};
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
        let execution_created_at = DateTime::from(
            events
                .first()
                .expect("not found is sent as an error")
                .created_at
                .expect("`created_at` is sent by the server"),
        );
        let rows: Vec<_> = events
            .iter()
            .map(|event| {
                let detail = match event.event.as_ref().expect("event is sent by the server") {
                    execution_event::Event::Created(Created {
                        function_name: Some(function_name),
                        params: Some(params),
                        scheduled_at,
                        config_id: _,
                        scheduled_by,
                    }) => {
                        let ffqn = FunctionFqn::from(function_name.clone());
                        let params: Vec<serde_json::Value> = serde_json::from_slice(&params.value)
                            .expect("`params` must be a JSON array");
                        let created_at = DateTime::from(
                            event
                                .created_at
                                .expect("`created_at` is sent by the server"),
                        );
                        let scheduled_at = DateTime::from(
                            scheduled_at.expect("`scheduled_at` is sent by the server"),
                        );
                        let scheduled_by = scheduled_by.clone();
                        html! {
                            <CreateEvent {ffqn} {params} {created_at} {scheduled_at} {scheduled_by} />
                        }
                    }
                    // execution_event::Event::Locked(_) => todo!(),
                    // execution_event::Event::Unlocked(_) => todo!(),
                    // execution_event::Event::Failed(_) => todo!(),
                    // execution_event::Event::TimedOut(_) => todo!(),
                    // execution_event::Event::Finished(_) => todo!(),
                    // execution_event::Event::HistoryVariant(_) => todo!(),
                    other => html! { {format!("unknown variant {other:?}")}},
                };
                let created_at =
                    DateTime::from(event.created_at.expect("`created_at` sent by the server"));
                let created_in = (created_at - execution_created_at)
                    .to_std()
                    .expect("must be non-negative");
                html! { <tr>
                    <td>{event.version}</td>
                    <td>{format!("{created_in:?}")}</td>
                    <td>{detail}</td>
                </tr>}
            })
            .collect();
        html! {
            <table>
            <tr>
                <th>{"Version"}</th>
                <th>{"Created after"}</th>
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
