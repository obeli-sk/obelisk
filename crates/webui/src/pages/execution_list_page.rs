use crate::{
    components::execution_status::ExecutionStatus,
    ffqn::FunctionFqn,
    grpc_client::{
        self,
        list_executions_request::{FirstAfter, Pagination},
    },
};
use log::debug;
use std::ops::Deref;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ExecutionListPageProps {
    #[prop_or_default]
    pub ffqn: Option<FunctionFqn>,
    #[prop_or_default]
    pub execution_id: Option<grpc_client::ExecutionId>,
}
#[function_component(ExecutionListPage)]
pub fn execution_list_page(
    ExecutionListPageProps { ffqn, execution_id }: &ExecutionListPageProps,
) -> Html {
    let executions_state = use_state(|| None);
    {
        let executions_state = executions_state.clone();
        let ffqn = ffqn.clone();
        let execution_id = execution_id.clone();
        use_effect_with((), move |_x| {
            wasm_bindgen_futures::spawn_local(async move {
                let base_url = "/api";
                let mut execution_client =
                    grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                        tonic_web_wasm_client::Client::new(base_url.to_string()),
                    );
                let pagination = if let Some(execution_id) = execution_id {
                    Some(Pagination::FirstAfter(FirstAfter {
                        first: 1,
                        including_cursor: true,
                        cursor: Some(execution_id),
                    }))
                } else {
                    None
                };
                let response = execution_client
                    .list_executions(grpc_client::ListExecutionsRequest {
                        function_name: ffqn.map(grpc_client::FunctionName::from),
                        pagination,
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .into_inner();
                debug!("Got ListExecutionsResponse");
                executions_state.set(Some(response.executions));
            })
        });
    }
    if let Some(executions) = executions_state.deref() {
        let rows = executions
            .iter()
            .map(|execution| {
                let ffqn = FunctionFqn::from(
                    execution
                        .function_name
                        .clone()
                        .expect("`function_name` is sent by the server"),
                );
                let status = execution
                    .current_status
                    .clone()
                    .expect("`current_status` is sent by the server")
                    .status
                    .expect("`current_status.status` is sent by the server");

                let execution_id = execution
                    .execution_id
                    .clone()
                    .expect("`execution_id` is sent by the server");
                html! {
                    <tr>
                    <td>{&execution_id.id}</td>
                    <td>{ffqn.to_string()}</td>
                    <td><ExecutionStatus {status} {execution_id} /></td>
                    </tr>
                }
            })
            .collect::<Vec<_>>();
        let rows = html! { for rows };
        html! {
        <table>
        <tr><th>{"Execution ID"}</th><th>{"Function"}</th><th>{"Status"}</th></tr>
        { rows }
        </table>
        }
    } else {
        html! {
            <p>{"Loading"}</p>
        }
    }
}
