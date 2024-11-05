use crate::{
    app::Route,
    components::execution_status::ExecutionStatus,
    grpc::ffqn::FunctionFqn,
    grpc::grpc_client::{
        self,
        list_executions_request::{FirstAfter, Pagination},
    },
};
use log::debug;
use std::ops::Deref;
use yew::prelude::*;
use yew_router::prelude::Link;

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
        debug!("Filtering by ffqn: {ffqn:?}");
        let execution_id = execution_id.clone();
        debug!("Filtering by execution_id: {execution_id:?}");
        use_effect_with((ffqn, execution_id), move |(ffqn, execution_id)| {
            let ffqn = ffqn.clone();
            let execution_id = execution_id.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let base_url = "/api";
                let mut execution_client =
                    grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                        tonic_web_wasm_client::Client::new(base_url.to_string()),
                    );
                let pagination = execution_id.map(|execution_id| {
                    Pagination::FirstAfter(FirstAfter {
                        first: 1,
                        including_cursor: true,
                        cursor: Some(execution_id),
                    })
                });
                let response = execution_client
                    .list_executions(grpc_client::ListExecutionsRequest {
                        function_name: ffqn.map(grpc_client::FunctionName::from),
                        pagination,
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
                        <td><Link<Route> to={Route::ExecutionListByFfqn { ffqn: ffqn.to_string() }}>{ffqn.to_string()}</Link<Route>></td>
                    <td><ExecutionStatus {status} {execution_id} /></td>
                    </tr>
                }
            })
            .collect::<Vec<_>>();
        let rows = html! { for rows };
        html! {<>
            if let Some(ffqn) = ffqn {
                <h3>{format!("Filtering by function: {ffqn}")}</h3>
                <p><Link<Route> to={Route::ExecutionSubmit { ffqn: ffqn.to_string() }}>{format!("Submit new execution")}</Link<Route>></p>
                <p><Link<Route> to={Route::ExecutionList}>{format!("Remove filter")}</Link<Route>></p>
            }
            if let Some(execution_id) = execution_id {
                <h3>{format!("Filtering by execution ID: {}", execution_id.id)}</h3>
                <p><Link<Route> to={Route::ExecutionList}>{format!("Remove filter")}</Link<Route>></p>
            }
            <table>
            <tr><th>{"Execution ID"}</th><th>{"Function"}</th><th>{"Status"}</th></tr>
            { rows }
            </table>
        </>}
    } else {
        html! {
            <p>{"Loading"}</p>
        }
    }
}
