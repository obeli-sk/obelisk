use crate::{components::execution_list::ExecutionStatus, ffqn::FunctionFqn, grpc_client};
use log::debug;
use std::ops::Deref;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ExecutionListPageProps {}
#[function_component(ExecutionListPage)]
pub fn execution_list_page(_props: &ExecutionListPageProps) -> Html {
    let executions_state = use_state(|| None);

    {
        let executions_state = executions_state.clone();
        use_effect_with((), move |_x| {
            wasm_bindgen_futures::spawn_local(async move {
                let base_url = "/api";
                let mut execution_client =
                    grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                        tonic_web_wasm_client::Client::new(base_url.to_string()),
                    );
                let response = execution_client
                    .list_executions(grpc_client::ListExecutionsRequest {
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
        let rows = executions.iter().map(|execution| {
            let ffqn = FunctionFqn::from(execution.function_name.clone().expect("`function_name` is sent by the server"));
            let status = execution.current_status.clone().expect("`current_status` is sent by the server");
            html! {
                <tr>
                <td>{execution.execution_id.clone().expect("`execution_id` is sent by the server").id}</td>
                <td>{ffqn.to_string()}</td>
                <td><ExecutionStatus {status} /></td>
                </tr>
            }
        }).collect::<Vec<_>>();
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
