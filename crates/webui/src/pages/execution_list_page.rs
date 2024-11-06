use crate::{
    app::{AppState, Route},
    components::{
        component_tree::ComponentTree, component_tree_ffqn_link::ComponentTreeFfqnLink,
        execution_status::ExecutionStatus,
    },
    grpc::{
        ffqn::FunctionFqn,
        grpc_client::{
            self,
            list_executions_request::{FirstAfter, Pagination},
        },
    },
};
use log::debug;
use std::ops::Deref;
use yew::prelude::*;
use yew_router::prelude::Link;

#[derive(Default, Clone, PartialEq)]
pub enum ExecutionFilter {
    #[default]
    All,
    ExecutionId {
        execution_id: grpc_client::ExecutionId,
    },
    Before {
        execution_id: grpc_client::ExecutionId,
    },
    After {
        execution_id: grpc_client::ExecutionId,
    },
    Ffqn {
        ffqn: FunctionFqn,
    },
}

#[derive(Properties, PartialEq)]
pub struct ExecutionListPageProps {
    #[prop_or_default]
    pub filter: ExecutionFilter,
}
#[function_component(ExecutionListPage)]
pub fn execution_list_page(ExecutionListPageProps { filter }: &ExecutionListPageProps) -> Html {
    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");
    let executions_state = use_state(|| None);
    {
        let executions_state = executions_state.clone();
        use_effect_with(filter.clone(), move |filter| {
            let filter = filter.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let base_url = "/api";
                let mut execution_client =
                    grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                        tonic_web_wasm_client::Client::new(base_url.to_string()),
                    );
                let (ffqn, pagination) = match filter {
                    ExecutionFilter::All => (None, None),
                    ExecutionFilter::ExecutionId { execution_id } => (
                        None,
                        Some(Pagination::FirstAfter(FirstAfter {
                            first: 1,
                            including_cursor: true,
                            cursor: Some(execution_id.clone()),
                        })),
                    ),
                    ExecutionFilter::Ffqn { ffqn } => (Some(ffqn), None),
                    _ => todo!(),
                };
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
                    <td>{&execution_id}</td>
                        <td><Link<Route> to={Route::ExecutionListByFfqn { ffqn: ffqn.to_string() }}>{ffqn.to_string()}</Link<Route>></td>
                    <td><ExecutionStatus {status} {execution_id} /></td>
                    </tr>
                }
            })
            .collect::<Vec<_>>();
        let rows = html! { for rows };
        let submittable_link_fn =
            Callback::from(|ffqn: FunctionFqn| html! { <ComponentTreeFfqnLink {ffqn} /> });

        html! {<>
            if let ExecutionFilter::Ffqn{ffqn} = filter {
                <h3>{format!("Filtering by function: {ffqn}")}</h3>
                <p><Link<Route> to={Route::ExecutionSubmit { ffqn: ffqn.to_string() }}>{format!("Submit new execution")}</Link<Route>></p>
                <p><Link<Route> to={Route::ExecutionList}>{format!("Remove filter")}</Link<Route>></p>
            }
            if let ExecutionFilter::ExecutionId{execution_id} = filter {
                <h3>{format!("Filtering by execution ID: {execution_id}")}</h3>
                <p><Link<Route> to={Route::ExecutionList}>{format!("Remove filter")}</Link<Route>></p>
            }
            <ComponentTree components={app_state.components} show_extensions={ false } {submittable_link_fn} show_submittable_only={true}/>
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
