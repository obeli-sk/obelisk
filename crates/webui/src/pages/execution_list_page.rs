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
            list_executions_request::{Latest, NewerThan, OlderThan, Pagination},
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
    Older {
        execution_id: grpc_client::ExecutionId,
        including_cursor: bool,
    },
    Newer {
        execution_id: grpc_client::ExecutionId,
        including_cursor: bool,
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
        let page_size = 20;
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
                    ExecutionFilter::All => {
                        (None, Some(Pagination::Latest(Latest { latest: page_size })))
                    }
                    ExecutionFilter::ExecutionId { execution_id } => (
                        None,
                        Some(Pagination::NewerThan(NewerThan {
                            cursor: Some(execution_id.clone()),
                            next: 1,
                            including_cursor: true,
                        })),
                    ),
                    ExecutionFilter::Ffqn { ffqn } => (Some(ffqn), None),
                    ExecutionFilter::Older {
                        execution_id,
                        including_cursor,
                    } => (
                        None,
                        Some(Pagination::OlderThan(OlderThan {
                            cursor: Some(execution_id.clone()),
                            previous: page_size,
                            including_cursor,
                        })),
                    ),
                    ExecutionFilter::Newer {
                        execution_id,
                        including_cursor,
                    } => (
                        None,
                        Some(Pagination::NewerThan(NewerThan {
                            cursor: Some(execution_id.clone()),
                            next: page_size,
                            including_cursor,
                        })),
                    ),
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
        let (topmost_exe, bottommost_exe) = (
            executions
                .first()
                .and_then(|summary| summary.execution_id.clone()),
            executions
                .last()
                .and_then(|summary| summary.execution_id.clone()),
        );
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
                    <td>
                        <Link<Route> to={Route::ExecutionDetail { execution_id: execution_id.clone() }}>{&execution_id}</Link<Route>>
                    </td>
                        <td><Link<Route> to={Route::ExecutionListByFfqn { ffqn: ffqn.to_string() }}>{ffqn.to_string()}</Link<Route>></td>
                    <td><ExecutionStatus {status} {execution_id} /></td>
                    </tr>
                }
            })
            .collect::<Vec<_>>();

        let submittable_link_fn =
            Callback::from(|ffqn: FunctionFqn| html! { <ComponentTreeFfqnLink {ffqn} /> });

        html! {<>
            <h3>{"Executions"}</h3>
            if let ExecutionFilter::Ffqn{ffqn} = filter {
                <h4>{format!("Filtered by function: {ffqn}")}</h4>
                <p><Link<Route> to={Route::ExecutionSubmit { ffqn: ffqn.to_string() }}>{"Submit new execution"}</Link<Route>></p>
                <p><Link<Route> to={Route::ExecutionList}>{"Remove filter"}</Link<Route>></p>
            }
            if let ExecutionFilter::ExecutionId{execution_id} = filter {
                <h4>{format!("Filtered by execution ID: {execution_id}")}</h4>
                <p><Link<Route> to={Route::ExecutionList}>{"Remove filter"}</Link<Route>></p>
            }
            <ComponentTree components={app_state.components} show_extensions={ false } {submittable_link_fn} show_submittable_only={true}/>
            <table>
                <tr><th>{"Execution ID"}</th><th>{"Function"}</th><th>{"Status"}</th></tr>
                { rows }
            </table>
            if let (Some(topmost_exe), Some(bottommost_exe)) = (topmost_exe, bottommost_exe) {
                <p>
                    <Link<Route> to={Route::ExecutionListNewer { execution_id: topmost_exe }}>
                        {"Newer"}
                    </Link<Route>>
                </p>
                <p>
                    <Link<Route> to={Route::ExecutionListOlder { execution_id: bottommost_exe }}>
                        {"Older"}
                    </Link<Route>>
                </p>
            } else if let ExecutionFilter::Newer { execution_id, including_cursor: false } = filter {
                <p>
                    <Link<Route> to={Route::ExecutionListOlderIncluding { execution_id: execution_id.clone() }}>
                        {"Older"}
                    </Link<Route>>
                </p>
            } else if let ExecutionFilter::Older { execution_id, including_cursor: false } = filter {
                <p>
                    <Link<Route> to={Route::ExecutionListNewerIncluding { execution_id: execution_id.clone() }}>
                        {"Newer"}
                    </Link<Route>>
                </p>
            }
            <p>
                <Link<Route> to={Route::ExecutionList}>
                    {"Latest"}
                </Link<Route>>
            </p>
        </>}
    } else {
        html! {
            <p>{"Loading"}</p>
        }
    }
}
