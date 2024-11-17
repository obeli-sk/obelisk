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
            list_executions_request::{NewerThan, OlderThan, Pagination},
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
    Older {
        cursor: String,
        including_cursor: bool,
    },
    Newer {
        cursor: String,
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
    let response_state = use_state(|| None);
    {
        let page_size = 20;
        let response_state = response_state.clone();
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
                    ExecutionFilter::Ffqn { ffqn } => (Some(ffqn), None),
                    ExecutionFilter::Older {
                        cursor,
                        including_cursor,
                    } => (
                        None,
                        Some(Pagination::OlderThan(OlderThan {
                            cursor,
                            previous: page_size,
                            including_cursor,
                        })),
                    ),
                    ExecutionFilter::Newer {
                        cursor,
                        including_cursor,
                    } => (
                        None,
                        Some(Pagination::NewerThan(NewerThan {
                            cursor,
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
                response_state.set(Some(response));
            })
        });
    }

    if let Some(response) = response_state.deref() {
        let rows = response.executions
            .iter()
            .map(|execution| {
                let ffqn = FunctionFqn::from(
                    execution
                        .function_name
                        .clone()
                        .expect("`function_name` is sent by the server"),
                );
                let status = Some(execution
                    .current_status
                    .clone()
                    .expect("`current_status` is sent by the server")
                    .status
                    .expect("`current_status.status` is sent by the server"));

                let execution_id = execution
                    .execution_id
                    .clone()
                    .expect("`execution_id` is sent by the server");
                html! {
                    <tr>
                    <td>
                        <Link<Route> to={Route::ExecutionDetail { execution_id: execution_id.clone() }}>{&execution_id}</Link<Route>>
                    </td>
                        <td><Link<Route> to={Route::ExecutionListByFfqn { ffqn: ffqn.clone() }}>{ffqn.to_string()}</Link<Route>></td>
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
                <p><Link<Route> to={Route::ExecutionSubmit { ffqn: ffqn.clone() }}>{"Submit new execution"}</Link<Route>></p>
                <p><Link<Route> to={Route::ExecutionList}>{"Remove filter"}</Link<Route>></p>
            }
            <ComponentTree components={app_state.components} show_extensions={ false } {submittable_link_fn} show_submittable_only={true}/>
            <table>
                <tr><th>{"Execution ID"}</th><th>{"Function"}</th><th>{"Status"}</th></tr>
                { rows }
            </table>
            if let (Some(cursor_newest), Some(cursor_oldest)) = (&response.cursor_newest, &response.cursor_oldest) {
                <p>
                    <Link<Route> to={Route::ExecutionListNewer { cursor: cursor_newest.clone() }}>
                        {"Newer"}
                    </Link<Route>>
                </p>
                <p>
                    <Link<Route> to={Route::ExecutionListOlder { cursor: cursor_oldest.clone() }}>
                        {"Older"}
                    </Link<Route>>
                </p>
            } else if let ExecutionFilter::Newer { cursor, including_cursor: false } = filter {
                <p>
                    <Link<Route> to={Route::ExecutionListOlderIncluding { cursor: cursor.clone() }}>
                        {"Older"}
                    </Link<Route>>
                </p>
            } else if let ExecutionFilter::Older { cursor, including_cursor: false } = filter {
                <p>
                    <Link<Route> to={Route::ExecutionListNewerIncluding { cursor: cursor.clone() }}>
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
