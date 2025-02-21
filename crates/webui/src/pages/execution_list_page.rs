use crate::{
    app::{ExecutionsCursor, Route},
    components::{
        component_tree::{ComponentTree, ComponentTreeConfig},
        execution_status::ExecutionStatus,
    },
    grpc::{
        ffqn::FunctionFqn,
        grpc_client::{
            self, ExecutionSummary,
            list_executions_request::{NewerThan, OlderThan, Pagination, cursor},
        },
    },
};
use chrono::DateTime;
use log::debug;
use std::ops::Deref;
use yew::prelude::*;
use yew_router::prelude::Link;

#[derive(Default, Clone, PartialEq)]
pub enum ExecutionFilter {
    #[default]
    Latest,
    Older {
        cursor: ExecutionsCursor,
        including_cursor: bool,
    },
    Newer {
        cursor: ExecutionsCursor,
        including_cursor: bool,
    },
    Ffqn {
        ffqn: FunctionFqn,
    },
}

impl ExecutionFilter {
    fn get_cursor(&self) -> Option<ExecutionsCursor> {
        match self {
            ExecutionFilter::Older { cursor, .. } | ExecutionFilter::Newer { cursor, .. } => {
                Some(cursor.clone())
            }
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Default)]
enum CursorType {
    #[default]
    CreatedAt,
    ExecutionId,
}

impl ExecutionsCursor {
    fn into_grpc_cursor(self) -> grpc_client::list_executions_request::Cursor {
        match self {
            ExecutionsCursor::ExecutionId(execution_id) => {
                grpc_client::list_executions_request::Cursor {
                    cursor: Some(cursor::Cursor::ExecutionId(execution_id)),
                }
            }
            ExecutionsCursor::CreatedAt(created_at) => {
                grpc_client::list_executions_request::Cursor {
                    cursor: Some(cursor::Cursor::CreatedAt(created_at.into())),
                }
            }
        }
    }

    fn as_type(&self) -> CursorType {
        match self {
            ExecutionsCursor::CreatedAt(_) => CursorType::CreatedAt,
            ExecutionsCursor::ExecutionId(_) => CursorType::ExecutionId,
        }
    }
}

#[derive(Properties, PartialEq)]
pub struct ExecutionListPageProps {
    #[prop_or_default]
    pub filter: ExecutionFilter,
}
#[function_component(ExecutionListPage)]
pub fn execution_list_page(ExecutionListPageProps { filter }: &ExecutionListPageProps) -> Html {
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
                    ExecutionFilter::Latest => (None, None),
                    ExecutionFilter::Ffqn { ffqn } => (Some(ffqn), None),
                    ExecutionFilter::Older {
                        cursor,
                        including_cursor,
                    } => (
                        None,
                        Some(Pagination::OlderThan(OlderThan {
                            cursor: Some(cursor.into_grpc_cursor()),
                            length: page_size,
                            including_cursor,
                        })),
                    ),
                    ExecutionFilter::Newer {
                        cursor,
                        including_cursor,
                    } => (
                        None,
                        Some(Pagination::NewerThan(NewerThan {
                            cursor: Some(cursor.into_grpc_cursor()),
                            length: page_size,
                            including_cursor,
                        })),
                    ),
                };
                debug!("<ExecutionListPage /> pagination {pagination:?}");
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

        let to_executions_cursor = |execution: &ExecutionSummary| match filter
            .get_cursor()
            .map(|c| c.as_type())
            .unwrap_or_default()
        {
            CursorType::CreatedAt => ExecutionsCursor::CreatedAt(DateTime::from(
                execution
                    .created_at
                    .expect("`created_at` is sent by the server"),
            )),
            CursorType::ExecutionId => ExecutionsCursor::ExecutionId(
                execution
                    .execution_id
                    .clone()
                    .expect("`execution_id` is sent by the server"),
            ),
        };

        let cursor_later = response.executions.first().map(to_executions_cursor);
        let cursor_prev = response.executions.last().map(to_executions_cursor);

        html! {<>
            <h3>{"Executions"}</h3>
            if let ExecutionFilter::Ffqn{ffqn} = filter {
                <h4>{format!("Filtered by function: {ffqn}")}</h4>
                <p><Link<Route> to={Route::ExecutionSubmit { ffqn: ffqn.clone() }}>{"Submit new execution"}</Link<Route>></p>
                <p><Link<Route> to={Route::ExecutionList}>{"Remove filter"}</Link<Route>></p>
            }
            <ComponentTree config={ComponentTreeConfig::ExecutionListFiltering} />
            <table class="execution_list">
                <tr><th>{"Execution ID"}</th><th>{"Function"}</th><th>{"Status"}</th></tr>
                { rows }
            </table>
            <div class="pagination">
            <Link<Route> to={Route::ExecutionList}>
                {"<< Latest"}
            </Link<Route>>
            if let (Some(cursor_later), Some(cursor_prev)) = (cursor_later, cursor_prev) {
                <Link<Route> to={Route::ExecutionListNewer { cursor: cursor_later }}>
                    {"< Next"}
                </Link<Route>>
                <Link<Route> to={Route::ExecutionListOlder { cursor: cursor_prev }}>
                    {"Prev >"}
                </Link<Route>>
            // If no results are shown, we neeed to go back including the cursor.
            } else if let ExecutionFilter::Newer { cursor, including_cursor: false } = filter {
                <Link<Route> to={Route::ExecutionListOlderIncluding { cursor: cursor.clone() }}>
                    {"Prev >"}
                </Link<Route>>
            } else if let ExecutionFilter::Older { cursor, including_cursor: false } = filter {
                <Link<Route> to={Route::ExecutionListNewerIncluding { cursor: cursor.clone() }}>
                    {"< Next"}
                </Link<Route>>
            }
            </div>
        </>}
    } else {
        html! {
            <p>{"Loading"}</p>
        }
    }
}
