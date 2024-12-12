use crate::{
    grpc::{
        ffqn::FunctionFqn,
        grpc_client::{self, ExecutionId},
    },
    pages::{
        component_list_page::ComponentListPage,
        execution_detail_page::ExecutionDetailPage,
        execution_list_page::{ExecutionFilter, ExecutionListPage},
        execution_submit_page::ExecutionSubmitPage,
        not_found::NotFound,
    },
};
use chrono::{DateTime, Utc};
use std::{ops::Deref, str::FromStr};
use yew::prelude::*;
use yew_router::prelude::*;

#[derive(Clone, PartialEq)]
pub struct AppState {
    pub components: Vec<grpc_client::Component>, // TODO: Rc<[Component]>
    pub submittable_ffqns_to_details: hashbrown::HashMap<FunctionFqn, grpc_client::FunctionDetail>,
}

#[derive(Clone, PartialEq, derive_more::Display)]
pub enum ExecutionsCursor {
    #[display("{_0}")]
    ExecutionId(ExecutionId),
    #[display("C_{_0:?}")]
    CreatedAt(DateTime<Utc>),
}

impl FromStr for ExecutionsCursor {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once("_") {
            Some(("E", rest)) => Ok(ExecutionsCursor::ExecutionId(ExecutionId {
                id: format!("E_{rest}"),
            })),
            Some(("C", date)) => DateTime::from_str(date)
                .map(ExecutionsCursor::CreatedAt)
                .map_err(|_| ()),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Routable, PartialEq)]
pub enum Route {
    /// Dashboard showing stats like number of components, executions grouped by their pending state. Server stats like version, uptime, etc.
    #[at("/")]
    Home,
    /// List all components (workflows, activities, webhooks), show their exports and possibly imports. Allow filtering by name. Show redirect to execution submission.
    #[at("/component/list")]
    ComponentList,
    /// Show the parameters inputs with their WIT schemas. Allow submitting new execution.
    #[at("/execution/submit/:ffqn")]
    ExecutionSubmit { ffqn: FunctionFqn },
    /// Show paginated table of executions, fiterable by component, interface, ffqn, pending state etc.
    #[at("/execution/list")]
    ExecutionList,
    #[at("/execution/list/older/:cursor")]
    ExecutionListOlder { cursor: ExecutionsCursor },
    #[at("/execution/list/older_inc/:cursor")]
    ExecutionListOlderIncluding { cursor: ExecutionsCursor },
    #[at("/execution/list/newer/:cursor")]
    ExecutionListNewer { cursor: ExecutionsCursor },
    #[at("/execution/list/newer_inc/:cursor")]
    ExecutionListNewerIncluding { cursor: ExecutionsCursor },
    #[at("/execution/list/ffqn/:ffqn")]
    ExecutionListByFfqn { ffqn: FunctionFqn },

    /// Show details including pending state, event history
    #[at("/execution/:execution_id")]
    ExecutionDetail {
        execution_id: grpc_client::ExecutionId,
    },
    #[not_found]
    #[at("/404")]
    NotFound,
}

impl Route {
    pub fn render(route: Route) -> Html {
        match route {
            Route::Home | Route::ExecutionList => html! { <ExecutionListPage /> },
            Route::ComponentList => html! { <ComponentListPage /> },
            Route::ExecutionSubmit { ffqn } => html! { <ExecutionSubmitPage {ffqn} /> },
            Route::ExecutionDetail { execution_id } => {
                html! { <ExecutionDetailPage {execution_id} /> }
            }
            Route::ExecutionListOlder { cursor } => {
                html! { <ExecutionListPage filter={ExecutionFilter::Older { cursor, including_cursor: false }} /> }
            }
            Route::ExecutionListOlderIncluding { cursor } => {
                html! { <ExecutionListPage filter={ExecutionFilter::Older { cursor, including_cursor: true }} /> }
            }
            Route::ExecutionListNewer { cursor } => {
                html! { <ExecutionListPage filter={ExecutionFilter::Newer { cursor, including_cursor: false }} /> }
            }
            Route::ExecutionListNewerIncluding { cursor } => {
                html! { <ExecutionListPage filter={ExecutionFilter::Newer { cursor, including_cursor: true }} /> }
            }

            Route::ExecutionListByFfqn { ffqn } => {
                html! { <ExecutionListPage filter={ExecutionFilter::Ffqn { ffqn } } /> }
            }
            Route::NotFound => html! { <NotFound /> },
        }
    }
}

#[derive(PartialEq, Properties)]
pub struct AppProps {
    pub components: Vec<grpc_client::Component>,
}

#[function_component(App)]
pub fn app(AppProps { components }: &AppProps) -> Html {
    let mut submittable_ffqns_to_details = hashbrown::HashMap::new();
    for component in components {
        for exported_fn_detail in component
            .exports
            .iter()
            .filter(|fn_detail| fn_detail.submittable)
        {
            let ffqn = FunctionFqn::from_fn_detail(exported_fn_detail);
            submittable_ffqns_to_details.insert(ffqn, exported_fn_detail.clone());
        }
    }
    let app_state = use_state(|| AppState {
        components: components.clone(),
        submittable_ffqns_to_details,
    });
    html! {
        <ContextProvider<AppState> context={app_state.deref().clone()}>
            <div class="container">
                <BrowserRouter>
                    <nav>
                        <Link<Route> to={Route::ExecutionList }>
                            {"Execution List"}
                        </Link<Route>>
                        <Link<Route> to={Route::ComponentList }>
                            {"Component list"}
                        </Link<Route>>

                    </nav>
                    <Switch<Route> render={Route::render} />
                </BrowserRouter>
            </div>
        </ContextProvider<AppState>>
    }
}
