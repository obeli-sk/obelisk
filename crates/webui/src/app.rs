use crate::{
    ffqn::FunctionFqn,
    grpc_client,
    pages::{
        component_list_page::ComponentListPage, execution_list_page::ExecutionListPage,
        execution_submit_page::ExecutionSubmitPage, not_found::NotFound,
    },
};
use log::debug;
use std::{ops::Deref, str::FromStr};
use yew::prelude::*;
use yew_router::prelude::*;

#[derive(Clone, PartialEq)]
pub struct AppState {
    pub components: Vec<grpc_client::Component>,
    pub submittable_ffqns_to_details: hashbrown::HashMap<FunctionFqn, grpc_client::FunctionDetails>,
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
    ExecutionSubmit { ffqn: String },
    /// Show paginated table of executions, fiterable by component, interface, ffqn, pending state etc.
    #[at("/execution/list")]
    ExecutionList,
    #[at("/execution/list/ffqn/:ffqn")]
    ExecutionListByFfqn { ffqn: String },
    #[at("/execution/list/execution_id/:execution_id")]
    ExecutionListByExecutionId { execution_id: String },

    /// Show details including pending state, event history
    // #[at("/execution/:id")]
    // ExecutionDetail,
    /// Show WIT schema explorer, allow showing/hiding obelisk extensions.
    // #[at("/wit")]
    // WitExplore,
    #[not_found]
    #[at("/404")]
    NotFound,
}

impl Route {
    pub fn render(route: Route) -> Html {
        match route {
            Route::Home => html! { <ComponentListPage /> },
            Route::ComponentList => html! { <ComponentListPage /> },
            Route::ExecutionSubmit { ffqn } => html! { <ExecutionSubmitPage {ffqn} /> },
            Route::ExecutionList => html! { <ExecutionListPage /> },
            Route::ExecutionListByFfqn { ffqn } => {
                debug!("Parsing ffqn {ffqn}");
                let Ok(ffqn) = FunctionFqn::from_str(&ffqn) else {
                    return html! { <NotFound /> }; // TODO: Error page
                };
                html! { <ExecutionListPage {ffqn} /> }
            }
            Route::ExecutionListByExecutionId { execution_id } => {
                let execution_id = grpc_client::ExecutionId { id: execution_id };
                html! { <ExecutionListPage {execution_id} /> }
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
            <BrowserRouter>
                <Switch<Route> render={Route::render} />
            </BrowserRouter>
        </ContextProvider<AppState>>
    }
}
