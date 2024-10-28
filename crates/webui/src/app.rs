use crate::pages::{component_list_page::ComponentListPage, not_found::NotFound};
use yew::prelude::*;
use yew_router::prelude::*;

#[derive(Clone, Routable, PartialEq)]
pub enum Route {
    /// Dashboard showing stats like number of components, executions grouped by their pending state. Server stats like version, uptime, etc.
    #[at("/")]
    Home,
    /// List all components (workflows, activities, webhooks), show their exports and possibly imports. Allow filtering by name. Show redirect to execution submission.
    #[at("/component/list")]
    ComponentList,
    /// Show the parameters inputs with their WIT schemas. Allow submitting new execution.
    #[at("/execution/submit")]
    ExecutionSubmit,
    /// Show details including pending state, event history
    #[at("/execution/detail")]
    ExecutionDetail,
    /// Show paginated table of executions, fiterable by component, interface, ffqn, pending state etc.
    #[at("/execution/list")]
    ExecutionList,
    /// Show WIT schema explorer, allow showing/hiding obelisk extensions.
    #[at("/wit")]
    WitExplore,
    #[not_found]
    #[at("/404")]
    NotFound,
}

impl Route {
    pub fn render(route: Route) -> Html {
        match route {
            Route::Home => {
                html! { <ComponentListPage /> }
            }
            Route::NotFound => html! { <NotFound /> },
            _ => todo!(),
        }
    }
}

#[function_component(App)]
pub fn app() -> Html {
    html! {
        <BrowserRouter>
            <Switch<Route> render={Route::render} />
        </BrowserRouter>
    }
}
