use crate::{
    grpc_client,
    pages::{component_list_page::ComponentListPage, not_found::NotFound},
};
use log::debug;
use std::ops::Deref;
use yew::prelude::*;
use yew_router::prelude::*;

#[derive(Clone, PartialEq, Default)]
pub struct AppState {
    pub components: Option<Vec<grpc_client::Component>>,
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
    let app_state = use_state(|| AppState::default());
    {
        // Make gRPC request to `list_components`.
        let app_state = app_state.clone();
        use_effect_with((), move |_| {
            let app_state = app_state.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let base_url = "/api";
                let mut fn_repo_client =
                    grpc_client::function_repository_client::FunctionRepositoryClient::new(
                        tonic_web_wasm_client::Client::new(base_url.to_string()),
                    );
                let mut response = fn_repo_client
                    .list_components(grpc_client::ListComponentsRequest {
                        extensions: true,
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .into_inner();
                debug!("Got gRPC ListComponentsResponse");
                response.components.sort_by(|a, b| a.name.cmp(&b.name));
                app_state.set(AppState {
                    components: Some(response.components),
                });
            });
            || ()
        });
    }

    html! {
         <ContextProvider<AppState> context={app_state.deref().clone()}>
            <BrowserRouter>
                <Switch<Route> render={Route::render} />
            </BrowserRouter>
        </ContextProvider<AppState>>
    }
}
