use yew::prelude::*;
use yew_router::prelude::*;

use crate::{component_detail::ComponentDetail, component_list::ComponentList, grpc_client};

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

// trunk serve --proxy-backend=http://127.0.0.1:5005 --proxy-rewrite=/api/
#[function_component(App)]
pub fn app() -> Html {
    let components_state = use_state(std::vec::Vec::new);
    {
        let components = components_state.clone();
        use_effect_with((), move |_| {
            let components = components.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let base_url = "/api";
                let mut fn_repo_client =
                    grpc_client::function_repository_client::FunctionRepositoryClient::new(
                        tonic_web_wasm_client::Client::new(base_url.to_string()),
                    );
                let response = fn_repo_client
                    .list_components(grpc_client::ListComponentsRequest {
                        extensions: false,
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .into_inner();
                components.set(response.components);
            });
            || ()
        });
    }

    let selected_component_state = use_state(|| None);
    let on_component_select = {
        let selected_video = selected_component_state.clone();
        Callback::from(move |component: grpc_client::Component| selected_video.set(Some(component)))
    };
    let details = selected_component_state.as_ref().map(|component| {
        html! {
            <ComponentDetail component={component.clone()} />
        }
    });
    html! {
        <>
            <h1>{ "Obelisk Explorer" }</h1>
            <div>
                <h3>{"Components"}</h3>
                <ComponentList components={(*components_state).clone()} on_click={on_component_select} />
            </div>
            { for details }
        </>
    }
}
