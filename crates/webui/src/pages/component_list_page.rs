use std::ops::Deref;

use crate::{
    components::{component_detail::ComponentDetail, component_list::ComponentList},
    grpc_client,
};
use log::debug;
use yew::prelude::*;

#[function_component(ComponentListPage)]
pub fn component_list_page() -> Html {
    let extensions_state = use_state(|| false);
    let components_state = use_state(Vec::new);
    let selected_component_id_state = use_state(|| None);

    {
        // Make gRPC request to `list_components`, dependent on `extensions_state`.
        let components = components_state.clone();
        let extensions_state = extensions_state.clone();
        use_effect_with(extensions_state.clone(), move |_| {
            let components = components.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let base_url = "/api";
                let mut fn_repo_client =
                    grpc_client::function_repository_client::FunctionRepositoryClient::new(
                        tonic_web_wasm_client::Client::new(base_url.to_string()),
                    );
                debug!(
                    "Requesting ListComponents with extensions={}",
                    *extensions_state
                );
                let response = fn_repo_client
                    .list_components(grpc_client::ListComponentsRequest {
                        extensions: *extensions_state,
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .into_inner();
                debug!("Got gRPC ListComponentsResponse");
                components.set(response.components);
            });
            || ()
        });
    }
    // Save component's `ConfigId` into `selected_component_id_state`.
    let on_component_select = {
        let selected_component_id_state = selected_component_id_state.clone();
        Callback::from(move |component: grpc_client::Component| {
            selected_component_id_state.set(Some(component.config_id.expect("set by server")))
        })
    };
    // Save flipped `extensions_state`.
    let on_extensions_change = {
        let extensions_state = extensions_state.clone();
        Callback::from(move |_| {
            extensions_state.set(!*extensions_state);
        })
    };

    html! {<>
        <h1>{ "Obelisk WebUI" }</h1>
        <div>
            <h3>{"Components"}</h3>
            <p>
                <input type="checkbox" checked={*extensions_state} onclick={&on_extensions_change} />
                <label onclick={&on_extensions_change}> { "Show function extensions" }</label>
            </p>
            <ComponentList components={components_state.deref().clone()} on_click={on_component_select} />
        </div>
        if let Some(selected_config_id) = selected_component_id_state.as_ref() { // Some component is selected
            if let Some(component) = components_state.deref().into_iter().find(|c| c.config_id.as_ref().expect("set by server") == selected_config_id) { // and it is found in `components_state`
                <ComponentDetail component={component.clone()} />
            }
        }
    </>}
}
