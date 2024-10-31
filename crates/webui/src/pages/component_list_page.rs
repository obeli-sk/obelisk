use crate::{components::component_list::ComponentTree, grpc_client};
use log::debug;
use std::ops::Deref;
use yew::prelude::*;

#[function_component(ComponentListPage)]
pub fn component_list_page() -> Html {
    let extensions_state = use_state(|| false);
    let components_state = use_state(|| None);

    {
        // Make gRPC request to `list_components`, dependent on `extensions_state`.
        let components_state = components_state.clone();
        let extensions_state = extensions_state.clone();
        use_effect_with(extensions_state.clone(), move |_| {
            let components_state = components_state.clone();
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
                components_state.set(Some(response.components));
            });
            || ()
        });
    }
    // Save flipped `extensions_state`.
    let on_extensions_change = {
        let extensions_state = extensions_state.clone();
        Callback::from(move |event: MouseEvent| {
            event.prevent_default();
            extensions_state.set(!*extensions_state);
        })
    };

    debug!("Rendering component_list");

    html! {<>
        <h1>{ "Obelisk WebUI" }</h1>
        <div>
            <h3>{"Components"}</h3>
            <p>
                <input type="checkbox" checked={*extensions_state} onclick={&on_extensions_change} />
                <label onclick={&on_extensions_change}> { "Show function extensions" }</label>
            </p>
            if let Some(components) = components_state.deref() {
                <ComponentTree components={components.clone()}/>
            }
        </div>
    </>}
}
