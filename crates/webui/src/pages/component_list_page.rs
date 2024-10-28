use std::ops::Deref;

use crate::{
    components::{component_detail::ComponentDetail, component_list::ComponentList},
    grpc_client,
};
use log::debug;
use yew::prelude::*;

#[function_component(ComponentListPage)]
pub fn component_list_page() -> Html {
    let components_state = use_state(Vec::new);
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
                debug!("Got gRPC ListComponentsResponse: {:?}", response.components);
                components.set(response.components);
            });
            || ()
        });
    }

    let selected_component_state = use_state(|| None);
    let on_component_select = {
        let selected_component = selected_component_state.clone();
        Callback::from(move |component: grpc_client::Component| {
            selected_component.set(Some(component))
        })
    };
    let details = selected_component_state.as_ref().map(|component| {
        html! {
            <ComponentDetail component={component.clone()} />
        }
    });

    html! {
        <>
            <h1>{ "Obelisk WebUI" }</h1>
            <div>
                <h3>{"Components"}</h3>
                <ComponentList components={components_state.deref().clone()} on_click={on_component_select} />
            </div>
            { for details }
        </>
    }
}
