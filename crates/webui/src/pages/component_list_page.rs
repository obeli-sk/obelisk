use crate::{
    app::AppState,
    components::{
        code_block::CodeBlock,
        component_tree::{ComponentTree, ComponentTreeConfig},
        ffqn_with_links::FfqnWithLinks,
        function_signature::FunctionSignature,
    },
    grpc::{
        ffqn::FunctionFqn,
        function_detail::{map_interfaces_to_fn_details, InterfaceFilter},
        grpc_client::{self, ComponentId, ComponentType, FunctionDetail},
        ifc_fqn::IfcFqn,
    },
    util::wit_highlighter,
};
use std::ops::Deref;
use yew::prelude::*;

#[function_component(ComponentListPage)]
pub fn component_list_page() -> Html {
    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");
    let components = app_state.components;

    let selected_component_id_state: UseStateHandle<Option<ComponentId>> = use_state(|| None);
    let wit_state = use_state(|| None);
    // Fetch GetWit
    use_effect_with(selected_component_id_state.deref().clone(), {
        let id_to_component_map = components.clone();
        let wit_state = wit_state.clone();
        move |selected_component_id| {
            if let Some(selected_component_id) = selected_component_id {
                let component_id = id_to_component_map
                    .get(selected_component_id)
                    .expect("`selected_component_idx` must be valid")
                    .component_id
                    .clone()
                    .expect("`component_id` is sent");
                wasm_bindgen_futures::spawn_local(async move {
                    let base_url = "/api";
                    let mut fn_client =
                        grpc_client::function_repository_client::FunctionRepositoryClient::new(
                            tonic_web_wasm_client::Client::new(base_url.to_string()),
                        );
                    let wit = fn_client
                        .get_wit(grpc_client::GetWitRequest {
                            component_id: Some(component_id),
                        })
                        .await
                        .unwrap()
                        .into_inner()
                        .content;
                    wit_state.set(Some(wit));
                });
            } else {
                wit_state.set(None);
            }
        }
    });

    let component_detail = selected_component_id_state
        .deref()
        .as_ref()
        .and_then(|id| components.get(id))
        .map(|component| {
            let component_type = ComponentType::try_from(component.r#type).unwrap();
            let exports =
                map_interfaces_to_fn_details(&component.exports, InterfaceFilter::WithExtensions);

             let render_ifc_with_fns = |ifc_fqn: &IfcFqn, fn_details: &[FunctionDetail] | {
                let submittable_fn_details = fn_details
                    .iter()
                    .filter(|fn_detail| fn_detail.submittable)
                    .map(|fn_detail| {
                        let ffqn = FunctionFqn::from_fn_detail(fn_detail).expect("ffqn should be parseable");
                        html! {
                            <li>
                                <FfqnWithLinks {ffqn} />
                                {": "}
                                <span>
                                    <FunctionSignature params = {fn_detail.params.clone()} return_type={fn_detail.return_type.clone()} />
                                </span>
                            </li>
                        }
                    })
                    .collect::<Vec<_>>();

                html! {
                    <section class="types-interface">
                        <h3>
                            {format!("{}:{}/", ifc_fqn.pkg_fqn.namespace, ifc_fqn.pkg_fqn.package_name)}
                            <span class="highlight">
                                {&ifc_fqn.ifc_name}
                            </span>
                            if let Some(version) = &ifc_fqn.pkg_fqn.version {
                                {format!("@{version}")}
                            }
                        </h3>
                        <ul>
                            {submittable_fn_details}
                        </ul>
                    </section>
                }
             };

            let submittable_ifcs_fns = exports
                .iter()
                .filter(|(_, fn_details)| fn_details.iter().any(|f_d| f_d.submittable))
                .map(|(ifc_fqn, fn_details)| render_ifc_with_fns(ifc_fqn, fn_details))
                .collect::<Vec<_>>();

            html! { <>
                <h2>{&component.name}<span class="label">{component_type}</span></h2>
                {submittable_ifcs_fns}
            </>}
        });

    let wit = wit_state
        .deref()
        .as_ref()
        .map(|wit| wit_highlighter::print_all(wit));

    html! {<>
        <header>
            <h1>{"Components"}</h1>
        </header>

        <section class="component-selection">
            <ComponentTree config={ComponentTreeConfig::ComponentsOnly {
                selected_component_id_state: selected_component_id_state.clone()
            }
            } />
        </section>

        { component_detail }

        if let Some(wit) = wit {
            <h3>{"WIT"}</h3>
            <CodeBlock source={wit} />
        }
    </>}
}
