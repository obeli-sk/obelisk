use crate::{
    app::{AppState, Route},
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
use yew_router::hooks::use_navigator;

#[derive(Properties, PartialEq)]
pub struct ComponentListPageProps {
    #[prop_or_default]
    pub component_id: Option<ComponentId>,
}

#[function_component(ComponentListPage)]
pub fn component_list_page(
    ComponentListPageProps { component_id }: &ComponentListPageProps,
) -> Html {
    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");
    let components = app_state.components;

    let wit_state = use_state(|| None);
    // Fetch GetWit
    use_effect_with(component_id.clone(), {
        let wit_state = wit_state.clone();
        move |component_id| {
            if let Some(component_id) = component_id {
                let component_id = component_id.clone();
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

    let component_detail = component_id.as_ref()
        .and_then(|id| components.get(id))
        .map(|component| {
            let component_type = ComponentType::try_from(component.r#type).unwrap();
            let exports =
                map_interfaces_to_fn_details(&component.exports, InterfaceFilter::WithExtensions);

            let render_exported_ifc_with_fns = |ifc_fqn: &IfcFqn, fn_details: &[FunctionDetail] | {
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
                        <h4>
                            {format!("{}:{}/", ifc_fqn.pkg_fqn.namespace, ifc_fqn.pkg_fqn.package_name)}
                            <span class="highlight">
                                {&ifc_fqn.ifc_name}
                            </span>
                            if let Some(version) = &ifc_fqn.pkg_fqn.version {
                                {format!("@{version}")}
                            }
                        </h4>
                        <ul>
                            {submittable_fn_details}
                        </ul>
                    </section>
                }
            };

            let submittable_ifcs_fns = exports
                .iter()
                .filter(|(_, fn_details)| fn_details.iter().any(|f_d| f_d.submittable))
                .map(|(ifc_fqn, fn_details)| render_exported_ifc_with_fns(ifc_fqn, fn_details))
                .collect::<Vec<_>>();

            // imports:
            let imports =
                map_interfaces_to_fn_details(&component.imports, InterfaceFilter::WithExtensions);
            let imports: Vec<_> = imports.keys().map(|ifc| html!{ <>
                <h4>{ifc.to_string()}</h4>
            </>}).collect();

            html! { <>
                <h2>{&component.name}<span class="label">{component_type}</span></h2>
                <h3>{"Exported interfaces"}</h3>
                {submittable_ifcs_fns}
                <h3>{"Imported interfaces"}</h3>
                {imports}
            </>}
        });

    let wit = wit_state
        .deref()
        .as_ref()
        .map(|wit| wit_highlighter::print_all(wit));

    let navigator = use_navigator().unwrap();
    let on_component_selected =
        Callback::from(move |component_id| navigator.push(&Route::Component { component_id }));

    html! {<>
        <header>
            <h1>{"Components"}</h1>
        </header>

        <section class="component-selection">
            <ComponentTree config={ComponentTreeConfig::ComponentsOnly {
                on_component_selected
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
