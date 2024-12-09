use crate::{
    app::AppState,
    components::{
        component_tree::{ComponentTree, ComponentTreeConfig},
        ffqn_with_links::FfqnWithLinks,
        function_signature::FunctionSignature,
    },
    grpc::{
        ffqn::FunctionFqn,
        function_detail::{map_interfaces_to_fn_details, InterfaceFilter},
        grpc_client::{ComponentType, FunctionDetail},
        ifc_fqn::IfcFqn,
    },
};
use std::ops::Deref;
use yew::prelude::*;

#[function_component(ComponentListPage)]
pub fn component_list_page() -> Html {
    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");
    let components: Vec<_> = app_state.components;
    let selected_component_idx_state: UseStateHandle<Option<usize>> = use_state(|| None);
    let component_detail = selected_component_idx_state
        .deref()
        .and_then(|idx| components.get(idx))
        .map(|component| {
            let component_type = ComponentType::try_from(component.r#type).unwrap();
            let exports =
                map_interfaces_to_fn_details(&component.exports, InterfaceFilter::WithExtensions);
            let imports =
                map_interfaces_to_fn_details(&component.imports, InterfaceFilter::WithExtensions);
            let render_ifc_li = |ifc_fqn: &IfcFqn| {
                html! {
                    <li>
                        {format!("{}:{}/",ifc_fqn.namespace, ifc_fqn.package_name)}
                        <span>{&ifc_fqn.ifc_name}</span>
                    </li>
                }
            };

            let render_ifc_with_fns = |ifc_fqn: &IfcFqn, fn_details: &[FunctionDetail], is_import: bool | {
                let render_fn_detail = |fn_detail: &FunctionDetail| {
                    html! {
                        <li>
                            if fn_detail.submittable {
                                <FfqnWithLinks ffqn = {FunctionFqn::from_fn_detail(fn_detail)} />
                            } else {
                                {fn_detail.function_name.as_ref().map(|f| &f.function_name)}
                            }
                            {": "}
                            <span>
                                <FunctionSignature params = {fn_detail.params.clone()} return_type={fn_detail.return_type.clone()} />
                            </span>
                        </li>
                    }
                };

                let fn_details = fn_details.iter().map(render_fn_detail).collect::<Vec<_>>();

                html! {
                    <section class="types-interface">
                        <h2>
                            {ifc_fqn}
                            <span class="label">{"Export"}</span>
                            if is_import {
                                <span class="label">{"Import"}</span>
                            }
                        </h2>
                        <ul>
                            {fn_details}
                        </ul>
                    </section>
                }
            };

            let submittable_ifcs_fns = exports.iter()
                .filter(|(_, fn_details)| fn_details.iter().any(|f_d| f_d.submittable))
                .map(|(ifc_fqn, fn_details)| {
                render_ifc_with_fns(ifc_fqn, fn_details, imports.contains_key(ifc_fqn))
            }).collect::<Vec<_>>();


            html! { <>

                <section class="world-definition">
                    <h2>{&component.name}<span class="label">{component_type}</span></h2>
                    <div class="code-block">
                        <p><strong>{"Exports"}</strong></p>
                        <ul>
                            {exports.keys().map(render_ifc_li).collect::<Vec<_>>()}
                        </ul>
                        <p><strong>{"Imports"}</strong></p>
                        <ul>
                            {imports.keys().map(render_ifc_li).collect::<Vec<_>>()}
                        </ul>

                    </div>
                </section>

                {submittable_ifcs_fns}
            </>}
        });

    html! {<>
        <div class="container">
            <header>
                <h1>{"Components"}</h1>
            </header>

            <section class="component-selection">
                <ComponentTree components={components.into_iter().enumerate().collect::<Vec<_>>()} config={ComponentTreeConfig::ComponentsOnly {
                    selected_component_idx_state: selected_component_idx_state.clone()
                }
                } />
            </section>

            { component_detail }
        </div>

    </>}
}
