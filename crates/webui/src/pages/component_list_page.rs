use crate::{
    app::AppState,
    components::component_tree::{ComponentTree, ComponentTreeConfig},
    grpc::{
        function_detail::{map_interfaces_to_fn_details, InterfaceFilter},
        grpc_client::ComponentType,
        ifc_fqn::IfcFqn,
    },
};
use std::ops::Deref;
use yew::prelude::*;

#[function_component(ComponentListPage)]
pub fn component_list_page() -> Html {
    let extensions_state = use_state(|| InterfaceFilter::WithoutExtensions);
    // Save flipped `extensions_state`.
    let on_extensions_change = {
        let extensions_state = extensions_state.clone();
        Callback::from(move |event: MouseEvent| {
            event.prevent_default();
            extensions_state.set(extensions_state.deref().flip());
        })
    };
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
                map_interfaces_to_fn_details(&component.exports, *extensions_state.deref());
            let imports =
                map_interfaces_to_fn_details(&component.imports, InterfaceFilter::WithExtensions);
            let render_ifc = |ifc_fqn: &IfcFqn| {
                html! {
                    <li>
                        {format!("{}:{}/",ifc_fqn.namespace, ifc_fqn.package_name)}
                        <span>{&ifc_fqn.ifc_name}</span>
                    </li>
                }
            };
            let exports: Vec<_> = exports.keys().map(|ifc_fqn| render_ifc(ifc_fqn)).collect();
            let imports: Vec<_> = imports.keys().map(|ifc_fqn| render_ifc(ifc_fqn)).collect();
            html! { <>

                <section class="world-definition">
                    <h2>{&component.name}<span class="label">{component_type}</span></h2>
                    <div class="code-block">
                        <p><strong>{"Exports"}</strong></p>
                        <ul>
                            {exports}
                        </ul>
                        <p><strong>{"Imports"}</strong></p>
                        <ul>
                            {imports}
                        </ul>

                    </div>
                </section>



            </>}
        });

    /*
    <section class="types-interface">
        <h2><span class="label">{"types"}</span>{"interface"}</h2>
        <p>{"
            This interface defines all of the types and methods for
            implementing HTTP Requests and Responses, both incoming and
            outgoing, as well as their headers, trailers, and bodies.
            "}
        </p>
        <h3>{"Imported Types"}</h3>
        <ul>
            <li>{"wasi:clocks/monotonic-clock."}<span>{"duration"}</span></li>
            <li>{"wasi:io/streams."}<span>{"input-stream"}</span></li>
        </ul>
    </section>
    */
    let components = components
        .clone()
        .into_iter()
        .enumerate()
        .collect::<Vec<_>>();
    html! {<>
        <div class="container">
            <header>
                <h1>{"Components"}</h1>
            </header>

            <section class="component-selection">
                <ComponentTree {components} config={ComponentTreeConfig::ComponentsOnly {
                    selected_component_idx_state: selected_component_idx_state.clone()
                }
                } />
                <div>
                    <input type="checkbox" checked={extensions_state.deref().is_with_extensions()} onclick={&on_extensions_change} />
                    <label onclick={&on_extensions_change}> { "Show -obelisk-ext packages in exports" }</label>
                </div>
            </section>

            { component_detail }
        </div>

    </>}
}
