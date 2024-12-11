use crate::pages::wit_printer::WitPrinter;
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
        grpc_client::{self, ComponentType, FunctionDetail},
        ifc_fqn::IfcFqn,
    },
};
use indexmap::IndexMap;
use std::{ops::Deref, path::PathBuf};
use wit_parser::{Resolve, UnresolvedPackageGroup};
use yew::prelude::*;

#[function_component(ComponentListPage)]
pub fn component_list_page() -> Html {
    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");
    let components: Vec<_> = app_state.components;
    let components_with_idx = components
        .clone()
        .into_iter()
        .enumerate()
        .collect::<IndexMap<_, _>>();
    let selected_component_idx_state: UseStateHandle<Option<usize>> = use_state(|| None);
    let wit_state = use_state(|| None);
    // Fetch GetWit
    use_effect_with(*selected_component_idx_state.deref(), {
        let components_with_idx = components_with_idx.clone();
        let wit_state = wit_state.clone();
        move |selected_component_idx| {
            if let Some(selected_component_idx) = selected_component_idx {
                let component_id = components_with_idx
                    .get(selected_component_idx)
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

            let render_ifc_with_fns = |ifc_fqn: &IfcFqn, fn_details: &[FunctionDetail] | {
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
                        <h3>
                            {format!("{}:{}/", ifc_fqn.namespace, ifc_fqn.package_name)}
                            <span class="highlight">
                                {&ifc_fqn.ifc_name}
                            </span>
                            if let Some(version) = &ifc_fqn.version {
                                {format!("@{version}")}
                            }
                        </h3>
                        <ul>
                            {fn_details}
                        </ul>
                    </section>
                }
            };

            let submittable_ifcs_fns = exports.iter()
                .filter(|(_, fn_details)| fn_details.iter().any(|f_d| f_d.submittable))
                .map(|(ifc_fqn, fn_details)| {
                render_ifc_with_fns(ifc_fqn, fn_details)
            }).collect::<Vec<_>>();


            html! { <>

                <section class="world-definition">
                    <h2>{&component.name}<span class="label">{component_type}</span></h2>
                    <div class="code-block">
                        <p>{"Exports"}</p>
                        <ul>
                            {exports.keys().map(render_ifc_li).collect::<Vec<_>>()}
                        </ul>
                        <p>{"Imports"}</p>
                        <ul>
                            {imports.keys().map(render_ifc_li).collect::<Vec<_>>()}
                        </ul>

                    </div>
                </section>

                {submittable_ifcs_fns}
            </>}
        });

    let wit = wit_state.deref().as_ref().map(|wit| {
        let group = UnresolvedPackageGroup::parse(PathBuf::new(), wit).expect("FIXME");
        let mut resolve = Resolve::new();
        let main_id = resolve.push_group(group).expect("FIXME");
        let ids = resolve
            .packages
            .iter()
            .map(|(id, _)| id)
            .filter(|id| *id != main_id) // TODO Remove the first package ..; world {} instead.
            .collect::<Vec<_>>();
        let wit = WitPrinter::default()
            .print(&resolve, main_id, &ids)
            .expect("FIXME")
            .to_string();

        Html::from_html_unchecked(wit.into())
    });

    html! {<>
        <div class="container">
            <header>
                <h1>{"Components"}</h1>
            </header>

            <section class="component-selection">
                <ComponentTree components={components_with_idx} config={ComponentTreeConfig::ComponentsOnly {
                    selected_component_idx_state: selected_component_idx_state.clone()
                }
                } />
            </section>

            { component_detail }

            if let Some(wit) = wit {
                <h3>{"WIT"}</h3>
                <div class="code-block">
                    <pre>
                    { wit }
                    </pre>
                </div>
            }
        </div>

    </>}
}
