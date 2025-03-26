use hashbrown::HashMap;
use log::debug;
use std::rc::Rc;
use webui::{
    app::{App, AppProps},
    grpc::{
        function_detail::{map_interfaces_to_fn_details, InterfaceFilter},
        grpc_client,
        ifc_fqn::IfcFqn,
    },
};

fn main() {
    init_logging();
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
        let components_by_id: HashMap<_, _> = response
            .components
            .into_iter()
            .map(|component| {
                (
                    component
                        .component_id
                        .clone()
                        .expect("`component_id` is sent"),
                    Rc::new(component),
                )
            })
            .collect();

        let comopnents_by_exported_ifc: HashMap<IfcFqn, Rc<grpc_client::Component>> =
            components_by_id
                .values()
                .flat_map(|component| {
                    map_interfaces_to_fn_details(&component.exports, InterfaceFilter::default())
                        .keys()
                        .map(|ifc| (ifc.clone(), component.clone()))
                        .collect::<Vec<_>>()
                })
                .collect();

        yew::Renderer::<App>::with_props(AppProps {
            components_by_id,
            comopnents_by_exported_ifc,
        })
        .render();
    });
}

fn init_logging() {
    use log::Level;
    use wasm_logger::Config;

    // use debug level for debug builds, warn level for production builds.
    #[cfg(debug_assertions)]
    let level = Level::Trace;
    #[cfg(not(debug_assertions))]
    let level = Level::Warn;

    wasm_logger::init(Config::new(level));
}
