use log::debug;
use webui::{
    app::{App, AppProps},
    grpc_client,
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
        yew::Renderer::<App>::with_props(AppProps {
            components: response.components,
        })
        .render();
    });
}

fn init_logging() {
    use log::Level;
    use wasm_logger::Config;

    // use debug level for debug builds, warn level for production builds.
    #[cfg(debug_assertions)]
    let level = Level::Debug;
    #[cfg(not(debug_assertions))]
    let level = Level::Warn;

    wasm_logger::init(Config::new(level));
}
