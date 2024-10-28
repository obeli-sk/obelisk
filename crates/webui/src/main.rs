use yew::prelude::*;

pub(crate) mod grpc {
    tonic::include_proto!("obelisk");
}

#[derive(Properties, PartialEq)]
struct ComponentListProps {
    components: Vec<grpc::Component>,
    on_click: Callback<grpc::Component>,
}
#[function_component(ComponentList)]
fn component_list(
    ComponentListProps {
        components,
        on_click,
    }: &ComponentListProps,
) -> Html {
    let on_click = on_click.clone();
    components
        .iter()
        .map(|component| {
            let on_select = {
                let on_click = on_click.clone();
                let component = component.clone();
                Callback::from(move |_| on_click.emit(component.clone()))
            };
            html! {
                    <p key={component.config_id.as_ref().unwrap().id.as_str()}
                        onclick={on_select}>{format!("{}: {}", component.r#type, component.name)}</p>
            }
        })
        .collect()
}

#[derive(Properties, PartialEq)]
struct ComponentDetailsProps {
    component: grpc::Component,
}
#[function_component(ComponentDetails)]
fn component_details(ComponentDetailsProps { component }: &ComponentDetailsProps) -> Html {
    let id = &component.config_id.as_ref().unwrap().id;
    html! {
        <div>
            <h3>{ id }</h3>
            <h4>{"Exports"}</h4>
            <FunctionList functions={component.exports.clone()}  />
            if !component.imports.is_empty() {
                <h4>{"Imports"}</h4>
                <FunctionList functions={component.imports.clone()}  />
            }
        </div>
    }
}

#[derive(Properties, PartialEq)]
struct FunctionListProps {
    functions: Vec<grpc::FunctionDetails>,
}
#[function_component(FunctionList)]
fn function_list(FunctionListProps { functions }: &FunctionListProps) -> Html {
    functions
        .iter()
        .map(|function| {
            let fun = function.function.as_ref().unwrap();
            html! {
                <p>{format!("{}.{}", fun.interface_name, fun.function_name)}</p> // FIXME: FFQN should not have to be constructed.
            }
        })
        .collect()
}

// trunk serve --proxy-backend=http://127.0.0.1:5005 --proxy-rewrite=/api/
#[function_component(App)]
fn app() -> Html {
    let components_state = use_state(std::vec::Vec::new);
    {
        let components = components_state.clone();
        use_effect_with((), move |_| {
            let components = components.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let base_url = "/api";
                let mut fn_repo_client =
                    grpc::function_repository_client::FunctionRepositoryClient::new(
                        tonic_web_wasm_client::Client::new(base_url.to_string()),
                    );
                let response = fn_repo_client
                    .list_components(grpc::ListComponentsRequest {
                        extensions: false,
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .into_inner();
                components.set(response.components);
            });
            || ()
        });
    }

    let selected_component_state = use_state(|| None);
    let on_component_select = {
        let selected_video = selected_component_state.clone();
        Callback::from(move |component: grpc::Component| selected_video.set(Some(component)))
    };
    let details = selected_component_state.as_ref().map(|component| {
        html! {
            <ComponentDetails component={component.clone()} />
        }
    });
    html! {
        <>
            <h1>{ "Obelisk Explorer" }</h1>
            <div>
                <h3>{"Components"}</h3>
                <ComponentList components={(*components_state).clone()} on_click={on_component_select} />
            </div>
            { for details }
        </>
    }
}

fn main() {
    init_logging();
    yew::Renderer::<App>::new().render();
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
