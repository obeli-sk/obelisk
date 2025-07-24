use crate::{
    app::{AppState, Route},
    components::{
        code::code_block::CodeBlock, execution_submit::ExecutionSubmitForm,
        ffqn_with_links::FfqnWithLinks,
    },
    grpc::{ffqn::FunctionFqn, grpc_client},
    util::wit_highlighter,
};
use std::ops::Deref;
use yew::prelude::*;
use yew_router::prelude::Link;
use yewprint::Icon;

#[derive(Properties, PartialEq)]
pub struct ExecutionSubmitPageProps {
    pub ffqn: FunctionFqn,
}
#[function_component(ExecutionSubmitPage)]
pub fn execution_submit_page(ExecutionSubmitPageProps { ffqn }: &ExecutionSubmitPageProps) -> Html {
    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");

    let provided_by = app_state.comopnents_by_exported_ifc.get(&ffqn.ifc_fqn);

    let (function_detail, component_id) = match app_state.ffqns_to_details.get(ffqn) {
        Some((detail, id)) => (detail, id.clone()),
        None => {
            return html! {
                <p>{"function not found"}</p>
            };
        }
    };

    let wit_state: UseStateHandle<Option<String>> = use_state(|| None);
    // Fetch GetWit
    use_effect_with(ffqn.clone(), {
        let wit_state = wit_state.clone();
        let component_id = component_id.clone();
        move |_ffqn| {
            wasm_bindgen_futures::spawn_local(async move {
                let base_url = "/api";
                let mut fn_client =
                    grpc_client::function_repository_client::FunctionRepositoryClient::new(
                        tonic_web_wasm_client::Client::new(base_url.to_string()),
                    );
                let wit = fn_client
                    .get_wit(grpc_client::GetWitRequest {
                        component_id: Some(component_id.clone()),
                    })
                    .await
                    .unwrap()
                    .into_inner()
                    .content;
                wit_state.set(Some(wit));
            });
        }
    });

    let wit = wit_state
        .deref()
        .as_ref()
        .map(|wit| wit_highlighter::print_interface_with_single_fn(wit, ffqn));

    html! {<>
        <header>
            <h1>{"Execution submit"}</h1>
            <h2>
                <FfqnWithLinks ffqn={ffqn.clone()} fully_qualified={true} hide_submit={true}  />
            </h2>
            if let Some(found) = provided_by {
                <h3>
                    {"Provided by "}
                    <Link<Route> to={Route::Component { component_id: found.component_id.clone().expect("`component_id` is sent") } }>
                        <Icon icon = { found.as_type().as_icon() }/>
                        {" "}
                        {&found.name}
                    </Link<Route>>

                </h3>
            }
        </header>

        <ExecutionSubmitForm function_detail={function_detail.clone()} />
        if let Some(Ok(wit)) = wit {
            <h3>{"WIT"}</h3>
            <CodeBlock source={wit.clone()} />
        }
    </>}
}
