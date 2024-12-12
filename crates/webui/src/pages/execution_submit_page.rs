use crate::{
    app::AppState,
    components::{
        code_block::CodeBlock, execution_submit::ExecutionSubmitForm,
        ffqn_with_links::FfqnWithLinks, function_signature::FunctionSignature,
    },
    grpc::{ffqn::FunctionFqn, grpc_client},
    util::wit_highlighter,
};
use std::ops::Deref;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ExecutionSubmitPageProps {
    pub ffqn: FunctionFqn,
}
#[function_component(ExecutionSubmitPage)]
pub fn execution_submit_page(ExecutionSubmitPageProps { ffqn }: &ExecutionSubmitPageProps) -> Html {
    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");
    let (maybe_function_detail, maybe_component_id) =
        match app_state.submittable_ffqns_to_details.get(ffqn) {
            Some((detail, id)) => (Some(detail), Some(id.clone())),
            None => (None, None),
        };

    let wit_state: UseStateHandle<Option<String>> = use_state(|| None);
    // Fetch GetWit
    use_effect_with(ffqn.clone(), {
        let wit_state = wit_state.clone();
        let maybe_component_id = maybe_component_id.clone();
        move |_ffqn| {
            if let Some(component_id) = maybe_component_id {
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
        }
    });

    if let Some(function_detail) = maybe_function_detail {
        let wit = wit_state
            .deref()
            .as_ref()
            .map(|wit| wit_highlighter::print_interface(wit, &ffqn.ifc_fqn));

        html! {<>
            <header>
                <h1>{"Execution submit"}</h1>
                <h2><FfqnWithLinks ffqn={ffqn.clone()} fully_qualified={true} hide_submit={true}  />
                </h2>
            </header>

            <h4><FunctionSignature params = {function_detail.params.clone()} return_type = {function_detail.return_type.clone()} /></h4>
            <ExecutionSubmitForm {function_detail} />
            if let Some(Ok(wit)) = wit {
                <h3>{"WIT"}</h3>
                <CodeBlock source={wit.clone()} />
            }
        </>}
    } else {
        html! {
            <p>{"function not found"}</p>
        }
    }
}
