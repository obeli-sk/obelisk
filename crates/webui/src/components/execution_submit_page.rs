use crate::{
    app::{AppState, Route},
    components::{
        code_block::CodeBlock, execution_submit::ExecutionSubmitForm,
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
    } else {
        html! {
            <p>{"function not found"}</p>
        }
    }
}
