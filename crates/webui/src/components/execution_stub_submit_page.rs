use crate::{
    app::{AppState, Route},
    components::{code::code_block::CodeBlock, ffqn_with_links::FfqnWithLinks},
    grpc::{
        ffqn::FunctionFqn,
        grpc_client::{self, ExecutionId},
    },
    util::wit_highlighter,
};
use log::{debug, error, trace};
use std::ops::Deref;
use val_json::wast_val::WastValWithType;
use web_sys::HtmlInputElement;
use yew::prelude::*;
use yew_router::{hooks::use_navigator, prelude::Link};
use yewprint::Icon;

#[derive(Properties, PartialEq)]
pub struct ExecutionStubResultPageProps {
    pub ffqn: FunctionFqn,
    pub execution_id: ExecutionId,
}
#[function_component(ExecutionStubResultPage)]
pub fn execution_stub_result_page(
    ExecutionStubResultPageProps { ffqn, execution_id }: &ExecutionStubResultPageProps,
) -> Html {
    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");

    let Some(component) = app_state.comopnents_by_exported_ifc.get(&ffqn.ifc_fqn) else {
        return html! {
            <p>{"function not found"}</p>
        };
    };
    let (fn_detail, _) = app_state
        .ffqns_to_details
        .get(ffqn)
        .expect("`ffqns_to_details` and `comopnents_by_exported_ifc` must be consistent, based from `ListComponentsResponse`");

    let component_id = component
        .component_id
        .clone()
        .expect("`component_id` is sent");
    // disable the submit button while a request is inflight
    let request_processing_state = use_state(|| false);
    let input_state = use_state(NodeRef::default);
    let err_state = use_state(|| None);

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

    let on_submit = {
        let request_processing_state = request_processing_state.clone();
        let input_state = input_state.clone();
        let err_state = err_state.clone();
        let ffqn = ffqn.clone();
        let navigator = use_navigator().unwrap();
        let execution_id = execution_id.clone();
        Callback::from(move |e: SubmitEvent| {
            e.prevent_default(); // prevent form submission
            let input = input_state
                .deref()
                .cast::<HtmlInputElement>()
                .unwrap()
                .value();
            match serde_json::from_str::<serde_json::Value>(&input) {
                Ok(_) => {
                    debug!("serde ok")
                }
                Err(serde_err) => {
                    error!("Cannot serialize input - {serde_err:?}");
                    err_state.set(Some(format!("cannot serialize input - {serde_err}")));
                    return;
                }
            };
            debug!("Input: {input:?}");
            {
                err_state.set(None);
                request_processing_state.set(true); // disable the submit button
            }

            wasm_bindgen_futures::spawn_local({
                let input = input.clone();
                let ffqn = ffqn.clone();
                let execution_id = execution_id.clone();
                let err_state = err_state.clone();
                let navigator = navigator.clone();
                let request_processing_state = request_processing_state.clone();
                async move {
                    let base_url = "/api";
                    let mut client =
                        grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                            tonic_web_wasm_client::Client::new(base_url.to_string()),
                        );
                    let response = client
                        .stub(grpc_client::StubRequest {
                            execution_id: Some(execution_id.clone()),
                            return_value: Some(prost_wkt_types::Any {
                                type_url: format!("urn:obelisk:json:retval:{ffqn}"),
                                value: input.into_bytes(),
                            }),
                        })
                        .await;
                    request_processing_state.set(false); // reenable the submit button
                    trace!("Got gRPC {response:?}");
                    match response {
                        Ok(_response) => navigator.push(&Route::ExecutionTrace { execution_id }),
                        Err(err) => {
                            error!("Got error {err:?}");
                            err_state
                                .set(Some(format!("cannot stub the execution result - {err}")));
                        }
                    }
                }
            });
        })
    };

    // Validate on first render
    use_effect_with(input_state.deref().clone(), {
        let err_state = err_state.clone();
        let fn_detail = fn_detail.clone();
        let input_state = input_state.clone();
        move |_| {
            debug!("Validating the form after first render");
            let input = input_state
                .deref()
                .cast::<HtmlInputElement>()
                .unwrap()
                .value();
            if let Err(err) = validate_response(&fn_detail, &input) {
                err_state.set(Some(err));
            } else {
                err_state.set(None);
            }
        }
    });
    let oninput = {
        let input_state = input_state.clone();
        let err_state = err_state.clone();
        let fn_detail = fn_detail.clone();
        move |_| {
            let input = input_state
                .deref()
                .cast::<HtmlInputElement>()
                .unwrap()
                .value();
            if let Err(err) = validate_response(&fn_detail, &input) {
                err_state.set(Some(err));
            } else {
                err_state.set(None);
            }
        }
    };

    let wit = wit_state
        .deref()
        .as_ref()
        .map(|wit| wit_highlighter::print_interface_with_single_fn(wit, ffqn));

    let wit_type = fn_detail
        .return_type
        .as_ref()
        .expect("TODO")
        .wit_type
        .as_str();
    html! {<>
        <header>
            <h1>{"Stub execution result"}</h1>
            <h2>
                <FfqnWithLinks ffqn={ffqn.clone()} fully_qualified={true} hide_submit={true}  />
            </h2>
            <h3>
                {"Provided by "}
                <Link<Route> to={Route::Component { component_id: component_id.clone() } }>
                    <Icon icon = { component.as_type().as_icon() }/>
                    {" "}
                    {&component.name}
                </Link<Route>>
            </h3>
        </header>
        <form id="execution-stub-result-form" onsubmit = {on_submit }>
            <p>
                <label for="input">{wit_type}</label>
                <input id="input" type="text" ref={input_state.deref()} oninput = {oninput}/>
            </p>
            <button type="submit" disabled={*request_processing_state}>
                {"Submit response"}
            </button>
        </form>
        if let Some(err) = err_state.deref() {
            <p style={"color:red"}>{err}</p>
        }
        if let Some(Ok(wit)) = wit {
            <h3>{"WIT"}</h3>
            <CodeBlock source={wit.clone()} />
        }
    </>}
}

fn validate_response(
    function_detail: &grpc_client::FunctionDetail,
    value: &str,
) -> Result<(), String> {
    match serde_json::from_str::<serde_json::Value>(value) {
        Ok(value) => {
            let type_wrapper = function_detail
                .return_type
                .as_ref()
                .expect("TODO")
                .type_wrapper
                .as_str();
            let type_and_value_json = format!("{{\"type\": {type_wrapper}, \"value\": {value}}}");
            match serde_json::from_str::<WastValWithType>(&type_and_value_json) {
                Ok(_) => Ok(()),
                Err(err) => Err(format!("Typecheck error: {err}")),
            }
        }
        Err(err) => Err(format!("Cannot serialize value to JSON: {err}")),
    }
}
