use crate::{
    app::Route,
    grpc::{
        execution_id::ExecutionIdExt,
        ffqn::FunctionFqn,
        grpc_client::{self, ExecutionId},
    },
};
use log::{debug, error, trace, warn};
use std::ops::Deref;
use val_json::wast_val::WastValWithType;
use web_sys::HtmlInputElement;
use yew::prelude::*;
use yew_router::hooks::use_navigator;

#[derive(Debug, Clone, PartialEq)]
struct FormData {
    param_refs: Vec<NodeRef>,
    param_errs: Vec<Option<String>>,
}

impl FormData {
    fn validate(&mut self, function_detail: &grpc_client::FunctionDetail) -> Result<(), String> {
        let mut is_err = false;
        for (idx, param_ref) in self.param_refs.iter().enumerate() {
            let param_value = param_ref.cast::<HtmlInputElement>().unwrap().value();
            debug!("oninput[{idx}] value {param_value}");
            let param_value: serde_json::Value = match serde_json::from_str(&param_value) {
                Ok(v) => v,
                Err(err) => {
                    warn!("oninput[{idx}] - cannot serialize value to JSON - {err:?}");
                    self.param_errs[idx] = Some(format!("Cannot serialize value to JSON: {err}"));
                    is_err = true;
                    continue;
                }
            };
            let type_wrapper = function_detail
                .params
                .get(idx)
                .as_ref()
                .expect("FunctionDetail.params cardinality must match FormData.param_refs")
                .r#type
                .as_ref()
                .expect("`FunctionParameter.type` is sent")
                .type_wrapper
                .as_str();
            let type_and_value_json =
                format!("{{\"type\": {type_wrapper}, \"value\": {param_value}}}");
            match serde_json::from_str::<WastValWithType>(&type_and_value_json) {
                Ok(_) => {
                    self.param_errs[idx] = None;
                }
                Err(err) => {
                    warn!("oninput[{idx}] - typecheck error {err:?}");
                    self.param_errs[idx] = Some(format!("Typecheck error: {err}"));
                    is_err = true;
                }
            }
        }
        if is_err {
            Err("Cannot serialize parameters".to_string())
        } else {
            Ok(())
        }
    }
}

#[derive(Properties, PartialEq)]
pub struct ExecutionSubmitFormProps {
    pub function_detail: grpc_client::FunctionDetail,
}
#[function_component(ExecutionSubmitForm)]
pub fn execution_submit_form(
    ExecutionSubmitFormProps {
        function_detail: fn_detail,
    }: &ExecutionSubmitFormProps,
) -> Html {
    let ffqn = FunctionFqn::from_fn_detail(fn_detail).expect("ffqn should be parseable");

    // disable the submit button while a request is inflight
    let request_processing_state = use_state(|| false);
    // Initialize form state with default values
    let form_data_state = use_state(|| FormData {
        param_refs: std::iter::repeat_with(NodeRef::default)
            .take(fn_detail.params.len())
            .collect(),
        param_errs: std::iter::repeat_n(None, fn_detail.params.len()).collect(),
    });
    let submit_err_state = use_state(|| None);

    use_effect_with(form_data_state.deref().clone(), {
        let submit_err_state = submit_err_state.clone();
        let fn_detail = fn_detail.clone();
        let form_data_state = form_data_state.clone();
        move |form_data| {
            debug!("Validating the form after first render");
            let mut form_data = form_data.clone();
            if let Err(err) = form_data.validate(&fn_detail) {
                log::warn!("Validation failed - {err}");
                submit_err_state.set(Some(err));
            } else {
                submit_err_state.set(None);
            }
            form_data_state.set(form_data);
        }
    });

    let on_submit = {
        let request_processing_state = request_processing_state.clone();
        let form_data_state = form_data_state.clone();
        let submit_err_state = submit_err_state.clone();
        let ffqn = ffqn.clone();
        let navigator = use_navigator().unwrap();
        Callback::from(move |e: SubmitEvent| {
            e.prevent_default(); // prevent form submission
            let params = match form_data_state
                .deref()
                .param_refs
                .iter()
                .map(|param_ref| {
                    let param_value = param_ref.cast::<HtmlInputElement>().unwrap().value();
                    serde_json::from_str(&param_value)
                })
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(params) => params,
                Err(serde_err) => {
                    error!("Cannot serialize parameters - {serde_err:?}");
                    submit_err_state.set(Some("Cannot serialize parameters".to_string()));
                    return;
                }
            };
            debug!("Params: {params:?}");
            {
                submit_err_state.set(None);
                request_processing_state.set(true); // disable the submit button
            }

            wasm_bindgen_futures::spawn_local({
                let params = params.clone();
                let ffqn = ffqn.clone();
                let submit_err_state = submit_err_state.clone();
                let navigator = navigator.clone();
                let request_processing_state = request_processing_state.clone();
                async move {
                    let base_url = "/api";
                    let mut client =
                        grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                            tonic_web_wasm_client::Client::new(base_url.to_string()),
                        );
                    let response = client
                        .submit(grpc_client::SubmitRequest {
                            execution_id: Some(ExecutionId::generate()),
                            params: Some(prost_wkt_types::Any {
                                type_url: format!("urn:obelisk:json:params:{ffqn}"),
                                value: serde_json::Value::Array(params).to_string().into_bytes(),
                            }),
                            function_name: Some(grpc_client::FunctionName::from(ffqn)),
                        })
                        .await;
                    request_processing_state.set(false); // reenable the submit button
                    trace!("Got gRPC {response:?}");
                    match response {
                        Ok(response) => {
                            let execution_id = response
                                .into_inner()
                                .execution_id
                                .expect("SubmitResponse.execution_id is sent by the server");
                            debug!("Submitted as {execution_id}");
                            navigator.push(&Route::ExecutionTrace { execution_id })
                        }
                        Err(err) => {
                            error!("Got error {err:?}");
                            submit_err_state
                                .set(Some(format!("Cannot submit the execution - {err}")));
                        }
                    }
                }
            });
        })
    };

    let params_html: Vec<_> = fn_detail
        .params
        .iter()
        .enumerate()
        .map(|(idx, param)| {
            let ty = param.r#type
                .as_ref()
                .expect("`FunctionParameter.type` is sent")
                ;
            let id = format!("param_{ffqn}_{idx}");

            let on_param_change = {
                let form_data_state = form_data_state.clone();
                let submit_err_state = submit_err_state.clone();
                let fn_detail = fn_detail.clone();
                move || {
                    let mut form_data = form_data_state.deref().clone();
                    if let Err(err) = form_data.validate(&fn_detail) {
                        log::warn!("Validation failed - {err}");
                        submit_err_state.set(Some(err));
                    } else {
                        submit_err_state.set(None);
                    }
                    form_data_state.set(form_data);
                }
            };

            html! {<p>
                <label for={id.clone()}>{ format!("{}: {}", param.name, ty.wit_type) }</label>
                <input id={id} type="text" ref={&form_data_state.param_refs[idx]} oninput = {Callback::from(move |_| { on_param_change()})} />
                if let Some(err) = form_data_state.param_errs.get(idx) {
                    <span style={"color:red"}>{err}</span>
                }
            </p>}
        })
        .collect();

    html! {<>
        <form onsubmit = {on_submit }>
            {for params_html}
            <button type="submit" disabled={*request_processing_state}>
                {"Submit"}
            </button>
        </form>
        if let Some(err) = submit_err_state.deref() {
            <p style={"color:red"}>{err}</p>
        }
    </>}
}
