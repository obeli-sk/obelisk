use crate::{app::Route, grpc::ffqn::FunctionFqn, grpc::grpc_client};
use log::{debug, error, trace};
use std::ops::Deref;
use web_sys::HtmlInputElement;
use yew::prelude::*;
use yew_router::hooks::use_navigator;

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

    #[derive(Debug, Clone, PartialEq)]
    struct FormData {
        param_refs: Vec<NodeRef>,
        param_errs: Vec<Option<String>>,
        request_processing: bool,
    }

    // Initialize form state with default values
    let form_data_state = use_state(|| FormData {
        param_refs: std::iter::repeat_with(NodeRef::default)
            .take(fn_detail.params.len())
            .collect(),
        param_errs: std::iter::repeat_n(None, fn_detail.params.len()).collect(),
        request_processing: false, // disable the submit button while a request is inflight
    });

    let submit_err_state = use_state(|| None);

    let on_submit = {
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
                    submit_err_state.set(Some(format!("Cannot serialize parameters")));
                    return;
                }
            };
            debug!("Params: {params:?}");
            {
                submit_err_state.set(None);
                // disable the submit button
                let mut form_data = form_data_state.deref().clone();
                form_data.request_processing = true;
                form_data_state.set(form_data);
            }

            wasm_bindgen_futures::spawn_local({
                let params = params.clone();
                let ffqn = ffqn.clone();
                let form_data_state = form_data_state.clone();
                let submit_err_state = submit_err_state.clone();
                let navigator = navigator.clone();
                async move {
                    let base_url = "/api";
                    let mut client =
                        grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                            tonic_web_wasm_client::Client::new(base_url.to_string()),
                        );
                    let response = client
                        .submit(grpc_client::SubmitRequest {
                            params: Some(prost_wkt_types::Any {
                                type_url: format!("urn:obelisk:json:params:{ffqn}"),
                                value: serde_json::Value::Array(params).to_string().into_bytes(),
                            }),
                            function_name: Some(grpc_client::FunctionName::from(ffqn)),
                        })
                        .await;
                    trace!("Got gRPC {response:?}");
                    match response {
                        Ok(response) => {
                            let execution_id = response
                                .into_inner()
                                .execution_id
                                .expect("SubmitResponse.execution_id is sent by the server");
                            debug!("Submitted as {execution_id}");
                            navigator.push(&Route::ExecutionDetail { execution_id })
                        }
                        Err(err) => {
                            error!("Got error {err:?}");
                            submit_err_state
                                .set(Some(format!("Cannot submit the execution - {err}")));
                            // reenable the submit button
                            let mut form_data = form_data_state.deref().clone();
                            form_data.request_processing = false;
                            form_data_state.set(form_data);
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
            let r#type = param
                .r#type
                .as_ref()
                .and_then(|wit_type| wit_type.wit_type.as_deref())
                .unwrap_or("<unknown_type>");
            let id = format!("param_{ffqn}_{idx}");

            let on_param_change = {
                    let form_data_state = form_data_state.clone();
                    Callback::from(move |_| {
                        let mut form_data = form_data_state.deref().clone();
                        let param_ref = &form_data.param_refs[idx];
                        let param_value = param_ref.cast::<HtmlInputElement>().unwrap().value();
                        // TODO: param validation, including serialization to JSON
                        let param_value: serde_json::Value = match serde_json::from_str(&param_value) {
                            Ok(v) => {
                                form_data.param_errs[idx] = None;
                                form_data_state.set(form_data);
                                v
                            },
                            Err(err) => {
                                form_data.param_errs[idx] = Some(format!("Cannot serialize value to JSON: {err}"));
                                form_data_state.set(form_data);
                                return;
                            }
                        };
                    })
                };

            html! {<p>
                <label for={id.clone()}>{ format!("{}: {}", param.name, r#type) }</label>
                <input id={id} type="text" ref={&form_data_state.param_refs[idx]} oninput = {on_param_change} />
                if let Some(err) = form_data_state.param_errs.get(idx) {
                    <span style={"color:red"}>{err}</span>
                }
            </p>}
        })
        .collect();

    html! {<>
        <form onsubmit = {on_submit }>
            {for params_html}
            <button type="submit" disabled={form_data_state.request_processing}>
                {"Submit"}
            </button>
        </form>
        if let Some(err) = submit_err_state.deref() {
            <p style={"color:red"}>{err}</p>
        }
    </>}
}
