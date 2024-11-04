use crate::{app::Route, ffqn::FunctionFqn, grpc_client};
use log::{debug, error};
use std::ops::Deref;
use yew::prelude::*;
use yew_router::hooks::use_navigator;

#[derive(Properties, PartialEq)]
pub struct ExecutionSubmitFormProps {
    pub function_detail: grpc_client::FunctionDetails,
}
#[function_component(ExecutionSubmitForm)]
pub fn execution_submit_form(
    ExecutionSubmitFormProps { function_detail }: &ExecutionSubmitFormProps,
) -> Html {
    let ffqn = FunctionFqn::from_fn_detail(function_detail);

    #[derive(Debug, Clone, PartialEq)]
    struct FormData {
        params: Vec<String>,
        request_processing: bool,
    }

    // Initialize form state with default values
    let form_data_handle = use_state(|| FormData {
        params: vec![String::new(); function_detail.params.len()],
        request_processing: false,
    });

    let on_param_change = {
        let form_data_handle = form_data_handle.clone();
        let ffqn = ffqn.clone();
        Callback::from(move |e: InputEvent| {
            let input: web_sys::HtmlInputElement = e.target_unchecked_into();
            let mut form_data = form_data_handle.deref().clone();
            let idx: usize = input
                .id()
                .strip_prefix(&ffqn.to_string()) // TODO: Switch to refs to avoid parsing IDs: https://github.com/yewstack/yew/blob/master/examples/node_refs/src/main.rs
                .expect("input id starts with ffqn")
                .parse()
                .expect("id of input fields is numeric");
            form_data.params[idx] = input.value();
            form_data_handle.set(form_data);
        })
    };

    let on_submit = {
        let form_data_handle = form_data_handle.clone();
        let ffqn = ffqn.clone();
        let navigator = use_navigator().unwrap();
        Callback::from(move |e: SubmitEvent| {
            e.prevent_default(); // prevent form submission
            let params = match form_data_handle
                .deref()
                .params
                .iter()
                .map(|str_param| serde_json::from_str(str_param))
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(params) => params,
                Err(serde_err) => {
                    error!("Cannot serialize parameters - {serde_err:?}");
                    panic!("cannot serialize parameters")
                }
            };
            {
                // disable the submit button
                let mut form_data = form_data_handle.deref().clone();
                form_data.request_processing = true;
                form_data_handle.set(form_data);
            }

            wasm_bindgen_futures::spawn_local({
                let params = params.clone();
                let ffqn = ffqn.clone();
                let form_data_handle = form_data_handle.clone();
                let navigator = navigator.clone();
                async move {
                    let base_url = "/api";
                    let mut scheduler_client = grpc_client::scheduler_client::SchedulerClient::new(
                        tonic_web_wasm_client::Client::new(base_url.to_string()),
                    );
                    let response = scheduler_client
                        .submit(grpc_client::SubmitRequest {
                            params: Some(prost_wkt_types::Any {
                                type_url: format!("urn:obelisk:json:params:{ffqn}"),
                                value: serde_json::Value::Array(params).to_string().into_bytes(),
                            }),
                            function: Some(grpc_client::FunctionName::from(ffqn)),
                            execution_id: None,
                        })
                        .await;
                    debug!("Got gRPC {response:?}");
                    match response {
                        Ok(response) => {
                            let execution_id = response
                                .into_inner()
                                .execution_id
                                .expect("SubmitResponse.execution_id is sent by the server")
                                .id;
                            debug!("Submitted as {execution_id}");
                            navigator.push(&Route::ExecutionListByExecutionId { execution_id })
                        }
                        Err(err) => {
                            error!("Got error {err:?}");
                            // reenable the submit button
                            let mut form_data = form_data_handle.deref().clone();
                            form_data.request_processing = false;
                            form_data_handle.set(form_data);
                        }
                    }
                }
            });
        })
    };

    let params_html: Vec<_> = function_detail
        .params
        .iter()
        .enumerate()
        .map(|(idx, param)| {
            let name = param.name.as_deref().unwrap_or("unknown_param_name");
            let r#type = param
                .r#type
                .as_ref()
                .and_then(|wit_type| wit_type.wit_type.as_deref())
                .unwrap_or("<unknown_type>");
            let id = format!("{ffqn}{idx}");
            html! {<p>
                <label for={id.clone()}>{ format!("{}: {}", name, r#type) }</label>
                <input id={id} type="text" value={form_data_handle.params[idx].clone()} oninput = {&on_param_change} />
            </p>}
        })
        .collect();

    html! {<>
        <form onsubmit = {on_submit }>
            {for params_html}
            <button type="submit" disabled={form_data_handle.request_processing}>
                {"Submit"}
            </button>
        </form>
    </>}
}
