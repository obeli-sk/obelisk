use yew::prelude::*;

use crate::grpc_client;

#[derive(Properties, PartialEq)]
pub struct FunctionSignatureProps {
    pub params: Vec<grpc_client::FunctionParameter>,
    pub return_type: Option<grpc_client::WitType>,
}
#[function_component(FunctionSignature)]
pub fn function_signature(
    FunctionSignatureProps {
        params,
        return_type,
    }: &FunctionSignatureProps,
) -> Html {
    html! {<>
        {"func ("}
            <FunctionParameterList params = {params.clone()} />
        {")"}
        if let Some(return_type) = return_type {

            {" -> "}
            if let Some(wit_type) = &return_type.wit_type {
                { wit_type }
            } else {
                { "<unknown type>" }
            }
        }
    </>}
}

#[derive(Properties, PartialEq)]
pub struct FunctionParameterListProps {
    pub params: Vec<grpc_client::FunctionParameter>,
}
#[function_component(FunctionParameterList)]
pub fn function_parameter_list(
    FunctionParameterListProps { params }: &FunctionParameterListProps,
) -> Html {
    params
        .iter()
        .enumerate()
        .map(|(idx, param)| {
            let name = param.name.as_deref().unwrap_or("unknown_param_name");
            let r#type = param
                .r#type
                .as_ref()
                .and_then(|wit_type| wit_type.wit_type.as_deref())
                .unwrap_or("<unknown_type>");
            html! {<>
                if idx > 0 {
                    {", "}
                }
                { format!("{}: {}", name, r#type) }
            </>}
        })
        .collect()
}
