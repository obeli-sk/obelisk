use crate::grpc::grpc_client;
use yew::prelude::*;

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
            { &return_type.wit_type }
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
            let ty = param
                .r#type
                .as_ref()
                .expect("`FunctionParameter.type` is sent")
                .wit_type
                .as_str();
            html! {<>
                if idx > 0 {
                    {", "}
                }
                { format!("{}: {}", param.name, ty) }
            </>}
        })
        .collect()
}
