use crate::grpc_client;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ComponentDetailProps {
    pub component: grpc_client::Component,
}
#[function_component(ComponentDetail)]
pub fn component_detail(ComponentDetailProps { component }: &ComponentDetailProps) -> Html {
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
pub struct FunctionListProps {
    functions: Vec<grpc_client::FunctionDetails>,
}
#[function_component(FunctionList)]
pub fn function_list(FunctionListProps { functions }: &FunctionListProps) -> Html {
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
