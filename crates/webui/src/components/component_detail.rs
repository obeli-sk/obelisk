use crate::grpc_client::{self, FunctionDetails};
use indexmap::IndexMap;
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
    let mut interfaces_to_fn_details: IndexMap<String, Vec<FunctionDetails>> = IndexMap::new();
    for function_detail in functions {
        let function_name = function_detail
            .function
            .clone()
            .expect("function and its name is sent by the server");
        interfaces_to_fn_details
            .entry(function_name.interface_name.clone())
            .or_default()
            .push(function_detail.clone());
    }

    let mut interface_rows: Vec<Html> = vec![];

    for (ifc_name, function_detail_vec) in interfaces_to_fn_details {
        let function_detail_vec: Vec<Html> = function_detail_vec
            .into_iter()
            .map(|function_detail| {
                let function_name = function_detail
                    .function
                    .expect("`.function` is sent by the server");
                html! {

                    <p>
                        {format!("{} ", function_name.function_name)}
                        if function_detail.submittable {
                            <button>{"Submit"}</button>
                        }
                    </p>

                }
            })
            .collect();
        interface_rows.push(html! { <>
            <p>{format!("{}", ifc_name)}</p>
            {function_detail_vec}
        </>});
    }
    html! {
        for interface_rows
    }
}
