use std::str::FromStr;

use super::{grpc_client::FunctionDetail, ifc_fqn::IfcFqn};
use indexmap::IndexMap;

pub fn is_extension_interface(ifc: &IfcFqn) -> bool {
    ifc.pkg_fqn.is_extension()
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum InterfaceFilter {
    All,
    // without -ext, -stub interfaces
    WithoutExtensions,
}

pub fn map_interfaces_to_fn_details(
    functions: &[FunctionDetail],
    filter: InterfaceFilter,
) -> IndexMap<IfcFqn, Vec<FunctionDetail>> {
    let mut interfaces_to_fn_details: IndexMap<IfcFqn, Vec<FunctionDetail>> = IndexMap::new();
    for function_detail in functions {
        let function_name = function_detail
            .function_name
            .clone()
            .expect("function and its name is sent by the server");
        let ifc_fqn = IfcFqn::from_str(&function_name.interface_name)
            .expect("received interface must be well-formed");
        if filter == InterfaceFilter::All || !ifc_fqn.pkg_fqn.is_extension() {
            interfaces_to_fn_details
                .entry(ifc_fqn)
                .or_default()
                .push(function_detail.clone());
        }
    }
    interfaces_to_fn_details.sort_keys();
    // sort functions in each interface
    for (_, fns) in interfaces_to_fn_details.iter_mut() {
        fns.sort_by(|a, b| {
            a.function_name
                .as_ref()
                .map(|f| &f.function_name)
                .cmp(&b.function_name.as_ref().map(|f| &f.function_name))
        });
    }
    interfaces_to_fn_details
}
