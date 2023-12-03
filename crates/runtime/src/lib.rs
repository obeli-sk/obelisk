use std::fmt::Display;

pub mod activity;
pub mod event_history;
mod wasm_tools;
pub mod workflow;

#[derive(Hash, Clone, Debug, PartialEq, Eq)]
pub(crate) struct FunctionFqn {
    pub(crate) ifc_fqn: String,
    pub(crate) function_name: String,
}

impl Display for FunctionFqn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{ifc_fqn}.{function_name}",
            ifc_fqn = self.ifc_fqn,
            function_name = self.function_name
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FunctionMetadata {
    pub(crate) results_len: usize,
}
