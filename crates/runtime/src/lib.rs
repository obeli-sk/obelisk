use std::fmt::Display;

pub mod activity;
pub mod event_history;
mod wasm_tools;
pub mod workflow;

#[derive(Hash, Clone, PartialEq, Eq)]
pub(crate) struct FunctionFqn {
    pub(crate) ifc_fqn: Option<String>,
    pub(crate) function_name: String,
}

impl Display for FunctionFqn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{prefix}{function_name}",
            prefix = if let Some(ifc_fqn) = &self.ifc_fqn {
                ifc_fqn
            } else {
                ""
            },
            function_name = self.function_name
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FunctionMetadata {
    pub(crate) results_len: usize,
}
