use std::{borrow::Cow, fmt::Display};

use val_json::{TypeWrapper, UnsupportedTypeError, ValWrapper};

pub mod activity;
pub mod event_history;
mod wasm_tools;
pub mod workflow;

#[derive(Hash, Clone, Debug, PartialEq, Eq)]
pub struct FunctionFqn<'a> {
    pub ifc_fqn: Cow<'a, str>,
    pub function_name: Cow<'a, str>,
}

impl FunctionFqn<'_> {
    pub const fn new<'a>(ifc_fqn: &'a str, function_name: &'a str) -> FunctionFqn<'a> {
        FunctionFqn {
            ifc_fqn: Cow::Borrowed(ifc_fqn),
            function_name: Cow::Borrowed(function_name),
        }
    }

    pub fn new_owned(ifc_fqn: String, function_name: String) -> FunctionFqn<'static> {
        FunctionFqn {
            ifc_fqn: Cow::Owned(ifc_fqn),
            function_name: Cow::Owned(function_name),
        }
    }

    pub fn to_owned(&self) -> FunctionFqn<'static> {
        Self::new_owned(self.ifc_fqn.to_string(), self.function_name.to_string())
    }
}

impl Display for FunctionFqn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{ifc_fqn}.{function_name}",
            ifc_fqn = self.ifc_fqn,
            function_name = self.function_name
        )
    }
}

#[derive(thiserror::Error, Debug)]
pub enum FunctionMetadataError {
    #[error("{0}")]
    UnsupportedType(#[from] UnsupportedTypeError),

    #[error("unsupported return type in `{fqn}`, got type {ty}")]
    UnsupportedReturnType { fqn: String, ty: String },
}

#[derive(Clone, Debug)]
pub struct FunctionMetadata {
    pub results_len: usize,
    pub params: Vec<(String /*name*/, TypeWrapper)>,
}

impl FunctionMetadata {
    pub fn deserialize_params<V: From<ValWrapper>>(
        &self,
        param_vals: &str,
    ) -> Result<Vec<V>, serde_json::error::Error> {
        let param_types = self.params.iter().map(|(_, type_w)| type_w);
        val_json::deserialize_sequence(param_vals, param_types)
    }
}
