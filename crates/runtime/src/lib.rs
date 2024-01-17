use event_history::SupportedFunctionResult;
use std::{
    fmt::{Debug, Display},
    sync::Arc,
};
use val_json::{TypeWrapper, UnsupportedTypeError, ValWrapper};
use workflow_id::WorkflowId;

pub mod activity;
pub mod database;
pub mod event_history;
pub mod runtime;
mod wasm_tools;
pub mod workflow;

#[derive(Hash, Clone, Debug, PartialEq, Eq)]
pub struct FunctionFqn {
    pub ifc_fqn: Arc<String>, // format namespace:name/ifc_name@version
    pub function_name: Arc<String>,
}

impl FunctionFqn {
    pub fn new<T: ToString>(ifc_fqn: T, function_name: T) -> FunctionFqn {
        FunctionFqn {
            ifc_fqn: Arc::new(ifc_fqn.to_string()),
            function_name: Arc::new(function_name.to_string()),
        }
    }
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

impl std::cmp::PartialEq<FunctionFqnStr<'_>> for FunctionFqn {
    fn eq(&self, other: &FunctionFqnStr<'_>) -> bool {
        *self.ifc_fqn == other.ifc_fqn && *self.function_name == other.function_name
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FunctionFqnStr<'a> {
    pub ifc_fqn: &'a str,
    pub function_name: &'a str,
}

impl FunctionFqnStr<'_> {
    const fn new<'a>(ifc_fqn: &'a str, function_name: &'a str) -> FunctionFqnStr<'a> {
        FunctionFqnStr {
            ifc_fqn,
            function_name,
        }
    }

    fn to_owned(&self) -> FunctionFqn {
        FunctionFqn::new(self.ifc_fqn.to_string(), self.function_name.to_string())
    }
}

impl Display for FunctionFqnStr<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{ifc_fqn}.{{{function_name}}}",
            ifc_fqn = self.ifc_fqn,
            function_name = self.function_name
        )
    }
}

impl std::cmp::PartialEq<FunctionFqn> for FunctionFqnStr<'_> {
    fn eq(&self, other: &FunctionFqn) -> bool {
        self.ifc_fqn == *other.ifc_fqn && self.function_name == *other.function_name
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

pub type ActivityResponse = Result<SupportedFunctionResult, ActivityFailed>;

#[derive(thiserror::Error, Debug, Clone)]
pub enum ActivityFailed {
    // TODO: add run_id
    #[error("[{workflow_id}] limit reached for activity `{activity_fqn}` - {reason}")]
    LimitReached {
        workflow_id: WorkflowId,
        activity_fqn: FunctionFqn,
        reason: String,
    },
    #[error("[{workflow_id}] activity `{activity_fqn}` not found")]
    NotFound {
        workflow_id: WorkflowId,
        activity_fqn: FunctionFqn,
    },
    #[error("[{workflow_id}] activity `{activity_fqn}` failed - {reason}")]
    Other {
        workflow_id: WorkflowId,
        activity_fqn: FunctionFqn,
        reason: String,
    },
}

pub mod workflow_id {
    use std::{str::FromStr, sync::Arc};

    #[derive(Debug, Clone, derive_more::Display, PartialEq, Eq)]
    pub struct WorkflowId(Arc<String>);
    impl WorkflowId {
        pub fn generate() -> WorkflowId {
            ulid::Ulid::new().to_string().parse().unwrap() // ulid is 26 chars long
        }

        pub fn new(s: String) -> Self {
            Self(Arc::new(s))
        }
    }

    impl AsRef<WorkflowId> for WorkflowId {
        fn as_ref(&self) -> &WorkflowId {
            self
        }
    }

    const MIN_LEN: usize = 1;
    const MAX_LEN: usize = 32;

    impl FromStr for WorkflowId {
        type Err = WorkflowIdParseError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            if s.len() < MIN_LEN {
                return Err(WorkflowIdParseError::TooShort);
            }
            if s.len() > MAX_LEN {
                return Err(WorkflowIdParseError::TooLong);
            }
            if s.chars()
                .all(|x| x.is_alphanumeric() || x == '_' || x == '-')
            {
                Ok(Self(Arc::new(s.to_string())))
            } else {
                Err(WorkflowIdParseError::IllegalCharacters)
            }
        }
    }

    #[derive(Debug, thiserror::Error, PartialEq, Eq)]
    pub enum WorkflowIdParseError {
        #[error("workflow id too long, maximal length: {MAX_LEN}")]
        TooLong,
        #[error("workflow id too short, minimal length: {MIN_LEN}")]
        TooShort,
        #[error("only alphanumeric characters, `_` and `-` are allowed in workflow id")]
        IllegalCharacters,
    }

    #[cfg(test)]
    mod tests {
        use crate::workflow_id::MAX_LEN;

        use super::{WorkflowId, WorkflowIdParseError};

        #[test]
        fn parse_workflow_id() {
            assert_eq!("w1".parse::<WorkflowId>().unwrap().to_string(), "w1");
            assert_eq!(
                "w1-2_ID".parse::<WorkflowId>().unwrap().to_string(),
                "w1-2_ID"
            );
            assert_eq!(
                "w1\n".parse::<WorkflowId>().unwrap_err(),
                WorkflowIdParseError::IllegalCharacters
            );
            assert_eq!(
                "".parse::<WorkflowId>().unwrap_err(),
                WorkflowIdParseError::TooShort
            );
            assert_eq!(
                "x".repeat(MAX_LEN + 1).parse::<WorkflowId>().unwrap_err(),
                WorkflowIdParseError::TooLong
            );
        }
    }
}
