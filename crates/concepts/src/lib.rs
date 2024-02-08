use std::{
    borrow::Borrow,
    fmt::{Debug, Display},
    marker::PhantomData,
    ops::Deref,
    sync::Arc,
};
use val_json::{TypeWrapper, UnsupportedTypeError, ValWrapper};

#[derive(Hash, Clone, PartialEq, Eq)]
pub struct Name<T> {
    value: Arc<String>,
    phantom_data: PhantomData<fn(T) -> T>,
}

impl<T> Name<T> {
    #[must_use]
    pub fn new(value: String) -> Self {
        Self {
            value: Arc::new(value),
            phantom_data: PhantomData,
        }
    }
}

impl<T> Display for Name<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.value)
    }
}

impl<T> Deref for Name<T> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.value.deref().deref()
    }
}

impl<T> Borrow<str> for Name<T> {
    fn borrow(&self) -> &str {
        self.deref()
    }
}

#[derive(Hash, Clone, PartialEq, Eq)]
pub struct IfcFqnMarker;

pub type IfcFqnName = Name<IfcFqnMarker>; // namespace:name/ifc_name@version

#[derive(Hash, Clone, PartialEq, Eq)]
pub struct FnMarker;

pub type FnName = Name<FnMarker>;

#[derive(Hash, Clone, PartialEq, Eq)]
pub struct FunctionFqn {
    pub ifc_fqn: IfcFqnName,
    pub function_name: FnName,
}

impl FunctionFqn {
    #[must_use]
    pub fn new(ifc_fqn: String, function_name: String) -> FunctionFqn {
        FunctionFqn {
            ifc_fqn: Name::new(ifc_fqn),
            function_name: Name::new(function_name),
        }
    }
}

impl Display for FunctionFqn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "`{ifc_fqn}.{function_name}`",
            ifc_fqn = self.ifc_fqn,
            function_name = self.function_name
        )
    }
}

impl Debug for FunctionFqn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

impl std::cmp::PartialEq<FunctionFqnStr<'_>> for FunctionFqn {
    fn eq(&self, other: &FunctionFqnStr<'_>) -> bool {
        *self.ifc_fqn == *other.ifc_fqn && *self.function_name == *other.function_name
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FunctionFqnStr<'a> {
    pub ifc_fqn: &'a str,
    pub function_name: &'a str,
}

impl FunctionFqnStr<'_> {
    #[must_use]
    pub const fn new<'a>(ifc_fqn: &'a str, function_name: &'a str) -> FunctionFqnStr<'a> {
        FunctionFqnStr {
            ifc_fqn,
            function_name,
        }
    }

    #[must_use]
    pub fn to_owned(&self) -> FunctionFqn {
        FunctionFqn::new(self.ifc_fqn.to_owned(), self.function_name.to_owned())
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
        *self.ifc_fqn == *other.ifc_fqn && *self.function_name == *other.function_name
    }
}

#[derive(thiserror::Error, Debug)]
pub enum FunctionMetadataError {
    #[error("{0}")]
    UnsupportedType(#[from] UnsupportedTypeError),

    #[error("unsupported return type in {fqn}, got type `{ty}`")]
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

#[derive(Clone, Debug, PartialEq)]
pub enum SupportedFunctionResult {
    None,
    Single(wasmtime::component::Val),
}

impl SupportedFunctionResult {
    #[must_use]
    pub fn new(mut vec: Vec<wasmtime::component::Val>) -> Self {
        if vec.is_empty() {
            Self::None
        } else if vec.len() == 1 {
            Self::Single(vec.pop().unwrap())
        } else {
            unimplemented!("multi-value return types are not supported")
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Single(_) => 1,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::None)
    }
}

impl IntoIterator for SupportedFunctionResult {
    type Item = wasmtime::component::Val;
    type IntoIter = std::option::IntoIter<wasmtime::component::Val>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::None => None.into_iter(),
            Self::Single(item) => Some(item).into_iter(),
        }
    }
}

#[derive(Debug, Clone, Default, derive_more::Deref)]
pub struct Params(Arc<Vec<wasmtime::component::Val>>);

impl Params {
    pub fn new(params: Vec<wasmtime::component::Val>) -> Self {
        Self(Arc::new(params))
    }
}

impl From<&[wasmtime::component::Val]> for Params {
    fn from(value: &[wasmtime::component::Val]) -> Self {
        Self(Arc::new(Vec::from(value)))
    }
}

impl<const N: usize> From<[wasmtime::component::Val; N]> for Params {
    fn from(value: [wasmtime::component::Val; N]) -> Self {
        Self(Arc::new(Vec::from(value)))
    }
}

pub mod workflow_id {
    use std::{str::FromStr, sync::Arc};

    #[derive(Debug, Clone, derive_more::Display, PartialEq, Eq, Hash)]
    pub struct WorkflowId(Arc<String>);
    impl WorkflowId {
        #[must_use]
        pub fn generate() -> WorkflowId {
            ulid::Ulid::new().to_string().parse().unwrap() // ulid is 26 chars long
        }

        #[must_use]
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
        type Err = ParseError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            if s.len() < MIN_LEN {
                return Err(ParseError::TooShort);
            }
            if s.len() > MAX_LEN {
                return Err(ParseError::TooLong);
            }
            if s.chars()
                .all(|x| x.is_alphanumeric() || x == '_' || x == '-')
            {
                Ok(Self(Arc::new(s.to_string())))
            } else {
                Err(ParseError::IllegalCharacters)
            }
        }
    }

    #[derive(Debug, thiserror::Error, PartialEq, Eq)]
    pub enum ParseError {
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

        use super::{ParseError, WorkflowId};

        #[test]
        fn parse_workflow_id() {
            assert_eq!("w1".parse::<WorkflowId>().unwrap().to_string(), "w1");
            assert_eq!(
                "w1-2_ID".parse::<WorkflowId>().unwrap().to_string(),
                "w1-2_ID"
            );
            assert_eq!(
                "w1\n".parse::<WorkflowId>().unwrap_err(),
                ParseError::IllegalCharacters
            );
            assert_eq!("".parse::<WorkflowId>().unwrap_err(), ParseError::TooShort);
            assert_eq!(
                "x".repeat(MAX_LEN + 1).parse::<WorkflowId>().unwrap_err(),
                ParseError::TooLong
            );
        }
    }
}
