use std::{
    borrow::Borrow,
    fmt::{Debug, Display},
    hash::Hash,
    marker::PhantomData,
    ops::Deref,
    sync::Arc,
};
use tracing_unwrap::OptionExt;
use val_json::{TypeWrapper, ValWrapper};

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

impl<'a> arbitrary::Arbitrary<'a> for FunctionFqn {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(FunctionFqn::new(
            u.arbitrary::<String>()?,
            u.arbitrary::<String>()?,
        ))
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

#[derive(Clone, Debug, PartialEq, Eq)]
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
            Self::Single(vec.pop().unwrap_or_log())
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

#[derive(Debug, Clone, Default, derive_more::Deref, PartialEq, Eq)]
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

pub trait ExecutionId:
    Clone + Hash + Display + Debug + Eq + PartialEq + Send + Sync + Ord + 'static
{
    #[must_use]
    fn generate() -> Self;
}

pub mod prefixed_ulid {
    use arbitrary::Arbitrary;

    use crate::ExecutionId;
    use std::{
        fmt::{Debug, Display},
        hash::Hash,
        marker::PhantomData,
    };

    #[derive(derive_more::Display)]
    #[display(fmt = "{prefix}_{id}")]
    pub struct PrefixedUlid<T: 'static> {
        prefix: &'static str,
        id: ulid::Ulid,
        phantom_data: PhantomData<fn(T) -> T>,
    }

    impl<T> PrefixedUlid<T> {
        fn new(id: ulid::Ulid) -> Self {
            Self {
                prefix: std::any::type_name::<T>().rsplit("::").next().unwrap(),
                id,
                phantom_data: PhantomData,
            }
        }
    }

    impl<T> ExecutionId for PrefixedUlid<T> {
        fn generate() -> Self {
            Self::new(ulid::Ulid::new())
        }
    }

    mod impls {
        use super::*;

        impl<T> Debug for PrefixedUlid<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                Display::fmt(&self, f)
            }
        }

        impl<T> Clone for PrefixedUlid<T> {
            fn clone(&self) -> Self {
                Self {
                    prefix: self.prefix,
                    id: self.id.clone(),
                    phantom_data: self.phantom_data.clone(),
                }
            }
        }

        impl<T> Hash for PrefixedUlid<T> {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.prefix.hash(state);
                self.id.hash(state);
                self.phantom_data.hash(state);
            }
        }

        impl<T> PartialEq for PrefixedUlid<T> {
            fn eq(&self, other: &Self) -> bool {
                self.id == other.id
            }
        }

        impl<T> Eq for PrefixedUlid<T> {}

        impl<T> PartialOrd for PrefixedUlid<T> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                self.id.partial_cmp(&other.id)
            }
        }

        impl<T> Ord for PrefixedUlid<T> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.id.cmp(&other.id)
            }
        }
    }

    pub mod prefix {
        pub struct Act;
        pub struct Wrk;
        pub struct Wfw;
    }

    pub type ActivityId = PrefixedUlid<prefix::Act>;
    pub type WorkerId = PrefixedUlid<prefix::Wrk>;
    pub type WorkflowId = PrefixedUlid<prefix::Wfw>;

    impl<'a> Arbitrary<'a> for WorkflowId {
        fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(Self::new(ulid::Ulid::from_parts(
                u.arbitrary()?,
                u.arbitrary()?,
            )))
        }
    }
}
