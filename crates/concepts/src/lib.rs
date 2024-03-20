use std::{
    borrow::Borrow,
    error::Error,
    fmt::{Debug, Display},
    hash::Hash,
    marker::PhantomData,
    ops::Deref,
    sync::Arc,
};
use val_json::{wast_val::WastVal, TypeWrapper, ValWrapper};

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

impl<T> Debug for Name<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
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
    Fallible(WastVal, Result<(), ()>),
    Infallible(WastVal),
}

#[derive(Debug, thiserror::Error)]
pub enum ResultParsingError {
    #[error("multi-value results are not supported")]
    MultiValue,
    #[error("conversion error: {0:?}")]
    ConversionError(#[from] val_json::wast_val::ConversionError),
}

impl SupportedFunctionResult {
    #[must_use]
    pub fn new(mut vec: Vec<wasmtime::component::Val>) -> Result<Self, ResultParsingError> {
        if vec.is_empty() {
            Ok(Self::None)
        } else if vec.len() == 1 {
            let res = vec.pop().unwrap();
            let wast_val = WastVal::try_from(res)?;
            match &wast_val {
                WastVal::Result(res) => {
                    let res = res.as_ref().map(|_| ()).map_err(|_| ());
                    Ok(Self::Fallible(wast_val, res))
                }
                _ => Ok(Self::Infallible(wast_val)),
            }
        } else {
            Err(ResultParsingError::MultiValue)
        }
    }

    pub fn is_fallible_err(&self) -> bool {
        matches!(self, Self::Fallible(_, Err(())))
    }

    pub fn fallible_err(&self) -> Option<Option<&WastVal>> {
        match self {
            SupportedFunctionResult::Fallible(WastVal::Result(Err(err)), Err(())) => {
                Some(err.as_deref())
            }
            _ => None,
        }
    }

    pub fn value(&self) -> Option<&WastVal> {
        match self {
            SupportedFunctionResult::None => None,
            SupportedFunctionResult::Fallible(v, _) => Some(v),
            SupportedFunctionResult::Infallible(v) => Some(v),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            SupportedFunctionResult::None => 0,
            _ => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Params {
    Empty,
    // TODO Serialized(Arc<Vec<String>>),
    WastVals(Arc<Vec<WastVal>>),
    Vals(Arc<Vec<wasmtime::component::Val>>),
}

impl Default for Params {
    fn default() -> Self {
        Self::Empty
    }
}

#[derive(Debug, thiserror::Error)]

pub enum ParamsParsingError {
    #[error("arity mismatch")]
    ArityMismatch,
    #[error("error parsing {idx}-th parameter: `{err:?}`")]
    ParameterError {
        idx: usize,
        err: Box<dyn Error + Send>,
    },
}

impl Params {
    pub fn new(params: Vec<wasmtime::component::Val>) -> Self {
        Self::Vals(Arc::new(params))
    }

    // TODO: optimize allocations
    pub fn as_vals(
        &self,
        types: &[wasmtime::component::Type],
    ) -> Result<Arc<Vec<wasmtime::component::Val>>, ParamsParsingError> {
        match self {
            Self::Empty => Ok(Default::default()),
            Self::Vals(vals) => Ok(vals.clone()),
            Self::WastVals(wast_vals) => {
                if types.len() != wast_vals.len() {
                    return Err(ParamsParsingError::ArityMismatch);
                }
                let mut vec = Vec::with_capacity(types.len());
                for (idx, (ty, wast_val)) in types.iter().zip(wast_vals.iter()).enumerate() {
                    let val = val_json::wast_val::val(wast_val, ty).map_err(|err| {
                        ParamsParsingError::ParameterError {
                            idx,
                            err: err.into(),
                        }
                    })?;
                    vec.push(val);
                }
                Ok(Arc::new(vec))
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Vals(vals) => vals.len(),
            Self::WastVals(vals) => vals.len(),
        }
    }
}

impl From<&[wasmtime::component::Val]> for Params {
    fn from(value: &[wasmtime::component::Val]) -> Self {
        Self::Vals(Arc::new(Vec::from(value)))
    }
}

impl<const N: usize> From<[wasmtime::component::Val; N]> for Params {
    fn from(value: [wasmtime::component::Val; N]) -> Self {
        Self::Vals(Arc::new(Vec::from(value)))
    }
}

pub mod prefixed_ulid {
    use arbitrary::Arbitrary;
    use std::{
        fmt::{Debug, Display},
        hash::Hash,
        marker::PhantomData,
        sync::Arc,
    };

    #[derive(derive_more::Display)]
    #[display(fmt = "{arc}")]
    pub struct PrefixedUlid<T: 'static> {
        arc: Arc<String>,
        phantom_data: PhantomData<fn(T) -> T>,
    }

    impl<T> PrefixedUlid<T> {
        fn new(id: ulid::Ulid) -> Self {
            let prefix = Self::prefix();
            let arc = Arc::new(format!("{prefix}_{id}"));
            Self {
                arc,
                phantom_data: PhantomData,
            }
        }

        fn prefix() -> &'static str {
            std::any::type_name::<T>().rsplit("::").next().unwrap()
        }
    }

    impl<T> PrefixedUlid<T> {
        pub fn generate() -> Self {
            Self::new(ulid::Ulid::new())
        }
    }

    mod impls {
        use super::*;

        impl<T> Into<Arc<String>> for PrefixedUlid<T> {
            fn into(self) -> Arc<String> {
                self.arc
            }
        }

        impl<T> TryFrom<Arc<String>> for PrefixedUlid<T> {
            type Error = &'static str;

            fn try_from(arc: Arc<String>) -> Result<Self, Self::Error> {
                if !arc.starts_with(Self::prefix()) {
                    return Err("wrong prefix");
                }
                Ok(Self {
                    arc,
                    phantom_data: PhantomData,
                })
            }
        }

        impl<T> Debug for PrefixedUlid<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                Display::fmt(&self, f)
            }
        }

        impl<T> Clone for PrefixedUlid<T> {
            fn clone(&self) -> Self {
                Self {
                    arc: self.arc.clone(),
                    phantom_data: self.phantom_data.clone(),
                }
            }
        }

        impl<T> Hash for PrefixedUlid<T> {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.arc.hash(state);
                self.phantom_data.hash(state);
            }
        }

        impl<T> PartialEq for PrefixedUlid<T> {
            fn eq(&self, other: &Self) -> bool {
                self.arc == other.arc
            }
        }

        impl<T> Eq for PrefixedUlid<T> {}

        impl<T> PartialOrd for PrefixedUlid<T> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                self.arc.partial_cmp(&other.arc)
            }
        }

        impl<T> Ord for PrefixedUlid<T> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.arc.cmp(&other.arc)
            }
        }
    }

    pub mod prefix {
        pub struct Exe;
        pub struct Exr;
        pub struct Conf;
        pub struct JoinSet;
    }

    pub type ExecutorId = PrefixedUlid<prefix::Exr>;
    pub type ConfigId = PrefixedUlid<prefix::Conf>;
    pub type JoinSetId = PrefixedUlid<prefix::JoinSet>;
    pub type ExecutionId = PrefixedUlid<prefix::Exe>;

    impl<'a> Arbitrary<'a> for ExecutionId {
        fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(Self::new(ulid::Ulid::from_parts(
                u.arbitrary()?,
                u.arbitrary()?,
            )))
        }
    }
    impl<'a> Arbitrary<'a> for JoinSetId {
        fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(Self::new(ulid::Ulid::from_parts(
                u.arbitrary()?,
                u.arbitrary()?,
            )))
        }
    }
}
pub use prefixed_ulid::ExecutionId;
