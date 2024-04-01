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

#[derive(Clone, Eq, derive_more::Display)]
pub enum StrVariant {
    Static(&'static str),
    Arc(Arc<str>),
}

impl PartialEq for StrVariant {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Static(left), Self::Static(right)) => left == right,
            (Self::Static(left), Self::Arc(right)) => *left == right.deref(),
            (Self::Arc(left), Self::Arc(right)) => left == right,
            (Self::Arc(left), Self::Static(right)) => left.deref() == *right,
        }
    }
}

impl Hash for StrVariant {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            StrVariant::Static(val) => val.hash(state),
            StrVariant::Arc(val) => {
                let str: &str = val.deref();
                str.hash(state);
            }
        }
    }
}

impl Debug for StrVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Deref for StrVariant {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Arc(v) => v,
            Self::Static(v) => v,
        }
    }
}

#[derive(Hash, Clone, PartialEq, Eq, derive_more::Display)]
#[display(fmt = "{value}")]
pub struct Name<T> {
    value: StrVariant,
    phantom_data: PhantomData<fn(T) -> T>,
}

impl<T> Name<T> {
    #[must_use]
    pub fn new_owned(value: Arc<str>) -> Self {
        Self {
            value: StrVariant::Arc(value),
            phantom_data: PhantomData,
        }
    }

    #[must_use]
    pub const fn new_static(value: &'static str) -> Self {
        Self {
            value: StrVariant::Static(value),
            phantom_data: PhantomData,
        }
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
        self.value.deref()
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
    pub fn new_owned(ifc_fqn: Arc<str>, function_name: Arc<str>) -> FunctionFqn {
        FunctionFqn {
            ifc_fqn: Name::new_owned(ifc_fqn),
            function_name: Name::new_owned(function_name),
        }
    }

    #[must_use]
    pub const fn new_static(ifc_fqn: &'static str, function_name: &'static str) -> FunctionFqn {
        FunctionFqn {
            ifc_fqn: Name::new_static(ifc_fqn),
            function_name: Name::new_static(function_name),
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

impl<'a> arbitrary::Arbitrary<'a> for FunctionFqn {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(FunctionFqn::new_owned(
            Arc::from(u.arbitrary::<String>()?),
            Arc::from(u.arbitrary::<String>()?),
        ))
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

    #[must_use]
    pub fn is_fallible_err(&self) -> bool {
        matches!(self, Self::Fallible(_, Err(())))
    }

    #[must_use]
    pub fn fallible_err(&self) -> Option<Option<&WastVal>> {
        match self {
            SupportedFunctionResult::Fallible(WastVal::Result(Err(err)), Err(())) => {
                Some(err.as_deref())
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn value(&self) -> Option<&WastVal> {
        match self {
            SupportedFunctionResult::None => None,
            SupportedFunctionResult::Fallible(v, _) | SupportedFunctionResult::Infallible(v) => {
                Some(v)
            }
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            SupportedFunctionResult::None => 0,
            _ => 1,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::None)
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
        err: Box<dyn Error + Send + Sync>,
    },
}

impl Params {
    #[must_use]
    pub fn new(params: Vec<wasmtime::component::Val>) -> Self {
        Self::Vals(Arc::new(params))
    }

    // TODO: optimize allocations
    pub fn as_vals(
        &self,
        types: &[wasmtime::component::Type],
    ) -> Result<Arc<Vec<wasmtime::component::Val>>, ParamsParsingError> {
        match self {
            Self::Empty => Ok(Arc::default()),
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

    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Vals(vals) => vals.len(),
            Self::WastVals(vals) => vals.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
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
    use std::marker::PhantomData;
    use ulid::Ulid;

    #[derive(derive_more::Display)]
    #[display(fmt = "{prefix}_{ulid}")]
    pub struct PrefixedUlid<T: 'static> {
        prefix: &'static str,
        ulid: Ulid,
        phantom_data: PhantomData<fn(T) -> T>,
    }

    impl<T> PrefixedUlid<T> {
        fn new(ulid: Ulid) -> Self {
            let prefix = Self::prefix();
            Self {
                prefix,
                ulid,
                phantom_data: PhantomData,
            }
        }

        fn prefix() -> &'static str {
            std::any::type_name::<T>().rsplit("::").next().unwrap()
        }
    }

    impl<T> PrefixedUlid<T> {
        #[must_use]
        pub fn generate() -> Self {
            Self::new(Ulid::new())
        }

        #[must_use]
        pub fn from_parts(timestamp_ms: u64, random: u128) -> Self {
            Self::new(Ulid::from_parts(timestamp_ms, random))
        }

        #[must_use]
        pub fn timestamp(&self) -> u64 {
            self.ulid.timestamp_ms()
        }

        #[must_use]
        #[allow(clippy::cast_possible_truncation)]
        pub fn random(&self) -> u64 {
            self.ulid.random() as u64
        }
    }

    mod impls {
        use super::{PrefixedUlid, Ulid};
        use std::{fmt::Debug, fmt::Display, hash::Hash, marker::PhantomData, str::FromStr};

        impl<T> FromStr for PrefixedUlid<T> {
            type Err = &'static str;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                let prefix = Self::prefix();
                let mut input_chars = input.chars();
                for exp in prefix.chars() {
                    if input_chars.next() != Some(exp) {
                        return Err("wrong prefix");
                    }
                }
                if input_chars.next() != Some('_') {
                    return Err("wrong prefix");
                }
                let Ok(ulid) = Ulid::from_string(input_chars.as_str()) else {
                    return Err("wrong suffix");
                };
                Ok(Self {
                    prefix,
                    ulid,
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
                *self
            }
        }

        impl<T> Copy for PrefixedUlid<T> {}

        impl<T> Hash for PrefixedUlid<T> {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.prefix.hash(state);
                self.ulid.hash(state);
                self.phantom_data.hash(state);
            }
        }

        impl<T> PartialEq for PrefixedUlid<T> {
            fn eq(&self, other: &Self) -> bool {
                self.ulid == other.ulid
            }
        }

        impl<T> Eq for PrefixedUlid<T> {}

        impl<T> PartialOrd for PrefixedUlid<T> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl<T> Ord for PrefixedUlid<T> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.ulid.cmp(&other.ulid)
            }
        }
    }

    pub mod prefix {
        pub struct Exe;
        pub struct Exr;
        pub struct Conf;
        pub struct JoinSet;
        pub struct Run;
        pub struct Delay;
    }

    pub type ExecutorId = PrefixedUlid<prefix::Exr>;
    pub type ConfigId = PrefixedUlid<prefix::Conf>;
    pub type JoinSetId = PrefixedUlid<prefix::JoinSet>;
    pub type ExecutionId = PrefixedUlid<prefix::Exe>;
    pub type RunId = PrefixedUlid<prefix::Run>;
    pub type DelayId = PrefixedUlid<prefix::Delay>;

    impl<'a, T> Arbitrary<'a> for PrefixedUlid<T> {
        fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(Self::new(ulid::Ulid::from_parts(
                u.arbitrary()?,
                u.arbitrary()?,
            )))
        }
    }
}
pub use prefixed_ulid::ExecutionId;

#[cfg(test)]
mod tests {
    use std::{
        hash::{DefaultHasher, Hash, Hasher},
        sync::Arc,
    };

    use crate::{ExecutionId, StrVariant};

    #[cfg(madsim)]
    #[test]
    fn ulid_generation_should_be_deterministic() {
        let seed: u64 = 0;
        // https://github.com/madsim-rs/madsim/issues/201
        // first run is ignored
        madsim::runtime::Builder {
            seed,
            count: 1,
            jobs: 1,
            config: madsim::Config::default(),
            time_limit: None,
            check: false,
        }
        .run(|| async { ulid::Ulid::new() });
        assert_eq!(
            madsim::runtime::Builder {
                seed,
                count: 1,
                jobs: 1,
                config: madsim::Config::default(),
                time_limit: None,
                check: false,
            }
            .run(|| async { ulid::Ulid::new() }),
            madsim::runtime::Builder {
                seed,
                count: 1,
                jobs: 1,
                config: madsim::Config::default(),
                time_limit: None,
                check: false,
            }
            .run(|| async { ulid::Ulid::new() })
        );
    }

    // FIXME https://github.com/madsim-rs/madsim/issues/201
    #[cfg(madsim)]
    #[test]
    fn madsim_getrandom_should_be_deterministic() {
        let rnd_fn = || async {
            let mut dst = [0];
            getrandom::getrandom(&mut dst).unwrap();
            println!("{dst:?}");
            dst
        };
        let builder = madsim::runtime::Builder::from_env();
        let seed = builder.seed;
        for _ in 0..10 {
            madsim::runtime::Builder {
                seed,
                count: 1,
                jobs: 1,
                config: madsim::Config::default(),
                time_limit: None,
                check: false,
            }
            .run(rnd_fn);
        }
    }

    #[test]
    fn ulid_parsing() {
        let generated = ExecutionId::generate();
        let str = generated.to_string();
        let parsed = str.parse().unwrap();
        assert_eq!(generated, parsed);
    }

    #[test]
    fn hash_of_str_variants_should_be_equal() {
        let input = "foo"; // TODO: use arbitrary
        let left = StrVariant::Arc(Arc::from(input));
        let right = StrVariant::Static(input);
        assert_eq!(left, right);
        let mut left_hasher = DefaultHasher::new();
        left.hash(&mut left_hasher);
        let mut right_hasher = DefaultHasher::new();
        right.hash(&mut right_hasher);
        let left_hasher = left_hasher.finish();
        let right_hasher = right_hasher.finish();
        println!("left: {left_hasher:x}, right: {right_hasher:x}");
        assert_eq!(left_hasher, right_hasher);
    }
}
