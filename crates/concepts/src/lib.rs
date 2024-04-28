use ::serde::{Deserialize, Serialize};
pub use prefixed_ulid::ExecutionId;
use std::{
    borrow::Borrow,
    error::Error,
    fmt::{Debug, Display},
    hash::Hash,
    marker::PhantomData,
    ops::Deref,
    sync::Arc,
};
use val_json::{
    type_wrapper::TypeWrapper,
    wast_val::{WastVal, WastValWithType},
};

pub mod storage;

pub type FinishedExecutionResult = Result<SupportedFunctionResult, FinishedExecutionError>;

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FinishedExecutionError {
    #[error("permanent timeout")]
    PermanentTimeout,
    #[error("non-determinism detected, reason: `{0}`")]
    NonDeterminismDetected(StrVariant),
    #[error("uncategorized error: `{0}`")]
    PermanentFailure(StrVariant), // intermittent failure that is not retried (anymore)
    #[error("cancelled, reason: `{0}`")]
    Cancelled(StrVariant),
    #[error("continuing as {execution_id}")]
    ContinueAsNew { execution_id: ExecutionId },
}

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
mod serde_strvariant {
    use crate::StrVariant;
    use serde::{
        de::{self, Visitor},
        Deserialize, Deserializer, Serialize, Serializer,
    };
    use std::{ops::Deref, sync::Arc};

    impl Serialize for StrVariant {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_str(self.deref())
        }
    }

    impl<'de> Deserialize<'de> for StrVariant {
        fn deserialize<D>(deserializer: D) -> Result<StrVariant, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_str(StrVariantVisitor)
        }
    }

    struct StrVariantVisitor;

    impl<'de> Visitor<'de> for StrVariantVisitor {
        type Value = StrVariant;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(StrVariant::Arc(Arc::from(v)))
        }
    }
}

#[derive(Hash, Clone, PartialEq, Eq, derive_more::Display, Serialize, Deserialize)]
#[display(fmt = "{value}")]
#[serde(transparent)]
pub struct Name<T> {
    value: StrVariant,
    #[serde(skip)]
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

#[derive(Hash, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionFqn {
    pub ifc_fqn: IfcFqnName,
    pub function_name: FnName,
}

impl FunctionFqn {
    #[must_use]
    pub fn new_owned(ifc_fqn: Arc<str>, function_name: Arc<str>) -> Self {
        Self {
            ifc_fqn: Name::new_owned(ifc_fqn),
            function_name: Name::new_owned(function_name),
        }
    }

    #[must_use]
    pub const fn new_static(ifc_fqn: &'static str, function_name: &'static str) -> Self {
        Self {
            ifc_fqn: Name::new_static(ifc_fqn),
            function_name: Name::new_static(function_name),
        }
    }

    #[must_use]
    pub const fn new_static_tuple(tuple: (&'static str, &'static str)) -> Self {
        Self::new_static(tuple.0, tuple.1)
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SupportedFunctionResult {
    None,
    Fallible(WastValWithType),
    Infallible(WastValWithType),
}

#[derive(Debug, thiserror::Error)]
pub enum ResultParsingError {
    #[error("multi-value results are not supported")]
    MultiValue,
    #[error(transparent)]
    TypeConversionError(#[from] val_json::type_wrapper::TypeConversionError),
    #[error(transparent)]
    ValueConversionError(#[from] val_json::wast_val::WastValConversionError),
}

impl SupportedFunctionResult {
    pub fn new<
        I: ExactSizeIterator<Item = (wasmtime::component::Val, wasmtime::component::Type)>,
    >(
        mut iter: I,
    ) -> Result<Self, ResultParsingError> {
        if iter.len() == 0 {
            Ok(Self::None)
        } else if iter.len() == 1 {
            let (val, r#type) = iter.next().unwrap();
            let r#type = TypeWrapper::try_from(r#type)?;
            let val = WastVal::try_from(val)?;
            match &val {
                WastVal::Result(_) => Ok(Self::Fallible(WastValWithType { r#type, value: val })),
                _ => Ok(Self::Infallible(WastValWithType { r#type, value: val })),
            }
        } else {
            Err(ResultParsingError::MultiValue)
        }
    }

    #[must_use]
    pub fn fallible_err(&self) -> Option<Option<&WastVal>> {
        match self {
            SupportedFunctionResult::Fallible(WastValWithType {
                value: WastVal::Result(Err(err)),
                ..
            }) => Some(err.as_deref()),
            _ => None,
        }
    }

    #[must_use]
    pub fn fallible_ok(&self) -> Option<Option<&WastVal>> {
        match self {
            SupportedFunctionResult::Fallible(WastValWithType {
                value: WastVal::Result(Ok(ok)),
                ..
            }) => Some(ok.as_deref()),
            _ => None,
        }
    }

    #[must_use]
    pub fn value(&self) -> Option<&WastVal> {
        match self {
            SupportedFunctionResult::None => None,
            SupportedFunctionResult::Fallible(v) | SupportedFunctionResult::Infallible(v) => {
                Some(&v.value)
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
pub struct Params(ParamsInternal);

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParamsInternal {
    WastValParams(Arc<[WastValWithType]>),
    Vals {
        vals: Arc<[wasmtime::component::Val]>,
        r#types: Arc<[TypeWrapper]>,
    },
    Empty,
}

impl Default for Params {
    fn default() -> Self {
        Self(ParamsInternal::Empty)
    }
}

mod serde_params {
    use crate::{Params, ParamsInternal};
    use serde::de::{SeqAccess, Visitor};
    use serde::ser::SerializeSeq;
    use serde::{Deserialize, Serialize};
    use std::marker::PhantomData;
    use std::ops::Deref;
    use std::sync::Arc;
    use val_json::wast_val::{WastVal, WastValWithType};

    impl Serialize for Params {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: ::serde::Serializer,
        {
            match &self.0 {
                ParamsInternal::WastValParams(params) => {
                    let mut seq = serializer.serialize_seq(Some(params.len()))?;
                    for element in params.deref() {
                        seq.serialize_element(element)?;
                    }
                    seq.end()
                }
                ParamsInternal::Vals { vals, r#types } => {
                    let mut seq = serializer.serialize_seq(Some(vals.len()))?; // size must be equal, checked when constructed.
                    for (val, r#type) in vals.iter().zip(r#types.iter()) {
                        let value = WastVal::try_from(val.clone())
                            .map_err(|err| serde::ser::Error::custom(err.to_string()))?;
                        seq.serialize_element(&WastValWithType {
                            value,
                            r#type: r#type.clone(),
                        })?;
                    }
                    seq.end()
                }
                ParamsInternal::Empty => serializer.serialize_seq(Some(0))?.end(),
            }
        }
    }

    struct VecVisitor<T>(PhantomData<T>);
    impl<T> Default for VecVisitor<T> {
        fn default() -> Self {
            Self(PhantomData)
        }
    }

    impl<'de, T: Deserialize<'de>> Visitor<'de> for VecVisitor<T> {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a sequence of `WastValWithType` structs")
        }

        #[inline]
        fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
        where
            V: SeqAccess<'de>,
        {
            let mut vec = Vec::new();
            while let Some(elem) = visitor.next_element()? {
                vec.push(elem);
            }
            Ok(vec)
        }
    }

    impl<'de> Deserialize<'de> for Params {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let vec: Vec<WastValWithType> = deserializer.deserialize_seq(VecVisitor::default())?;
            if vec.is_empty() {
                Ok(Self(ParamsInternal::Empty))
            } else {
                Ok(Self(ParamsInternal::WastValParams(Arc::from(vec))))
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]

pub enum ParamsParsingError {
    #[error("error parsing {idx}-th parameter: `{err:?}`")]
    ParameterError {
        idx: usize,
        err: Box<dyn Error + Send + Sync>,
    },
}

impl Params {
    #[must_use]
    pub const fn default() -> Self {
        Self(ParamsInternal::Empty)
    }

    pub fn from_wasmtime(
        vals: Arc<[wasmtime::component::Val]>,
        r#types: Arc<[TypeWrapper]>,
    ) -> Result<Self, &'static str> {
        if vals.len() == r#types.len() {
            Ok(Self(ParamsInternal::Vals { vals, types }))
        } else {
            Err("mismatch between `vals` and `types` length")
        }
    }

    #[must_use]
    pub fn new(vec: Arc<[WastValWithType]>) -> Self {
        Self(ParamsInternal::WastValParams(vec))
    }

    // TODO: optimize allocations
    pub fn as_vals(
        &self,
        _param_types: &[Type],
    ) -> Result<Arc<[wasmtime::component::Val]>, ParamsParsingError> {
        match &self.0 {
            ParamsInternal::Vals { vals, .. } => Ok(vals.clone()),
            ParamsInternal::WastValParams(wast_vals) => Ok(wast_vals
                .iter()
                .map(
                    |WastValWithType {
                         r#type: _,
                         value: wast_val,
                     }| wast_val.as_val(),
                )
                .collect()),
            ParamsInternal::Empty => Ok(Arc::from([])),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match &self.0 {
            ParamsInternal::Vals { vals, .. } => vals.len(),
            ParamsInternal::WastValParams(vals) => vals.len(),
            ParamsInternal::Empty => 0,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl From<&[WastValWithType]> for Params {
    fn from(value: &[WastValWithType]) -> Self {
        Self::new(Arc::from(value))
    }
}

impl<const N: usize> From<[WastValWithType; N]> for Params {
    fn from(value: [WastValWithType; N]) -> Self {
        Self::new(Arc::from(value))
    }
}

pub mod prefixed_ulid {
    use arbitrary::Arbitrary;
    use serde_with::{DeserializeFromStr, SerializeDisplay};
    use std::marker::PhantomData;
    use ulid::Ulid;

    #[derive(derive_more::Display, SerializeDisplay, DeserializeFromStr)]
    #[display(fmt = "{}_{ulid}", "Self::prefix()")]
    pub struct PrefixedUlid<T: 'static> {
        ulid: Ulid,
        phantom_data: PhantomData<fn(T) -> T>,
    }

    impl<T> PrefixedUlid<T> {
        const fn new(ulid: Ulid) -> Self {
            Self {
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
        pub const fn from_parts(timestamp_ms: u64, random: u128) -> Self {
            Self::new(Ulid::from_parts(timestamp_ms, random))
        }

        #[must_use]
        pub fn timestamp_part(&self) -> u64 {
            self.ulid.timestamp_ms()
        }

        #[must_use]
        #[allow(clippy::cast_possible_truncation)]
        pub fn random_part(&self) -> u64 {
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
                Self::prefix().hash(state);
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
        pub struct E;
        pub struct Exr;
        pub struct Conf;
        pub struct JoinSet;
        pub struct Run;
        pub struct Delay;
    }

    pub type ExecutorId = PrefixedUlid<prefix::Exr>;
    pub type ConfigId = PrefixedUlid<prefix::Conf>;
    pub type JoinSetId = PrefixedUlid<prefix::JoinSet>;
    pub type ExecutionId = PrefixedUlid<prefix::E>;
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
        let mut builder_a = madsim::runtime::Builder::from_env();
        builder_a.check = true;

        let mut builder_b = madsim::runtime::Builder::from_env(); // Builder: Clone would be useful
        builder_b.check = true;
        builder_b.seed = builder_a.seed;

        assert_eq!(
            builder_a.run(|| async { ulid::Ulid::new() }),
            builder_b.run(|| async { ulid::Ulid::new() })
        );
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
        let input = "foo";
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
