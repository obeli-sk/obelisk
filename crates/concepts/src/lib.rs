use ::serde::{Deserialize, Serialize};
use async_trait::async_trait;
pub use prefixed_ulid::ExecutionId;
use serde_json::Value;
use std::{
    borrow::Borrow,
    fmt::{Debug, Display},
    hash::Hash,
    marker::PhantomData,
    ops::Deref,
    str::FromStr,
    sync::Arc,
};
use val_json::{
    type_wrapper::{TypeConversionError, TypeWrapper},
    wast_val::{WastVal, WastValWithType},
    wast_val_ser::params,
};
use wasmtime::component::{Type, Val};

pub mod storage;

pub type FinishedExecutionResult = Result<SupportedFunctionResult, FinishedExecutionError>;

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FinishedExecutionError {
    #[error("permanent timeout")]
    PermanentTimeout,
    #[error("non-determinism detected, reason: `{0}`")]
    NonDeterminismDetected(StrVariant),
    #[error("uncategorized error: `{0}`")]
    PermanentFailure(StrVariant), // intermittent failure that is not retried (anymore) and cannot be mapped to the function result type
}

#[derive(Clone, Eq, derive_more::Display)]
pub enum StrVariant {
    Static(&'static str),
    // TODO String(String)
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
    pub fn new_arc(value: Arc<str>) -> Self {
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

impl IfcFqnName {
    #[must_use]
    pub fn namespace(&self) -> &str {
        self.deref().split_once(':').unwrap().0
    }

    #[must_use]
    pub fn package_name(&self) -> &str {
        let after_colon = self.deref().split_once(':').unwrap().1;
        after_colon.split_once('/').unwrap().0
    }

    #[must_use]
    pub fn version(&self) -> Option<&str> {
        self.deref().split_once('@').map(|(_, version)| version)
    }

    #[must_use]
    pub fn ifc_name(&self) -> &str {
        let after_colon = self.deref().split_once(':').unwrap().1;
        let after_slash = after_colon.split_once('/').unwrap().1;
        after_slash
            .split_once('@')
            .map_or(after_slash, |(ifc, _)| ifc)
    }

    #[must_use]
    pub fn from_parts(
        namespace: &str,
        package_name: &str,
        ifc_name: &str,
        version: Option<&str>,
    ) -> Self {
        let mut str = format!("{namespace}:{package_name}/{ifc_name}");
        if let Some(version) = version {
            str += "@";
            str += version;
        }
        Self::new_arc(Arc::from(str))
    }
}

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
    pub fn new_arc(ifc_fqn: Arc<str>, function_name: Arc<str>) -> Self {
        Self {
            ifc_fqn: Name::new_arc(ifc_fqn),
            function_name: Name::new_arc(function_name),
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

#[derive(Debug, thiserror::Error)]
pub enum FunctionFqnParseError {
    #[error("delimiter `.` not found")]
    DelimiterNotFound,
}

impl FromStr for FunctionFqn {
    type Err = FunctionFqnParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((ifc_fqn, function_name)) = s.split_once('.') {
            Ok(Self::new_arc(Arc::from(ifc_fqn), Arc::from(function_name)))
        } else {
            Err(FunctionFqnParseError::DelimiterNotFound)
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

impl Debug for FunctionFqn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

impl<'a> arbitrary::Arbitrary<'a> for FunctionFqn {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let illegal = [':', '@', '.'];
        let namespace = u.arbitrary::<String>()?.replace(illegal, "");
        let pkg_name = u.arbitrary::<String>()?.replace(illegal, "");
        let ifc_name = u.arbitrary::<String>()?.replace(illegal, "");
        let fn_name = u.arbitrary::<String>()?.replace(illegal, "");

        Ok(FunctionFqn::new_arc(
            Arc::from(format!("{namespace}:{pkg_name}/{ifc_name}")),
            Arc::from(fn_name),
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
// TODO: rename to SupportedFunctionReturnValue
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
    pub fn fallible_result(&self) -> Option<Result<Option<&WastVal>, Option<&WastVal>>> {
        match self {
            SupportedFunctionResult::Fallible(WastValWithType {
                value: WastVal::Result(Ok(ok)),
                ..
            }) => Some(Ok(ok.as_deref())),
            SupportedFunctionResult::Fallible(WastValWithType {
                value: WastVal::Result(Err(err)),
                ..
            }) => Some(Err(err.as_deref())),
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
    JsonValues(Vec<Value>),
    Vals {
        //TODO: is Arc needed here? Or move upwards?
        vals: Arc<[wasmtime::component::Val]>,
    },
    Empty,
}

impl Default for Params {
    fn default() -> Self {
        Self(ParamsInternal::Empty)
    }
}

pub type FunctionMetadata = (FunctionFqn, ParameterTypes, ReturnType);

mod serde_params {
    use crate::{Params, ParamsInternal};
    use serde::de::{SeqAccess, Visitor};
    use serde::ser::SerializeSeq;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::marker::PhantomData;
    use val_json::wast_val::WastVal;

    impl Serialize for Params {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: ::serde::Serializer,
        {
            match &self.0 {
                ParamsInternal::Vals { vals } => {
                    let mut seq = serializer.serialize_seq(Some(vals.len()))?; // size must be equal, checked when constructed.
                    for val in vals.iter() {
                        let value = WastVal::try_from(val.clone())
                            .map_err(|err| serde::ser::Error::custom(err.to_string()))?;
                        seq.serialize_element(&value)?;
                    }
                    seq.end()
                }
                ParamsInternal::Empty => serializer.serialize_seq(Some(0))?.end(),
                ParamsInternal::JsonValues(vec) => {
                    let mut seq = serializer.serialize_seq(Some(vec.len()))?;
                    for item in vec {
                        seq.serialize_element(item)?;
                    }
                    seq.end()
                }
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
            let vec: Vec<Value> = deserializer.deserialize_seq(VecVisitor::default())?;
            if vec.is_empty() {
                Ok(Self(ParamsInternal::Empty))
            } else {
                Ok(Self(ParamsInternal::JsonValues(vec)))
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParamsParsingError {
    #[error("error converting type {idx}-th parameter: `{err}`")]
    ParameterTypeError {
        idx: usize,
        err: TypeConversionError,
    },
    #[error(transparent)]
    ParamsDeserializationError(#[from] serde_json::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ParamsFromJsonError {
    #[error("value must be a json array containing function parameters")]
    MustBeArray,
}

impl Params {
    #[must_use]
    pub const fn default() -> Self {
        Self(ParamsInternal::Empty)
    }

    #[must_use]
    pub fn from_wasmtime(vals: Arc<[wasmtime::component::Val]>) -> Self {
        if vals.is_empty() {
            Self::default()
        } else {
            Self(ParamsInternal::Vals { vals })
        }
    }

    pub fn from_json_value(value: Value) -> Result<Self, ParamsFromJsonError> {
        match value {
            Value::Array(vec) if vec.is_empty() => Ok(Self::default()),
            Value::Array(vec) => Ok(Self(ParamsInternal::JsonValues(vec))),
            _ => Err(ParamsFromJsonError::MustBeArray),
        }
    }

    pub fn as_vals(
        &self,
        param_types: Box<[Type]>,
    ) -> Result<Arc<[wasmtime::component::Val]>, ParamsParsingError> {
        match &self.0 {
            ParamsInternal::JsonValues(params) => {
                let param_types = param_types
                    .into_vec()
                    .into_iter()
                    .enumerate()
                    .map(|(idx, ty)| TypeWrapper::try_from(ty).map_err(|err| (idx, err)))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|(idx, err)| ParamsParsingError::ParameterTypeError { idx, err })?;
                Ok(params::deserialize_values(params, param_types.iter())?
                    .into_iter()
                    .map(Val::from)
                    .collect())
            }

            ParamsInternal::Vals { vals, .. } => Ok(vals.clone()),
            ParamsInternal::Empty => Ok(Arc::from([])),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match &self.0 {
            ParamsInternal::JsonValues(vec) => vec.len(),
            ParamsInternal::Vals { vals, .. } => vals.len(),
            ParamsInternal::Empty => 0,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

        #[derive(Debug, thiserror::Error)]
        pub enum ParseError {
            #[error("wrong prefix in `{input}`, expected prefix `{expected}`")]
            WrongPrefix { input: String, expected: String },
            #[error("cannot parse ULID suffix from `{input}`")]
            CannotParseUlid { input: String },
        }

        use super::{PrefixedUlid, Ulid};
        use std::{fmt::Debug, fmt::Display, hash::Hash, marker::PhantomData, str::FromStr};

        impl<T> FromStr for PrefixedUlid<T> {
            type Err = ParseError;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                let prefix = Self::prefix();
                let mut input_chars = input.chars();
                for exp in prefix.chars() {
                    if input_chars.next() != Some(exp) {
                        return Err(ParseError::WrongPrefix {
                            input: input.to_string(),
                            expected: format!("{prefix}_"),
                        });
                    }
                }
                if input_chars.next() != Some('_') {
                    return Err(ParseError::WrongPrefix {
                        input: input.to_string(),
                        expected: format!("{prefix}_"),
                    });
                }
                let Ok(ulid) = Ulid::from_string(input_chars.as_str()) else {
                    return Err(ParseError::CannotParseUlid {
                        input: input.to_string(),
                    });
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
        // pub struct Conf;
        pub struct JoinSet;
        pub struct Run;
        pub struct Delay;
    }

    pub type ExecutorId = PrefixedUlid<prefix::Exr>;
    // pub type ConfigId = PrefixedUlid<prefix::Conf>;
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

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, derive_more::Display,
)]
#[display(fmt = "{component_type}:{config_hash}")]
pub struct ComponentConfigHash {
    pub component_type: ComponentType,
    pub config_hash: ContentDigest,
}

impl ComponentConfigHash {
    #[must_use]
    pub const fn dummy() -> Self {
        Self {
            component_type: ComponentType::WasmActivity,
            config_hash: ContentDigest::empty(),
        }
    }
}

#[derive(Debug, thiserror::Error)]

pub enum ComponentConfigHashParseErrror {
    #[error("cannot parse ComponentConfigHash - delimiter ':' not found")]
    DelimiterNotFound,
    #[error("cannot parse prefix of ComponentConfigHash - {0}")]
    ComponentTypeParseError(#[from] strum::ParseError),
    #[error("cannot parse suffix of ComponentConfigHash - {0}")]
    ContentDigestParseErrror(#[from] ContentDigestParseErrror),
}

impl FromStr for ComponentConfigHash {
    type Err = ComponentConfigHashParseErrror;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (component_type, config_hash) =
            input.split_once(':').ok_or(Self::Err::DelimiterNotFound)?;
        let component_type = component_type.parse()?;
        let config_hash = config_hash.parse()?;
        Ok(Self {
            component_type,
            config_hash,
        })
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    strum::Display,
    strum::EnumString,
    PartialEq,
    Eq,
    Hash,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum HashType {
    Sha256,
}

#[derive(
    Debug,
    Clone,
    derive_more::Display,
    PartialEq,
    Eq,
    Hash,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
#[display(fmt = "{hash_type}:{hash_base16}")]
pub struct ContentDigest {
    pub hash_type: HashType,
    pub hash_base16: StrVariant,
}

impl ContentDigest {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            hash_type: HashType::Sha256,
            hash_base16: StrVariant::Static(
                "0000000000000000000000000000000000000000000000000000000000000000",
            ),
        }
    }

    #[must_use]
    pub fn new(hash_type: HashType, hash_base16: String) -> Self {
        Self {
            hash_type,
            hash_base16: StrVariant::Arc(Arc::from(hash_base16)),
        }
    }
}

#[derive(Debug, thiserror::Error)]

pub enum ContentDigestParseErrror {
    #[error("cannot parse ContentDigest - delimiter ':' not found")]
    DelimiterNotFound,
    #[error("cannot parse ContentDigest - invalid prefix `{hash_type}`")]
    TypeParseError { hash_type: String },
    #[error("cannot parse ContentDigest - invalid suffix length, expected 64 hex digits, got {0}")]
    SuffixLength(usize),
    #[error(
        "cannot parse ContentDigest - suffix must be hex-encoded, got invalid character `{0}`"
    )]
    SuffixInvalid(char),
}

impl FromStr for ContentDigest {
    type Err = ContentDigestParseErrror;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (hash_type, hash_base16) = input.split_once(':').ok_or(Self::Err::DelimiterNotFound)?;
        let hash_type =
            HashType::from_str(hash_type).map_err(|_err| Self::Err::TypeParseError {
                hash_type: hash_type.to_string(),
            })?;
        if hash_base16.len() != 64 {
            return Err(Self::Err::SuffixLength(hash_base16.len()));
        }
        if let Some(invalid) = hash_base16.chars().find(|c| !c.is_ascii_hexdigit()) {
            return Err(Self::Err::SuffixInvalid(invalid));
        }
        Ok(Self {
            hash_type,
            hash_base16: StrVariant::Arc(Arc::from(hash_base16)),
        })
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    strum::Display,
    PartialEq,
    Eq,
    strum::EnumString,
    Hash,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum ComponentType {
    WasmActivity,
    WasmWorkflow,
}

pub type ReturnType = Option<TypeWrapper>;

#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct ParameterTypes(pub Vec<(String, TypeWrapper)>);

impl ParameterTypes {
    #[must_use]
    pub fn empty() -> Self {
        ParameterTypes(Vec::new())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl Debug for ParameterTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(")?;
        let mut iter = self.0.iter().peekable();
        while let Some((name, ty)) = iter.next() {
            write!(f, "{name}: {ty:?}")?; // TODO: implement Display for TypeWrapper
            if iter.peek().is_some() {
                write!(f, ", ")?;
            }
        }
        write!(f, ")")
    }
}

#[async_trait]
pub trait FunctionRegistry: Send + Sync {
    async fn get_by_exported_function(
        &self,
        ffqn: &FunctionFqn,
    ) -> Option<(FunctionMetadata, ComponentConfigHash)>;
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
