use ::serde::{Deserialize, Serialize};
use arbitrary::Arbitrary;
use assert_matches::assert_matches;
use async_trait::async_trait;
use derivative::Derivative;
pub use indexmap;
use indexmap::IndexMap;
use opentelemetry::propagation::{Extractor, Injector};
pub use prefixed_ulid::ExecutionId;
use prefixed_ulid::ExecutionIdParseError;
use serde_json::Value;
use std::{
    borrow::Borrow,
    fmt::{Debug, Display},
    hash::Hash,
    marker::PhantomData,
    ops::Deref,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use storage::{PendingStateFinishedError, PendingStateFinishedResultKind};
use tracing::Span;
use val_json::{
    type_wrapper::{TypeConversionError, TypeWrapper},
    wast_val::{WastVal, WastValWithType},
    wast_val_ser::params,
};
use wasmtime::component::{Type, Val};

#[cfg(feature = "rusqlite")]
mod rusqlite_ext;
pub mod storage;

pub const NAMESPACE_OBELISK: &str = "obelisk";
pub const SUFFIX_PKG_EXT: &str = "-obelisk-ext";

pub type FinishedExecutionResult = Result<SupportedFunctionReturnValue, FinishedExecutionError>;

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FinishedExecutionError {
    // Activity only
    #[error("permanent timeout")]
    PermanentTimeout,
    // Workflow only
    #[error("unhandled child execution error {child_execution_id}")]
    UnhandledChildExecutionError {
        child_execution_id: ExecutionId,
        root_cause_id: ExecutionId,
    },
    #[error("permanent failure: {reason_full}")]
    PermanentFailure {
        // Exists just for extracting reason of an activity trap, to avoid "activity trap: " prefix.
        reason_inner: String,
        // Contains reason_inner embedded in the error message
        reason_full: String,
        kind: PermanentFailureKind,
        detail: Option<String>,
    },
}
impl FinishedExecutionError {
    #[must_use]
    pub fn as_pending_state_finished_error(&self) -> PendingStateFinishedError {
        match self {
            FinishedExecutionError::PermanentTimeout => PendingStateFinishedError::Timeout,
            FinishedExecutionError::UnhandledChildExecutionError { .. } => {
                PendingStateFinishedError::UnhandledChildExecutionError
            }
            FinishedExecutionError::PermanentFailure { .. } => {
                PendingStateFinishedError::ExecutionFailure
            }
        }
    }
}

#[derive(Debug, Clone, Copy, derive_more::Display, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermanentFailureKind {
    /// Applicable to Workflow
    NondeterminismDetected,
    /// Applicable to Workflow, Activity
    ParamsParsingError,
    /// Applicable to Workflow, Activity
    CannotInstantiate,
    /// Applicable to Workflow, Activity
    ResultParsingError,
    /// Applicable to Workflow
    ImportedFunctionCallError,
    /// Applicable to Activity
    ActivityTrap,
    /// Applicable to Workflow
    WorkflowTrap,
    /// Applicable to Workflow
    JoinSetNameConflict,
    /// Applicable to webhook endpoint
    WebhookEndpointError,
}

#[derive(Debug, Clone, Copy, derive_more::Display, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrapKind {
    #[display("trap")]
    Trap,
    #[display("post_return_trap")]
    PostReturnTrap,
}

#[derive(Clone, Eq, derive_more::Display)]
pub enum StrVariant {
    Static(&'static str),
    Arc(Arc<str>),
}

impl StrVariant {
    #[must_use]
    pub const fn empty() -> StrVariant {
        StrVariant::Static("")
    }
}

impl From<String> for StrVariant {
    fn from(value: String) -> Self {
        StrVariant::Arc(Arc::from(value))
    }
}

impl From<&'static str> for StrVariant {
    fn from(value: &'static str) -> Self {
        StrVariant::Static(value)
    }
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

impl AsRef<str> for StrVariant {
    fn as_ref(&self) -> &str {
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

    impl Visitor<'_> for StrVariantVisitor {
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
#[display("{value}")]
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

impl<T> From<String> for Name<T> {
    fn from(value: String) -> Self {
        Self::new_arc(Arc::from(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PkgFqn {
    pub namespace: String,
    pub package_name: String,
    pub version: Option<String>,
}
impl Display for PkgFqn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let PkgFqn {
            namespace,
            package_name,
            version,
        } = self;
        if let Some(version) = version {
            write!(f, "{namespace}:{package_name}@{version}")
        } else {
            write!(f, "{namespace}:{package_name}")
        }
    }
}

impl PkgFqn {
    #[must_use]
    pub fn is_extension(&self) -> bool {
        self.package_name.ends_with(SUFFIX_PKG_EXT)
    }

    #[must_use]
    pub fn package_strip_extension_suffix(&self) -> Option<&str> {
        self.package_name.as_str().strip_suffix(SUFFIX_PKG_EXT)
    }

    #[must_use]
    pub fn is_namespace_obelisk(&self) -> bool {
        self.namespace == NAMESPACE_OBELISK
    }

    #[must_use]
    pub fn ifc_fqn_name(&self, ifc_name: &str) -> IfcFqnName {
        IfcFqnName::from_parts(
            &self.namespace,
            &self.package_name,
            ifc_name,
            self.version.as_deref(),
        )
    }
}

#[derive(Hash, Clone, PartialEq, Eq)]
pub struct IfcFqnMarker;

pub type IfcFqnName = Name<IfcFqnMarker>; // namespace:name/ifc_name OR namespace:name/ifc_name@version

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
    pub fn pkg_fqn_name(&self) -> PkgFqn {
        let (namespace, rest) = self.deref().split_once(':').unwrap();
        let (package_name, rest) = rest.split_once('/').unwrap();
        let version = rest.split_once('@').map(|(_, version)| version);
        PkgFqn {
            namespace: namespace.to_string(),
            package_name: package_name.to_string(),
            version: version.map(std::string::ToString::to_string),
        }
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

    #[must_use]
    pub fn is_extension(&self) -> bool {
        self.package_name().ends_with(SUFFIX_PKG_EXT)
    }

    #[must_use]
    pub fn package_strip_extension_suffix(&self) -> Option<&str> {
        self.package_name().strip_suffix(SUFFIX_PKG_EXT)
    }

    #[must_use]
    pub fn is_namespace_obelisk(&self) -> bool {
        self.namespace() == NAMESPACE_OBELISK
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

    pub fn try_from_tuple(
        ifc_fqn: &str,
        function_name: &str,
    ) -> Result<Self, FunctionFqnParseError> {
        if ifc_fqn.contains('.') || function_name.contains('.') {
            Err(FunctionFqnParseError::DelimiterFoundMoreThanOnce)
        } else {
            Ok(Self::new_arc(Arc::from(ifc_fqn), Arc::from(function_name)))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FunctionFqnParseError {
    #[error("delimiter `.` not found")]
    DelimiterNotFound,
    #[error("delimiter `.` found more than once")]
    DelimiterFoundMoreThanOnce,
}

impl FromStr for FunctionFqn {
    type Err = FunctionFqnParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((ifc_fqn, function_name)) = s.split_once('.') {
            if function_name.contains('.') {
                Err(FunctionFqnParseError::DelimiterFoundMoreThanOnce)
            } else {
                Ok(Self::new_arc(Arc::from(ifc_fqn), Arc::from(function_name)))
            }
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
pub enum SupportedFunctionReturnValue {
    None,
    // Top level type is result<_,_> with Err variant
    FallibleResultErr(WastValWithType),
    // All other top level types
    InfallibleOrResultOk(WastValWithType),
}

#[derive(Debug, thiserror::Error)]
pub enum ResultParsingError {
    #[error("result cannot be parsed, multi-value results are not supported")]
    MultiValue,
    #[error("result cannot be parsed, {0}")]
    TypeConversionError(#[from] val_json::type_wrapper::TypeConversionError),
    #[error("result cannot be parsed, {0}")]
    ValueConversionError(#[from] val_json::wast_val::WastValConversionError),
}

impl SupportedFunctionReturnValue {
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
                WastVal::Result(Err(_)) => Ok(Self::FallibleResultErr(WastValWithType {
                    r#type,
                    value: val,
                })),
                _ => Ok(Self::InfallibleOrResultOk(WastValWithType {
                    r#type,
                    value: val,
                })),
            }
        } else {
            Err(ResultParsingError::MultiValue)
        }
    }

    #[cfg(feature = "test")]
    #[must_use]
    pub fn fallible_err(&self) -> Option<Option<&WastVal>> {
        match self {
            SupportedFunctionReturnValue::FallibleResultErr(WastValWithType {
                value: WastVal::Result(Err(err)),
                ..
            }) => Some(err.as_deref()),
            _ => None,
        }
    }

    #[cfg(feature = "test")]
    #[must_use]
    pub fn fallible_ok(&self) -> Option<Option<&WastVal>> {
        match self {
            SupportedFunctionReturnValue::InfallibleOrResultOk(WastValWithType {
                value: WastVal::Result(Ok(ok)),
                ..
            }) => Some(ok.as_deref()),
            _ => None,
        }
    }

    #[cfg(feature = "test")]
    #[must_use]
    pub fn val_type(&self) -> Option<&TypeWrapper> {
        match self {
            SupportedFunctionReturnValue::None => None,
            SupportedFunctionReturnValue::FallibleResultErr(v)
            | SupportedFunctionReturnValue::InfallibleOrResultOk(v) => Some(&v.r#type),
        }
    }

    #[must_use]
    pub fn value(&self) -> Option<&WastVal> {
        match self {
            SupportedFunctionReturnValue::None => None,
            SupportedFunctionReturnValue::FallibleResultErr(v)
            | SupportedFunctionReturnValue::InfallibleOrResultOk(v) => Some(&v.value),
        }
    }

    #[must_use]
    pub fn into_value(self) -> Option<WastVal> {
        match self {
            SupportedFunctionReturnValue::None => None,
            SupportedFunctionReturnValue::FallibleResultErr(v)
            | SupportedFunctionReturnValue::InfallibleOrResultOk(v) => Some(v.value),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            SupportedFunctionReturnValue::None => 0,
            _ => 1,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::None)
    }

    #[must_use]
    pub fn as_pending_state_finished_result(&self) -> PendingStateFinishedResultKind {
        if let SupportedFunctionReturnValue::FallibleResultErr(_) = self {
            PendingStateFinishedResultKind(Err(PendingStateFinishedError::FallibleError))
        } else {
            PendingStateFinishedResultKind(Ok(()))
        }
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

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum FunctionExtension {
    Submit,
    AwaitNext,
    Schedule,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FunctionMetadata {
    pub ffqn: FunctionFqn,
    pub parameter_types: ParameterTypes,
    pub return_type: Option<ReturnType>,
    pub extension: Option<FunctionExtension>,
    pub submittable: bool,
}
impl Display for FunctionMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{ffqn}: func{params}",
            ffqn = self.ffqn,
            params = self.parameter_types
        )?;
        if let Some(return_type) = &self.return_type {
            write!(f, " -> {return_type}")?;
        }
        Ok(())
    }
}

pub mod serde_params {
    use crate::{Params, ParamsInternal};
    use serde::de::{SeqAccess, Visitor};
    use serde::ser::SerializeSeq;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
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

    pub struct VecVisitor;

    impl<'de> Visitor<'de> for VecVisitor {
        type Value = Vec<Value>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a sequence of `Value`")
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
            let vec: Vec<Value> = deserializer.deserialize_seq(VecVisitor)?;
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
    #[error("parameters cannot be parsed, cannot convert type of {idx}-th parameter")]
    ParameterTypeError {
        idx: usize,
        err: TypeConversionError,
    },
    #[error("parameters cannot be deserialized: {0}")]
    ParamsDeserializationError(serde_json::Error),
    #[error("parameter cardinality mismatch, expected: {expected}, specified: {specified}")]
    ParameterCardinalityMismatch { expected: usize, specified: usize },
}

impl ParamsParsingError {
    #[must_use]
    pub fn detail(&self) -> Option<String> {
        match self {
            ParamsParsingError::ParameterTypeError { err, .. } => Some(format!("{err:?}")),
            ParamsParsingError::ParamsDeserializationError(err) => Some(format!("{err:?}")),
            ParamsParsingError::ParameterCardinalityMismatch { .. } => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParamsFromJsonError {
    #[error("value must be a json array containing function parameters")]
    MustBeArray,
}

impl Params {
    #[must_use]
    pub const fn empty() -> Self {
        Self(ParamsInternal::Empty)
    }

    #[must_use]
    pub fn from_wasmtime(vals: Arc<[wasmtime::component::Val]>) -> Self {
        if vals.is_empty() {
            Self::empty()
        } else {
            Self(ParamsInternal::Vals { vals })
        }
    }

    #[must_use]
    pub fn from_json_values(vec: Vec<Value>) -> Self {
        if vec.is_empty() {
            Self::empty()
        } else {
            Self(ParamsInternal::JsonValues(vec))
        }
    }

    pub fn typecheck<'a>(
        &self,
        param_types: impl ExactSizeIterator<Item = &'a TypeWrapper>,
    ) -> Result<(), ParamsParsingError> {
        if param_types.len() != self.len() {
            return Err(ParamsParsingError::ParameterCardinalityMismatch {
                expected: param_types.len(),
                specified: self.len(),
            });
        }
        match &self.0 {
            ParamsInternal::Vals { .. } /* already typechecked */ | ParamsInternal::Empty => {}
            ParamsInternal::JsonValues(params) => {
                params::deserialize_values(params, param_types)
                .map_err(ParamsParsingError::ParamsDeserializationError)?;
            }
        }
        Ok(())
    }

    pub fn as_vals(
        &self,
        param_types: Box<[(String, Type)]>,
    ) -> Result<Arc<[wasmtime::component::Val]>, ParamsParsingError> {
        if param_types.len() != self.len() {
            return Err(ParamsParsingError::ParameterCardinalityMismatch {
                expected: param_types.len(),
                specified: self.len(),
            });
        }
        match &self.0 {
            ParamsInternal::JsonValues(json_vec) => {
                let param_types = param_types
                    .into_vec()
                    .into_iter()
                    .enumerate()
                    .map(|(idx, (_param_name, ty))| {
                        TypeWrapper::try_from(ty).map_err(|err| (idx, err))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|(idx, err)| ParamsParsingError::ParameterTypeError { idx, err })?;
                Ok(params::deserialize_values(json_vec, param_types.iter())
                    .map_err(ParamsParsingError::ParamsDeserializationError)?
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
    use derivative::Derivative;
    use serde_with::{DeserializeFromStr, SerializeDisplay};
    use smallvec::SmallVec;
    use std::{
        fmt::{Debug, Display},
        marker::PhantomData,
        num::ParseIntError,
        str::FromStr,
    };
    use ulid::Ulid;

    #[derive(derive_more::Display, SerializeDisplay, DeserializeFromStr, Derivative)]
    #[display("{}_{ulid}", Self::prefix())]
    #[derivative(Clone(bound = ""))]
    #[derivative(Copy(bound = ""))]
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
        #[expect(clippy::cast_possible_truncation)]
        pub fn random_part(&self) -> u64 {
            self.ulid.random() as u64
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum PrefixedUlidParseError {
        #[error("wrong prefix in `{input}`, expected prefix `{expected}`")]
        WrongPrefix { input: String, expected: String },
        #[error("cannot parse ULID suffix from `{input}`")]
        CannotParseUlid { input: String },
    }

    mod impls {
        use super::{PrefixedUlid, PrefixedUlidParseError, Ulid};
        use std::{fmt::Debug, fmt::Display, hash::Hash, marker::PhantomData, str::FromStr};

        impl<T> FromStr for PrefixedUlid<T> {
            type Err = PrefixedUlidParseError;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                let prefix = Self::prefix();
                let mut input_chars = input.chars();
                for exp in prefix.chars() {
                    if input_chars.next() != Some(exp) {
                        return Err(PrefixedUlidParseError::WrongPrefix {
                            input: input.to_string(),
                            expected: format!("{prefix}_"),
                        });
                    }
                }
                if input_chars.next() != Some('_') {
                    return Err(PrefixedUlidParseError::WrongPrefix {
                        input: input.to_string(),
                        expected: format!("{prefix}_"),
                    });
                }
                let Ok(ulid) = Ulid::from_string(input_chars.as_str()) else {
                    return Err(PrefixedUlidParseError::CannotParseUlid {
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
        pub struct Run;
        pub struct Delay;
    }

    pub type ExecutorId = PrefixedUlid<prefix::Exr>;
    pub type TopLevelExecutionId = PrefixedUlid<prefix::E>;
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

    type SuffixVec = SmallVec<[u64; 5]>;

    #[derive(Hash, PartialEq, Eq, PartialOrd, Ord, SerializeDisplay, DeserializeFromStr, Clone)]
    pub struct ExecutionId(ExecutionIdInner);

    #[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
    enum ExecutionIdInner {
        TopLevel(TopLevelExecutionId),
        Derived(TopLevelExecutionId, SuffixVec),
    }

    impl ExecutionId {
        #[must_use]
        pub fn generate() -> Self {
            ExecutionId(ExecutionIdInner::TopLevel(PrefixedUlid::generate()))
        }

        fn get_top_level(&self) -> TopLevelExecutionId {
            match &self.0 {
                ExecutionIdInner::TopLevel(prefixed_ulid) => *prefixed_ulid,
                ExecutionIdInner::Derived(prefixed_uild, _) => *prefixed_uild,
            }
        }

        #[must_use]
        pub fn timestamp_part(&self) -> u64 {
            self.get_top_level().timestamp_part()
        }

        #[must_use]
        pub fn random_part(&self) -> u64 {
            self.get_top_level().random_part()
        }

        #[must_use]
        pub const fn from_parts(timestamp_ms: u64, random_part: u128) -> Self {
            ExecutionId(ExecutionIdInner::TopLevel(TopLevelExecutionId::from_parts(
                timestamp_ms,
                random_part,
            )))
        }

        #[must_use]
        pub fn increment(&self) -> Self {
            match &self.0 {
                ExecutionIdInner::TopLevel(_) => {
                    // FIXME: Make this a compile error
                    panic!("called `increment` on the top level ExecutionId")
                }
                ExecutionIdInner::Derived(top_level, suffix_vec) => {
                    let mut suffix_vec = suffix_vec.clone();
                    *suffix_vec
                        .last_mut()
                        .expect("empty smallvec is never constructed") += 1;
                    ExecutionId(ExecutionIdInner::Derived(*top_level, suffix_vec))
                }
            }
        }
        #[must_use]
        pub fn next_level(&self) -> Self {
            match &self.0 {
                ExecutionIdInner::TopLevel(top_level) => {
                    let mut suffix: SuffixVec = SmallVec::new();
                    suffix.push(0);
                    ExecutionId(ExecutionIdInner::Derived(*top_level, suffix))
                }
                ExecutionIdInner::Derived(top_level, suffix) => {
                    let mut suffix = suffix.clone();
                    suffix.push(0);
                    ExecutionId(ExecutionIdInner::Derived(*top_level, suffix))
                }
            }
        }

        fn display_or_debug(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match &self.0 {
                ExecutionIdInner::TopLevel(top_level) => Display::fmt(top_level, f),
                ExecutionIdInner::Derived(top_level, suffix) => {
                    write!(f, "{top_level}{EXECUTION_ID_INFIX}")?;
                    for (idx, part) in suffix.iter().enumerate() {
                        if idx > 0 {
                            write!(f, "{EXECUTION_ID_INFIX}")?;
                        }
                        write!(f, "{part}")?;
                    }
                    Ok(())
                }
            }
        }
    }

    const EXECUTION_ID_INFIX: char = '.';

    #[derive(Debug, thiserror::Error)]
    pub enum ExecutionIdParseError {
        #[error(transparent)]
        PrefixedUlidParseError(#[from] PrefixedUlidParseError),
        #[error(transparent)]
        ParseIntError(#[from] ParseIntError),
    }

    impl FromStr for ExecutionId {
        type Err = ExecutionIdParseError;

        fn from_str(input: &str) -> Result<Self, Self::Err> {
            if let Some((prefix, suffix)) = input.split_once(EXECUTION_ID_INFIX) {
                let top_level = PrefixedUlid::from_str(prefix)?;
                let suffix = suffix
                    .split(EXECUTION_ID_INFIX)
                    .map(u64::from_str)
                    .collect::<Result<SuffixVec, _>>()?;
                Ok(ExecutionId(ExecutionIdInner::Derived(top_level, suffix)))
            } else {
                Ok(ExecutionId(ExecutionIdInner::TopLevel(
                    PrefixedUlid::from_str(input)?,
                )))
            }
        }
    }

    impl Debug for ExecutionId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.display_or_debug(f)
        }
    }

    impl Display for ExecutionId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.display_or_debug(f)
        }
    }

    impl<'a> Arbitrary<'a> for ExecutionId {
        fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(ExecutionId(ExecutionIdInner::TopLevel(
                PrefixedUlid::arbitrary(u)?,
            )))
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    derive_more::Display,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
#[non_exhaustive] // force using the constructor as much as possible due to validation
#[display("{execution_id}{JOIN_SET_ID_INFIX}{kind}{JOIN_SET_ID_INFIX}{name}")]
pub struct JoinSetId {
    pub execution_id: ExecutionId,
    pub kind: JoinSetKind,
    pub name: StrVariant,
}

impl JoinSetId {
    pub fn new(
        execution_id: ExecutionId,
        kind: JoinSetKind,
        name: StrVariant,
    ) -> Result<Self, InvalidNameError<JoinSetId>> {
        Ok(Self {
            execution_id,
            kind,
            name: check_name(name, CHARSET_EXTRA_JSON_SET)?,
        })
    }

    pub fn random_name(
        rng: &mut rand::rngs::StdRng,
        min_length: u16,
        max_length_exclusive: u16,
    ) -> String {
        random_string(rng, min_length, max_length_exclusive, CHARSET_JOIN_SET_NAME)
    }
}
const CHARSET_JOIN_SET_NAME: &str =
    const_format::concatcp!(CHARSET_ALPHANUMERIC, CHARSET_EXTRA_JSON_SET);

pub const CHARSET_ALPHANUMERIC: &str =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
pub fn random_string(
    rng: &mut rand::rngs::StdRng,
    min_length: u16,
    max_length_exclusive: u16,
    charset: &'static str,
) -> String {
    let length_inclusive = rand::Rng::gen_range(rng, min_length..max_length_exclusive);
    (0..=length_inclusive)
        .map(|_| {
            let idx = rand::Rng::gen_range(rng, 0..charset.len());
            charset.chars().nth(idx).expect("idx is < charset.len()")
        })
        .collect()
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    derive_more::Display,
    Serialize,
    Deserialize,
    strum::EnumIter,
    Arbitrary,
)]
#[display("{}", self.as_code())]
pub enum JoinSetKind {
    OneOff,
    UserDefinedNamed,
    UserDefinedGenerated,
}
impl JoinSetKind {
    fn as_code(&self) -> &'static str {
        match self {
            JoinSetKind::OneOff => "o",
            JoinSetKind::UserDefinedNamed => "n",
            JoinSetKind::UserDefinedGenerated => "g",
        }
    }
}
impl FromStr for JoinSetKind {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use strum::IntoEnumIterator;
        Self::iter()
            .find(|variant| s == variant.as_code())
            .ok_or("unknown join set kind")
    }
}

const JOIN_SET_ID_INFIX: char = ':';
const CHARSET_EXTRA_JSON_SET: &str = "_-/";

impl FromStr for JoinSetId {
    type Err = JoinSetIdParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = input.splitn(3, JOIN_SET_ID_INFIX).collect();
        if parts.len() != 3 {
            return Err(JoinSetIdParseError::WrongParts);
        }
        let execution_id = parts[0].parse()?;
        let kind = parts[1]
            .parse()
            .map_err(JoinSetIdParseError::JoinSetKindParseError)?;
        let name = parts[2]; // checked on the next line using the constructor
        Ok(JoinSetId::new(
            execution_id,
            kind,
            StrVariant::from(name.to_string()),
        )?)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JoinSetIdParseError {
    #[error("join set must consist of three parts separated by {JOIN_SET_ID_INFIX} ")]
    WrongParts,
    #[error("cannot parse join set id's execution id - {0}")]
    ExecutionIdParseError(#[from] ExecutionIdParseError),
    #[error("cannot parse join set kind - {0}")]
    JoinSetKindParseError(&'static str),
    #[error("cannot parse join set id - {0}")]
    InvalidName(#[from] InvalidNameError<JoinSetId>),
}

impl<'a> Arbitrary<'a> for JoinSetId {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let name: String = {
            let length_inclusive = u.int_in_range(0..=10).unwrap();
            (0..=length_inclusive)
                .map(|_| {
                    let idx = u.choose_index(CHARSET_JOIN_SET_NAME.len()).unwrap();
                    CHARSET_JOIN_SET_NAME
                        .chars()
                        .nth(idx)
                        .expect("idx is < charset.len()")
                })
                .collect()
        };

        Ok(JoinSetId::new(
            ExecutionId::arbitrary(u)?,
            JoinSetKind::UserDefinedGenerated,
            StrVariant::from(name),
        )
        .unwrap())
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
    ActivityWasm,
    Workflow,
    WebhookEndpoint,
}

#[derive(
    derive_more::Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
    derive_more::Display,
)]
#[display("{component_type}:{name}:{hash}")]
#[debug("{}", self)]
#[non_exhaustive] // force using the constructor as much as possible due to validation
pub struct ComponentId {
    pub component_type: ComponentType,
    pub name: StrVariant,
    pub hash: StrVariant,
}
impl ComponentId {
    pub fn new(
        component_type: ComponentType,
        name: StrVariant,
        hash: StrVariant,
    ) -> Result<Self, InvalidNameError<Self>> {
        Ok(Self {
            component_type,
            name: check_name(name, "_")?,
            hash,
        })
    }

    #[must_use]
    pub const fn dummy_activity() -> Self {
        Self {
            component_type: ComponentType::ActivityWasm,
            name: StrVariant::empty(),
            hash: StrVariant::empty(),
        }
    }

    #[must_use]
    pub const fn dummy_workflow() -> ComponentId {
        ComponentId {
            component_type: ComponentType::Workflow,
            name: StrVariant::empty(),
            hash: StrVariant::empty(),
        }
    }
}

pub fn check_name<T>(
    name: StrVariant,
    special: &'static str,
) -> Result<StrVariant, InvalidNameError<T>> {
    if let Some(invalid) = name
        .as_ref()
        .chars()
        .find(|c| !c.is_ascii_alphanumeric() && !special.contains(*c))
    {
        Err(InvalidNameError::<T> {
            invalid,
            name: name.as_ref().to_string(),
            special,
            phantom_data: PhantomData,
        })
    } else {
        Ok(name)
    }
}
#[derive(Debug, thiserror::Error)]
#[error(
    "name of {} `{name}` contains invalid character `{invalid}`, must only contain alphanumeric characters and following characters {special}",
    std::any::type_name::<T>().rsplit("::").next().unwrap()
)]
pub struct InvalidNameError<T> {
    invalid: char,
    name: String,
    special: &'static str,
    phantom_data: PhantomData<T>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigIdParseError {
    #[error("cannot parse ComponentConfigHash - delimiter ':' not found")]
    DelimiterNotFound,
    #[error("cannot parse prefix of ComponentConfigHash - {0}")]
    ComponentTypeParseError(#[from] strum::ParseError),
    #[error("cannot parse suffix of ComponentConfigHash - {0}")]
    ContentDigestParseErrror(#[from] DigestParseErrror),
}

impl FromStr for ComponentId {
    type Err = ConfigIdParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (component_type, input) = input.split_once(':').ok_or(Self::Err::DelimiterNotFound)?;
        let (name, hash) = input.split_once(':').ok_or(Self::Err::DelimiterNotFound)?;
        let component_type = component_type.parse()?;
        Ok(Self {
            component_type,
            name: StrVariant::from(name.to_string()),
            hash: StrVariant::from(hash.to_string()),
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
    derive_more::FromStr,
    derive_more::Deref,
    PartialEq,
    Eq,
    Hash,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
pub struct ContentDigest(Digest);

impl ContentDigest {
    #[must_use]
    pub fn new(hash_type: HashType, hash_base16: String) -> Self {
        Self(Digest::new(hash_type, hash_base16))
    }
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
#[display("{hash_type}:{hash_base16}")]
pub struct Digest {
    hash_type: HashType,
    hash_base16: StrVariant,
}
impl Digest {
    #[must_use]
    pub fn new(hash_type: HashType, hash_base16: String) -> Self {
        Self {
            hash_type,
            hash_base16: StrVariant::Arc(Arc::from(hash_base16)),
        }
    }

    #[must_use]
    pub fn hash_type(&self) -> HashType {
        self.hash_type
    }

    #[must_use]
    pub fn digest_base16(&self) -> &str {
        &self.hash_base16
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DigestParseErrror {
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

impl FromStr for Digest {
    type Err = DigestParseErrror;

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
    Debug, Clone, serde::Serialize, serde::Deserialize, Derivative, Eq, derive_more::Display,
)]
#[derivative(PartialEq)]
#[display("{wit_type}")]
pub struct ReturnType {
    pub type_wrapper: TypeWrapper,
    #[derivative(PartialEq = "ignore")]
    pub wit_type: StrVariant,
}

#[derive(
    Debug, Clone, serde::Serialize, serde::Deserialize, Derivative, Eq, derive_more::Display,
)]
#[derivative(PartialEq)]
#[display("{name}: {wit_type}")]
pub struct ParameterType {
    pub type_wrapper: TypeWrapper,
    #[derivative(PartialEq = "ignore")]
    pub name: StrVariant,
    #[derivative(PartialEq = "ignore")]
    pub wit_type: StrVariant,
}

#[derive(
    Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default, derive_more::Deref,
)]
pub struct ParameterTypes(pub Vec<ParameterType>);

impl Debug for ParameterTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(")?;
        let mut iter = self.0.iter().peekable();
        while let Some(p) = iter.next() {
            write!(f, "{p:?}")?;
            if iter.peek().is_some() {
                write!(f, ", ")?;
            }
        }
        write!(f, ")")
    }
}

impl Display for ParameterTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(")?;
        let mut iter = self.0.iter().peekable();
        while let Some(p) = iter.next() {
            write!(f, "{p}")?;
            if iter.peek().is_some() {
                write!(f, ", ")?;
            }
        }
        write!(f, ")")
    }
}

#[derive(Debug, Clone)]
pub struct PackageIfcFns {
    pub ifc_fqn: IfcFqnName,
    pub extension: bool,
    pub fns: IndexMap<FnName, FunctionMetadata>,
}

#[derive(Debug, Clone, Copy)]
pub struct ComponentRetryConfig {
    pub max_retries: u32,
    pub retry_exp_backoff: Duration,
}

/// Implementation must not return `-obelisk-ext` suffix in any package name, nor `obelisk` namespace.
#[async_trait]
pub trait FunctionRegistry: Send + Sync {
    async fn get_by_exported_function(
        &self,
        ffqn: &FunctionFqn,
    ) -> Option<(FunctionMetadata, ComponentId, ComponentRetryConfig)>;

    fn all_exports(&self) -> &[PackageIfcFns];
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, derive_more::Display, PartialEq, Eq)]
#[display("{_0:?}")]
pub struct ExecutionMetadata(Option<hashbrown::HashMap<String, String>>);

impl ExecutionMetadata {
    const LINKED_KEY: &str = "obelisk-tracing-linked";
    #[must_use]
    pub const fn empty() -> Self {
        // Remove `Optional` when const hashmap creation is allowed - https://github.com/rust-lang/rust/issues/123197
        Self(None)
    }

    #[must_use]
    pub fn from_parent_span(less_specific: &Span) -> Self {
        ExecutionMetadata::create(less_specific, false)
    }

    #[must_use]
    pub fn from_linked_span(less_specific: &Span) -> Self {
        ExecutionMetadata::create(less_specific, true)
    }

    /// Attempt to use `Span::current()` to fill the trace and parent span.
    /// If that fails, which can happen due to interference with e.g.
    /// the stdout layer of the subscriber, use the `span` which is guaranteed
    /// to be on info level.
    #[must_use]
    #[expect(clippy::items_after_statements)]
    fn create(span: &Span, link_marker: bool) -> Self {
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;
        let mut metadata = Self(Some(hashbrown::HashMap::default()));
        let mut metadata_view = ExecutionMetadataInjectorView {
            metadata: &mut metadata,
        };
        // inject the current context through the amqp headers
        fn inject(s: &Span, metadata_view: &mut ExecutionMetadataInjectorView) {
            opentelemetry::global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&s.context(), metadata_view);
            });
        }
        inject(&Span::current(), &mut metadata_view);
        if metadata_view.is_empty() {
            // The subscriber sent us a current span that is actually disabled
            inject(span, &mut metadata_view);
        }
        if link_marker {
            metadata_view.set(Self::LINKED_KEY, String::new());
        }
        metadata
    }

    pub fn enrich(&self, span: &Span) {
        use opentelemetry::trace::TraceContextExt as _;
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;

        let metadata_view = ExecutionMetadataExtractorView { metadata: self };
        let otel_context = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&metadata_view)
        });
        if metadata_view.get(Self::LINKED_KEY).is_some() {
            let linked_span_context = otel_context.span().span_context().clone();
            span.add_link(linked_span_context);
        } else {
            span.set_parent(otel_context);
        }
    }
}

struct ExecutionMetadataInjectorView<'a> {
    metadata: &'a mut ExecutionMetadata,
}

impl ExecutionMetadataInjectorView<'_> {
    fn is_empty(&self) -> bool {
        self.metadata
            .0
            .as_ref()
            .is_some_and(hashbrown::HashMap::is_empty)
    }
}

impl opentelemetry::propagation::Injector for ExecutionMetadataInjectorView<'_> {
    fn set(&mut self, key: &str, value: String) {
        let key = format!("tracing:{key}");
        let map = if let Some(map) = self.metadata.0.as_mut() {
            map
        } else {
            self.metadata.0 = Some(hashbrown::HashMap::new());
            assert_matches!(&mut self.metadata.0, Some(ref mut map) => map)
        };
        map.insert(key, value);
    }
}

struct ExecutionMetadataExtractorView<'a> {
    metadata: &'a ExecutionMetadata,
}

impl opentelemetry::propagation::Extractor for ExecutionMetadataExtractorView<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.metadata
            .0
            .as_ref()
            .and_then(|map| map.get(&format!("tracing:{key}")))
            .map(std::string::String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        match &self.metadata.0.as_ref() {
            Some(map) => map
                .keys()
                .filter_map(|key| key.strip_prefix("tracing:"))
                .collect(),
            None => vec![],
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{prefixed_ulid::ExecutorId, ExecutionId, StrVariant};
    use std::{
        hash::{DefaultHasher, Hash, Hasher},
        sync::Arc,
    };

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
        let generated = ExecutorId::generate();
        let str = generated.to_string();
        let parsed = str.parse().unwrap();
        assert_eq!(generated, parsed);
    }

    #[test]
    fn execution_id_parsing_top_level() {
        let generated = ExecutionId::generate();
        let str = generated.to_string();
        let parsed = str.parse().unwrap();
        assert_eq!(generated, parsed);
    }

    #[test]
    #[should_panic(expected = "called `increment` on the top level ExecutionId")]
    fn execution_id_increment_on_top_level_should_panic() {
        let top_level = ExecutionId::generate();
        let _ = top_level.increment();
    }

    #[test]
    fn execution_id_next_level_should_work() {
        let top_level = ExecutionId::generate();
        let first_child = top_level.next_level();
        assert_eq!(format!("{top_level}.0"), first_child.to_string());
    }

    #[test]
    fn execution_id_increment_twice() {
        let top_level = ExecutionId::generate();
        let second_child = top_level.next_level().increment();
        assert_eq!(format!("{top_level}.1"), second_child.to_string());
    }

    #[test]
    fn execution_id_next_level_twice() {
        let top_level = ExecutionId::generate();
        let second_child = top_level.next_level().next_level().increment();
        assert_eq!(format!("{top_level}.0.1"), second_child.to_string());
    }

    #[test]
    fn execution_id_parsing_derived() {
        let generated = ExecutionId::generate().next_level().increment();
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

    #[cfg(madsim)]
    #[tokio::test]
    async fn join_set_serde_should_be_consistent() {
        use crate::{JoinSetId, JoinSetKind};
        use rand::{rngs::StdRng, SeedableRng as _};

        let execution_id = ExecutionId::generate();
        let raw_data: Vec<_> = (0..10).map(|_| madsim::rand::random::<u8>()).collect();
        let mut unstructured = arbitrary::Unstructured::new(&raw_data);
        let kind: JoinSetKind = unstructured.arbitrary().unwrap();
        let mut rng = StdRng::seed_from_u64(madsim::rand::random::<u64>());
        let name = JoinSetId::random_name(&mut rng, 0, 5);
        let join_set_id = JoinSetId::new(execution_id, kind, StrVariant::from(name)).unwrap();
        let ser = serde_json::to_string(&join_set_id).unwrap();
        let deser = serde_json::from_str(&ser).unwrap();
        assert_eq!(join_set_id, deser);
    }
}
