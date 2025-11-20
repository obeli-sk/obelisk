#[cfg(feature = "rusqlite")]
mod rusqlite_ext;
pub mod storage;
pub mod time;

use ::serde::{Deserialize, Serialize};
use assert_matches::assert_matches;
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
use tracing::{Span, error};
use val_json::{
    type_wrapper::{TypeConversionError, TypeWrapper},
    wast_val::{WastVal, WastValWithType},
    wast_val_ser::params,
};
use wasmtime::component::{Type, Val};

pub const NAMESPACE_OBELISK: &str = "obelisk";
const NAMESPACE_WASI: &str = "wasi";
pub const SUFFIX_PKG_EXT: &str = "-obelisk-ext";
pub const SUFFIX_PKG_SCHEDULE: &str = "-obelisk-schedule";
pub const SUFFIX_PKG_STUB: &str = "-obelisk-stub";

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FinishedExecutionError {
    // Activity only, because workflows will be retried forever
    #[error("permanent timeout")]
    PermanentTimeout,
    #[error("permanent failure: {reason_full}")]
    PermanentFailure {
        // Exists just for extracting reason of an activity trap, to avoid "activity trap: " prefix.
        reason_inner: String, // FIXME: remove
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
            FinishedExecutionError::PermanentFailure { .. } => {
                PendingStateFinishedError::ExecutionFailure
            }
        }
    }

    #[must_use]
    pub fn new_stubbed_error() -> Self {
        let reason = "stubbed error".to_string();
        Self::PermanentFailure {
            reason_inner: reason.clone(),
            reason_full: reason,
            kind: PermanentFailureKind::StubbedError,
            detail: None,
        }
    }
}

#[derive(Debug, Clone, Copy, derive_more::Display, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermanentFailureKind {
    /// Applicable to Workflow
    NondeterminismDetected,
    /// Applicable to Workflow, WASM Activity
    ParamsParsingError,
    /// Applicable to Workflow, WASM Activity
    CannotInstantiate,
    /// Applicable to Workflow, WASM Activity
    ResultParsingError,
    /// Applicable to Workflow
    ImportedFunctionCallError,
    /// Applicable to WASM Activity
    ActivityTrap,
    /// Applicable to Workflow
    WorkflowTrap,
    /// Applicable to Webhook
    WebhookEndpointError,
    /// Applicable to Stub Activity
    StubbedError,
    /// Applicable to Webhook, Workflow, WASM Activity
    OutOfFuel,
}

#[derive(Debug, Clone, Copy, derive_more::Display, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrapKind {
    #[display("trap")]
    Trap,
    #[display("post_return_trap")]
    PostReturnTrap,
    #[display("out of fuel")]
    OutOfFuel,
    #[display("host function error")]
    HostFunctionError,
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
        Deserialize, Deserializer, Serialize, Serializer,
        de::{self, Visitor},
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
    pub value: StrVariant,
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

#[derive(Clone, Copy, Debug, PartialEq, strum::EnumIter, derive_more::Display)]
#[display("{}", self.suffix())]
pub enum PackageExtension {
    ObeliskExt,
    ObeliskSchedule,
    ObeliskStub,
}
impl PackageExtension {
    fn suffix(&self) -> &'static str {
        match self {
            PackageExtension::ObeliskExt => SUFFIX_PKG_EXT,
            PackageExtension::ObeliskSchedule => SUFFIX_PKG_SCHEDULE,
            PackageExtension::ObeliskStub => SUFFIX_PKG_STUB,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "test", derive(Serialize))]
pub struct PkgFqn {
    pub namespace: String, // TODO: StrVariant or reference
    pub package_name: String,
    pub version: Option<String>,
}
impl PkgFqn {
    #[must_use]
    pub fn is_extension(&self) -> bool {
        Self::is_package_name_ext(&self.package_name)
    }

    #[must_use]
    pub fn split_ext(&self) -> Option<(PkgFqn, PackageExtension)> {
        use strum::IntoEnumIterator;
        for package_ext in PackageExtension::iter() {
            if let Some(package_name) = self.package_name.strip_suffix(package_ext.suffix()) {
                return Some((
                    PkgFqn {
                        namespace: self.namespace.clone(),
                        package_name: package_name.to_string(),
                        version: self.version.clone(),
                    },
                    package_ext,
                ));
            }
        }
        None
    }

    fn is_package_name_ext(package_name: &str) -> bool {
        package_name.ends_with(SUFFIX_PKG_EXT)
            || package_name.ends_with(SUFFIX_PKG_SCHEDULE)
            || package_name.ends_with(SUFFIX_PKG_STUB)
    }
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
    /// Returns true if this is an `-obelisk-*` extension interface.
    pub fn is_extension(&self) -> bool {
        PkgFqn::is_package_name_ext(self.package_name())
    }

    #[must_use]
    pub fn package_strip_obelisk_ext_suffix(&self) -> Option<&str> {
        self.package_name().strip_suffix(SUFFIX_PKG_EXT)
    }

    #[must_use]
    pub fn package_strip_obelisk_schedule_suffix(&self) -> Option<&str> {
        self.package_name().strip_suffix(SUFFIX_PKG_SCHEDULE)
    }

    #[must_use]
    pub fn package_strip_obelisk_stub_suffix(&self) -> Option<&str> {
        self.package_name().strip_suffix(SUFFIX_PKG_STUB)
    }

    #[must_use]
    pub fn is_namespace_obelisk(&self) -> bool {
        self.namespace() == NAMESPACE_OBELISK
    }

    #[must_use]
    pub fn is_namespace_wasi(&self) -> bool {
        self.namespace() == NAMESPACE_WASI
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
        if function_name.contains('.') {
            Err(FunctionFqnParseError::DelimiterFoundInFunctionName)
        } else {
            Ok(Self::new_arc(Arc::from(ifc_fqn), Arc::from(function_name)))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FunctionFqnParseError {
    #[error("delimiter `.` not found")]
    DelimiterNotFound,
    #[error("delimiter `.` found in function name")]
    DelimiterFoundInFunctionName,
}

impl FromStr for FunctionFqn {
    type Err = FunctionFqnParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((ifc_fqn, function_name)) = s.rsplit_once('.') {
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

#[cfg(any(test, feature = "test"))]
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TypeWrapperTopLevel {
    pub ok: Option<Box<TypeWrapper>>,
    pub err: Option<Box<TypeWrapper>>,
}
impl From<TypeWrapperTopLevel> for TypeWrapper {
    fn from(value: TypeWrapperTopLevel) -> TypeWrapper {
        TypeWrapper::Result {
            ok: value.ok,
            err: value.err,
        }
    }
}

#[derive(Clone, derive_more::Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SupportedFunctionReturnValue {
    Ok {
        #[debug(skip)]
        ok: Option<WastValWithType>,
    },
    Err {
        #[debug(skip)]
        err: Option<WastValWithType>,
    },
    ExecutionError(FinishedExecutionError),
}
pub const SUPPORTED_RETURN_VALUE_OK_EMPTY: SupportedFunctionReturnValue =
    SupportedFunctionReturnValue::Ok { ok: None };

#[derive(Debug, thiserror::Error)]
pub enum ResultParsingError {
    #[error("return value must not be empty")]
    NoValue,
    #[error("return value cannot be parsed, multi-value results are not supported")]
    MultiValue,
    #[error("return value cannot be parsed, {0}")]
    TypeConversionError(val_json::type_wrapper::TypeConversionError),
    #[error(transparent)]
    ResultParsingErrorFromVal(ResultParsingErrorFromVal),
}

#[derive(Debug, thiserror::Error)]
pub enum ResultParsingErrorFromVal {
    #[error("return value cannot be parsed, {0}")]
    WastValConversionError(val_json::wast_val::WastValConversionError),
    #[error("top level type must be a result")]
    TopLevelTypeMustBeAResult,
    #[error("value does not type check")]
    TypeCheckError,
}

impl SupportedFunctionReturnValue {
    pub fn new<
        I: ExactSizeIterator<Item = (wasmtime::component::Val, wasmtime::component::Type)>,
    >(
        mut iter: I,
    ) -> Result<Self, ResultParsingError> {
        if iter.len() == 0 {
            Err(ResultParsingError::NoValue)
        } else if iter.len() == 1 {
            let (val, r#type) = iter.next().unwrap();
            let r#type =
                TypeWrapper::try_from(r#type).map_err(ResultParsingError::TypeConversionError)?;
            Self::from_val_and_type_wrapper(val, r#type)
                .map_err(ResultParsingError::ResultParsingErrorFromVal)
        } else {
            Err(ResultParsingError::MultiValue)
        }
    }

    #[expect(clippy::result_unit_err)]
    pub fn from_wast_val_with_type(
        value: WastValWithType,
    ) -> Result<SupportedFunctionReturnValue, ()> {
        match value {
            WastValWithType {
                r#type: TypeWrapper::Result { ok: None, err: _ },
                value: WastVal::Result(Ok(None)),
            } => Ok(SupportedFunctionReturnValue::Ok { ok: None }),
            WastValWithType {
                r#type:
                    TypeWrapper::Result {
                        ok: Some(ok),
                        err: _,
                    },
                value: WastVal::Result(Ok(Some(value))),
            } => Ok(SupportedFunctionReturnValue::Ok {
                ok: Some(WastValWithType {
                    r#type: *ok,
                    value: *value,
                }),
            }),
            WastValWithType {
                r#type: TypeWrapper::Result { ok: _, err: None },
                value: WastVal::Result(Err(None)),
            } => Ok(SupportedFunctionReturnValue::Err { err: None }),
            WastValWithType {
                r#type:
                    TypeWrapper::Result {
                        ok: _,
                        err: Some(err),
                    },
                value: WastVal::Result(Err(Some(value))),
            } => Ok(SupportedFunctionReturnValue::Err {
                err: Some(WastValWithType {
                    r#type: *err,
                    value: *value,
                }),
            }),
            _ => Err(()),
        }
    }

    pub fn from_val_and_type_wrapper(
        value: wasmtime::component::Val,
        ty: TypeWrapper,
    ) -> Result<Self, ResultParsingErrorFromVal> {
        let TypeWrapper::Result { ok, err } = ty else {
            return Err(ResultParsingErrorFromVal::TopLevelTypeMustBeAResult);
        };
        let ty = TypeWrapperTopLevel { ok, err };
        Self::from_val_and_type_wrapper_tl(value, ty)
    }

    pub fn from_val_and_type_wrapper_tl(
        value: wasmtime::component::Val,
        ty: TypeWrapperTopLevel,
    ) -> Result<Self, ResultParsingErrorFromVal> {
        let wasmtime::component::Val::Result(value) = value else {
            return Err(ResultParsingErrorFromVal::TopLevelTypeMustBeAResult);
        };

        match (ty.ok, ty.err, value) {
            (None, _, Ok(None)) => Ok(SupportedFunctionReturnValue::Ok { ok: None }),
            (Some(ok_type), _, Ok(Some(value))) => Ok(SupportedFunctionReturnValue::Ok {
                ok: Some(WastValWithType {
                    r#type: *ok_type,
                    value: WastVal::try_from(*value)
                        .map_err(ResultParsingErrorFromVal::WastValConversionError)?,
                }),
            }),
            (_, None, Err(None)) => Ok(SupportedFunctionReturnValue::Err { err: None }),
            (_, Some(err_type), Err(Some(value))) => Ok(SupportedFunctionReturnValue::Err {
                err: Some(WastValWithType {
                    r#type: *err_type,
                    value: WastVal::try_from(*value)
                        .map_err(ResultParsingErrorFromVal::WastValConversionError)?,
                }),
            }),
            _other => Err(ResultParsingErrorFromVal::TypeCheckError),
        }
    }

    #[must_use]
    pub fn into_wast_val(self, get_return_type: impl FnOnce() -> TypeWrapperTopLevel) -> WastVal {
        match self {
            SupportedFunctionReturnValue::Ok { ok: None } => WastVal::Result(Ok(None)),
            SupportedFunctionReturnValue::Ok { ok: Some(v) } => {
                WastVal::Result(Ok(Some(Box::new(v.value))))
            }
            SupportedFunctionReturnValue::Err { err: None } => WastVal::Result(Err(None)),
            SupportedFunctionReturnValue::Err { err: Some(v) } => {
                WastVal::Result(Err(Some(Box::new(v.value))))
            }
            SupportedFunctionReturnValue::ExecutionError(_) => {
                execution_error_to_wast_val(&get_return_type())
            }
        }
    }

    #[must_use]
    pub fn as_pending_state_finished_result(&self) -> PendingStateFinishedResultKind {
        match self {
            SupportedFunctionReturnValue::Ok { ok: _ } => PendingStateFinishedResultKind(Ok(())),
            SupportedFunctionReturnValue::Err { err: _ } => {
                PendingStateFinishedResultKind(Err(PendingStateFinishedError::FallibleError))
            }
            SupportedFunctionReturnValue::ExecutionError(_) => {
                PendingStateFinishedResultKind(Err(PendingStateFinishedError::ExecutionFailure))
            }
        }
    }
}

#[must_use]
pub fn execution_error_to_wast_val(ret_type: &TypeWrapperTopLevel) -> WastVal {
    match ret_type {
        TypeWrapperTopLevel { ok: _, err: None } => return WastVal::Result(Err(None)),
        TypeWrapperTopLevel {
            ok: _,
            err: Some(inner),
        } => match inner.as_ref() {
            TypeWrapper::String => {
                return WastVal::Result(Err(Some(Box::new(WastVal::String(
                    EXECUTION_FAILED_STRING_OR_VARIANT.to_string(),
                )))));
            }
            TypeWrapper::Variant(variants) => {
                if variants.get(EXECUTION_FAILED_STRING_OR_VARIANT) == Some(&None) {
                    return WastVal::Result(Err(Some(Box::new(WastVal::Variant(
                        EXECUTION_FAILED_STRING_OR_VARIANT.to_string(),
                        None,
                    )))));
                }
            }
            _ => {}
        },
    }
    unreachable!("unexpected top-level return type {ret_type:?} cannot be ReturnTypeCompatible")
}

#[derive(Debug, Clone)]
pub struct Params(ParamsInternal);

#[derive(Debug, Clone)]
enum ParamsInternal {
    JsonValues(
        // TODO: change to Arc<[]>
        Vec<Value>,
    ),
    Vals {
        vals: Arc<[wasmtime::component::Val]>,
    },
    Empty,
}

impl Default for Params {
    fn default() -> Self {
        Self(ParamsInternal::Empty)
    }
}

pub const SUFFIX_FN_SUBMIT: &str = "-submit";
pub const SUFFIX_FN_AWAIT_NEXT: &str = "-await-next";
pub const SUFFIX_FN_SCHEDULE: &str = "-schedule";
pub const SUFFIX_FN_STUB: &str = "-stub";
pub const SUFFIX_FN_GET: &str = "-get";
pub const SUFFIX_FN_INVOKE: &str = "-invoke";

#[derive(
    Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq, strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
pub enum FunctionExtension {
    Submit,
    AwaitNext,
    Schedule,
    Stub,
    Get,
    Invoke,
}
impl FunctionExtension {
    #[must_use]
    pub fn suffix(&self) -> &'static str {
        match self {
            FunctionExtension::Submit => SUFFIX_FN_SUBMIT,
            FunctionExtension::AwaitNext => SUFFIX_FN_AWAIT_NEXT,
            FunctionExtension::Schedule => SUFFIX_FN_SCHEDULE,
            FunctionExtension::Stub => SUFFIX_FN_STUB,
            FunctionExtension::Get => SUFFIX_FN_GET,
            FunctionExtension::Invoke => SUFFIX_FN_INVOKE,
        }
    }

    #[must_use]
    pub fn belongs_to(&self, pkg_ext: PackageExtension) -> bool {
        matches!(
            (pkg_ext, self),
            (
                PackageExtension::ObeliskExt,
                FunctionExtension::Submit
                    | FunctionExtension::AwaitNext
                    | FunctionExtension::Get
                    | FunctionExtension::Invoke
            ) | (
                PackageExtension::ObeliskSchedule,
                FunctionExtension::Schedule
            ) | (PackageExtension::ObeliskStub, FunctionExtension::Stub)
        )
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FunctionMetadata {
    pub ffqn: FunctionFqn,
    pub parameter_types: ParameterTypes,
    pub return_type: ReturnType,
    pub extension: Option<FunctionExtension>,
    /// Externally submittable: primary functions + `-schedule` extended, but no activity stubs
    pub submittable: bool,
}
impl FunctionMetadata {
    #[must_use]
    pub fn split_extension(&self) -> Option<(&str, FunctionExtension)> {
        self.extension.map(|extension| {
            let prefix = self
                .ffqn
                .function_name
                .value
                .strip_suffix(extension.suffix())
                .unwrap_or_else(|| {
                    panic!(
                        "extension function {} must end with expected suffix {}",
                        self.ffqn.function_name,
                        extension.suffix()
                    )
                });
            (prefix, extension)
        })
    }
}
impl Display for FunctionMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{ffqn}: func{params} -> {return_type}",
            ffqn = self.ffqn,
            params = self.parameter_types,
            return_type = self.return_type,
        )
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

impl PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        fn to_json(vals: &[wasmtime::component::Val]) -> Result<Vec<serde_json::Value>, ()> {
            let mut vec = Vec::with_capacity(vals.len());
            for val in vals.iter() {
                let value = match WastVal::try_from(val.clone()) {
                    Ok(ok) => ok,
                    Err(err) => {
                        error!("cannot compare Params, cannot convert to WastVal: {err:?}");
                        return Err(());
                    }
                };
                let value = match serde_json::to_value(&value) {
                    Ok(ok) => ok,
                    Err(err) => {
                        error!("cannot compare Params, cannot convert to JSON: {err:?}");
                        return Err(());
                    }
                };
                vec.push(value);
            }
            Ok(vec)
        }

        if self.is_empty() && other.is_empty() {
            return true;
        }
        if self.len() != other.len() {
            return false;
        }
        if let ParamsInternal::JsonValues(left) = &self.0
            && let ParamsInternal::JsonValues(right) = &other.0
        {
            return left == right;
        }
        if let ParamsInternal::Vals { vals: left } = &self.0
            && let ParamsInternal::Vals { vals: right } = &other.0
        {
            return left == right;
        }
        // Mixed case: convert the other side to JSON
        // TODO(perf): Store the JSON representation
        let temp_left;
        let temp_right;

        let left = match &self.0 {
            ParamsInternal::JsonValues(json) => json,
            ParamsInternal::Vals { vals } => {
                let Ok(vec) = to_json(vals) else {
                    return false;
                };
                temp_left = vec;
                &temp_left
            }
            ParamsInternal::Empty => unreachable!("0 length handled above"),
        };
        let right = match &other.0 {
            ParamsInternal::JsonValues(json) => json,
            ParamsInternal::Vals { vals } => {
                let Ok(vec) = to_json(vals) else {
                    return false;
                };
                temp_right = vec;
                &temp_right
            }
            ParamsInternal::Empty => unreachable!("0 length handled above"),
        };
        left == right
    }
}
impl Eq for Params {}

pub mod prefixed_ulid {
    use crate::{JoinSetId, JoinSetIdParseError};
    use serde_with::{DeserializeFromStr, SerializeDisplay};
    use std::{
        fmt::{Debug, Display},
        hash::Hasher,
        marker::PhantomData,
        num::ParseIntError,
        str::FromStr,
        sync::Arc,
    };
    use ulid::Ulid;

    #[derive(derive_more::Display, SerializeDisplay, DeserializeFromStr)]
    #[derive_where::derive_where(Clone, Copy)]
    #[display("{}_{ulid}", Self::prefix())]
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
        /// Fills only the lower 80 bits of the returned 128-bit value.
        pub fn random_part(&self) -> u128 {
            self.ulid.random()
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
    pub type ExecutionIdTopLevel = PrefixedUlid<prefix::E>;
    pub type RunId = PrefixedUlid<prefix::Run>;
    pub type DelayIdTopLevel = PrefixedUlid<prefix::Delay>; // Never used directly, tracking top level ExecutionId

    #[cfg(any(test, feature = "test"))]
    impl<'a, T> arbitrary::Arbitrary<'a> for PrefixedUlid<T> {
        fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(Self::new(ulid::Ulid::from_parts(
                u.arbitrary()?,
                u.arbitrary()?,
            )))
        }
    }

    #[derive(Hash, PartialEq, Eq, PartialOrd, Ord, SerializeDisplay, DeserializeFromStr, Clone)]
    pub enum ExecutionId {
        TopLevel(ExecutionIdTopLevel),
        Derived(ExecutionIdDerived),
    }

    #[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, SerializeDisplay, DeserializeFromStr)]
    pub struct ExecutionIdDerived {
        top_level: ExecutionIdTopLevel,
        infix: Arc<str>,
        idx: u64,
    }
    impl ExecutionIdDerived {
        #[must_use]
        pub fn get_incremented(&self) -> Self {
            self.get_incremented_by(1)
        }
        #[must_use]
        pub fn get_incremented_by(&self, count: u64) -> Self {
            ExecutionIdDerived {
                top_level: self.top_level,
                infix: self.infix.clone(),
                idx: self.idx + count,
            }
        }
        #[must_use]
        pub fn next_level(&self, join_set_id: &JoinSetId) -> ExecutionIdDerived {
            let ExecutionIdDerived {
                top_level,
                infix,
                idx,
            } = self;
            let infix = Arc::from(format!(
                "{infix}{EXECUTION_ID_JOIN_SET_INFIX}{idx}{EXECUTION_ID_INFIX}{join_set_id}"
            ));
            ExecutionIdDerived {
                top_level: *top_level,
                infix,
                idx: EXECUTION_ID_START_IDX,
            }
        }
        fn display_or_debug(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let ExecutionIdDerived {
                top_level,
                infix,
                idx,
            } = self;
            write!(
                f,
                "{top_level}{EXECUTION_ID_INFIX}{infix}{EXECUTION_ID_JOIN_SET_INFIX}{idx}"
            )
        }

        // Two cases:
        // A. infix does not contain dots -> first level, will be split into the top level, the whole infix must be JoinSetId.
        // B. infix must be split into old_infix _ old_idx . JoinSetId
        pub fn split_to_parts(
            &self,
        ) -> Result<(ExecutionId, JoinSetId), ExecutionIdDerivedSplitError> {
            if let Some((old_infix_and_index, join_set_id)) =
                self.infix.rsplit_once(EXECUTION_ID_INFIX)
            {
                let join_set_id = JoinSetId::from_str(join_set_id)?;
                let Some((old_infix, old_idx)) =
                    old_infix_and_index.rsplit_once(EXECUTION_ID_JOIN_SET_INFIX)
                else {
                    return Err(ExecutionIdDerivedSplitError::CannotFindJoinSetDelimiter);
                };
                let parent = ExecutionIdDerived {
                    top_level: self.top_level,
                    infix: Arc::from(old_infix),
                    idx: old_idx
                        .parse()
                        .map_err(ExecutionIdDerivedSplitError::CannotParseOldIndex)?,
                };
                Ok((ExecutionId::Derived(parent), join_set_id))
            } else {
                // This was the first level
                Ok((
                    ExecutionId::TopLevel(self.top_level),
                    JoinSetId::from_str(&self.infix)?,
                ))
            }
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ExecutionIdDerivedSplitError {
        #[error(transparent)]
        JoinSetIdParseError(#[from] JoinSetIdParseError),
        #[error("cannot parse index of parent execution - {0}")]
        CannotParseOldIndex(ParseIntError),
        #[error("cannot find join set delimiter")]
        CannotFindJoinSetDelimiter,
    }

    impl Debug for ExecutionIdDerived {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.display_or_debug(f)
        }
    }
    impl Display for ExecutionIdDerived {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.display_or_debug(f)
        }
    }
    impl FromStr for ExecutionIdDerived {
        type Err = DerivedIdParseError;

        fn from_str(input: &str) -> Result<Self, Self::Err> {
            let (top_level, infix, idx) = derived_from_str(input)?;
            Ok(ExecutionIdDerived {
                top_level,
                infix,
                idx,
            })
        }
    }

    fn derived_from_str<T: 'static>(
        input: &str,
    ) -> Result<(PrefixedUlid<T>, Arc<str>, u64), DerivedIdParseError> {
        if let Some((prefix, suffix)) = input.split_once(EXECUTION_ID_INFIX) {
            let top_level = PrefixedUlid::from_str(prefix)
                .map_err(DerivedIdParseError::PrefixedUlidParseError)?;
            let Some((infix, idx)) = suffix.rsplit_once(EXECUTION_ID_JOIN_SET_INFIX) else {
                return Err(DerivedIdParseError::SecondDelimiterNotFound);
            };
            let infix = Arc::from(infix);
            let idx = u64::from_str(idx).map_err(DerivedIdParseError::ParseIndexError)?;
            Ok((top_level, infix, idx))
        } else {
            Err(DerivedIdParseError::FirstDelimiterNotFound)
        }
    }

    #[cfg(any(test, feature = "test"))]
    impl<'a> arbitrary::Arbitrary<'a> for ExecutionIdDerived {
        fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
            let top_level = ExecutionId::TopLevel(ExecutionIdTopLevel::arbitrary(u)?);
            let join_set_id = JoinSetId::arbitrary(u)?;
            Ok(top_level.next_level(&join_set_id))
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum DerivedIdParseError {
        #[error(transparent)]
        PrefixedUlidParseError(PrefixedUlidParseError),
        #[error("cannot parse derived id - delimiter `{EXECUTION_ID_INFIX}` not found")]
        FirstDelimiterNotFound,
        #[error("cannot parse derived id - delimiter `{EXECUTION_ID_JOIN_SET_INFIX}` not found")]
        SecondDelimiterNotFound,
        #[error(
            "cannot parse derived id - suffix after `{EXECUTION_ID_JOIN_SET_INFIX}` must be a number"
        )]
        ParseIndexError(ParseIntError),
    }

    impl ExecutionId {
        #[must_use]
        pub fn generate() -> Self {
            ExecutionId::TopLevel(PrefixedUlid::generate())
        }

        #[must_use]
        pub fn get_top_level(&self) -> ExecutionIdTopLevel {
            match &self {
                ExecutionId::TopLevel(prefixed_ulid) => *prefixed_ulid,
                ExecutionId::Derived(ExecutionIdDerived { top_level, .. }) => *top_level,
            }
        }

        #[must_use]
        pub fn is_top_level(&self) -> bool {
            matches!(self, ExecutionId::TopLevel(_))
        }

        #[must_use]
        pub fn random_seed(&self) -> u64 {
            let mut hasher = fxhash::FxHasher::default();
            // `Self::random_part` uses only the lower 80 bits of a 128-bit value.
            // Truncate to 64 bits, since including the remaining 16 bits
            // would not increase the entropy of the 64-bit output.
            #[expect(clippy::cast_possible_truncation)]
            let random_part = self.get_top_level().random_part() as u64;
            hasher.write_u64(random_part);
            hasher.write_u64(self.get_top_level().timestamp_part());
            if let ExecutionId::Derived(ExecutionIdDerived {
                top_level: _,
                infix,
                idx,
            }) = self
            {
                // Each derived execution ID should return different seed.
                hasher.write(infix.as_bytes());
                hasher.write_u64(*idx);
            }
            hasher.finish()
        }

        #[must_use]
        pub const fn from_parts(timestamp_ms: u64, random_part: u128) -> Self {
            ExecutionId::TopLevel(ExecutionIdTopLevel::from_parts(timestamp_ms, random_part))
        }

        #[must_use]
        pub fn next_level(&self, join_set_id: &JoinSetId) -> ExecutionIdDerived {
            match &self {
                ExecutionId::TopLevel(top_level) => ExecutionIdDerived {
                    top_level: *top_level,
                    infix: Arc::from(join_set_id.to_string()),
                    idx: EXECUTION_ID_START_IDX,
                },
                ExecutionId::Derived(derived) => derived.next_level(join_set_id),
            }
        }

        fn display_or_debug(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match &self {
                ExecutionId::TopLevel(top_level) => Display::fmt(top_level, f),
                ExecutionId::Derived(derived) => Display::fmt(derived, f),
            }
        }
    }

    const EXECUTION_ID_INFIX: char = '.';
    const EXECUTION_ID_JOIN_SET_INFIX: char = '_';
    const EXECUTION_ID_START_IDX: u64 = 1;
    pub const JOIN_SET_START_IDX: u64 = 1;
    const DELAY_ID_START_IDX: u64 = 1;

    #[derive(Debug, thiserror::Error)]
    pub enum ExecutionIdParseError {
        #[error(transparent)]
        PrefixedUlidParseError(#[from] PrefixedUlidParseError),
        #[error(
            "cannot parse derived execution id - first delimiter `{EXECUTION_ID_INFIX}` not found"
        )]
        FirstDelimiterNotFound,
        #[error(
            "cannot parse derived execution id - second delimiter `{EXECUTION_ID_INFIX}` not found"
        )]
        SecondDelimiterNotFound,
        #[error("cannot parse derived execution id - last suffix must be a number")]
        ParseIndexError(#[from] ParseIntError),
    }

    impl FromStr for ExecutionId {
        type Err = ExecutionIdParseError;

        fn from_str(input: &str) -> Result<Self, Self::Err> {
            if input.contains(EXECUTION_ID_INFIX) {
                ExecutionIdDerived::from_str(input)
                    .map(ExecutionId::Derived)
                    .map_err(|err| match err {
                        DerivedIdParseError::FirstDelimiterNotFound => {
                            unreachable!("first delimiter checked")
                        }
                        DerivedIdParseError::SecondDelimiterNotFound => {
                            ExecutionIdParseError::SecondDelimiterNotFound
                        }
                        DerivedIdParseError::PrefixedUlidParseError(err) => {
                            ExecutionIdParseError::PrefixedUlidParseError(err)
                        }
                        DerivedIdParseError::ParseIndexError(err) => {
                            ExecutionIdParseError::ParseIndexError(err)
                        }
                    })
            } else {
                Ok(ExecutionId::TopLevel(PrefixedUlid::from_str(input)?))
            }
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ExecutionIdStructuralParseError {
        #[error(transparent)]
        ExecutionIdParseError(#[from] ExecutionIdParseError),
        #[error("execution-id must be a record with `id` field of type string")]
        TypeError,
    }

    impl TryFrom<&wasmtime::component::Val> for ExecutionId {
        type Error = ExecutionIdStructuralParseError;

        fn try_from(execution_id: &wasmtime::component::Val) -> Result<Self, Self::Error> {
            if let wasmtime::component::Val::Record(key_vals) = execution_id
                && key_vals.len() == 1
                && let Some((key, execution_id)) = key_vals.first()
                && key == "id"
                && let wasmtime::component::Val::String(execution_id) = execution_id
            {
                ExecutionId::from_str(execution_id)
                    .map_err(ExecutionIdStructuralParseError::ExecutionIdParseError)
            } else {
                Err(ExecutionIdStructuralParseError::TypeError)
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

    #[cfg(any(test, feature = "test"))]
    impl<'a> arbitrary::Arbitrary<'a> for ExecutionId {
        fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
            Ok(ExecutionId::TopLevel(PrefixedUlid::arbitrary(u)?))
        }
    }

    /// Mirrors [`ExecutionId`], with different prefix and `idx` for tracking each delay within the join set.
    #[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, SerializeDisplay, DeserializeFromStr)]
    pub struct DelayId {
        top_level: DelayIdTopLevel,
        infix: Arc<str>,
        idx: u64,
    }
    impl DelayId {
        #[must_use]
        pub fn new(execution_id: &ExecutionId, join_set_id: &JoinSetId) -> DelayId {
            Self::new_with_index(execution_id, join_set_id, DELAY_ID_START_IDX)
        }

        #[must_use]
        pub fn new_with_index(
            execution_id: &ExecutionId,
            join_set_id: &JoinSetId,
            idx: u64,
        ) -> DelayId {
            let ExecutionIdDerived {
                top_level: PrefixedUlid { ulid, .. },
                infix,
                idx: _,
            } = execution_id.next_level(join_set_id);
            let top_level = DelayIdTopLevel::new(ulid);
            DelayId {
                top_level,
                infix,
                idx,
            }
        }

        #[must_use]
        pub fn get_incremented(&self) -> Self {
            Self {
                top_level: self.top_level,
                infix: self.infix.clone(),
                idx: self.idx + 1,
            }
        }

        fn display_or_debug(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let DelayId {
                top_level,
                infix,
                idx,
            } = self;
            write!(
                f,
                "{top_level}{EXECUTION_ID_INFIX}{infix}{EXECUTION_ID_JOIN_SET_INFIX}{idx}"
            )
        }
    }

    pub mod delay_impl {
        use super::{DelayId, DerivedIdParseError, derived_from_str};
        use std::{
            fmt::{Debug, Display},
            str::FromStr,
        };

        impl Debug for DelayId {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.display_or_debug(f)
            }
        }

        impl Display for DelayId {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.display_or_debug(f)
            }
        }

        impl FromStr for DelayId {
            type Err = DerivedIdParseError;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                let (top_level, infix, idx) = derived_from_str(input)?;
                Ok(DelayId {
                    top_level,
                    infix,
                    idx,
                })
            }
        }

        #[cfg(any(test, feature = "test"))]
        impl<'a> arbitrary::Arbitrary<'a> for DelayId {
            fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                use super::{ExecutionId, JoinSetId};
                let execution_id = ExecutionId::arbitrary(u)?;
                let mut join_set_id = JoinSetId::arbitrary(u)?;
                join_set_id.kind = crate::JoinSetKind::OneOff;
                Ok(DelayId::new(&execution_id, &join_set_id))
            }
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
#[display("{kind}{JOIN_SET_ID_INFIX}{name}")]
pub struct JoinSetId {
    pub kind: JoinSetKind,
    pub name: StrVariant,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, Serialize, Deserialize, Default,
)]
#[cfg_attr(any(test, feature = "test"), derive(arbitrary::Arbitrary))]
#[serde(rename_all = "snake_case")]
pub enum ClosingStrategy {
    /// All submitted child execution requests that were not awaited by the workflow are awaited during join set close.
    /// Delay requests are not awaited.
    #[default]
    Complete,
}

impl JoinSetId {
    pub fn new(kind: JoinSetKind, name: StrVariant) -> Result<Self, InvalidNameError<JoinSetId>> {
        Ok(Self {
            kind,
            name: check_name(name, CHARSET_EXTRA_JSON_SET)?,
        })
    }
}

pub const CHARSET_ALPHANUMERIC: &str =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

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
)]
#[cfg_attr(any(test, feature = "test"), derive(arbitrary::Arbitrary))]
#[display("{}", self.as_code())]
pub enum JoinSetKind {
    OneOff,
    Named,
    Generated,
}
impl JoinSetKind {
    fn as_code(&self) -> &'static str {
        match self {
            JoinSetKind::OneOff => "o",
            JoinSetKind::Named => "n",
            JoinSetKind::Generated => "g",
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

pub const JOIN_SET_ID_INFIX: char = ':';
const CHARSET_EXTRA_JSON_SET: &str = "_-/";

impl FromStr for JoinSetId {
    type Err = JoinSetIdParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let Some((kind, name)) = input.split_once(JOIN_SET_ID_INFIX) else {
            return Err(JoinSetIdParseError::WrongParts);
        };
        let kind = kind
            .parse()
            .map_err(JoinSetIdParseError::JoinSetKindParseError)?;
        Ok(JoinSetId::new(kind, StrVariant::from(name.to_string()))?)
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

#[cfg(any(test, feature = "test"))]
const CHARSET_JOIN_SET_NAME: &str =
    const_format::concatcp!(CHARSET_ALPHANUMERIC, CHARSET_EXTRA_JSON_SET);
#[cfg(any(test, feature = "test"))]
impl<'a> arbitrary::Arbitrary<'a> for JoinSetId {
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

        Ok(JoinSetId::new(JoinSetKind::Named, StrVariant::from(name)).unwrap())
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
    ActivityStub,
    ActivityExternal,
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
#[display("{component_type}:{name}")]
#[debug("{}", self)]
#[non_exhaustive] // force using the constructor as much as possible due to validation
pub struct ComponentId {
    pub component_type: ComponentType,
    pub name: StrVariant,
}
impl ComponentId {
    pub fn new(
        component_type: ComponentType,
        name: StrVariant,
    ) -> Result<Self, InvalidNameError<Self>> {
        Ok(Self {
            component_type,
            name: check_name(name, "_")?,
        })
    }

    #[must_use]
    pub const fn dummy_activity() -> Self {
        Self {
            component_type: ComponentType::ActivityWasm,
            name: StrVariant::empty(),
        }
    }

    #[must_use]
    pub const fn dummy_workflow() -> ComponentId {
        ComponentId {
            component_type: ComponentType::Workflow,
            name: StrVariant::empty(),
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
}

impl FromStr for ComponentId {
    type Err = ConfigIdParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (component_type, name) = input.split_once(':').ok_or(Self::Err::DelimiterNotFound)?;
        let component_type = component_type.parse()?;
        Ok(Self {
            component_type,
            name: StrVariant::from(name.to_string()),
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
pub struct ContentDigest(pub Digest);
#[cfg(any(test, feature = "test"))]
pub const CONTENT_DIGEST_DUMMY: ContentDigest = ContentDigest(Digest {
    hash_type: HashType::Sha256,
    hash_base16: StrVariant::empty(),
});

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
    #[error("cannot parse ContentDigest - suffix must be hex-encoded, got invalid character `{0}`")]
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

const EXECUTION_FAILED_STRING_OR_VARIANT: &str = "execution-failed";
#[derive(
    Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, derive_more::Display,
)]
pub enum ReturnType {
    Extendable(ReturnTypeExtendable), // Execution failures can be converted to this return type, e.g. result<_, string>
    NonExtendable(ReturnTypeNonExtendable), // e.g. -submit returns ExecutionId
}
impl ReturnType {
    /// Evaluate whether the return type is one of supported types:
    /// * `result`
    /// * `result<T>`
    /// * `result<_, string>`
    /// * `result<T, string>`
    /// * `result<T, E>` where T can be `_` and E is a `variant` containing `execution-failed`
    ///   variant with no associated value.
    #[must_use]
    pub fn detect(type_wrapper: TypeWrapper, wit_type: StrVariant) -> ReturnType {
        if let TypeWrapper::Result { ok, err: None } = type_wrapper {
            return ReturnType::Extendable(ReturnTypeExtendable {
                type_wrapper_tl: TypeWrapperTopLevel { ok, err: None },
                wit_type,
            });
        } else if let TypeWrapper::Result { ok, err: Some(err) } = type_wrapper {
            if let TypeWrapper::String = err.as_ref() {
                return ReturnType::Extendable(ReturnTypeExtendable {
                    type_wrapper_tl: TypeWrapperTopLevel { ok, err: Some(err) },
                    wit_type,
                });
            } else if let TypeWrapper::Variant(fields) = err.as_ref()
                && let Some(None) = fields.get(EXECUTION_FAILED_STRING_OR_VARIANT)
            {
                return ReturnType::Extendable(ReturnTypeExtendable {
                    type_wrapper_tl: TypeWrapperTopLevel { ok, err: Some(err) },
                    wit_type,
                });
            }
            return ReturnType::NonExtendable(ReturnTypeNonExtendable {
                type_wrapper: TypeWrapper::Result { ok, err: Some(err) },
                wit_type,
            });
        }
        ReturnType::NonExtendable(ReturnTypeNonExtendable {
            type_wrapper: type_wrapper.clone(),
            wit_type,
        })
    }

    #[must_use]
    pub fn wit_type(&self) -> &str {
        match self {
            ReturnType::Extendable(compatible) => compatible.wit_type.as_ref(),
            ReturnType::NonExtendable(incompatible) => incompatible.wit_type.as_ref(),
        }
    }

    #[must_use]
    pub fn type_wrapper(&self) -> TypeWrapper {
        match self {
            ReturnType::Extendable(compatible) => {
                TypeWrapper::from(compatible.type_wrapper_tl.clone())
            }
            ReturnType::NonExtendable(incompatible) => incompatible.type_wrapper.clone(),
        }
    }
}

#[derive(
    Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, derive_more::Display,
)]
#[display("{wit_type}")]
pub struct ReturnTypeNonExtendable {
    pub type_wrapper: TypeWrapper,
    pub wit_type: StrVariant,
}

#[derive(
    Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, derive_more::Display,
)]
#[display("{wit_type}")]
pub struct ReturnTypeExtendable {
    pub type_wrapper_tl: TypeWrapperTopLevel,
    pub wit_type: StrVariant,
}

#[cfg(any(test, feature = "test"))]
pub const RETURN_TYPE_DUMMY: ReturnType = ReturnType::Extendable(ReturnTypeExtendable {
    type_wrapper_tl: TypeWrapperTopLevel {
        ok: None,
        err: None,
    },
    wit_type: StrVariant::Static("result"),
});

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Eq, derive_more::Display)]
#[derive_where::derive_where(PartialEq)]
#[display("{name}: {wit_type}")]
pub struct ParameterType {
    pub type_wrapper: TypeWrapper,
    #[derive_where(skip)]
    // Names are read from how a component names the parameter and thus might differ between export and import.
    pub name: StrVariant,
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
    pub extension: bool, // one of `-obelisk-ext`, `-obelisk-schedule`, `-obelisk-stub`
    pub fns: IndexMap<FnName, FunctionMetadata>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentRetryConfig {
    pub max_retries: u32,
    pub retry_exp_backoff: Duration,
}
impl ComponentRetryConfig {
    pub const ZERO: ComponentRetryConfig = ComponentRetryConfig {
        max_retries: 0,
        retry_exp_backoff: Duration::ZERO,
    };

    #[cfg(feature = "test")]
    pub const WORKFLOW_TEST: ComponentRetryConfig = ComponentRetryConfig {
        max_retries: u32::MAX,
        retry_exp_backoff: Duration::ZERO,
    };
}

/// Implementation must not return `-obelisk-*` extended function, nor functions from `obelisk` namespace.
pub trait FunctionRegistry: Send + Sync {
    fn get_by_exported_function(
        &self,
        ffqn: &FunctionFqn,
    ) -> Option<(FunctionMetadata, ComponentId)>;

    // TODO: return Option<&TypeWrapperTopLevel>, optimize
    /// Get return type of a non-ext function, otherwise return `None`.
    fn get_ret_type(&self, ffqn: &FunctionFqn) -> Option<TypeWrapperTopLevel> {
        self.get_by_exported_function(ffqn)
            .and_then(|(fn_meta, _)| {
                if let ReturnType::Extendable(ReturnTypeExtendable {
                    type_wrapper_tl: type_wrapper,
                    wit_type: _,
                }) = fn_meta.return_type
                {
                    Some(type_wrapper)
                } else {
                    None
                }
            })
    }

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
            let _ = span.set_parent(otel_context);
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
            assert_matches!(&mut self.metadata.0, Some(map) => map)
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

    use rstest::rstest;

    use crate::{
        ExecutionId, FunctionFqn, JoinSetId, JoinSetKind, StrVariant, prefixed_ulid::ExecutorId,
    };
    use std::{
        hash::{DefaultHasher, Hash, Hasher},
        str::FromStr,
        sync::Arc,
    };

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
    fn execution_id_with_one_level_should_parse() {
        let top_level = ExecutionId::generate();
        let join_set_id = JoinSetId::new(JoinSetKind::Named, StrVariant::Static("name")).unwrap();
        let first_child = ExecutionId::Derived(top_level.next_level(&join_set_id));
        let ser = first_child.to_string();
        assert_eq!(format!("{top_level}.n:name_1"), ser);
        let parsed = ExecutionId::from_str(&ser).unwrap();
        assert_eq!(first_child, parsed);
    }

    #[test]
    fn execution_id_increment_twice() {
        let top_level = ExecutionId::generate();
        let join_set_id = JoinSetId::new(JoinSetKind::Named, StrVariant::Static("name")).unwrap();
        let first_child = top_level.next_level(&join_set_id);
        let second_child = ExecutionId::Derived(first_child.get_incremented());
        let ser = second_child.to_string();
        assert_eq!(format!("{top_level}.n:name_2"), ser);
        let parsed = ExecutionId::from_str(&ser).unwrap();
        assert_eq!(second_child, parsed);
    }

    #[test]
    fn execution_id_next_level_twice() {
        let top_level = ExecutionId::generate();
        let join_set_id_outer =
            JoinSetId::new(JoinSetKind::Generated, StrVariant::Static("gg")).unwrap();
        let join_set_id_inner =
            JoinSetId::new(JoinSetKind::OneOff, StrVariant::Static("oo")).unwrap();
        let execution_id = ExecutionId::Derived(
            top_level
                .next_level(&join_set_id_outer)
                .get_incremented()
                .next_level(&join_set_id_inner)
                .get_incremented(),
        );
        let ser = execution_id.to_string();
        assert_eq!(format!("{top_level}.g:gg_2.o:oo_2"), ser);
        let parsed = ExecutionId::from_str(&ser).unwrap();
        assert_eq!(execution_id, parsed);
    }

    #[test]
    fn execution_id_split_first_level() {
        let top_level = ExecutionId::generate();
        let join_set_id =
            JoinSetId::new(JoinSetKind::Generated, StrVariant::Static("some")).unwrap();
        let execution_id = top_level.next_level(&join_set_id);
        let (actual_top_level, actual_join_set) = execution_id.split_to_parts().unwrap();
        assert_eq!(top_level, actual_top_level);
        assert_eq!(join_set_id, actual_join_set);
    }

    #[rstest]
    fn execution_id_split_second_level(#[values(0, 1)] outer_idx: u64) {
        let top_level = ExecutionId::generate();
        let join_set_id_outer =
            JoinSetId::new(JoinSetKind::Generated, StrVariant::Static("some")).unwrap();
        let first_level = top_level
            .next_level(&join_set_id_outer)
            .get_incremented_by(outer_idx);

        let join_set_id_inner =
            JoinSetId::new(JoinSetKind::Generated, StrVariant::Static("other")).unwrap();
        let second_level = first_level.next_level(&join_set_id_inner);

        let (actual_first_level, actual_join_set) = second_level.split_to_parts().unwrap();
        assert_eq!(ExecutionId::Derived(first_level), actual_first_level);
        assert_eq!(join_set_id_inner, actual_join_set);
    }

    #[test]
    fn execution_id_hash_should_be_stable() {
        let parent = ExecutionId::from_parts(1, 2);
        let join_set_id = JoinSetId::new(JoinSetKind::Named, StrVariant::Static("name")).unwrap();
        let sibling_1 = parent.next_level(&join_set_id);
        let sibling_2 = ExecutionId::Derived(sibling_1.get_incremented());
        let sibling_1 = ExecutionId::Derived(sibling_1);
        let join_set_id_inner =
            JoinSetId::new(JoinSetKind::OneOff, StrVariant::Static("oo")).unwrap();
        let child =
            ExecutionId::Derived(sibling_1.next_level(&join_set_id_inner).get_incremented());
        let parent = parent.random_seed();
        let sibling_1 = sibling_1.random_seed();
        let sibling_2 = sibling_2.random_seed();
        let child = child.random_seed();
        let vec = vec![parent, sibling_1, sibling_2, child];
        insta::assert_debug_snapshot!(vec);
        // check that every hash is unique
        let set: hashbrown::HashSet<_> = vec.into_iter().collect();
        assert_eq!(4, set.len());
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

    #[test]
    fn ffqn_from_tuple_with_version_should_work() {
        let ffqn = FunctionFqn::try_from_tuple("wasi:cli/run@0.2.0", "run").unwrap();
        assert_eq!(FunctionFqn::new_static("wasi:cli/run@0.2.0", "run"), ffqn);
    }

    #[test]
    fn ffqn_from_str_with_version_should_work() {
        let ffqn = FunctionFqn::from_str("wasi:cli/run@0.2.0.run").unwrap();
        assert_eq!(FunctionFqn::new_static("wasi:cli/run@0.2.0", "run"), ffqn);
    }

    #[tokio::test]
    async fn join_set_serde_should_be_consistent() {
        use crate::{JoinSetId, JoinSetKind};
        use strum::IntoEnumIterator;
        for kind in JoinSetKind::iter() {
            let join_set_id = JoinSetId::new(kind, StrVariant::from("name")).unwrap();
            let ser = serde_json::to_string(&join_set_id).unwrap();
            let deser = serde_json::from_str(&ser).unwrap();
            assert_eq!(join_set_id, deser);
        }
    }
}
