//! Core naming types: string variants, package/interface/function fully
//! qualified names. Moved from the `concepts` crate so they can be shared
//! with wasm targets; `concepts` re-exports them.

use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    fmt::{Debug, Display, Write},
    hash::Hash,
    marker::PhantomData,
    ops::Deref,
    str::FromStr,
    sync::Arc,
};

pub const NAMESPACE_OBELISK: &str = "obelisk";
const NAMESPACE_WASI: &str = "wasi";
pub const SUFFIX_PKG_EXT: &str = "-obelisk-ext";
pub const SUFFIX_PKG_SCHEDULE: &str = "-obelisk-schedule";
pub const SUFFIX_PKG_STUB: &str = "-obelisk-stub";

#[derive(Clone, Eq, derive_more::Display, schemars::JsonSchema)]
#[schemars(with = "String")]
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
    use super::StrVariant;
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

    fn fmt(&self, f: &mut dyn Write, delimiter: &'static str) -> std::fmt::Result {
        let PkgFqn {
            namespace,
            package_name,
            version,
        } = self;
        if let Some(version) = version {
            write!(f, "{namespace}{delimiter}{package_name}@{version}")
        } else {
            write!(f, "{namespace}{delimiter}{package_name}")
        }
    }

    #[must_use]
    pub fn as_file_name(&self) -> String {
        let mut buffer = String::new();
        self.fmt(&mut buffer, "_").expect("writing to string");
        buffer
    }
}
impl Display for PkgFqn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt(f, ":")
    }
}

#[derive(Hash, Clone, PartialEq, Eq)]
pub struct IfcFqnMarker;

pub type IfcFqnName = Name<IfcFqnMarker>; // namespace:name/ifc_name OR namespace:name/ifc_name@version

impl IfcFqnName {
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
    pub fn from_pkg_fqn(pkg_fqn: &PkgFqn, ifc_name: &str) -> Self {
        let mut str = format!(
            "{namespace}:{package_name}/{ifc_name}",
            namespace = pkg_fqn.namespace,
            package_name = pkg_fqn.package_name
        );
        if let Some(version) = &pkg_fqn.version {
            str += "@";
            str += version;
        }
        Self::new_arc(Arc::from(str))
    }

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
impl FromStr for IfcFqnName {
    type Err = IfcFqnParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let input = s.to_string();

        let (namespace, rest) = s
            .split_once(':')
            .ok_or_else(|| IfcFqnParseError::MissingNamespace(input.clone()))?;
        if namespace.is_empty() {
            return Err(IfcFqnParseError::EmptyNamespace(input));
        }
        let (package_and_ifc, version) = match rest.split_once('@') {
            Some((left, ver)) => (left, Some(ver)),
            None => (rest, None),
        };
        let (package_name, ifc_name) = package_and_ifc
            .split_once('/')
            .ok_or_else(|| IfcFqnParseError::MissingPackageOrIfc(input.clone()))?;
        if package_name.is_empty() {
            return Err(IfcFqnParseError::EmptyPackageName(input));
        }
        if ifc_name.is_empty() {
            return Err(IfcFqnParseError::EmptyIfcName(input));
        }
        if let Some(v) = version
            && v.is_empty()
        {
            return Err(IfcFqnParseError::EmptyVersion(input));
        }
        Ok(Self::new_arc(Arc::from(s)))
    }
}

#[derive(Debug, thiserror::Error, Clone)]
pub enum IfcFqnParseError {
    #[error("missing namespace separator ':' in `{0}`")]
    MissingNamespace(String),

    #[error("namespace is empty in `{0}`")]
    EmptyNamespace(String),

    #[error("missing package/ifc separator '/' in `{0}`")]
    MissingPackageOrIfc(String),

    #[error("package name is empty in `{0}`")]
    EmptyPackageName(String),

    #[error("ifc name is empty in `{0}`")]
    EmptyIfcName(String),

    #[error("version is empty in `{0}`")]
    EmptyVersion(String),
}

#[derive(Hash, Clone, PartialEq, Eq)]
pub struct FnMarker;

pub type FnName = Name<FnMarker>;

#[derive(
    Hash,
    Clone,
    PartialEq,
    Eq,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
    schemars::JsonSchema,
)]
#[schemars(with = "String")]
pub struct FunctionFqn {
    // TODO: Consider storing parsed IfcFqn instead.
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
        // Attept to parse IfcFqn
        IfcFqnName::from_str(ifc_fqn)?;

        if function_name.contains('.') {
            Err(FunctionFqnParseError::DelimiterFoundInFunctionName(
                function_name.to_string(),
            ))
        } else {
            Ok(Self::new_arc(Arc::from(ifc_fqn), Arc::from(function_name)))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FunctionFqnParseError {
    #[error(transparent)]
    IfcFqnParseError(#[from] IfcFqnParseError),
    #[error("delimiter `.` between interface FQN and function name not found in `{0}`")]
    DelimiterNotFound(String),
    #[error("delimiter `.` found in function name `{0}`")]
    DelimiterFoundInFunctionName(String),
}

impl FromStr for FunctionFqn {
    type Err = FunctionFqnParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some((ifc_fqn, function_name)) = value.rsplit_once('.') {
            // Attept to parse IfcFqn
            IfcFqnName::from_str(ifc_fqn)?;
            Ok(Self::new_arc(Arc::from(ifc_fqn), Arc::from(function_name)))
        } else {
            Err(FunctionFqnParseError::DelimiterNotFound(value.to_string()))
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

#[cfg(feature = "test")]
impl<'a> arbitrary::Arbitrary<'a> for FunctionFqn {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let illegal = [':', '@', '.'];
        let namespace = format!("ns{}", u.arbitrary::<String>()?.replace(illegal, "")); // must not be empty

        let pkg_name = format!("pkg{}", u.arbitrary::<String>()?.replace(illegal, ""));
        let ifc_name = format!("ifc{}", u.arbitrary::<String>()?.replace(illegal, ""));
        let fn_name = format!("fn{}", u.arbitrary::<String>()?.replace(illegal, ""));

        Ok(FunctionFqn::new_arc(
            Arc::from(format!("{namespace}:{pkg_name}/{ifc_name}")),
            Arc::from(fn_name),
        ))
    }
}
