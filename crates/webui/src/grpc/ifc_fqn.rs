use super::{NAMESPACE_OBELISK, SUFFIX_PKG_EXT};
use anyhow::bail;
use std::{fmt::Display, str::FromStr};

#[derive(Debug, PartialEq, Eq, Hash, Ord)]
// TODO: Unify with IfcFqnName
pub struct IfcFqn {
    pub namespace: String,
    pub package_name: String,
    pub ifc_name: String,
    pub version: Option<String>,
}

impl Display for IfcFqn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let IfcFqn {
            namespace,
            package_name,
            ifc_name,
            version,
        } = self;
        if let Some(version) = version {
            write!(f, "{namespace}:{package_name}/{ifc_name}@{version}")
        } else {
            write!(f, "{namespace}:{package_name}/{ifc_name}")
        }
    }
}

impl PartialOrd for IfcFqn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.namespace
                .cmp(&other.namespace)
                .then_with(|| self.package_name.cmp(&other.package_name))
                .then_with(|| self.ifc_name.cmp(&other.ifc_name))
                .then_with(|| self.version.cmp(&other.version)),
        )
    }
}

impl IfcFqn {
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
}

impl FromStr for IfcFqn {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let Some((namespace, rest)) = input.split_once(':') else {
            bail!("Separator `:` is missing")
        };
        let Some((package_name, rest)) = rest.split_once('/') else {
            bail!("Separator `/` is missing")
        };
        let (ifc_name, version) = if let Some((ifc_name, version)) = rest.split_once("@") {
            (ifc_name, Some(version.to_string()))
        } else {
            (rest, None)
        };

        Ok(Self {
            namespace: namespace.to_string(),
            package_name: package_name.to_string(),
            ifc_name: ifc_name.to_string(),
            version,
        })
    }
}
