use super::{NAMESPACE_OBELISK, SUFFIX_PKG_EXT, SUFFIX_PKG_STUB, ifc_fqn::IfcFqn};
use anyhow::Context;
use std::fmt::Display;
use wit_parser::{Interface, PackageName};

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
        self.package_name.ends_with(SUFFIX_PKG_EXT) || self.package_name.ends_with(SUFFIX_PKG_STUB)
    }

    #[must_use]
    pub fn is_namespace_obelisk(&self) -> bool {
        self.namespace == NAMESPACE_OBELISK
    }

    pub fn ifc_fqn(&self, interface: &Interface) -> Result<IfcFqn, anyhow::Error> {
        Ok(IfcFqn {
            pkg_fqn: self.clone(),
            ifc_name: interface
                .name
                .clone()
                .context("inline interfaces are not supported")?,
        })
    }
}

impl From<&PackageName> for PkgFqn {
    fn from(value: &PackageName) -> Self {
        Self {
            namespace: value.namespace.clone(),
            package_name: value.name.clone(),
            version: value.version.as_ref().map(|v| v.to_string()),
        }
    }
}
