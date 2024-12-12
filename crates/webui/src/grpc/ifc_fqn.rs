use super::pkg_fqn::PkgFqn;
use anyhow::bail;
use std::{fmt::Display, str::FromStr};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IfcFqn {
    pub pkg_fqn: PkgFqn,
    pub ifc_name: String,
}

impl Display for IfcFqn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let IfcFqn {
            pkg_fqn:
                PkgFqn {
                    namespace,
                    package_name,
                    version,
                },
            ifc_name,
        } = self;
        if let Some(version) = version {
            write!(f, "{namespace}:{package_name}/{ifc_name}@{version}")
        } else {
            write!(f, "{namespace}:{package_name}/{ifc_name}")
        }
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
            pkg_fqn: PkgFqn {
                namespace: namespace.to_string(),
                package_name: package_name.to_string(),
                version,
            },
            ifc_name: ifc_name.to_string(),
        })
    }
}
