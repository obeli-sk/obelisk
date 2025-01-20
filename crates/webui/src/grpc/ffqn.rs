use anyhow::bail;
use yew::ToHtml;

use crate::grpc::grpc_client;
use std::{fmt::Display, str::FromStr};

use super::ifc_fqn::IfcFqn;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct FunctionFqn {
    pub ifc_fqn: IfcFqn,
    pub function_name: String,
}
impl FunctionFqn {
    pub fn from_fn_detail(
        fn_detail: &grpc_client::FunctionDetail,
    ) -> Result<FunctionFqn, anyhow::Error> {
        let Some(function) = &fn_detail.function_name else {
            unreachable!("FunctionDetails.function is sent by the server");
        };
        Ok(FunctionFqn {
            ifc_fqn: IfcFqn::from_str(&function.interface_name)?,
            function_name: function.function_name.clone(),
        })
    }
}

impl FromStr for FunctionFqn {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((ifc_fqn, function_name)) = s.split_once('.') {
            if function_name.contains(".") {
                bail!("delimiter `.` found more than once")
            } else {
                Ok(FunctionFqn {
                    ifc_fqn: IfcFqn::from_str(ifc_fqn)?,
                    function_name: function_name.to_string(),
                })
            }
        } else {
            bail!("delimiter `.` not found")
        }
    }
}

impl Display for FunctionFqn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.ifc_fqn, self.function_name)
    }
}

impl From<FunctionFqn> for grpc_client::FunctionName {
    fn from(value: FunctionFqn) -> Self {
        Self {
            interface_name: value.ifc_fqn.to_string(),
            function_name: value.function_name,
        }
    }
}

impl From<grpc_client::FunctionName> for FunctionFqn {
    fn from(value: grpc_client::FunctionName) -> Self {
        Self {
            ifc_fqn: IfcFqn::from_str(&value.interface_name).expect("ffqn must be parseable"),
            function_name: value.function_name,
        }
    }
}

impl ToHtml for FunctionFqn {
    fn to_html(&self) -> yew::Html {
        self.to_string().into_html()
    }
}
