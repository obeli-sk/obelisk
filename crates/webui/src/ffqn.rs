use crate::grpc_client;
use std::{fmt::Display, str::FromStr};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct FunctionFqn {
    pub ifc_fqn: String,
    pub function_name: String,
}
impl FunctionFqn {
    pub fn from_fn_detail(fn_detail: &grpc_client::FunctionDetails) -> FunctionFqn {
        let Some(function) = &fn_detail.function else {
            unreachable!("FunctionDetails.function is sent by the server");
        };
        FunctionFqn {
            ifc_fqn: function.interface_name.clone(),
            function_name: function.function_name.clone(),
        }
    }
}

impl FromStr for FunctionFqn {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((ifc_fqn, function_name)) = s.split_once('.') {
            if function_name.contains(".") {
                Err("delimiter `.` found more than once")
            } else {
                Ok(FunctionFqn {
                    ifc_fqn: ifc_fqn.to_string(),
                    function_name: function_name.to_string(),
                })
            }
        } else {
            Err("delimiter `.` not found")
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
            interface_name: value.ifc_fqn,
            function_name: value.function_name,
        }
    }
}

impl From<grpc_client::FunctionName> for FunctionFqn {
    fn from(value: grpc_client::FunctionName) -> Self {
        Self {
            ifc_fqn: value.interface_name,
            function_name: value.function_name,
        }
    }
}
