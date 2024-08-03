use std::{borrow::Borrow, sync::Arc};

use super::TonicRespResult;
use crate::command::grpc;
use anyhow::anyhow;
use concepts::{
    prefixed_ulid::{ExecutorId, JoinSetId, RunId},
    ExecutionId, FunctionFqn,
};

impl From<ExecutionId> for grpc::ExecutionId {
    fn from(value: ExecutionId) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}

impl From<JoinSetId> for grpc::JoinSetId {
    fn from(value: JoinSetId) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}

impl From<ExecutorId> for grpc::ExecutorId {
    fn from(value: ExecutorId) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}

impl From<RunId> for grpc::RunId {
    fn from(value: RunId) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}

impl TryFrom<grpc::ExecutionId> for ExecutionId {
    type Error = tonic::Status;

    fn try_from(value: grpc::ExecutionId) -> Result<Self, Self::Error> {
        value.id.parse().map_err(|parse_err| {
            tonic::Status::invalid_argument(format!("ExecutionId cannot be parsed - {parse_err}"))
        })
    }
}

pub trait OptionExt<T> {
    fn argument_must_exist(self, argument: &str) -> Result<T, tonic::Status>;
}

impl<T> OptionExt<T> for Option<T> {
    fn argument_must_exist(self, argument: &str) -> Result<T, tonic::Status> {
        self.ok_or_else(|| {
            tonic::Status::invalid_argument(format!("argument `{argument}` must exist"))
        })
    }
}

pub(crate) fn unwrap_resp_or_get_err_message<T>(
    res: TonicRespResult<T>,
) -> Result<tonic::Response<T>, anyhow::Error> {
    res.map_err(|err| {
        let msg = err.message();
        if msg.is_empty() {
            anyhow!("{err}")
        } else {
            anyhow!("{msg}")
        }
    })
}

impl From<grpc::FunctionName> for FunctionFqn {
    fn from(value: grpc::FunctionName) -> Self {
        FunctionFqn::new_arc(
            Arc::from(value.interface_name),
            Arc::from(value.function_name),
        )
    }
}

impl<T: Borrow<FunctionFqn>> From<T> for grpc::FunctionName {
    fn from(ffqn: T) -> Self {
        let ffqn = ffqn.borrow();
        grpc::FunctionName {
            interface_name: ffqn.ifc_fqn.to_string(),
            function_name: ffqn.function_name.to_string(),
        }
    }
}

pub trait PendingStatusExt {
    fn is_finished(&self) -> bool;
}

impl PendingStatusExt for grpc::ExecutionStatus {
    fn is_finished(&self) -> bool {
        use grpc::execution_status::Status;
        matches!(
            self,
            grpc::ExecutionStatus {
                status: Some(Status::Finished(..))
            }
        )
    }
}
