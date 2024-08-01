use super::TonicRespResult;
use crate::command::grpc;
use anyhow::anyhow;
use concepts::{
    prefixed_ulid::{ExecutorId, JoinSetId, RunId},
    ExecutionId,
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

pub(crate) fn unwrap_friendly_resp<T>(
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
