use std::{borrow::Cow, fmt::Display, sync::Arc};

use activity::Activities;
use event_history::SupportedFunctionResult;
use queue::activity_queue::{ActivityQueueReceiver, ActivityQueueSender, QueueItem};
use tokio::{sync::mpsc, task::AbortHandle};
use val_json::{TypeWrapper, UnsupportedTypeError, ValWrapper};

pub mod activity;
pub mod event_history;
mod queue;
mod wasm_tools;
pub mod workflow;

#[derive(Hash, Clone, Debug, PartialEq, Eq)]
pub struct FunctionFqn<'a> {
    pub ifc_fqn: Cow<'a, str>,
    pub function_name: Cow<'a, str>,
}

impl FunctionFqn<'_> {
    pub const fn new<'a>(ifc_fqn: &'a str, function_name: &'a str) -> FunctionFqn<'a> {
        FunctionFqn {
            ifc_fqn: Cow::Borrowed(ifc_fqn),
            function_name: Cow::Borrowed(function_name),
        }
    }

    pub fn new_owned(ifc_fqn: String, function_name: String) -> FunctionFqn<'static> {
        FunctionFqn {
            ifc_fqn: Cow::Owned(ifc_fqn),
            function_name: Cow::Owned(function_name),
        }
    }

    pub fn to_owned(&self) -> FunctionFqn<'static> {
        Self::new_owned(self.ifc_fqn.to_string(), self.function_name.to_string())
    }
}

impl Display for FunctionFqn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{ifc_fqn}.{function_name}",
            ifc_fqn = self.ifc_fqn,
            function_name = self.function_name
        )
    }
}

#[derive(thiserror::Error, Debug)]
pub enum FunctionMetadataError {
    #[error("{0}")]
    UnsupportedType(#[from] UnsupportedTypeError),

    #[error("unsupported return type in `{fqn}`, got type {ty}")]
    UnsupportedReturnType { fqn: String, ty: String },
}

#[derive(Clone, Debug)]
pub struct FunctionMetadata {
    pub results_len: usize,
    pub params: Vec<(String /*name*/, TypeWrapper)>,
}

impl FunctionMetadata {
    pub fn deserialize_params<V: From<ValWrapper>>(
        &self,
        param_vals: &str,
    ) -> Result<Vec<V>, serde_json::error::Error> {
        let param_types = self.params.iter().map(|(_, type_w)| type_w);
        val_json::deserialize_sequence(param_vals, param_types)
    }
}

pub(crate) type ActivityResponse = Result<SupportedFunctionResult, ActivityFailed>;

#[derive(thiserror::Error, Debug, Clone)]
#[error("activity `{activity_fqn}` failed: {reason}")]
pub(crate) struct ActivityFailed {
    activity_fqn: Arc<FunctionFqn<'static>>,
    reason: String,
}

pub struct Runtime {
    activities: Arc<Activities>,
    queue_sender: mpsc::Sender<QueueItem>,
    queue_task: AbortHandle,
}

impl Runtime {
    pub fn new(activities: Arc<Activities>) -> Self {
        let (queue_sender, queue_receiver) = mpsc::channel(100); // FIXME
        let mut activity_queue_receiver = ActivityQueueReceiver {
            receiver: queue_receiver,
            activities: activities.clone(),
        };
        let queue_task =
            tokio::spawn(async move { activity_queue_receiver.process().await }).abort_handle();
        Self {
            activities,
            queue_sender,
            queue_task,
        }
    }

    fn activity_queue_writer(&self) -> ActivityQueueSender {
        ActivityQueueSender {
            sender: self.queue_sender.clone(),
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.queue_task.abort();
    }
}

mod workflow_id {
    use std::{str::FromStr, sync::Arc};

    #[derive(Debug, Clone, derive_more::Display)]
    pub struct WorkflowId(Arc<String>);
    impl WorkflowId {
        pub(crate) fn generate() -> WorkflowId {
            ulid::Ulid::new().to_string().parse().unwrap() // ulid is 26 chars long
        }
    }

    const MIN_LEN: usize = 1;
    const MAX_LEN: usize = 32;

    impl FromStr for WorkflowId {
        type Err = WorkflowIdParseError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            if s.len() < MIN_LEN {
                return Err(WorkflowIdParseError::TooShort);
            }
            if s.len() > MAX_LEN {
                return Err(WorkflowIdParseError::TooLong);
            }
            if s.chars()
                .all(|x| x.is_alphanumeric() || x == '_' || x == '-')
            {
                Ok(Self(Arc::new(s.to_string())))
            } else {
                Err(WorkflowIdParseError::IllegalCharacters)
            }
        }
    }

    #[derive(Debug, thiserror::Error, PartialEq, Eq)]
    pub enum WorkflowIdParseError {
        #[error("workflow id too long, maximal length: {MAX_LEN}")]
        TooLong,
        #[error("workflow id too short, minimal length: {MIN_LEN}")]
        TooShort,
        #[error("only alphanumeric characters, `_` and `-` are allowed in workflow id")]
        IllegalCharacters,
    }

    #[cfg(test)]
    mod tests {
        use crate::workflow_id::MAX_LEN;

        use super::{WorkflowId, WorkflowIdParseError};

        #[test]
        fn parse_workflow_id() {
            assert_eq!("w1".parse::<WorkflowId>().unwrap().to_string(), "w1");
            assert_eq!(
                "w1-2_ID".parse::<WorkflowId>().unwrap().to_string(),
                "w1-2_ID"
            );
            assert_eq!(
                "w1\n".parse::<WorkflowId>().unwrap_err(),
                WorkflowIdParseError::IllegalCharacters
            );
            assert_eq!(
                "".parse::<WorkflowId>().unwrap_err(),
                WorkflowIdParseError::TooShort
            );
            assert_eq!(
                "x".repeat(MAX_LEN + 1).parse::<WorkflowId>().unwrap_err(),
                WorkflowIdParseError::TooLong
            );
        }
    }
}
