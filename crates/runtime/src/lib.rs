use concepts::SupportedFunctionResult;
use error::ActivityFailed;

pub mod activity;
pub mod database;
pub mod error;
pub mod event_history;
mod host_activity;
pub mod runtime;
pub mod workflow;

pub type ActivityResponse = Result<SupportedFunctionResult, ActivityFailed>;
