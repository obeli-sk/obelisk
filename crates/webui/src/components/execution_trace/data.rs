use chrono::{DateTime, Utc};
use std::time::Duration;
use yew::Html;

#[derive(Debug, Clone, PartialEq)]
pub enum TraceData {
    Root(TraceDataRoot),
    Child(TraceDataChild),
}
impl TraceData {
    pub fn name(&self) -> &Html {
        match self {
            TraceData::Root(root) => &root.name,
            TraceData::Child(child) => &child.name,
        }
    }

    pub fn busy(&self) -> &[BusyInterval] {
        match self {
            TraceData::Root(TraceDataRoot { busy, .. }) => busy,
            TraceData::Child(TraceDataChild { busy, .. }) => busy,
        }
    }

    pub fn busy_duration(&self, root_last_event_at: DateTime<Utc>) -> Duration {
        self.busy()
            .iter()
            .map(|interval| interval.duration(root_last_event_at))
            .reduce(|acc, current| acc + current)
            .unwrap_or_default()
    }

    pub fn children(&self) -> &[TraceData] {
        match self {
            TraceData::Root(root) => &root.children,
            TraceData::Child(child) => &child.children,
        }
    }

    pub fn title(&self) -> &str {
        match self {
            TraceData::Root(root) => &root.title,
            TraceData::Child(child) => &child.title,
        }
    }
}

#[derive(Debug, Clone, PartialEq, derive_more::Display)]
pub enum BusyIntervalStatus {
    #[display("Finished")]
    HttpTraceFinished,
    #[display("Unfinished")]
    HttpTraceUnfinished,
    #[display("Error")]
    HttpTraceError,
    #[display("Temporary timeout")]
    ExecutionTimeoutTemporary,
    #[display("Permanent timeout")]
    ExecutionTimeoutPermanent,
    #[display("Temporary error")]
    ExecutionErrorTemporary,
    #[display("Permanent error")]
    ExecutionErrorPermanent,
    #[display("Locked & unlocked")]
    ExecutionLockedAndUnlocked,
    #[display("Finished")]
    ExecutionFinished,
    #[display("Finished with error")]
    ExecutionReturnedErrorVariant,
    #[display("Unfinished")]
    ExecutionUnfinished,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BusyInterval {
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub title: Option<String>,
    pub status: BusyIntervalStatus,
}
impl BusyInterval {
    pub fn as_percentage(
        &self,
        root_scheduled_at: DateTime<Utc>,
        root_last_event_at: DateTime<Utc>,
    ) -> (f64, f64) {
        let total_duration = root_last_event_at - root_scheduled_at;
        let start_percentage = 100.0
            * (self.started_at - root_scheduled_at).num_milliseconds() as f64
            / total_duration.num_milliseconds() as f64;

        let end_percentage = 100.0
            * (self.finished_at.unwrap_or(root_last_event_at) - self.started_at).num_milliseconds()
                as f64
            / total_duration.num_milliseconds() as f64;
        (start_percentage, end_percentage)
    }

    fn duration(&self, root_last_event_at: DateTime<Utc>) -> Duration {
        (self.finished_at.unwrap_or(root_last_event_at) - self.started_at)
            .to_std()
            .expect("started_at must be <= finished_at")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TraceDataRoot {
    pub name: Html,
    pub title: String,
    pub scheduled_at: DateTime<Utc>,
    pub last_event_at: DateTime<Utc>,
    pub busy: Vec<BusyInterval>,
    pub children: Vec<TraceData>,
}
impl TraceDataRoot {
    pub fn total_duration(&self) -> Duration {
        (self.last_event_at - self.scheduled_at)
            .to_std()
            .expect("scheduled_at must be <= last_event_at")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TraceDataChild {
    pub name: Html,
    pub title: String,
    pub busy: Vec<BusyInterval>,
    pub children: Vec<TraceData>,
}

mod grpc {
    use super::BusyIntervalStatus;
    use crate::grpc::grpc_client::result_detail;

    impl From<&result_detail::Value> for BusyIntervalStatus {
        fn from(result_detail_value: &result_detail::Value) -> Self {
            match result_detail_value {
                result_detail::Value::Ok(_) => BusyIntervalStatus::ExecutionFinished,
                result_detail::Value::FallibleError(_) => {
                    BusyIntervalStatus::ExecutionReturnedErrorVariant
                }
                result_detail::Value::Timeout(_) => BusyIntervalStatus::ExecutionTimeoutPermanent,
                result_detail::Value::ExecutionFailure(_) => {
                    BusyIntervalStatus::ExecutionErrorPermanent
                }
                result_detail::Value::UnhandledChildExecutionError(_) => {
                    BusyIntervalStatus::ExecutionErrorPermanent
                }
            }
        }
    }
}
