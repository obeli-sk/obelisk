use chrono::{DateTime, TimeDelta, Utc};
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
            .filter(|interval| interval.status != BusyIntervalStatus::ExecutionSinceScheduled)
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

    pub fn load_button(&self) -> Option<Html> {
        match self {
            TraceData::Root(root) => root.load_button.clone(),
            TraceData::Child(child) => child.load_button.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, derive_more::Display)]
pub enum BusyIntervalStatus {
    #[display("Finished")]
    HttpTraceFinished(u32),
    #[display("Timeout")]
    HttpTraceNotResponded,
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
    #[display("Locked")]
    ExecutionLocked,
    #[display("Finished")]
    ExecutionFinished,
    #[display("Finished with error")]
    ExecutionReturnedErrorVariant,
    #[display("Unfinished")]
    ExecutionUnfinished,
    #[display("Since scheduled")]
    ExecutionSinceScheduled,
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
        let total_duration_micros =
            root_last_event_at.timestamp_micros() - root_scheduled_at.timestamp_micros();
        let start_percentage = 100.0
            * (self.started_at.timestamp_micros() - root_scheduled_at.timestamp_micros()) as f64
            / total_duration_micros as f64;

        let end_percentage = 100.0
            * (self
                .finished_at
                .unwrap_or(root_last_event_at)
                .timestamp_micros()
                - self.started_at.timestamp_micros()) as f64
            / total_duration_micros as f64;

        (start_percentage, end_percentage)
    }

    fn duration(&self, root_last_event_at: DateTime<Utc>) -> Duration {
        TimeDelta::microseconds(
            self.finished_at
                .unwrap_or(root_last_event_at)
                .timestamp_micros()
                - self.started_at.timestamp_micros(),
        )
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
    pub load_button: Option<Html>,
}
impl TraceDataRoot {
    pub fn total_duration(&self) -> Duration {
        TimeDelta::microseconds(
            self.last_event_at.timestamp_micros() - self.scheduled_at.timestamp_micros(),
        )
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
    pub load_button: Option<Html>,
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
            }
        }
    }
}

mod css {
    use super::BusyIntervalStatus;

    impl BusyIntervalStatus {
        pub fn get_css_class(&self) -> &'static str {
            match self {
                BusyIntervalStatus::HttpTraceFinished(_) => "busy-http-trace-finished",
                BusyIntervalStatus::HttpTraceNotResponded => "busy-http-trace-unfinished",
                BusyIntervalStatus::HttpTraceError => "busy-http-trace-error",
                BusyIntervalStatus::ExecutionTimeoutTemporary => "busy-execution-timeout-temporary",
                BusyIntervalStatus::ExecutionTimeoutPermanent => "busy-execution-timeout-permanent",
                BusyIntervalStatus::ExecutionErrorTemporary => "busy-execution-error-temporary",
                BusyIntervalStatus::ExecutionErrorPermanent => "busy-execution-error-permanent",
                BusyIntervalStatus::ExecutionLocked => "busy-execution-locked",
                BusyIntervalStatus::ExecutionFinished => "busy-execution-finished",
                BusyIntervalStatus::ExecutionReturnedErrorVariant => {
                    "busy-execution-returned-error-variant"
                }
                BusyIntervalStatus::ExecutionUnfinished => "busy-execution-unfinished",
                BusyIntervalStatus::ExecutionSinceScheduled => "busy-execution-since-scheduled",
            }
        }
    }
}
