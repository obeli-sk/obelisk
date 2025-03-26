use chrono::{DateTime, Utc};
use std::time::Duration;

#[derive(Clone, PartialEq)]
pub enum TraceData {
    Root(TraceDataRoot),
    Child(TraceDataChild),
}
impl TraceData {
    pub fn name(&self) -> &str {
        match self {
            TraceData::Root(root) => root.name.as_str(),
            TraceData::Child(child) => child.name.as_str(),
        }
    }

    pub fn start_percentage(
        &self,
        root_scheduled_at: DateTime<Utc>,
        root_last_event_at: DateTime<Utc>,
    ) -> f64 {
        match self {
            TraceData::Root(_) => 0.0,
            TraceData::Child(child) => {
                let total_duration = root_last_event_at - root_scheduled_at;
                100.0 * (child.started_at - root_scheduled_at).num_milliseconds() as f64
                    / total_duration.num_milliseconds() as f64
            }
        }
    }

    pub fn busy_end_percentage(
        &self,
        root_scheduled_at: DateTime<Utc>,
        root_last_event_at: DateTime<Utc>,
    ) -> f64 {
        match self {
            TraceData::Root(_) => 100.0,
            TraceData::Child(child) => {
                let total_duration = root_last_event_at - root_scheduled_at;
                100.0
                    * (child.finished_at.unwrap_or(root_last_event_at) - child.started_at)
                        .num_milliseconds() as f64
                    / total_duration.num_milliseconds() as f64
            }
        }
    }

    pub fn start_to_end(&self, root_last_event_at: DateTime<Utc>) -> Duration {
        match self {
            TraceData::Root(TraceDataRoot {
                scheduled_at,
                last_event_at,
                ..
            }) => (*last_event_at - *scheduled_at)
                .to_std()
                .expect("scheduled_at must be <= last_event_at"),
            TraceData::Child(TraceDataChild {
                started_at,
                finished_at,
                ..
            }) => (finished_at.unwrap_or(root_last_event_at) - *started_at)
                .to_std()
                .expect("started_at must be <= finished_at"),
        }
    }

    pub fn children(&self) -> &[TraceDataChild] {
        match self {
            TraceData::Root(root) => &root.children,
            TraceData::Child(child) => &child.children,
        }
    }
}

// #[derive(Clone, PartialEq)]
// pub struct BusyInterval {
//     pub started_at: DateTime<Utc>,
//     pub finished_at: DateTime<Utc>,
// }

#[derive(Clone, PartialEq)]
pub struct TraceDataRoot {
    pub name: String,
    pub scheduled_at: DateTime<Utc>,
    pub last_event_at: DateTime<Utc>,
    // pub busy: Vec<BusyInterval>,
    pub children: Vec<TraceDataChild>,
}

#[derive(Clone, PartialEq)]
pub struct TraceDataChild {
    pub name: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub children: Vec<TraceDataChild>,
}
