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

    pub fn started_at(&self) -> Duration {
        match self {
            TraceData::Root(_) => Duration::ZERO,
            TraceData::Child(child) => child.started_at,
        }
    }

    pub fn finished_at(&self) -> Duration {
        match self {
            TraceData::Root(root) => root.finished_at,
            TraceData::Child(child) => child.finished_at,
        }
    }

    pub fn children(&self) -> &[TraceDataChild] {
        match self {
            TraceData::Root(root) => &root.children,
            TraceData::Child(child) => &child.children,
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct TraceDataRoot {
    pub name: String,
    pub finished_at: Duration,
    pub children: Vec<TraceDataChild>,
}

#[derive(Clone, PartialEq)]
pub struct TraceDataChild {
    pub name: String,
    pub started_at: Duration,
    pub finished_at: Duration,
    pub children: Vec<TraceDataChild>,
}
