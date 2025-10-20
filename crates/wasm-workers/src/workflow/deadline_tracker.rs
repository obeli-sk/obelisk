use std::{pin::Pin, sync::Arc, time::Duration};

pub trait DeadlineTracker: Send + Sync {
    fn track(&self) -> Option<Pin<Box<dyn Future<Output = ()> + Send>>>;
}

pub trait DeadlineTrackerFactory: Send + Sync {
    fn create(&self, deadline_duration: Duration) -> Arc<dyn DeadlineTracker>;
}

pub(crate) struct DeadlineTrackerTokio {
    deadline: tokio::time::Instant,
}
impl DeadlineTrackerTokio {
    pub(crate) fn new(deadline_duration: Duration) -> DeadlineTrackerTokio {
        let deadline = tokio::time::Instant::now() + deadline_duration;
        DeadlineTrackerTokio { deadline }
    }
}
impl DeadlineTracker for DeadlineTrackerTokio {
    fn track(&self) -> Option<Pin<Box<dyn Future<Output = ()> + Send>>> {
        if self.deadline <= tokio::time::Instant::now() {
            None
        } else {
            Some(Box::pin(tokio::time::sleep_until(self.deadline)))
        }
    }
}

#[derive(Clone, Copy)]
pub struct DeadlineTrackerFactoryTokio;

impl DeadlineTrackerFactory for DeadlineTrackerFactoryTokio {
    fn create(&self, deadline_duration: Duration) -> Arc<dyn DeadlineTracker> {
        Arc::new(DeadlineTrackerTokio::new(deadline_duration))
    }
}
