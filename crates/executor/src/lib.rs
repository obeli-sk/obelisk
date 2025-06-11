pub mod executor;
pub mod expired_timers_watcher;
pub mod worker;

use tokio::task::AbortHandle;

pub struct AbortOnDropHandle {
    handle: AbortHandle,
}
impl AbortOnDropHandle {
    #[must_use]
    pub fn new(handle: AbortHandle) -> Self {
        Self { handle }
    }
}
impl Drop for AbortOnDropHandle {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
