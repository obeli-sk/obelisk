use tokio::task::AbortHandle;

pub mod executor;
pub mod expired_timers_watcher;
pub mod worker;

pub struct AbortOnDropHandle(pub AbortHandle);
impl Drop for AbortOnDropHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}
