use std::time::Duration;
use tokio::task::AbortHandle;
use tracing::info;
use wasmtime::EngineWeak;

pub struct EpochTicker {
    abort_handle: AbortHandle,
}

impl EpochTicker {
    #[must_use]
    pub fn spawn_new(engines: Vec<EngineWeak>, epoch: Duration) -> Self {
        info!("Spawning the epoch ticker");
        let abort_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(epoch).await;
                for engine in &engines {
                    if let Some(engine) = engine.upgrade() {
                        engine.increment_epoch();
                    }
                }
            }
        })
        .abort_handle();
        Self { abort_handle }
    }
}

impl Drop for EpochTicker {
    fn drop(&mut self) {
        info!("Aborting the epoch ticker");
        self.abort_handle.abort();
    }
}
