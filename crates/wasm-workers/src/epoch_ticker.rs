use std::time::Duration;

use tokio::task::AbortHandle;
use wasmtime::EngineWeak;

pub struct EpochTicker {
    abort_handle: AbortHandle,
}

impl EpochTicker {
    pub fn spawn_new(engines: Vec<EngineWeak>, epoch: Duration) -> Self {
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
        self.abort_handle.abort();
    }
}
