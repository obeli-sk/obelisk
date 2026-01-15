use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tracing::{debug, instrument};
use wasmtime::EngineWeak;

pub struct EpochTicker {
    shutdown: Arc<AtomicBool>,
}

impl EpochTicker {
    #[must_use]
    pub fn spawn_new(engines: Vec<EngineWeak>, epoch: Duration) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        {
            let shutdown = shutdown.clone();
            std::thread::spawn(move || Self::epoch_ticker(&engines, epoch, &shutdown));
        }
        Self { shutdown }
    }

    #[instrument(skip_all)]
    fn epoch_ticker(engines: &[EngineWeak], epoch: Duration, shutdown: &Arc<AtomicBool>) {
        debug!("Spawned the epoch ticker");
        while !shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            std::thread::sleep(epoch);
            for engine in engines {
                if let Some(engine) = engine.upgrade() {
                    engine.increment_epoch();
                }
            }
        }
    }
}

impl Drop for EpochTicker {
    fn drop(&mut self) {
        debug!("Closing the epoch ticker");
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
