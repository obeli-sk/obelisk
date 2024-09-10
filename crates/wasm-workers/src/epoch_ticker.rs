use std::{
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use tracing::info;
use wasmtime::EngineWeak;

pub struct EpochTicker {
    shutdown: Arc<AtomicBool>,
}

impl EpochTicker {
    #[must_use]
    pub fn spawn_new(engines: Vec<EngineWeak>, epoch: Duration) -> Self {
        info!("Spawning the epoch ticker");
        let shutdown = Arc::new(AtomicBool::new(false));
        std::thread::spawn({
            let shutdown = shutdown.clone();
            move || {
                while !shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                    std::thread::sleep(epoch);
                    for engine in &engines {
                        if let Some(engine) = engine.upgrade() {
                            engine.increment_epoch();
                        }
                    }
                }
            }
        });
        Self { shutdown }
    }
}

impl Drop for EpochTicker {
    fn drop(&mut self) {
        info!("Aborting the epoch ticker");
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
