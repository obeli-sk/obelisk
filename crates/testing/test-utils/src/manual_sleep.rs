use concepts::time::Sleep;
use std::{future::Future, pin::Pin, time::Duration};
use tokio::sync::watch;

pub struct ManualSleep {
    elapsed: watch::Sender<bool>,
}

impl Default for ManualSleep {
    fn default() -> Self {
        Self {
            elapsed: watch::channel(false).0,
        }
    }
}

impl ManualSleep {
    pub fn expire(&self) {
        self.elapsed.send_replace(true);
    }
}

impl Sleep for ManualSleep {
    fn sleep(&self, _duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let mut elapsed = self.elapsed.subscribe();
        Box::pin(async move {
            elapsed.wait_for(|elapsed| *elapsed).await.unwrap();
        })
    }
}
