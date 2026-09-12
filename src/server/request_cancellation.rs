use std::future::Future;
use tokio::sync::watch;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ServerTerminated;

pub(crate) async fn until_terminated<T>(
    mut termination_watcher: watch::Receiver<()>,
    work: impl Future<Output = T>,
) -> Result<T, ServerTerminated> {
    tokio::select! {
        result = work => Ok(result),
        _ = termination_watcher.changed() => Err(ServerTerminated),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    struct DropMarker(Arc<AtomicBool>);

    impl Drop for DropMarker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn termination_drops_work_future() {
        let (termination_sender, termination_watcher) = watch::channel(());
        let dropped = Arc::new(AtomicBool::new(false));
        let marker = DropMarker(dropped.clone());
        let work = async move {
            let _marker = marker;
            std::future::pending::<()>().await;
        };

        drop(termination_sender);
        assert!(until_terminated(termination_watcher, work).await.is_err());
        assert!(dropped.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn returns_completed_work() {
        let (_termination_sender, termination_watcher) = watch::channel(());
        assert_eq!(
            until_terminated(termination_watcher, async { 42 }).await,
            Ok(42)
        );
    }
}
