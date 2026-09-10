//! Task spawning helpers that attach a root tracing span and a name visible in `tokio-console`.
//!
//! `tokio::task::Builder::name` only exists under `--cfg tokio_unstable`
//! (the same flag `tokio-console` needs), so the named path is compiled in
//! only then; otherwise these fall back to plain `tokio::spawn`.

use std::future::Future;
use tokio::task::JoinHandle;
use tracing::Instrument as _;

fn instrument_task<F>(name: &str, future: F) -> tracing::instrument::Instrumented<F>
where
    F: Future,
{
    future.instrument(tracing::info_span!(parent: None, "task", task.name = name))
}

/// Spawn a task carrying `name`, shown in `tokio-console`.
#[cfg(tokio_unstable)]
pub fn spawn_named<F>(name: &str, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::task::Builder::new()
        .name(name)
        .spawn(instrument_task(name, future))
        .expect("spawning a task never fails")
}

/// Spawn a task carrying `name`, shown in `tokio-console`.
#[cfg(not(tokio_unstable))]
pub fn spawn_named<F>(name: &str, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(instrument_task(name, future))
}
