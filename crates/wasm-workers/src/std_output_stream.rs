// Based on https://github.com/bytecodealliance/wasmtime/blob/v36.0.1/src/commands/serve.rs#L874
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use tokio::io::AsyncWrite;
use wasmtime_wasi::p2::{StreamError, StreamResult};

#[derive(Clone, Copy, Debug)]
pub enum StdOutput {
    Stdout,
    Stderr,
}

impl StdOutput {
    fn write_all(&self, buf: &[u8]) -> Result<(), std::io::Error> {
        use std::io::Write;

        match self {
            StdOutput::Stdout => std::io::stdout().write_all(buf),
            StdOutput::Stderr => std::io::stderr().write_all(buf),
        }
    }
}

#[derive(Clone)]
pub(crate) struct LogStream {
    output: StdOutput,
    state: Arc<LogStreamState>,
}

struct LogStreamState {
    prefix: String,
    needs_prefix_on_next_write: AtomicBool,
}

impl LogStream {
    pub(crate) fn new(prefix: String, output: StdOutput) -> LogStream {
        LogStream {
            output,
            state: Arc::new(LogStreamState {
                prefix,
                needs_prefix_on_next_write: AtomicBool::new(true),
            }),
        }
    }
}

impl wasmtime_wasi::cli::StdoutStream for LogStream {
    fn p2_stream(&self) -> Box<dyn wasmtime_wasi::p2::OutputStream> {
        Box::new(self.clone())
    }
    fn async_stream(&self) -> Box<dyn tokio::io::AsyncWrite + Send + Sync> {
        Box::new(self.clone())
    }
}

impl wasmtime_wasi::cli::IsTerminal for LogStream {
    fn is_terminal(&self) -> bool {
        match &self.output {
            StdOutput::Stdout => std::io::stdout().is_terminal(),
            StdOutput::Stderr => std::io::stderr().is_terminal(),
        }
    }
}

impl wasmtime_wasi::p2::OutputStream for LogStream {
    fn write(&mut self, bytes: bytes::Bytes) -> StreamResult<()> {
        self.output
            .write_all(&bytes)
            .map_err(|e| StreamError::LastOperationFailed(e.into()))?;
        Ok(())
    }

    fn flush(&mut self) -> StreamResult<()> {
        Ok(())
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        Ok(1024 * 1024)
    }
}

impl LogStream {
    fn write_all(&mut self, mut bytes: &[u8]) -> std::io::Result<()> {
        while !bytes.is_empty() {
            if self
                .state
                .needs_prefix_on_next_write
                .load(Ordering::Relaxed)
            {
                self.output.write_all(self.state.prefix.as_bytes())?;
                self.state
                    .needs_prefix_on_next_write
                    .store(false, Ordering::Relaxed);
            }
            if let Some(i) = bytes.iter().position(|b| *b == b'\n') {
                let (a, b) = bytes.split_at(i + 1);
                bytes = b;
                self.output.write_all(a)?;
                self.state
                    .needs_prefix_on_next_write
                    .store(true, Ordering::Relaxed);
            } else {
                self.output.write_all(bytes)?;
                break;
            }
        }

        Ok(())
    }
}

impl AsyncWrite for LogStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(self.write_all(buf).map(|()| buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[async_trait::async_trait]
impl wasmtime_wasi::p2::Pollable for LogStream {
    async fn ready(&mut self) {}
}
