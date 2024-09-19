// https://github.com/bytecodealliance/wasmtime/tree/v24.0.0/src/commands/serve.rs
use wasmtime_wasi::{StreamError, StreamResult};

#[derive(Clone)]
pub enum Output {
    Stdout,
    Stderr,
}

impl Output {
    fn write_all(&self, buf: &[u8]) -> Result<(), wasmtime::Error> {
        use std::io::Write;

        match self {
            Output::Stdout => std::io::stdout()
                .write_all(buf)
                .map_err(wasmtime::Error::from),
            Output::Stderr => std::io::stderr()
                .write_all(buf)
                .map_err(wasmtime::Error::from),
        }
    }
}

#[derive(Clone)]
pub(crate) struct LogStream {
    prefix: String,
    output: Output,
    needs_prefix_on_next_write: bool,
}

impl LogStream {
    pub(crate) fn new(prefix: String, output: Output) -> LogStream {
        LogStream {
            prefix,
            output,
            needs_prefix_on_next_write: true,
        }
    }
}

impl wasmtime_wasi::StdoutStream for LogStream {
    fn stream(&self) -> Box<dyn wasmtime_wasi::HostOutputStream> {
        Box::new(self.clone())
    }

    fn isatty(&self) -> bool {
        use std::io::IsTerminal;

        match &self.output {
            Output::Stdout => std::io::stdout().is_terminal(),
            Output::Stderr => std::io::stderr().is_terminal(),
        }
    }
}

impl wasmtime_wasi::HostOutputStream for LogStream {
    fn write(&mut self, bytes: bytes::Bytes) -> StreamResult<()> {
        let mut bytes = &bytes[..];

        while !bytes.is_empty() {
            if self.needs_prefix_on_next_write {
                self.output
                    .write_all(self.prefix.as_bytes())
                    .map_err(StreamError::LastOperationFailed)?;
                self.needs_prefix_on_next_write = false;
            }
            match bytes.iter().position(|b| *b == b'\n') {
                Some(i) => {
                    let (a, b) = bytes.split_at(i + 1);
                    bytes = b;
                    self.output
                        .write_all(a)
                        .map_err(StreamError::LastOperationFailed)?;
                    self.needs_prefix_on_next_write = true;
                }
                None => {
                    self.output
                        .write_all(bytes)
                        .map_err(StreamError::LastOperationFailed)?;
                    break;
                }
            }
        }

        Ok(())
    }

    fn flush(&mut self) -> StreamResult<()> {
        Ok(())
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        Ok(1024 * 1024)
    }
}

#[async_trait::async_trait]
impl wasmtime_wasi::Subscribe for LogStream {
    async fn ready(&mut self) {}
}