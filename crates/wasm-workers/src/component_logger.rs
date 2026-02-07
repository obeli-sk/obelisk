use chrono::Utc;
use concepts::{
    ExecutionId,
    prefixed_ulid::RunId,
    storage::{LogEntry, LogInfoAppendRow, LogLevel},
};
use tokio::sync::mpsc;
use tracing::{Span, debug, error, info, trace, warn};

pub(crate) struct ComponentLogger {
    pub(crate) span: Span,
    pub(crate) execution_id: ExecutionId,
    pub(crate) run_id: RunId,
    pub(crate) logs_storage_config: Option<LogStrageConfig>,
}

#[derive(Clone, derive_more::Debug)]
pub struct LogStrageConfig {
    pub min_level: LogLevel,
    #[debug(skip)]
    pub log_sender: mpsc::Sender<LogInfoAppendRow>,
}

const TARGET: &str = "app";
impl ComponentLogger {
    pub(crate) fn log(&mut self, level: LogLevel, message: String) {
        // publish via tracing subscriber
        self.span.in_scope(|| match level {
            LogLevel::Trace => trace!(target: TARGET, "{message}"),
            LogLevel::Debug => debug!(target: TARGET, "{message}"),
            LogLevel::Info => info!(target: TARGET, "{message}"),
            LogLevel::Warn => warn!(target: TARGET, "{message}"),
            LogLevel::Error => error!(target: TARGET, "{message}"),
        });
        // store
        if let Some(logs_storage_config) = &mut self.logs_storage_config
            && level as u8 >= logs_storage_config.min_level as u8
        {
            let res = logs_storage_config.log_sender.try_send(LogInfoAppendRow {
                execution_id: self.execution_id.clone(),
                run_id: self.run_id,
                log_entry: LogEntry::Log {
                    created_at: Utc::now(),
                    level,
                    message,
                },
            });
            if let Err(err) = res {
                debug!("Dropping message: {err:?}");
            }
        }
    }
}

pub(crate) mod log_activities {

    // Generate `obelisk::log::log`
    wasmtime::component::bindgen!({
        path: "host-wit-log/",
            // interfaces: "import obelisk:log@1.0.0/log;", // Broken in 26.0.0
        inline: "package any:any;
                    world bindings {
                        import obelisk:log/log@1.0.0;
                    }",
        world: "any:any/bindings",
    });
}
