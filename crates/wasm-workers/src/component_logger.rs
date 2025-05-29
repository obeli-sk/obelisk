use tracing::{Span, debug, error, info, trace, warn};

pub(crate) struct ComponentLogger {
    pub(crate) span: Span,
    // TODO: dynamic level filtering
}
const TARGET: &str = "app";
impl ComponentLogger {
    pub(crate) fn trace(&self, message: &str) {
        self.span.in_scope(|| trace!(target: TARGET, "{}", message));
    }

    pub(crate) fn debug(&self, message: &str) {
        self.span.in_scope(|| debug!(target: TARGET, "{}", message));
    }

    pub(crate) fn info(&self, message: &str) {
        self.span.in_scope(|| info!(target: TARGET, "{}", message));
    }

    pub(crate) fn warn(&self, message: &str) {
        self.span.in_scope(|| warn!(target: TARGET, "{}", message));
    }

    pub(crate) fn error(&self, message: &str) {
        self.span.in_scope(|| error!(target: TARGET, "{}", message));
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
        async: false,
        trappable_imports: false,
    });
}
