use tracing::{debug, error, info, trace, warn, Span};

pub(crate) struct ComponentLogger {
    pub(crate) span: Span,
    // TODO: level configuration
}

impl ComponentLogger {
    pub(crate) fn trace(&self, message: &str) {
        self.span.in_scope(|| trace!("{}", message));
    }

    pub(crate) fn debug(&self, message: &str) {
        self.span.in_scope(|| debug!("{}", message));
    }

    pub(crate) fn info(&self, message: &str) {
        self.span.in_scope(|| info!("{}", message));
    }

    pub(crate) fn warn(&self, message: &str) {
        self.span.in_scope(|| warn!("{}", message));
    }

    pub(crate) fn error(&self, message: &str) {
        self.span.in_scope(|| error!("{}", message));
    }
}
