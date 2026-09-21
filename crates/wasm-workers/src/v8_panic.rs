use deno_core::futures::FutureExt;
use deno_error::JsErrorBox;
use std::cell::RefCell;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

#[derive(Clone)]
pub(crate) struct V8PanicState(Rc<RefCell<V8PanicStateInner>>);

struct V8PanicStateInner {
    reason: Option<String>,
    isolate: deno_core::v8::IsolateHandle,
}

impl V8PanicState {
    pub(crate) fn new(isolate: deno_core::v8::IsolateHandle) -> Self {
        Self(Rc::new(RefCell::new(V8PanicStateInner {
            reason: None,
            isolate,
        })))
    }

    pub(crate) fn catch<T>(
        &self,
        f: impl FnOnce() -> Result<T, JsErrorBox>,
    ) -> Result<T, JsErrorBox> {
        match std::panic::catch_unwind(AssertUnwindSafe(f)) {
            Ok(result) => result,
            Err(panic) => Err(self.record(&*panic)),
        }
    }

    pub(crate) async fn catch_async<T>(
        &self,
        future: impl Future<Output = Result<T, JsErrorBox>>,
    ) -> Result<T, JsErrorBox> {
        match AssertUnwindSafe(future).catch_unwind().await {
            Ok(result) => result,
            Err(panic) => Err(self.record(&*panic)),
        }
    }

    pub(crate) fn take(&self) -> Option<String> {
        self.0.borrow_mut().reason.take()
    }

    fn record(&self, panic: &(dyn std::any::Any + Send)) -> JsErrorBox {
        let reason = if let Some(message) = panic.downcast_ref::<&str>() {
            (*message).to_owned()
        } else if let Some(message) = panic.downcast_ref::<String>() {
            message.clone()
        } else {
            "unknown panic payload".to_owned()
        };
        let mut inner = self.0.borrow_mut();
        inner.reason = Some(reason.clone());
        inner.isolate.terminate_execution();
        JsErrorBox::generic(format!("native V8 host operation panicked: {reason}"))
    }
}
