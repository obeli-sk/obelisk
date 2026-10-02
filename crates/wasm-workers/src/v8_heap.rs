use deno_core::{JsRuntime, RuntimeOptions};
use std::{cell::Cell, rc::Rc};

pub(crate) const EXHAUSTED: &str = "JavaScript heap limit exceeded";

pub(crate) struct HeapState(Rc<Cell<bool>>);

impl HeapState {
    pub(crate) fn exhausted(&self) -> bool {
        self.0.get()
    }
}

pub(crate) fn new_runtime(
    mut options: RuntimeOptions,
    max_heap_size: Option<usize>,
) -> (JsRuntime, HeapState) {
    options.create_params = Some(
        max_heap_size.map_or_else(deno_core::v8::CreateParams::default, |max| {
            deno_core::v8::CreateParams::default().heap_limits(0, max)
        }),
    );
    let mut runtime = JsRuntime::new(options);
    let exhausted = Rc::new(Cell::new(false));
    let state = HeapState(exhausted.clone());
    let isolate = runtime.v8_isolate().thread_safe_handle();
    runtime.add_near_heap_limit_callback(move |current, initial| {
        exhausted.set(true);
        isolate.terminate_execution();
        // V8 needs allocation headroom to unwind termination instead of entering fatal OOM.
        current.saturating_add(initial.max(16 * 1024 * 1024))
    });
    (runtime, state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn v8_heap_limits_reset_between_isolates() {
        const CHILD_ENV: &str = "OBELISK_TEST_V8_HEAP_LIMITS_CHILD";
        if std::env::var_os(CHILD_ENV).is_none() {
            let test_name = format!(
                "{}::v8_heap_limits_reset_between_isolates",
                module_path!().split_once("::").unwrap().1
            );
            let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &test_name, "--nocapture"])
                .env(CHILD_ENV, "1")
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            if let Ok(status) = tokio::time::timeout(Duration::from_secs(30), child.wait()).await {
                assert!(status.unwrap().success(), "heap subprocess failed");
            } else {
                child.kill().await.unwrap();
                panic!("heap subprocess timed out");
            }
        } else {
            let mut initial_limit = None;
            for exhaust in [true, false, true, false] {
                let (mut runtime, heap) = new_runtime(
                    RuntimeOptions {
                        startup_snapshot: Some(crate::v8_snapshot::STARTUP_SNAPSHOT),
                        ..Default::default()
                    },
                    Some(32 * 1024 * 1024),
                );
                let limit = runtime.v8_isolate().get_heap_statistics().heap_size_limit();
                assert_eq!(*initial_limit.get_or_insert(limit), limit);
                if exhaust {
                    assert!(runtime.execute_script("heap-test", "const arrays = []; while (true) arrays.push(new Array(200000).fill(1));").is_err());
                    assert!(heap.exhausted());
                    assert!(runtime.v8_isolate().get_heap_statistics().heap_size_limit() > limit);
                } else {
                    assert!(runtime.execute_script("heap-test", "'ok'").is_ok());
                    assert!(!heap.exhausted());
                }
            }
        }
    }
}
