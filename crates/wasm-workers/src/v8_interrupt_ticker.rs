use deno_core::v8::{IsolateHandle, UnsafeRawIsolatePtr};
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, instrument};

/// Same as `v8::InterruptCallback`, which the `v8` crate does not re-export.
pub(crate) type InterruptCallback = unsafe extern "C" fn(UnsafeRawIsolatePtr, *mut c_void);

/// Isolates to be poked, keyed by registration id. Process-global so that a V8 runtime can
/// register without threading a ticker handle through every workflow invocation.
static REGISTRY: Mutex<BTreeMap<u64, InterruptEntry>> = Mutex::new(BTreeMap::new());
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// The V8 analog of [`crate::epoch_ticker::EpochTicker`]: a single background thread that
/// periodically asks each registered isolate to run its interrupt callback. wasmtime bumps an
/// engine epoch; V8 has no epoch, so we drive [`IsolateHandle::request_interrupt`], whose
/// callback fires on the isolate thread and can trap a CPU-bound workflow. Registered isolates
/// are not poked unless a ticker is running.
pub struct V8InterruptTicker {
    shutdown: Arc<AtomicBool>,
}

impl V8InterruptTicker {
    #[must_use]
    pub fn spawn_new(period: Duration) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        {
            let shutdown = shutdown.clone();
            std::thread::Builder::new()
                .name("obelisk-v8-epoch".to_owned())
                .spawn(move || Self::interrupt_ticker(period, &shutdown))
                .expect("spawning the V8 interrupt ticker must succeed");
        }
        Self { shutdown }
    }

    #[instrument(skip_all)]
    fn interrupt_ticker(period: Duration, shutdown: &AtomicBool) {
        debug!("Spawned the V8 interrupt ticker");
        while !shutdown.load(Ordering::Relaxed) {
            std::thread::sleep(period);
            for entry in REGISTRY.lock().unwrap().values() {
                entry
                    .handle
                    .request_interrupt(entry.callback, entry.data.0.cast_mut());
            }
        }
    }
}

impl Drop for V8InterruptTicker {
    fn drop(&mut self) {
        debug!("Closing the V8 interrupt ticker");
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

/// Data pointer handed back to the interrupt callback, which runs on the isolate's own thread.
#[derive(Clone, Copy)]
struct InterruptDataPtr(*const c_void);
// SAFETY: the pointee outlives the registration (see the caller of `register`), and is
// dereferenced only on the isolate thread from the callback.
unsafe impl Send for InterruptDataPtr {}

struct InterruptEntry {
    handle: IsolateHandle,
    callback: InterruptCallback,
    data: InterruptDataPtr,
}

/// Registers `handle` to be interrupted on every tick until the returned guard is dropped.
/// The caller must keep `data` alive for at least as long as the guard.
pub(crate) fn register(
    handle: IsolateHandle,
    callback: InterruptCallback,
    data: *const c_void,
) -> InterruptGuard {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    REGISTRY.lock().unwrap().insert(
        id,
        InterruptEntry {
            handle,
            callback,
            data: InterruptDataPtr(data),
        },
    );
    InterruptGuard { id }
}

/// Removes the isolate from the ticker registry on drop.
pub(crate) struct InterruptGuard {
    id: u64,
}

impl Drop for InterruptGuard {
    fn drop(&mut self) {
        REGISTRY.lock().unwrap().remove(&self.id);
    }
}
