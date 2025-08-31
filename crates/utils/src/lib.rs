pub mod sha256sum;
pub mod wasm_tools;
mod wit;

pub fn tracing_panic_hook(panic_info: &std::panic::PanicHookInfo) {
    let payload = panic_info.payload();
    #[expect(clippy::manual_map)]
    let payload = if let Some(s) = payload.downcast_ref::<&str>() {
        Some(&**s)
    } else if let Some(s) = payload.downcast_ref::<String>() {
        Some(s.as_str())
    } else {
        None
    };
    let location = panic_info.location().map(ToString::to_string);
    let backtrace = std::backtrace::Backtrace::capture();
    if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
        tracing::error!(
            panic.payload = payload,
            panic.location = location,
            "A panic occurred: {backtrace}"
        );
        if let Some(payload) = payload {
            eprintln!("A panic occurred: {payload}\n{backtrace}");
        } else {
            eprintln!("A panic occurred\n{backtrace}");
        }
    } else {
        tracing::error!(
            panic.payload = payload,
            panic.location = location,
            "A panic occurred",
        );
        if let Some(payload) = payload {
            eprintln!("A panic occurred: {payload}");
        } else {
            eprintln!("A panic occurred");
        }
    }
}
