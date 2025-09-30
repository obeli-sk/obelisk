pub fn tracing_panic_hook(panic_info: &std::panic::PanicHookInfo) {
    let payload = panic_info.payload();
    #[expect(clippy::manual_map)]
    let payload = if let Some(s) = payload.downcast_ref::<&str>() {
        Some(&**s)
    } else if let Some(s) = payload.downcast_ref::<String>() {
        Some(s.as_str())
    } else {
        None
    }
    .unwrap_or_default();
    let location = panic_info.location().map(ToString::to_string);
    let backtrace = std::backtrace::Backtrace::capture();
    if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
        if tracing::enabled!(tracing::Level::ERROR) {
            tracing::error!(
                panic.location = location,
                "A panic occurred: `{payload}` {backtrace}"
            );
        } else {
            eprintln!("A panic occurred: `{payload}`\n{backtrace}");
        }
    } else if tracing::enabled!(tracing::Level::ERROR) {
        tracing::error!(panic.location = location, "A panic occurred: `{payload}`",);
    } else {
        eprintln!("A panic occurred: `{payload}`");
        eprintln!("A panic occurred");
    }
}
