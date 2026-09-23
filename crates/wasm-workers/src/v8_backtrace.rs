//! Call-site backtraces for the native V8 runtimes, in the shape the Boa runtimes produce.

use concepts::storage::{FrameInfo, FrameSymbol, WasmBacktrace};
use deno_core::{ModuleSpecifier, v8};
use std::collections::HashMap;

/// Deepest JS frames kept in a captured backtrace.
const FRAME_LIMIT: usize = 32;

/// Reverse of a module loader's path -> specifier map: V8 reports the specifier as a frame's
/// script name, while a backtrace must carry the deployment-relative name that
/// `GetBacktraceSource` resolves.
pub(crate) fn user_module_paths(
    paths: &HashMap<String, ModuleSpecifier>,
) -> HashMap<String, String> {
    paths
        .iter()
        .map(|(path, specifier)| (specifier.to_string(), path.clone()))
        .collect()
}

/// Capture the JS call stack of the running host op: one frame per user call site, carrying the
/// deployment-relative file name plus line and column. Frames of the runtime's own modules (the
/// `obelisk:*` builtins, the generated import shims, the entry wrapper) have no source to show,
/// so they are dropped; a stack with none left yields `None`, like an empty wasmtime backtrace.
pub(crate) fn capture(
    scope: &v8::PinScope,
    user_module_paths: &HashMap<String, String>,
) -> Option<WasmBacktrace> {
    let stack = v8::StackTrace::current_stack_trace(scope, FRAME_LIMIT)?;
    let frames: Vec<_> = (0..stack.get_frame_count())
        .filter_map(|index| stack.get_frame(scope, index))
        .filter_map(|frame| {
            let script = frame.get_script_name_or_source_url(scope)?;
            let file = user_module_paths.get(&script.to_rust_string_lossy(scope))?;
            let func_name = frame
                .get_function_name(scope)
                .map(|name| name.to_rust_string_lossy(scope))
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| "<anonymous>".to_owned());
            Some(FrameInfo {
                module: file.clone(),
                func_name,
                symbols: vec![FrameSymbol {
                    func_name: None,
                    file: Some(file.clone()),
                    line: position(frame.get_line_number()),
                    col: position(frame.get_column()),
                }],
            })
        })
        .collect();
    (!frames.is_empty()).then_some(WasmBacktrace { frames })
}

/// V8 line and column numbers are 1-based; it reports `0` when it has no such information.
fn position(value: usize) -> Option<u32> {
    u32::try_from(value).ok().filter(|value| *value > 0)
}
