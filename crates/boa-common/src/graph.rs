//! Multi-file ES module loading via `MapModuleLoader`.
//!
//! Parses each source in the closed module graph into a `Module` keyed by its
//! path, so Boa's `MapModuleLoader` can resolve relative `./foo.js` imports
//! against the importer's path.

use boa_engine::{Context, Module, Source, module::MapModuleLoader};
use std::collections::BTreeMap;
use std::path::Path;

/// Errors raised while registering a closed JS module graph with the loader.
#[derive(Debug)]
pub enum GraphLoadError {
    /// `entry_path` was not present in `files`.
    EntryNotFound,
    /// Failed to parse a module's source code.
    ParseError { path: String, message: String },
}

/// Parse every `(path, source)` pair in `files` and insert the resulting
/// `Module` into `loader` keyed by `path`. Returns the module identified by
/// `entry_path`.
///
/// Each module's `path` is set to its key so the loader can resolve relative
/// imports against it via `Module::path()`.
///
/// # Panics
///
/// Panics if `entry_path` or any key in `files` is empty. The host produces
/// these graphs and is required to assign every module a non-empty path:
/// inline payloads use a synthetic name (e.g. `"componentname.js"`), real files
/// use their relative path.
pub fn register_source_modules(
    loader: &MapModuleLoader,
    files: &BTreeMap<String, String>,
    entry_path: &str,
    context: &mut Context,
) -> Result<Module, GraphLoadError> {
    assert!(
        !entry_path.is_empty(),
        "register_source_modules: entry_path must not be empty",
    );
    let mut entry: Option<Module> = None;
    for (path, source) in files {
        assert!(
            !path.is_empty(),
            "register_source_modules: file path must not be empty",
        );
        let module = Module::parse(
            Source::from_bytes(source.as_bytes()).with_path(Path::new(path.as_str())),
            None,
            context,
        )
        .map_err(|err| GraphLoadError::ParseError {
            path: path.clone(),
            message: err.to_string(),
        })?;
        if path == entry_path {
            entry = Some(module.clone());
        }
        loader.insert(path.as_str(), module);
    }
    entry.ok_or(GraphLoadError::EntryNotFound)
}
