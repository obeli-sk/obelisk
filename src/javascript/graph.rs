//! Host-side walker that collects a closed JS module graph rooted at an entry
//! file inside the deployment directory. Each `import` (relative or bare) is
//! parsed with Boa's parser; relative specifiers are followed and bare
//! specifiers are either passed through (WIT-style host imports) or rejected.
//!
//! Every file is keyed by its deployment-relative path (forward slashes), the
//! same containment the CAS deployment-file model enforces elsewhere, so the
//! graph round-trips as ordinary deployment files.

use crate::config::deployment::sanitize_deployment_relative_path;
use anyhow::{Context, bail};
use boa_engine::Source;
use std::collections::{BTreeMap, VecDeque};
use std::path::{Component, Path, PathBuf};

/// Reads a JS module's source by its deployment-relative path (forward slashes).
///
/// Lets the graph walker be driven from either the submitter's disk
/// ([`DiskModuleReader`]) or the content-addressed store (a CAS-backed reader),
/// so the same closure check runs client-side and server-side.
#[async_trait::async_trait]
pub(crate) trait ModuleSourceReader: Send + Sync {
    async fn read_source(&self, dep_path: &str) -> anyhow::Result<String>;
}

/// Reads module sources from the submitter's disk, under `deployment_dir`.
struct DiskModuleReader<'a> {
    deployment_dir: &'a Path,
}

#[async_trait::async_trait]
impl ModuleSourceReader for DiskModuleReader<'_> {
    async fn read_source(&self, dep_path: &str) -> anyhow::Result<String> {
        let path = self.deployment_dir.join(dep_path);
        tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("cannot read JS file {path:?}"))
    }
}

/// A closed multi-file JS module graph.
///
/// `entry_path` is one of the keys in `files`. Every relative `import` in the
/// graph resolves to another key in `files`. `BTreeMap` iterates sorted by
/// path so digests built from a `JsGraph` are deterministic.
#[derive(Debug, Clone)]
pub(crate) struct JsGraph {
    pub(crate) entry_path: String,
    pub(crate) files: BTreeMap<String, String>,
}

/// Walk the module graph starting from `entry_rel` (a deployment-relative path)
/// under `deployment_dir`.
///
/// The graph is collected by repeatedly parsing each reachable file with
/// `boa_engine::parser::Parser::parse_module` and following every relative
/// (`./`, `../`) specifier. Bare WIT-style specifiers (`ns:pkg/ifc`) are passed
/// through (not followed). Any other bare specifier (e.g. `lodash`) is rejected.
///
/// Each file's key is its path relative to `deployment_dir` using `/` as the
/// separator. A `..` that would escape `deployment_dir` is rejected.
pub(crate) async fn collect_graph(
    deployment_dir: &Path,
    entry_rel: &str,
) -> anyhow::Result<JsGraph> {
    collect_graph_with(&DiskModuleReader { deployment_dir }, entry_rel).await
}

/// Walk the module graph starting from `entry_rel`, reading each reachable file
/// through `reader`. See [`collect_graph`]; this is the provider-agnostic core.
pub(crate) async fn collect_graph_with(
    reader: &dyn ModuleSourceReader,
    entry_rel: &str,
) -> anyhow::Result<JsGraph> {
    let entry_key = sanitize_deployment_relative_path(entry_rel)?;
    reject_typescript(&entry_key)?;

    let mut files: BTreeMap<String, String> = BTreeMap::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    queue.push_back(entry_key.clone());

    while let Some(key) = queue.pop_front() {
        if files.contains_key(&key) {
            // Cycle: an earlier walk path already pulled this file in.
            continue;
        }
        let source = reader
            .read_source(&key)
            .await
            .with_context(|| format!("cannot read JS module `{key}`"))?;

        let specifiers = extract_module_specifiers(&source)
            .with_context(|| format!("parse error in `{key}`"))?;

        let importer_dir = Path::new(&key).parent().map(Path::to_path_buf);

        for specifier in specifiers {
            if is_relative_specifier(&specifier) {
                let joined = match &importer_dir {
                    Some(dir) => dir.join(&specifier),
                    None => PathBuf::from(&specifier),
                };
                let normalised = normalise_relative(&joined).with_context(|| {
                    format!(
                        "relative import `{specifier}` in `{key}` escapes the deployment directory"
                    )
                })?;
                let dep_key = sanitize_deployment_relative_path(&normalised).with_context(|| {
                    format!("relative import `{specifier}` in `{key}` is not a valid deployment path")
                })?;
                reject_typescript(&dep_key)?;
                if !files.contains_key(&dep_key) {
                    queue.push_back(dep_key);
                }
            } else if is_passthrough_specifier(&specifier) {
                // WIT-style host import: leave to the runtime.
            } else {
                bail!(
                    "import specifier `{specifier}` in `{key}` is not a relative path \
                     (must start with `./` or `../`) and does not look like a \
                     host (WIT) import (`ns:pkg/ifc`); bare module specifiers are not supported"
                );
            }
        }

        files.insert(key, source);
    }

    Ok(JsGraph {
        entry_path: entry_key,
        files,
    })
}

fn reject_typescript(path: &str) -> anyhow::Result<()> {
    if Path::new(path).extension().and_then(|ext| ext.to_str()) == Some("ts") {
        bail!("TypeScript module `{path}` is not supported; use JavaScript files")
    }
    Ok(())
}

fn is_relative_specifier(spec: &str) -> bool {
    spec.starts_with("./") || spec.starts_with("../")
}

/// True for specifiers the runtime should resolve via host imports
/// (`ns:pkg/ifc`, `obelisk:*`). False for relative paths (those are followed
/// here) and bare module names (those are rejected).
fn is_passthrough_specifier(spec: &str) -> bool {
    !is_relative_specifier(spec)
        && !spec.starts_with('/')
        && !spec.starts_with("http://")
        && !spec.starts_with("https://")
        && spec.contains(':')
        && spec.contains('/')
}

/// Extract every module specifier (import + re-export) from the given JS source.
fn extract_module_specifiers(js_code: &str) -> anyhow::Result<Vec<String>> {
    let mut interner = boa_engine::interner::Interner::new();
    let mut parser = boa_engine::parser::Parser::new(Source::from_bytes(js_code));
    let scope = boa_engine::ast::scope::Scope::new_global();
    let module = parser
        .parse_module(&scope, &mut interner)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // `requests()` returns every imported/re-exported module specifier.
    let requests = module.items().requests();
    let mut specifiers: Vec<String> = Vec::with_capacity(requests.len());
    for sym in requests {
        let s = interner
            .resolve_expect(sym)
            .utf8()
            .ok_or_else(|| anyhow::anyhow!("non-UTF-8 module specifier"))?
            .to_string();
        specifiers.push(s);
    }
    Ok(specifiers)
}

/// Collapse `.`/`..` in a relative path, returning a forward-slash string.
/// Errors when a `..` would pop above the root (escape the deployment dir).
fn normalise_relative(path: &Path) -> anyhow::Result<String> {
    let mut out: Vec<String> = Vec::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::Normal(part) => out.push(
                part.to_str()
                    .context("non-UTF-8 path component")?
                    .to_string(),
            ),
            Component::ParentDir => {
                if out.pop().is_none() {
                    bail!("path traversal above the deployment directory");
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                bail!("absolute path component")
            }
        }
    }
    if out.is_empty() {
        bail!("path resolved to an empty key");
    }
    Ok(out.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    async fn write(dir: &Path, rel: &str, content: &str) {
        let path = dir.join(rel);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        let mut f = tokio::fs::File::create(&path).await.unwrap();
        f.write_all(content.as_bytes()).await.unwrap();
        // `write_all` can return before tokio's background blocking task has
        // actually performed the OS-level write; `flush` waits for it.
        f.flush().await.unwrap();
    }

    #[tokio::test]
    async fn collects_two_files() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "src/index.js",
            "import { greet } from './lib.js';\nexport default () => greet();\n",
        )
        .await;
        write(
            dir.path(),
            "src/lib.js",
            "export function greet() { return 'hi'; }\n",
        )
        .await;

        let graph = collect_graph(dir.path(), "src/index.js").await.unwrap();
        assert_eq!(graph.entry_path, "src/index.js");
        let keys: Vec<&str> = graph.files.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["src/index.js", "src/lib.js"]);
    }

    #[tokio::test]
    async fn passes_through_wit_imports() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "index.js",
            "import { fibo } from 'testing:fibo/fibo';\nexport default () => fibo(10);\n",
        )
        .await;

        let graph = collect_graph(dir.path(), "index.js").await.unwrap();
        assert_eq!(graph.files.len(), 1);
        assert_eq!(graph.entry_path, "index.js");
    }

    #[tokio::test]
    async fn rejects_bare_specifier() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "index.js",
            "import _ from 'lodash';\nexport default () => 0;\n",
        )
        .await;

        let err = collect_graph(dir.path(), "index.js").await.unwrap_err();
        assert!(format!("{err:#}").contains("lodash"));
    }

    #[tokio::test]
    async fn follows_subdir_imports() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "index.js",
            "import { log } from './util/log.js';\nexport default () => log();\n",
        )
        .await;
        write(
            dir.path(),
            "util/log.js",
            "export function log() { return 1; }\n",
        )
        .await;

        let graph = collect_graph(dir.path(), "index.js").await.unwrap();
        let keys: Vec<&str> = graph.files.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["index.js", "util/log.js"]);
    }

    #[tokio::test]
    async fn parent_import_within_root_is_allowed() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "src/index.js",
            "import { log } from '../log.js';\nexport default () => log();\n",
        )
        .await;
        write(
            dir.path(),
            "log.js",
            "export function log() { return 1; }\n",
        )
        .await;

        let graph = collect_graph(dir.path(), "src/index.js").await.unwrap();
        let keys: Vec<&str> = graph.files.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["log.js", "src/index.js"]);
    }

    #[tokio::test]
    async fn parent_import_escaping_root_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "index.js",
            "import { x } from '../secret.js';\nexport default () => x();\n",
        )
        .await;

        let err = collect_graph(dir.path(), "index.js").await.unwrap_err();
        assert!(format!("{err:#}").contains("escapes") || format!("{err:#}").contains("traversal"));
    }

    #[tokio::test]
    async fn cycles_are_allowed() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "a.js",
            "import { y } from './b.js';\nexport function x() { return y(); }\nexport default x;\n",
        )
        .await;
        write(
            dir.path(),
            "b.js",
            "import { x } from './a.js';\nexport function y() { return 1; }\n",
        )
        .await;

        let graph = collect_graph(dir.path(), "a.js").await.unwrap();
        assert_eq!(graph.files.len(), 2);
    }
}
