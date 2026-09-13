use anyhow::{Context as _, bail};
use concepts::{
    ContentDigest, ExecutionId,
    cas::Cas,
    component_id::ComponentDigest,
    storage::{DbConnection, Version, WorkflowSnapshot},
};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use tracing::{debug, info};
use walrus::{
    FunctionBuilder, FunctionId, ModuleConfig, ValType,
    ir::{BinaryOp, Call, LoadKind, MemArg, StoreKind, VisitorMut, dfs_pre_order_mut},
};
use wasm_encoder::{Component, ComponentSection, Encode, reencode::ReencodeComponent};
use wasmparser::{Parser, Payload, TypeRef};
use wasmtime_wizer::Wizer;
use wit_component::ComponentEncoder;

const PREPARED_FORMAT_VERSION: u8 = 1;

/// Snapshot metadata together with the component bytes fetched from the CAS.
#[derive(Debug)]
pub struct LoadedSnapshot {
    pub metadata: WorkflowSnapshot,
    pub component: Vec<u8>,
}

/// Write snapshot bytes to the CAS before publishing their durable execution index.
pub async fn persist_snapshot(
    cas: &dyn Cas,
    db: &dyn DbConnection,
    execution_id: ExecutionId,
    version: Version,
    component_digest: ComponentDigest,
    prepared_component_digest: ContentDigest,
    component: &[u8],
) -> anyhow::Result<WorkflowSnapshot> {
    let snapshot_digest = cas.write_blob(component).await?;
    let metadata = WorkflowSnapshot {
        execution_id,
        version,
        component_digest,
        prepared_component_digest,
        snapshot_digest,
    };
    db.upsert_workflow_snapshot(metadata.clone()).await?;
    Ok(metadata)
}

/// Load the newest snapshot compatible with both the user input and prepared component.
pub async fn load_latest_snapshot(
    cas: &dyn Cas,
    db: &dyn DbConnection,
    execution_id: &ExecutionId,
    component_digest: &ComponentDigest,
    prepared_component_digest: &ContentDigest,
) -> anyhow::Result<Option<LoadedSnapshot>> {
    let Some(metadata) = db
        .get_latest_workflow_snapshot(execution_id, component_digest, prepared_component_digest)
        .await?
    else {
        return Ok(None);
    };
    let component = cas
        .read_blob(&metadata.snapshot_digest)
        .await?
        .with_context(|| {
            format!(
                "workflow snapshot {} referenced by {execution_id} is missing from the CAS",
                metadata.snapshot_digest
            )
        })?;
    Ok(Some(LoadedSnapshot {
        metadata,
        component,
    }))
}

struct EmbeddedModule<'a>(&'a [u8]);

impl Encode for EmbeddedModule<'_> {
    fn encode(&self, sink: &mut Vec<u8>) {
        self.0.encode(sink);
    }
}

impl ComponentSection for EmbeddedModule<'_> {
    fn id(&self) -> u8 {
        wasm_encoder::ComponentSectionId::CoreModule.into()
    }
}

struct AsyncifyWorkflowModules {
    transformed_modules: usize,
    snapshot_interval: u32,
}

impl wasm_encoder::reencode::Reencode for AsyncifyWorkflowModules {
    type Error = anyhow::Error;
}

impl ReencodeComponent for AsyncifyWorkflowModules {
    fn parse_component_submodule(
        &mut self,
        component: &mut Component,
        _parser: Parser,
        module: &[u8],
    ) -> Result<(), wasm_encoder::reencode::Error<Self::Error>> {
        let transformed = if imports_obelisk_function(module).map_err(Self::user_error)? {
            self.transformed_modules += 1;
            let asyncified = asyncify(module).map_err(Self::user_error)?;
            wrap_durable_imports(&asyncified, self.snapshot_interval).map_err(Self::user_error)?
        } else {
            module.to_vec()
        };
        component.section(&EmbeddedModule(&transformed));
        Ok(())
    }
}

impl AsyncifyWorkflowModules {
    fn user_error(error: anyhow::Error) -> wasm_encoder::reencode::Error<anyhow::Error> {
        wasm_encoder::reencode::Error::UserError(error)
    }
}

fn imports_obelisk_function(module: &[u8]) -> anyhow::Result<bool> {
    for payload in Parser::new(0).parse_all(module) {
        if let Payload::ImportSection(imports) = payload? {
            for import in imports.into_imports() {
                let import = import?;
                if import.module.starts_with("obelisk:") && matches!(import.ty, TypeRef::Func(_)) {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

fn asyncify(module: &[u8]) -> anyhow::Result<Vec<u8>> {
    let input = NamedTempFile::new().context("creating Asyncify input")?;
    let output = NamedTempFile::new().context("creating Asyncify output")?;
    std::fs::write(input.path(), module).context("writing Asyncify input")?;
    let status = std::process::Command::new("wasm-opt")
        .arg(input.path())
        .arg("--asyncify")
        .arg("--pass-arg=asyncify-imports@obelisk:*")
        .arg("--pass-arg=asyncify-asserts")
        .arg("-O2")
        .arg("-o")
        .arg(output.path())
        .status()
        .context("running wasm-opt; workflow snapshots require Binaryen on PATH")?;
    if !status.success() {
        bail!("wasm-opt failed with {status}");
    }
    std::fs::read(output.path()).context("reading Asyncify output")
}

fn storage_type(ty: ValType) -> anyhow::Result<(u32, LoadKind, StoreKind)> {
    match ty {
        ValType::I32 => Ok((
            4,
            LoadKind::I32 { atomic: false },
            StoreKind::I32 { atomic: false },
        )),
        ValType::I64 => Ok((
            8,
            LoadKind::I64 { atomic: false },
            StoreKind::I64 { atomic: false },
        )),
        ValType::F32 => Ok((4, LoadKind::F32, StoreKind::F32)),
        ValType::F64 => Ok((8, LoadKind::F64, StoreKind::F64)),
        ValType::V128 => Ok((16, LoadKind::V128, StoreKind::V128)),
        ValType::Ref(_) => bail!("durable imports with lowered reference results are unsupported"),
    }
}

struct ReplaceCalls {
    original: FunctionId,
    wrapper: FunctionId,
}

impl VisitorMut for ReplaceCalls {
    fn visit_call_mut(&mut self, call: &mut Call) {
        if call.func == self.original {
            call.func = self.wrapper;
        }
    }
}

fn wrap_durable_imports(module: &[u8], snapshot_interval: u32) -> anyhow::Result<Vec<u8>> {
    if snapshot_interval == 0 {
        bail!("workflow snapshot interval must be greater than zero");
    }
    let mut module = ModuleConfig::new()
        .generate_name_section(false)
        .parse(module)
        .context("parsing Asyncified workflow module")?;
    let memory = module
        .memories
        .iter()
        .next()
        .map(|memory| memory.id())
        .context("Asyncified workflow module has no linear memory")?;
    let memory_ty = module.memories.get_mut(memory);
    if memory_ty.memory64 || memory_ty.page_size_log2.is_some_and(|size| size != 16) {
        bail!("workflow snapshot stack requires a 32-bit memory with 64 KiB pages");
    }
    let reserved_page = u32::try_from(memory_ty.initial)
        .context("workflow memory is too large for a 32-bit snapshot stack")?;
    let new_initial = memory_ty.initial + 1;
    if memory_ty
        .maximum
        .is_some_and(|maximum| new_initial > maximum)
    {
        bail!("workflow memory maximum leaves no page for the Asyncify snapshot stack");
    }
    memory_ty.initial = new_initial;
    let snapshot_base = reserved_page
        .checked_mul(65_536)
        .context("workflow memory is too large for a 32-bit snapshot stack")?;
    let snapshot_end = snapshot_base
        .checked_add(65_536)
        .context("workflow memory is too large for a 32-bit snapshot stack")?;
    const EVENT_COUNT_OFFSET: u32 = 8;
    const CHECKPOINT_ACTIVE_OFFSET: u32 = 12;
    const SAVED_RESULTS_OFFSET: u32 = 16;
    const ASYNCIFY_STACK_OFFSET: u32 = 4096;
    let start_unwind = module.exports.get_func("asyncify_start_unwind")?;
    let start_rewind = module.exports.get_func("asyncify_start_rewind")?;
    let stop_unwind = module.exports.get_func("asyncify_stop_unwind")?;
    let stop_rewind = module.exports.get_func("asyncify_stop_rewind")?;
    let get_state = module.exports.get_func("asyncify_get_state")?;
    let durable_imports: Vec<_> = module
        .imports
        .iter()
        .filter_map(|import| {
            if !import.module.starts_with("obelisk:") {
                return None;
            }
            match import.kind {
                walrus::ImportKind::Function(function) => Some(function),
                _ => None,
            }
        })
        .collect();
    let existing_functions: Vec<_> = module.funcs.iter_local().map(|(id, _)| id).collect();
    let mut next_saved_result = snapshot_base + SAVED_RESULTS_OFFSET;

    for imported in durable_imports {
        let ty = module.funcs.get(imported).ty();
        let (params, results) = module.types.params_results(ty);
        let params = params.to_vec();
        let results = results.to_vec();
        let param_locals: Vec<_> = params.iter().map(|ty| module.locals.add(*ty)).collect();
        let result_locals: Vec<_> = results.iter().map(|ty| module.locals.add(*ty)).collect();
        let saved_results: Vec<_> = results
            .iter()
            .map(|ty| {
                let (size, load, store) = storage_type(*ty)?;
                next_saved_result = next_saved_result.next_multiple_of(size);
                let offset = next_saved_result;
                next_saved_result += size;
                Ok((offset, load, store))
            })
            .collect::<anyhow::Result<_>>()?;
        if next_saved_result > snapshot_base + ASYNCIFY_STACK_OFFSET {
            bail!("lowered durable import results exceed reserved snapshot metadata space");
        }
        let wrapper_result_type = module.types.add(&[], &results);

        let mut builder = FunctionBuilder::new(&mut module.types, &params, &results);
        builder.name("obelisk:snapshot/durable-wrapper".to_owned());
        let mut body = builder.func_body();
        body.call(get_state)
            .i32_const(2)
            .binop(BinaryOp::I32Eq)
            .if_else(
                wrapper_result_type,
                |rewind| {
                    rewind
                        .call(stop_rewind)
                        .i32_const(snapshot_base.cast_signed())
                        .i32_const(0)
                        .store(
                            memory,
                            StoreKind::I32 { atomic: false },
                            MemArg {
                                align: 4,
                                offset: CHECKPOINT_ACTIVE_OFFSET.into(),
                            },
                        );
                    for (offset, load, _) in &saved_results {
                        rewind.i32_const(offset.cast_signed()).load(
                            memory,
                            *load,
                            MemArg {
                                align: 1,
                                offset: 0,
                            },
                        );
                    }
                },
                |forward| {
                    for param in &param_locals {
                        forward.local_get(*param);
                    }
                    forward.call(imported);
                    for (local, (offset, _, store)) in
                        result_locals.iter().zip(&saved_results).rev()
                    {
                        forward
                            .local_set(*local)
                            .i32_const(offset.cast_signed())
                            .local_get(*local)
                            .store(
                                memory,
                                *store,
                                MemArg {
                                    align: 1,
                                    offset: 0,
                                },
                            );
                    }
                    forward
                        .i32_const(snapshot_base.cast_signed())
                        .i32_const(snapshot_base.cast_signed())
                        .load(
                            memory,
                            LoadKind::I32 { atomic: false },
                            MemArg {
                                align: 4,
                                offset: EVENT_COUNT_OFFSET.into(),
                            },
                        )
                        .i32_const(1)
                        .binop(BinaryOp::I32Add)
                        .store(
                            memory,
                            StoreKind::I32 { atomic: false },
                            MemArg {
                                align: 4,
                                offset: EVENT_COUNT_OFFSET.into(),
                            },
                        )
                        .i32_const(snapshot_base.cast_signed())
                        .load(
                            memory,
                            LoadKind::I32 { atomic: false },
                            MemArg {
                                align: 4,
                                offset: EVENT_COUNT_OFFSET.into(),
                            },
                        )
                        .i32_const(snapshot_interval.cast_signed())
                        .binop(BinaryOp::I32RemU)
                        .i32_const(0)
                        .binop(BinaryOp::I32Eq)
                        .if_else(
                            None,
                            |checkpoint| {
                                checkpoint
                                    .i32_const(snapshot_base.cast_signed())
                                    .i32_const(1)
                                    .store(
                                        memory,
                                        StoreKind::I32 { atomic: false },
                                        MemArg {
                                            align: 4,
                                            offset: CHECKPOINT_ACTIVE_OFFSET.into(),
                                        },
                                    )
                                    .i32_const(snapshot_base.cast_signed())
                                    .call(start_unwind);
                            },
                            |_| {},
                        );
                    for local in &result_locals {
                        forward.local_get(*local);
                    }
                },
            );
        drop(body);
        let wrapper = builder.finish(param_locals, &mut module.funcs);
        for function_id in &existing_functions {
            let function = module.funcs.get_mut(*function_id).kind.unwrap_local_mut();
            let entry = function.entry_block();
            dfs_pre_order_mut(
                &mut ReplaceCalls {
                    original: imported,
                    wrapper,
                },
                function,
                entry,
            );
        }
    }

    let boundary_exports: Vec<_> = module
        .exports
        .iter()
        .filter_map(|export| match export.item {
            walrus::ExportItem::Function(function) if !export.name.starts_with("asyncify_") => {
                Some((export.id(), function))
            }
            _ => None,
        })
        .collect();
    for (export_id, exported) in boundary_exports {
        let ty = module.funcs.get(exported).ty();
        let (params, results) = module.types.params_results(ty);
        let params = params.to_vec();
        let results = results.to_vec();
        let param_locals: Vec<_> = params.iter().map(|ty| module.locals.add(*ty)).collect();
        let result_locals: Vec<_> = results.iter().map(|ty| module.locals.add(*ty)).collect();
        let mut builder = FunctionBuilder::new(&mut module.types, &params, &results);
        builder.name("obelisk:snapshot/export-boundary".to_owned());
        let mut body = builder.func_body();
        // Wizer removes a core start after running it while producing a snapshot. Starting rewind
        // again at the first component-to-core export keeps restored snapshots self-starting too.
        body.i32_const(snapshot_base.cast_signed())
            .load(
                memory,
                LoadKind::I32 { atomic: false },
                MemArg {
                    align: 4,
                    offset: CHECKPOINT_ACTIVE_OFFSET.into(),
                },
            )
            .call(get_state)
            .i32_const(0)
            .binop(BinaryOp::I32Eq)
            .binop(BinaryOp::I32And)
            .if_else(
                None,
                |resume| {
                    resume
                        .i32_const(snapshot_base.cast_signed())
                        .call(start_rewind);
                },
                |_| {},
            );
        for param in &param_locals {
            body.local_get(*param);
        }
        body.call(exported);
        for local in result_locals.iter().rev() {
            body.local_set(*local);
        }
        body.call(get_state)
            .i32_const(1)
            .binop(BinaryOp::I32Eq)
            .if_else(
                None,
                |unwound| {
                    unwound.call(stop_unwind);
                },
                |_| {},
            );
        for local in &result_locals {
            body.local_get(*local);
        }
        drop(body);
        let boundary = builder.finish(param_locals, &mut module.funcs);
        module.exports.get_mut(export_id).item = walrus::ExportItem::Function(boundary);
    }

    let old_start = module.start;
    let mut builder = FunctionBuilder::new(&mut module.types, &[], &[]);
    let mut body = builder.func_body();
    if let Some(old_start) = old_start {
        body.call(old_start);
    }
    body.i32_const(snapshot_base.cast_signed())
        .load(
            memory,
            LoadKind::I32 { atomic: false },
            MemArg {
                align: 4,
                offset: CHECKPOINT_ACTIVE_OFFSET.into(),
            },
        )
        .if_else(
            None,
            |resume| {
                resume
                    .i32_const(snapshot_base.cast_signed())
                    .call(start_rewind);
            },
            |initialize| {
                initialize
                    .i32_const(snapshot_base.cast_signed())
                    .i32_const((snapshot_base + ASYNCIFY_STACK_OFFSET).cast_signed())
                    .store(
                        memory,
                        StoreKind::I32 { atomic: false },
                        MemArg {
                            align: 4,
                            offset: 0,
                        },
                    )
                    .i32_const(snapshot_base.cast_signed())
                    .i32_const(snapshot_end.cast_signed())
                    .store(
                        memory,
                        StoreKind::I32 { atomic: false },
                        MemArg {
                            align: 4,
                            offset: 4,
                        },
                    );
            },
        );
    drop(body);
    module.start = Some(builder.finish(Vec::new(), &mut module.funcs));
    Ok(module.emit_wasm())
}

/// Prepare a component for transparent Asyncify and Wizer snapshots.
///
/// The exact user input remains unchanged. The returned file is the only derived artifact and is
/// keyed by the user input digest, preparation-format version, and snapshot interval.
pub async fn prepare_component(
    component_path: &Path,
    input_digest: &ContentDigest,
    output_parent: &Path,
    snapshot_interval: u32,
) -> anyhow::Result<PathBuf> {
    let output_path = output_parent.join(format!(
        "{}_workflow-prepared-v{PREPARED_FORMAT_VERSION}-n{snapshot_interval}.wasm",
        input_digest.with_infix("_")
    ));
    if output_path.exists() {
        debug!(?output_path, "Found prepared workflow component");
        return Ok(output_path);
    }

    let input = tokio::fs::read(component_path)
        .await
        .with_context(|| format!("reading workflow component {component_path:?}"))?;
    let component_input = if Parser::is_core_wasm(&input) {
        ComponentEncoder::default()
            .validate(true)
            .module(&input)?
            .encode()
            .context("componentizing workflow before snapshot preparation")?
    } else if Parser::is_component(&input) {
        input
    } else {
        bail!("workflow input is neither a core Wasm module nor a component");
    };
    let mut component = Component::new();
    let mut reencoder = AsyncifyWorkflowModules {
        transformed_modules: 0,
        snapshot_interval,
    };
    reencoder
        .parse_component(&mut component, Parser::new(0), &component_input)
        .map_err(|error| anyhow::anyhow!(error))?;
    if reencoder.transformed_modules == 0 {
        bail!("workflow component contains no core module importing an Obelisk function");
    }
    let asyncified = component.finish();
    let (_, prepared) = Wizer::new().instrument_component(&asyncified)?;

    let temporary_path = output_path.with_extension("wasm.tmp");
    tokio::fs::write(&temporary_path, prepared)
        .await
        .with_context(|| format!("writing prepared workflow component {temporary_path:?}"))?;
    tokio::fs::rename(&temporary_path, &output_path)
        .await
        .with_context(|| format!("installing prepared workflow component {output_path:?}"))?;
    info!(?output_path, "Prepared workflow component for snapshots");
    Ok(output_path)
}

#[cfg(test)]
mod tests {
    use super::{asyncify, wrap_durable_imports};
    use std::sync::{Arc, Mutex};
    use wasmtime::{Engine, Linker, Module, Store};
    use wasmtime_wizer::{WasmtimeWizer, Wizer};

    const WORKFLOW: &str = r#"
        (module
          (import "obelisk:test/events" "event" (func $event (param i32) (result i32)))
          (memory (export "memory") 1)
          (func (export "workflow") (param $events i32) (result i32)
            (local $index i32)
            (local $sum i32)
            (loop $next
              local.get $index
              call $event
              local.get $sum
              i32.add
              local.set $sum
              local.get $index
              i32.const 1
              i32.add
              local.tee $index
              local.get $events
              i32.lt_s
              br_if $next)
            local.get $sum))
    "#;

    fn linker(engine: &Engine) -> anyhow::Result<Linker<Arc<Mutex<Vec<i32>>>>> {
        let mut linker = Linker::new(engine);
        linker.func_wrap(
            "obelisk:test/events",
            "event",
            |caller: wasmtime::Caller<'_, Arc<Mutex<Vec<i32>>>>, value: i32| {
                caller.data().lock().unwrap().push(value);
                value
            },
        )?;
        Ok(linker)
    }

    #[tokio::test]
    async fn durable_wrapper_snapshots_and_resumes_without_repeating_host_call() {
        let core = wat::parse_str(WORKFLOW).unwrap();
        let transformed = wrap_durable_imports(&asyncify(&core).unwrap(), 3).unwrap();
        let wizer = Wizer::new();
        let (context, instrumented) = wizer.instrument(&transformed).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, instrumented).unwrap();
        let calls_before = Arc::new(Mutex::new(Vec::new()));
        let mut store = Store::new(&engine, calls_before.clone());
        let instance = linker(&engine)
            .unwrap()
            .instantiate_async(&mut store, &module)
            .await
            .unwrap();
        let partial = instance
            .get_typed_func::<i32, i32>(&mut store, "workflow")
            .unwrap()
            .call_async(&mut store, 5)
            .await
            .unwrap();
        assert_ne!(10, partial);
        assert_eq!(&[0, 1, 2], calls_before.lock().unwrap().as_slice());
        let memory = instance.get_memory(&mut store, "memory").unwrap();
        assert_eq!(&[1, 0, 0, 0], &memory.data(&store)[65_548..65_552]);

        let snapshot = wizer
            .snapshot(
                &context,
                &mut WasmtimeWizer {
                    store: &mut store,
                    instance,
                },
            )
            .await
            .unwrap();
        let module = Module::new(&engine, snapshot).unwrap();
        let calls_after = Arc::new(Mutex::new(Vec::new()));
        let mut store = Store::new(&engine, calls_after.clone());
        let instance = linker(&engine)
            .unwrap()
            .instantiate_async(&mut store, &module)
            .await
            .unwrap();
        let memory = instance.get_memory(&mut store, "memory").unwrap();
        assert_eq!(&[1, 0, 0, 0], &memory.data(&store)[65_548..65_552]);
        let result = instance
            .get_typed_func::<i32, i32>(&mut store, "workflow")
            .unwrap()
            .call_async(&mut store, 5)
            .await
            .unwrap();
        assert_eq!(
            (10, vec![3, 4]),
            (result, calls_after.lock().unwrap().clone())
        );
    }
}
