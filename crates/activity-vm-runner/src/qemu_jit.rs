use anyhow::{Context, ensure};
use std::cell::UnsafeCell;
use std::collections::HashMap;
use wasmtime::{
    Caller, Extern, ExternType, Instance, Linker, Memory, Module, Ref, SharedMemory, TypedFunc,
};

#[derive(Clone)]
pub(crate) enum QemuMemory {
    Plain(Memory),
    Shared(SharedMemory),
}

impl QemuMemory {
    pub(crate) fn as_extern(&self) -> Extern {
        match self {
            Self::Plain(memory) => Extern::Memory(*memory),
            Self::Shared(memory) => Extern::SharedMemory(memory.clone()),
        }
    }
}

pub(crate) struct QemuJit {
    pub(crate) memory: Option<QemuMemory>,
    blocks: HashMap<u32, TypedFunc<i32, i32>>,
    next_handle: u32,
    compiled_blocks: u64,
    executed_blocks: u64,
}

impl QemuJit {
    pub(crate) fn new(memory: Option<QemuMemory>) -> Self {
        Self {
            memory,
            blocks: HashMap::new(),
            next_handle: 1,
            compiled_blocks: 0,
            executed_blocks: 0,
        }
    }
}

pub(crate) trait HasQemuJit {
    fn qemu_jit(&mut self) -> &mut QemuJit;
}

pub(crate) fn add_to_linker<T: HasQemuJit + Send + 'static>(
    linker: &mut Linker<T>,
) -> anyhow::Result<()> {
    linker.func_wrap(
        "env",
        "instantiate_batch_js",
        |mut caller: Caller<'_, T>, bytes: i32, len: i32, nfuncs: i32, helpers: i32, nhelpers: i32, _dump: i32| {
            compile_batch(&mut caller, bytes, len, nfuncs, helpers, nhelpers)
                .map_err(|error| wasmtime::Error::msg(format!("{error:#}")))
        },
    )?;
    linker.func_wrap("env", "wasm_tail_calls_supported_js", || 0_i32)?;
    linker.func_wrap("env", "remove_batch_js", |_base: i32, _count: i32| {})?;
    linker.func_wrap(
        "env",
        "report_stats_js",
        |batches: i32, blocks: i32, _full: i32, _hot: i32, _evictions: i32, _bytes: i32, _sites: i32, _linked: i32| {
            eprintln!("QEMU JIT batches={batches} blocks={blocks}");
        },
    )?;
    linker.func_wrap("qemu_jit", "enabled", || -> i32 {
        i32::from(std::env::var_os("OBELISK_QEMU_DISABLE_JIT").is_none())
    })?;
    linker.func_wrap(
        "qemu_jit",
        "compile_batch",
        |mut caller: Caller<'_, T>, bytes: i32, len: i32, nfuncs: i32, helpers: i32, nhelpers: i32, _dump: i32| {
            compile_batch(&mut caller, bytes, len, nfuncs, helpers, nhelpers)
                .map_err(|error| wasmtime::Error::msg(format!("{error:#}")))
        },
    )?;
    // Establish generated-block correctness before enabling the optional
    // direct tail-call chaining optimization.
    linker.func_wrap("qemu_jit", "tail_calls_supported", || 0_i32)?;
    linker.func_wrap("qemu_jit", "remove_batch", |_base: i32, _count: i32| {})?;
    linker.func_wrap(
        "qemu_jit",
        "report_stats",
        |batches: i32, blocks: i32, _full: i32, _hot: i32, _evictions: i32, _bytes: i32, _sites: i32, _linked: i32| {
            eprintln!("QEMU JIT batches={batches} blocks={blocks}");
        },
    )?;
    linker.func_wrap(
        "qemu_jit",
        "compile",
        |mut caller: Caller<'_, T>,
         wasm_ptr: i32,
         wasm_len: i32,
         helpers_ptr: i32,
         helpers_len: i32|
         -> wasmtime::Result<i32> {
            compile(&mut caller, wasm_ptr, wasm_len, helpers_ptr, helpers_len)
                .map_err(|error| wasmtime::Error::msg(format!("{error:#}")))
        },
    )?;
    linker.func_wrap(
        "qemu_jit",
        "execute",
        |mut caller: Caller<'_, T>, handle: i32, context_ptr: i32| -> wasmtime::Result<i32> {
            caller.data_mut().qemu_jit().executed_blocks += 1;
            let count = caller.data_mut().qemu_jit().executed_blocks;
            if count == 1 || count.is_multiple_of(100_000) {
                eprintln!("QEMU executing first TCG block handle={handle}");
            }
            let Some(function) = caller
                .data_mut()
                .qemu_jit()
                .blocks
                .get(&(handle as u32))
                .cloned()
            else {
                return Err(wasmtime::Error::msg("unknown QEMU JIT translation block"));
            };
            let result = function.call(&mut caller, context_ptr);
            if count == 1 || count.is_multiple_of(100_000) {
                eprintln!("QEMU first TCG block returned {result:?}");
            }
            result
        },
    )?;
    linker.func_wrap(
        "qemu_jit",
        "remove",
        |mut caller: Caller<'_, T>, handle: i32| {
            caller.data_mut().qemu_jit().blocks.remove(&(handle as u32));
        },
    )?;
    Ok(())
}

fn compile_batch<T: HasQemuJit>(
    caller: &mut Caller<'_, T>,
    bytes_ptr: i32,
    bytes_len: i32,
    nfuncs: i32,
    helpers_ptr: i32,
    nhelpers: i32,
) -> anyhow::Result<i32> {
    ensure!(bytes_ptr >= 0 && bytes_len >= 0 && nfuncs > 0, "invalid QEMU JIT batch");
    ensure!(helpers_ptr >= 0 && nhelpers >= 0, "invalid QEMU JIT helper vector");
    let memory = caller.data_mut().qemu_jit().memory.clone().context("QEMU JIT shared memory is unavailable")?;
    let wasm = read_memory(caller, &memory, bytes_ptr as usize, bytes_len as usize)?;
    let helper_bytes = read_memory(caller, &memory, helpers_ptr as usize, nhelpers as usize * 4)?;
    let helper_indices = helper_bytes.chunks_exact(4).map(|b| u32::from_le_bytes(b.try_into().unwrap())).collect::<Vec<_>>();
    let module = Module::new(caller.engine(), wasm)?;
    let table = caller.get_export("__indirect_function_table").and_then(Extern::into_table).context("QEMU function table export is unavailable")?;
    let mut imports = Vec::new();
    let mut helper = 0;
    for import in module.imports() {
        match (import.module(), import.name(), import.ty()) {
            ("env", "buffer", ExternType::Memory(_)) => imports.push(memory.as_extern()),
            ("env", "table", ExternType::Table(_)) => imports.push(Extern::Table(table)),
            ("helper", _, ExternType::Func(_)) => {
                let index = *helper_indices.get(helper).context("QEMU JIT helper vector is too short")?;
                helper += 1;
                let function = match table.get(&mut *caller, index.into()) { Some(Ref::Func(Some(f))) => f, _ => anyhow::bail!("QEMU helper table entry {index} is not a function") };
                imports.push(Extern::Func(function));
            }
            (m, n, ty) => anyhow::bail!("unsupported QEMU JIT import {m}.{n}: {ty:?}"),
        }
    }
    let instance = Instance::new(&mut *caller, &module, &imports)?;
    let base = table.size(&mut *caller);
    table.grow(&mut *caller, nfuncs as u64, Ref::Func(None))?;
    for i in 0..nfuncs as u64 {
        let function = instance.get_func(&mut *caller, &format!("f{i}")).context("QEMU JIT batch export is missing")?;
        table.set(&mut *caller, base + i, Ref::Func(Some(function)))?;
    }
    eprintln!("QEMU compiled batch of {nfuncs} TCG blocks");
    Ok(base as i32)
}

fn compile<T: HasQemuJit>(
    caller: &mut Caller<'_, T>,
    wasm_ptr: i32,
    wasm_len: i32,
    helpers_ptr: i32,
    helpers_len: i32,
) -> anyhow::Result<i32> {
    ensure!(
        wasm_ptr >= 0 && wasm_len >= 0,
        "invalid QEMU JIT module range"
    );
    ensure!(
        helpers_ptr >= 0 && helpers_len >= 0 && helpers_len % 4 == 0,
        "invalid QEMU JIT helper vector"
    );
    let memory = caller
        .data_mut()
        .qemu_jit()
        .memory
        .clone()
        .context("QEMU JIT shared memory is unavailable")?;
    let wasm = read_memory(caller, &memory, wasm_ptr as usize, wasm_len as usize)?;
    let helper_bytes = read_memory(caller, &memory, helpers_ptr as usize, helpers_len as usize)?;
    let helper_indices = helper_bytes
        .chunks_exact(4)
        .map(|bytes| u32::from_le_bytes(bytes.try_into().expect("four-byte chunk")))
        .collect::<Vec<_>>();
    let module = Module::new(caller.engine(), wasm)?;
    let table = caller
        .get_export("__indirect_function_table")
        .and_then(Extern::into_table)
        .context("QEMU function table export is unavailable")?;
    let mut helper_index = 0;
    let mut imports = Vec::new();
    for import in module.imports() {
        match (import.module(), import.name(), import.ty()) {
            ("env", "buffer", ExternType::Memory(memory_type)) => {
                ensure!(
                    memory_type.is_shared() == matches!(memory, QemuMemory::Shared(_)),
                    "QEMU JIT block memory mode differs from outer QEMU"
                );
                imports.push(memory.as_extern());
            }
            ("helper", _, ExternType::Func(_)) => {
                let table_index = *helper_indices
                    .get(helper_index)
                    .context("QEMU JIT helper vector is too short")?;
                helper_index += 1;
                let function = match table.get(&mut *caller, table_index.into()) {
                    Some(Ref::Func(Some(function))) => function,
                    _ => anyhow::bail!("QEMU helper table entry {table_index} is not a function"),
                };
                imports.push(Extern::Func(function));
            }
            (module, name, ty) => {
                anyhow::bail!("unsupported QEMU JIT import {module}.{name}: {ty:?}")
            }
        }
    }
    ensure!(
        helper_index == helper_indices.len(),
        "QEMU JIT helper vector length mismatch"
    );
    let instance = Instance::new(&mut *caller, &module, &imports)?;
    let function = instance.get_typed_func::<i32, i32>(&mut *caller, "start")?;
    let jit = caller.data_mut().qemu_jit();
    jit.compiled_blocks += 1;
    if jit.compiled_blocks == 1 || jit.compiled_blocks % 100 == 0 {
        eprintln!("QEMU compiled {} TCG blocks", jit.compiled_blocks);
    }
    let handle = jit.next_handle;
    jit.next_handle = jit
        .next_handle
        .checked_add(1)
        .context("QEMU JIT handle space exhausted")?;
    jit.blocks.insert(handle, function);
    Ok(handle as i32)
}

fn read_memory<T>(
    caller: &mut Caller<'_, T>,
    memory: &QemuMemory,
    offset: usize,
    length: usize,
) -> anyhow::Result<Vec<u8>> {
    let end = offset
        .checked_add(length)
        .context("QEMU JIT memory range overflow")?;
    match memory {
        QemuMemory::Plain(memory) => {
            let mut result = vec![0; length];
            memory.read(caller, offset, &mut result)?;
            Ok(result)
        }
        QemuMemory::Shared(memory) => {
            let source = memory
                .data()
                .get(offset..end)
                .context("QEMU JIT memory range is out of bounds")?;
            Ok(source.iter().map(read_byte).collect())
        }
    }
}

fn read_byte(source: &UnsafeCell<u8>) -> u8 {
    // SAFETY: QEMU is suspended inside the host import while its translation buffer is copied.
    unsafe { source.get().read() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::{Config, Engine, MemoryType, Store};

    struct State(QemuJit);

    impl HasQemuJit for State {
        fn qemu_jit(&mut self) -> &mut QemuJit {
            &mut self.0
        }
    }

    #[test]
    fn binds_outer_helpers_and_memory_in_the_same_store() {
        let mut config = Config::new();
        config.shared_memory(true);
        let engine = Engine::new(&config).unwrap();
        let memory = SharedMemory::new(&engine, MemoryType::shared(1, 1)).unwrap();
        let block = wat::parse_str(
            r#"(module
                (import "env" "buffer" (memory 1 1 shared))
                (import "helper" "0" (func $helper (param i32) (result i32)))
                (func (export "start") (param i32) (result i32)
                    local.get 0
                    call $helper))"#,
        )
        .unwrap();
        write_shared(&memory, 0x1000, &block);
        write_shared(&memory, 0x2000, &0_u32.to_le_bytes());
        let outer = Module::new(
            &engine,
            &format!(
                r#"(module
                (import "env" "memory" (memory 1 1 shared))
                (import "qemu_jit" "compile" (func $compile (param i32 i32 i32 i32) (result i32)))
                (import "qemu_jit" "execute" (func $execute (param i32 i32) (result i32)))
                (import "qemu_jit" "remove" (func $remove (param i32)))
                (table (export "__indirect_function_table") 1 funcref)
                (func $helper (param i32) (result i32) local.get 0 i32.const 1 i32.add)
                (elem (i32.const 0) $helper)
                (func (export "run") (result i32)
                    i32.const 0x1000
                    i32.const {}
                    i32.const 0x2000
                    i32.const 4
                    call $compile
                    i32.const 41
                    call $execute))"#,
                block.len()
            ),
        )
        .unwrap();
        let mut store = Store::new(
            &engine,
            State(QemuJit::new(Some(QemuMemory::Shared(memory.clone())))),
        );
        let mut linker = Linker::new(&engine);
        linker.define(&mut store, "env", "memory", memory).unwrap();
        add_to_linker(&mut linker).unwrap();
        let instance = linker.instantiate(&mut store, &outer).unwrap();
        let run = instance
            .get_typed_func::<(), i32>(&mut store, "run")
            .unwrap();
        assert_eq!(run.call(&mut store, ()).unwrap(), 42);
    }

    fn write_shared(memory: &SharedMemory, offset: usize, bytes: &[u8]) {
        for (destination, source) in memory.data()[offset..offset + bytes.len()]
            .iter()
            .zip(bytes)
        {
            // SAFETY: no Wasm instance or other thread can access this test memory yet.
            unsafe { destination.get().write(*source) };
        }
    }
}
