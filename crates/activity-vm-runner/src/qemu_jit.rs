use anyhow::{Context, ensure};
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
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
    id: u64,
    pub(crate) memory: Option<QemuMemory>,
    blocks: HashMap<u32, TypedFunc<i32, i32>>,
    next_handle: u32,
    compiled_blocks: u64,
    executed_blocks: u64,
    tb_ptr_ptr: Option<i32>,
    remove_ptr: Option<i32>,
    remove_count_ptr: Option<i32>,
}

impl QemuJit {
    pub(crate) fn new(memory: Option<QemuMemory>) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            memory,
            blocks: HashMap::new(),
            next_handle: 1,
            compiled_blocks: 0,
            executed_blocks: 0,
            tb_ptr_ptr: None,
            remove_ptr: None,
            remove_count_ptr: None,
        }
    }

    pub(crate) fn initialize_legacy(
        &mut self,
        tb_ptr_ptr: i32,
        remove_ptr: i32,
        remove_count_ptr: i32,
    ) {
        self.tb_ptr_ptr = Some(tb_ptr_ptr);
        self.remove_ptr = Some(remove_ptr);
        self.remove_count_ptr = Some(remove_count_ptr);
        eprintln!(
            "QEMU JIT initialized store={} tb_ptr_ptr={tb_ptr_ptr:#x}",
            self.id
        );
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
        "init_wasm32_js",
        |mut caller: Caller<'_, T>, tb_ptr_ptr: i32, _core: i32, remove_ptr: i32, remove_count_ptr: i32, _gc_ptr: i32| {
            let jit = caller.data_mut().qemu_jit();
            eprintln!("QEMU JIT init store={} tb_ptr_ptr={tb_ptr_ptr:#x} remove_ptr={remove_ptr:#x} remove_count_ptr={remove_count_ptr:#x}", jit.id);
            jit.initialize_legacy(tb_ptr_ptr, remove_ptr, remove_count_ptr);
        },
    )?;
    linker.func_wrap(
        "env",
        "instantiate_wasm",
        |mut caller: Caller<'_, T>| -> wasmtime::Result<i32> {
            compile_legacy(&mut caller).map_err(|error| wasmtime::Error::msg(format!("{error:#}")))
        },
    )?;
    linker.func_wrap(
        "env",
        "remove_module_js",
        |mut caller: Caller<'_, T>| -> wasmtime::Result<()> {
            remove_legacy(&mut caller).map_err(|error| wasmtime::Error::msg(format!("{error:#}")))
        },
    )?;
    linker.func_wrap(
        "env",
        "instantiate_batch_js",
        |mut caller: Caller<'_, T>,
         bytes: i32,
         len: i32,
         nfuncs: i32,
         helpers: i32,
         nhelpers: i32,
         _dump: i32| {
            compile_batch(&mut caller, bytes, len, nfuncs, helpers, nhelpers)
                .map_err(|error| wasmtime::Error::msg(format!("{error:#}")))
        },
    )?;
    linker.func_wrap(
        "env",
        "wasm_tail_calls_supported_js",
        |caller: Caller<'_, T>| tail_calls_supported(caller.engine()),
    )?;
    linker.func_wrap(
        "env",
        "remove_batch_js",
        |mut caller: Caller<'_, T>, base: i32, count: i32| -> wasmtime::Result<()> {
            remove_batch(&mut caller, base, count)
                .map_err(|error| wasmtime::Error::msg(format!("{error:#}")))
        },
    )?;
    linker.func_wrap(
        "env",
        "report_stats_js",
        |batches: i32,
         blocks: i32,
         _full: i32,
         _hot: i32,
         _evictions: i32,
         _bytes: i32,
         _sites: i32,
         _linked: i32| {
            eprintln!("QEMU JIT batches={batches} blocks={blocks}");
        },
    )?;
    linker.func_wrap("qemu_jit", "enabled", || -> i32 {
        i32::from(std::env::var_os("OBELISK_QEMU_DISABLE_JIT").is_none())
    })?;
    linker.func_wrap(
        "qemu_jit",
        "compile_batch",
        |mut caller: Caller<'_, T>,
         bytes: i32,
         len: i32,
         nfuncs: i32,
         helpers: i32,
         nhelpers: i32,
         _dump: i32| {
            compile_batch(&mut caller, bytes, len, nfuncs, helpers, nhelpers)
                .map_err(|error| wasmtime::Error::msg(format!("{error:#}")))
        },
    )?;
    linker.func_wrap(
        "qemu_jit",
        "tail_calls_supported",
        |caller: Caller<'_, T>| tail_calls_supported(caller.engine()),
    )?;
    linker.func_wrap(
        "qemu_jit",
        "remove_batch",
        |mut caller: Caller<'_, T>, base: i32, count: i32| -> wasmtime::Result<()> {
            remove_batch(&mut caller, base, count)
                .map_err(|error| wasmtime::Error::msg(format!("{error:#}")))
        },
    )?;
    linker.func_wrap(
        "qemu_jit",
        "report_stats",
        |batches: i32,
         blocks: i32,
         _full: i32,
         _hot: i32,
         _evictions: i32,
         _bytes: i32,
         _sites: i32,
         _linked: i32| {
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

fn compile_legacy<T: HasQemuJit>(caller: &mut Caller<'_, T>) -> anyhow::Result<i32> {
    let jit = caller.data_mut().qemu_jit();
    eprintln!(
        "QEMU JIT instantiate store={} initialized={}",
        jit.id,
        jit.tb_ptr_ptr.is_some()
    );
    let memory = jit
        .memory
        .clone()
        .context("QEMU JIT shared memory is unavailable")?;
    let tb_ptr_ptr = jit.tb_ptr_ptr.context("QEMU JIT was not initialized")?;
    let tb_ptr = read_i32(caller, &memory, tb_ptr_ptr)?;
    let export_size = read_i32(caller, &memory, tb_ptr + 4)?;
    let counter_size_ptr = tb_ptr + 8 + export_size;
    let counter_size = read_i32(caller, &memory, counter_size_ptr)?;
    let body_size_ptr = counter_size_ptr + 4 + counter_size;
    let body_size = read_i32(caller, &memory, body_size_ptr)?;
    let wasm_size_ptr = body_size_ptr + 4 + body_size;
    let wasm_size = read_i32(caller, &memory, wasm_size_ptr)?;
    let wasm_ptr = wasm_size_ptr + 4;
    let helpers_size_ptr = wasm_ptr + wasm_size;
    let helpers_size = read_i32(caller, &memory, helpers_size_ptr)?;
    let helpers_ptr = helpers_size_ptr + 4;
    ensure!(
        wasm_size >= 0 && helpers_size >= 0 && helpers_size % 4 == 0,
        "invalid legacy QEMU JIT module"
    );

    let wasm = read_memory(caller, &memory, wasm_ptr as usize, wasm_size as usize)?;
    let helper_bytes = read_memory(caller, &memory, helpers_ptr as usize, helpers_size as usize)?;
    let helper_indices = helper_bytes
        .chunks_exact(4)
        .map(|bytes| u32::from_le_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();
    eprintln!("QEMU compiling TCG block bytes={wasm_size}");
    let module = Module::new(caller.engine(), wasm)?;
    let table = caller
        .get_export("__indirect_function_table")
        .and_then(Extern::into_table)
        .context("QEMU function table export is unavailable")?;
    let mut imports = Vec::new();
    let mut helper = 0;
    for import in module.imports() {
        match (import.module(), import.name(), import.ty()) {
            ("env", "buffer", ExternType::Memory(_)) => imports.push(memory.as_extern()),
            ("helper", _, ExternType::Func(_)) => {
                let index = *helper_indices
                    .get(helper)
                    .context("QEMU JIT helper vector is too short")?;
                helper += 1;
                let function = match table.get(&mut *caller, index.into()) {
                    Some(Ref::Func(Some(function))) => function,
                    _ => anyhow::bail!("QEMU helper table entry {index} is not a function"),
                };
                imports.push(Extern::Func(function));
            }
            (module, name, ty) => {
                anyhow::bail!("unsupported QEMU JIT import {module}.{name}: {ty:?}")
            }
        }
    }
    let instance = Instance::new(&mut *caller, &module, &imports)?;
    let function = instance
        .get_func(&mut *caller, "start")
        .context("QEMU JIT start export is missing")?;
    let index = table.size(&mut *caller);
    table.grow(&mut *caller, 1, Ref::Func(None))?;
    table.set(&mut *caller, index, Ref::Func(Some(function)))?;
    Ok(i32::try_from(index)?)
}

fn remove_legacy<T: HasQemuJit>(caller: &mut Caller<'_, T>) -> anyhow::Result<()> {
    let jit = caller.data_mut().qemu_jit();
    let memory = jit
        .memory
        .clone()
        .context("QEMU JIT shared memory is unavailable")?;
    let remove_ptr = jit
        .remove_ptr
        .context("QEMU JIT removal vector is unavailable")?;
    let count_ptr = jit
        .remove_count_ptr
        .context("QEMU JIT removal count is unavailable")?;
    let count = read_i32(caller, &memory, count_ptr)?;
    ensure!(count >= 0, "invalid QEMU JIT removal count");
    let table = caller
        .get_export("__indirect_function_table")
        .and_then(Extern::into_table)
        .context("QEMU function table export is unavailable")?;
    for offset in 0..count {
        let index = read_i32(caller, &memory, remove_ptr + offset * 4)?;
        ensure!(index >= 0, "invalid QEMU JIT table index");
        table.set(&mut *caller, index as u64, Ref::Func(None))?;
    }
    write_i32(caller, &memory, count_ptr, 0)
}

fn read_i32<T>(
    caller: &mut Caller<'_, T>,
    memory: &QemuMemory,
    offset: i32,
) -> anyhow::Result<i32> {
    ensure!(offset >= 0, "negative QEMU memory offset");
    let bytes = read_memory(caller, memory, offset as usize, 4)?;
    Ok(i32::from_le_bytes(bytes.try_into().unwrap()))
}

fn write_i32<T>(
    caller: &mut Caller<'_, T>,
    memory: &QemuMemory,
    offset: i32,
    value: i32,
) -> anyhow::Result<()> {
    ensure!(offset >= 0, "negative QEMU memory offset");
    match memory {
        QemuMemory::Plain(memory) => memory.write(caller, offset as usize, &value.to_le_bytes())?,
        QemuMemory::Shared(memory) => {
            let destination = memory
                .data()
                .get(offset as usize..offset as usize + 4)
                .context("QEMU memory write is out of bounds")?;
            for (destination, source) in destination.iter().zip(value.to_le_bytes()) {
                unsafe { destination.get().write_volatile(source) };
            }
        }
    }
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
    ensure!(
        bytes_ptr >= 0 && bytes_len >= 0 && nfuncs > 0,
        "invalid QEMU JIT batch"
    );
    ensure!(
        helpers_ptr >= 0 && nhelpers >= 0,
        "invalid QEMU JIT helper vector"
    );
    let memory = caller
        .data_mut()
        .qemu_jit()
        .memory
        .clone()
        .context("QEMU JIT shared memory is unavailable")?;
    let wasm = read_memory(caller, &memory, bytes_ptr as usize, bytes_len as usize)?;
    let helper_bytes = read_memory(caller, &memory, helpers_ptr as usize, nhelpers as usize * 4)?;
    let helper_indices = helper_bytes
        .chunks_exact(4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
        .collect::<Vec<_>>();
    let started = Instant::now();
    let module = Module::new(caller.engine(), wasm)?;
    let compiled = started.elapsed();
    let table = caller
        .get_export("__indirect_function_table")
        .and_then(Extern::into_table)
        .context("QEMU function table export is unavailable")?;
    let mut imports = Vec::new();
    let mut helper = 0;
    for import in module.imports() {
        match (import.module(), import.name(), import.ty()) {
            ("env", "buffer", ExternType::Memory(_)) => imports.push(memory.as_extern()),
            ("env", "table", ExternType::Table(_)) => imports.push(Extern::Table(table)),
            ("helper", _, ExternType::Func(_)) => {
                let index = *helper_indices
                    .get(helper)
                    .context("QEMU JIT helper vector is too short")?;
                helper += 1;
                let function = match table.get(&mut *caller, index.into()) {
                    Some(Ref::Func(Some(f))) => f,
                    _ => anyhow::bail!("QEMU helper table entry {index} is not a function"),
                };
                imports.push(Extern::Func(function));
            }
            (m, n, ty) => anyhow::bail!("unsupported QEMU JIT import {m}.{n}: {ty:?}"),
        }
    }
    ensure!(
        helper == helper_indices.len(),
        "QEMU JIT helper vector length mismatch"
    );
    let instantiate_started = Instant::now();
    let instance = Instance::new(&mut *caller, &module, &imports)?;
    let instantiated = instantiate_started.elapsed();
    let base = table.size(&mut *caller);
    table.grow(&mut *caller, nfuncs as u64, Ref::Func(None))?;
    for i in 0..nfuncs as u64 {
        let function = instance
            .get_func(&mut *caller, &format!("f{i}"))
            .context("QEMU JIT batch export is missing")?;
        table.set(&mut *caller, base + i, Ref::Func(Some(function)))?;
    }
    eprintln!(
        "QEMU compiled batch blocks={nfuncs} bytes={bytes_len} compile_us={} instantiate_us={} total_us={}",
        compiled.as_micros(),
        instantiated.as_micros(),
        started.elapsed().as_micros()
    );
    Ok(base as i32)
}

fn tail_calls_supported(engine: &wasmtime::Engine) -> i32 {
    // This is the same minimal return_call probe used by deployed Trynix.
    const PROBE: &[u8] = &[
        0, 97, 115, 109, 1, 0, 0, 0, 1, 4, 1, 96, 0, 0, 3, 2, 1, 0, 10, 6, 1, 4, 0, 18, 0, 11,
    ];
    i32::from(
        std::env::var_os("OBELISK_QEMU_DISABLE_TAIL_CALLS").is_none()
            && Module::new(engine, PROBE).is_ok(),
    )
}

fn remove_batch<T: HasQemuJit>(
    caller: &mut Caller<'_, T>,
    base: i32,
    count: i32,
) -> anyhow::Result<()> {
    ensure!(base >= 0 && count >= 0, "invalid QEMU JIT batch range");
    let table = caller
        .get_export("__indirect_function_table")
        .and_then(Extern::into_table)
        .context("QEMU function table export is unavailable")?;
    let end = u64::try_from(base)?
        .checked_add(u64::try_from(count)?)
        .context("QEMU JIT batch range overflow")?;
    ensure!(
        end <= table.size(&mut *caller),
        "QEMU JIT batch range exceeds table"
    );
    for index in u64::try_from(base)?..end {
        table.set(&mut *caller, index, Ref::Func(None))?;
    }
    Ok(())
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
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
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

    #[test]
    fn detects_tail_call_support_from_the_engine() {
        let mut disabled = Config::new();
        disabled.wasm_tail_call(false);
        assert_eq!(tail_calls_supported(&Engine::new(&disabled).unwrap()), 0);

        let mut enabled = Config::new();
        enabled.wasm_tail_call(true);
        assert_eq!(tail_calls_supported(&Engine::new(&enabled).unwrap()), 1);
    }

    #[test]
    fn loads_calls_and_removes_exact_style_batches_across_a_yielding_helper() {
        let mut config = Config::new();
        config.shared_memory(true);
        config.wasm_tail_call(true);
        let engine = Engine::new(&config).unwrap();
        let memory = SharedMemory::new(&engine, MemoryType::shared(2, 2)).unwrap();
        write_shared(&memory, 0, &2_u32.to_le_bytes());

        let ordinary = wat::parse_str(
            r#"(module
                (type $tb (func (param i32) (result i32)))
                (import "env" "buffer" (memory 2 2 shared))
                (import "env" "table" (table 1 funcref))
                (import "helper" "0" (func $helper (type $tb)))
                (func (export "f0") (type $tb) (param i32) (result i32)
                    local.get 0 call $helper))"#,
        )
        .unwrap();
        let tail = wat::parse_str(
            r#"(module
                (type $tb (func (param i32) (result i32)))
                (import "env" "buffer" (memory 2 2 shared))
                (import "env" "table" (table 1 funcref))
                (import "helper" "0" (func $helper (type $tb)))
                (func (export "f0") (type $tb) (param i32) (result i32)
                    local.get 0 return_call $helper))"#,
        )
        .unwrap();
        write_shared(&memory, 0x1000, &ordinary);
        write_shared(&memory, 0x4000, &tail);
        write_shared(&memory, 0x8000, &0_u32.to_le_bytes());

        let outer = Module::new(
            &engine,
            &format!(
                r#"(module
                    (type $tb (func (param i32) (result i32)))
                    (import "env" "memory" (memory 2 2 shared))
                    (import "env" "instantiate_batch_js"
                        (func $compile (param i32 i32 i32 i32 i32 i32) (result i32)))
                    (import "env" "remove_batch_js" (func $remove (param i32 i32)))
                    (import "env" "wasm_tail_calls_supported_js" (func $tail (result i32)))
                    (import "test" "yield" (func $yield))
                    (global $asyncify_state (mut i32) i32.const 0)
                    (table (export "__indirect_function_table") 1 funcref)
                    (func $helper (type $tb) (param i32) (result i32)
                        call $yield
                        local.get 0 i32.const 0 i32.load i32.add)
                    (elem (i32.const 0) $helper)
                    (func (export "compile_ordinary") (result i32)
                        i32.const 0x1000 i32.const {ordinary_len} i32.const 1
                        i32.const 0x8000 i32.const 1 i32.const 0 call $compile)
                    (func (export "compile_tail") (result i32)
                        i32.const 0x4000 i32.const {tail_len} i32.const 1
                        i32.const 0x8000 i32.const 1 i32.const 0 call $compile)
                    (func (export "call") (param i32 i32) (result i32)
                        local.get 1 local.get 0 call_indirect (type $tb))
                    (func (export "remove") (param i32) local.get 0 i32.const 1 call $remove)
                    (func (export "asyncify_get_state") (result i32) global.get $asyncify_state)
                    (func (export "asyncify_start_unwind") (param i32)
                        i32.const 1 global.set $asyncify_state)
                    (func (export "asyncify_stop_unwind")
                        i32.const 0 global.set $asyncify_state)
                    (func (export "asyncify_start_rewind") (param i32)
                        i32.const 2 global.set $asyncify_state)
                    (func (export "asyncify_stop_rewind")
                        i32.const 0 global.set $asyncify_state)
                    (func (export "tail_supported") (result i32) call $tail))"#,
                ordinary_len = ordinary.len(),
                tail_len = tail.len(),
            ),
        )
        .unwrap();
        let yields = Arc::new(AtomicUsize::new(0));
        let host_yields = yields.clone();
        let mut store = Store::new(
            &engine,
            State(QemuJit::new(Some(QemuMemory::Shared(memory.clone())))),
        );
        let mut linker = Linker::new(&engine);
        linker.define(&mut store, "env", "memory", memory).unwrap();
        linker
            .func_wrap(
                "test",
                "yield",
                move |mut caller: Caller<'_, State>| -> wasmtime::Result<()> {
                    host_yields.fetch_add(1, Ordering::Relaxed);
                    let state = caller
                        .get_export("asyncify_get_state")
                        .and_then(Extern::into_func)
                        .unwrap()
                        .typed::<(), i32>(&caller)?
                        .call(&mut caller, ())?;
                    match state {
                        0 => caller
                            .get_export("asyncify_start_unwind")
                            .and_then(Extern::into_func)
                            .unwrap()
                            .typed::<i32, ()>(&caller)?
                            .call(&mut caller, 123),
                        2 => caller
                            .get_export("asyncify_stop_rewind")
                            .and_then(Extern::into_func)
                            .unwrap()
                            .typed::<(), ()>(&caller)?
                            .call(&mut caller, ()),
                        other => Err(wasmtime::Error::msg(format!(
                            "unexpected Asyncify state {other}"
                        ))),
                    }
                },
            )
            .unwrap();
        add_to_linker(&mut linker).unwrap();
        let instance = linker.instantiate(&mut store, &outer).unwrap();
        let compile_ordinary = instance
            .get_typed_func::<(), i32>(&mut store, "compile_ordinary")
            .unwrap();
        let compile_tail = instance
            .get_typed_func::<(), i32>(&mut store, "compile_tail")
            .unwrap();
        let call = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "call")
            .unwrap();
        let remove = instance
            .get_typed_func::<i32, ()>(&mut store, "remove")
            .unwrap();
        let tail_supported = instance
            .get_typed_func::<(), i32>(&mut store, "tail_supported")
            .unwrap();
        let asyncify_state = instance
            .get_typed_func::<(), i32>(&mut store, "asyncify_get_state")
            .unwrap();
        let stop_unwind = instance
            .get_typed_func::<(), ()>(&mut store, "asyncify_stop_unwind")
            .unwrap();
        let start_rewind = instance
            .get_typed_func::<i32, ()>(&mut store, "asyncify_start_rewind")
            .unwrap();

        let ordinary_base = compile_ordinary.call(&mut store, ()).unwrap();
        assert_eq!(call.call(&mut store, (ordinary_base, 40)).unwrap(), 42);
        assert_eq!(asyncify_state.call(&mut store, ()).unwrap(), 1);
        stop_unwind.call(&mut store, ()).unwrap();
        start_rewind.call(&mut store, 123).unwrap();
        remove.call(&mut store, ordinary_base).unwrap();
        assert!(call.call(&mut store, (ordinary_base, 40)).is_err());

        assert_eq!(tail_supported.call(&mut store, ()).unwrap(), 1);
        let tail_base = compile_tail.call(&mut store, ()).unwrap();
        assert!(tail_base > ordinary_base);
        assert_eq!(call.call(&mut store, (tail_base, 40)).unwrap(), 42);
        assert_eq!(asyncify_state.call(&mut store, ()).unwrap(), 0);
        assert_eq!(yields.load(Ordering::Relaxed), 2);
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
