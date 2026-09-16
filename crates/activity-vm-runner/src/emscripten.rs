use anyhow::{Context, ensure};
use chrono::{Datelike, TimeZone as _, Timelike as _, Utc};
use std::io::Read as _;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use wasmtime::{
    Caller, Extern, ExternType, Instance, Linker, Module, Ref, Store, TypedFunc, Val, ValType,
};

use crate::VmState;

#[derive(Clone, Copy, Debug)]
pub(crate) enum FiberEntry {
    Start,
    Table { index: i32, argument: i32 },
}

#[derive(Debug)]
struct EmscriptenLongjmp;

impl std::fmt::Display for EmscriptenLongjmp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Emscripten longjmp")
    }
}

impl std::error::Error for EmscriptenLongjmp {}

const SUCCESS_SHIMS: &[&str] = &[
    "sync_file_range",
    "fallocate",
    "pthread_setname_np",
    "pthread_setaffinity_np",
    "pthread_getaffinity_np",
    "_emscripten_thread_set_strongref",
    "_emscripten_thread_cleanup",
    "__emscripten_thread_cleanup",
    "emscripten_check_blocking_allowed",
];

pub(crate) fn add_invoke_wrappers<T: Send + 'static>(
    linker: &mut Linker<T>,
    module: &Module,
) -> anyhow::Result<()> {
    for import in module.imports() {
        let ("env", name, ExternType::Func(function_type)) =
            (import.module(), import.name(), import.ty())
        else {
            continue;
        };
        if name.starts_with("invoke_") {
            let name = name.to_owned();
            linker.func_new(
                "env",
                &name.clone(),
                function_type,
                move |mut caller: Caller<'_, T>, params, results| {
                    invoke(&mut caller, &name, params, results)
                        .map_err(|error| wasmtime::Error::msg(format!("{error:#}")))
                },
            )?;
        }
    }
    Ok(())
}

/// Defines the small, synchronous part of Emscripten's JavaScript runtime.
///
/// Unsupported services are still linked so a real module can identify the
/// first service it exercises. They trap with the import name instead of
/// failing module instantiation with an arbitrary first missing import.
pub(crate) fn add_platform_shims<T: Send + 'static>(
    linker: &mut Linker<T>,
    module: &Module,
) -> anyhow::Result<()> {
    for import in module.imports() {
        let ("env", name, ExternType::Func(function_type)) =
            (import.module(), import.name(), import.ty())
        else {
            continue;
        };
        if name.starts_with("invoke_")
            || name.starts_with("_wasmfs_node_")
            || name == "__pthread_create_js"
            || name == "ffi_call_js"
            || name == "_emscripten_throw_longjmp"
            || name == "emscripten_fiber_swap"
            || name == "__emscripten_init_main_thread_js"
            || name == "_emscripten_thread_mailbox_await"
            || name == "_emscripten_notify_mailbox_postmessage"
            || name == "wasmtime_ppoll_js"
            || name == "instantiate_batch_js"
            || name == "wasm_tail_calls_supported_js"
            || name == "remove_batch_js"
            || name == "report_stats_js"
            || name == "init_wasm32_js"
            || name == "instantiate_wasm"
            || name == "remove_module_js"
            || name == "getentropy"
            || name == "emscripten_get_now"
            || name == "emscripten_date_now"
            || name == "_emscripten_get_now_is_monotonic"
            || name == "_tzset_js"
            || name == "_gmtime_js"
            || name == "_localtime_js"
            || name == "_mktime_js"
            || name == "exit"
        {
            continue;
        }
        let name = name.to_owned();
        let returns_core_count = name == "emscripten_num_logical_cores";
        let succeeds = returns_core_count || SUCCESS_SHIMS.contains(&name.as_str());
        let returns = function_type.results().collect::<Vec<_>>();
        linker.func_new(
            "env",
            &name.clone(),
            function_type,
            move |_caller, _params, results| {
                if !succeeds {
                    return Err(wasmtime::Error::msg(format!(
                        "unsupported Emscripten runtime import called: env::{name}"
                    )));
                }
                for (result, ty) in results.iter_mut().zip(&returns) {
                    *result = zero(ty)?;
                }
                if returns_core_count {
                    results[0] = Val::I32(1);
                }
                Ok(())
            },
        )?;
    }
    Ok(())
}

pub(crate) fn add_platform_services(linker: &mut Linker<VmState>) -> anyhow::Result<()> {
    linker.func_wrap(
        "env",
        "getentropy",
        |mut caller: Caller<'_, VmState>, pointer: i32, length: i32| -> wasmtime::Result<i32> {
            let length = usize::try_from(length).map_err(wasmtime::Error::msg)?;
            let mut bytes = vec![0; length];
            std::fs::File::open("/dev/urandom")
                .and_then(|mut source| source.read_exact(&mut bytes))
                .map_err(wasmtime::Error::msg)?;
            let memory = caller
                .data()
                .qemu_jit
                .memory
                .clone()
                .ok_or_else(|| wasmtime::Error::msg("missing Emscripten memory import"))?;
            write_extern(memory.as_extern(), &mut caller, pointer, &bytes)?;
            Ok(0)
        },
    )?;
    linker.func_wrap("env", "emscripten_get_now", || -> f64 {
        static ORIGIN: OnceLock<Instant> = OnceLock::new();
        ORIGIN.get_or_init(Instant::now).elapsed().as_secs_f64() * 1_000.0
    })?;
    linker.func_wrap("env", "emscripten_date_now", || -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
            * 1_000.0
    })?;
    linker.func_wrap("env", "_emscripten_get_now_is_monotonic", || -> i32 { 1 })?;
    linker.func_wrap(
        "env",
        "_tzset_js",
        |mut caller: Caller<'_, VmState>, timezone: i32, daylight: i32, tzname: i32| {
            let memory = caller
                .data()
                .qemu_jit
                .memory
                .clone()
                .ok_or_else(|| wasmtime::Error::msg("missing Emscripten memory import"))?;
            let malloc = caller
                .get_export("malloc")
                .and_then(Extern::into_func)
                .ok_or_else(|| wasmtime::Error::msg("missing Emscripten malloc export"))?
                .typed::<i32, i32>(&caller)?;
            let zone_name = malloc.call(&mut caller, 4)?;
            write_extern(memory.as_extern(), &mut caller, zone_name, b"GMT\0")?;
            write_extern_i32(memory.as_extern(), &mut caller, timezone, 0)?;
            write_extern_i32(memory.as_extern(), &mut caller, daylight, 0)?;
            write_extern_i32(memory.as_extern(), &mut caller, tzname, zone_name)?;
            write_extern_i32(memory.as_extern(), &mut caller, tzname + 4, zone_name)?;
            Ok(())
        },
    )?;
    for name in ["_gmtime_js", "_localtime_js"] {
        linker.func_wrap(
            "env",
            name,
            |mut caller: Caller<'_, VmState>, timestamp: i64, tm: i32| {
                let date = chrono::DateTime::<Utc>::from_timestamp(timestamp, 0)
                    .ok_or_else(|| wasmtime::Error::msg("timestamp is outside chrono's range"))?;
                let memory = caller
                    .data()
                    .qemu_jit
                    .memory
                    .clone()
                    .ok_or_else(|| wasmtime::Error::msg("missing Emscripten memory import"))?;
                let fields = [
                    date.second() as i32,
                    date.minute() as i32,
                    date.hour() as i32,
                    date.day() as i32,
                    date.month0() as i32,
                    date.year() - 1900,
                    date.weekday().num_days_from_sunday() as i32,
                    date.ordinal0() as i32,
                    0,
                    0,
                ];
                for (index, value) in fields.into_iter().enumerate() {
                    write_extern_i32(
                        memory.as_extern(),
                        &mut caller,
                        tm + i32::try_from(index * 4).unwrap(),
                        value,
                    )?;
                }
                Ok(())
            },
        )?;
    }
    linker.func_wrap(
        "env",
        "_mktime_js",
        |mut caller: Caller<'_, VmState>, tm: i32| -> wasmtime::Result<i64> {
            let memory = caller
                .data()
                .qemu_jit
                .memory
                .clone()
                .ok_or_else(|| wasmtime::Error::msg("missing Emscripten memory import"))?;
            let second = read_extern_i32(memory.as_extern(), &mut caller, tm)?;
            let minute = read_extern_i32(memory.as_extern(), &mut caller, tm + 4)?;
            let hour = read_extern_i32(memory.as_extern(), &mut caller, tm + 8)?;
            let day = read_extern_i32(memory.as_extern(), &mut caller, tm + 12)?;
            let month = read_extern_i32(memory.as_extern(), &mut caller, tm + 16)?;
            let year = read_extern_i32(memory.as_extern(), &mut caller, tm + 20)? + 1900;
            let date = Utc
                .with_ymd_and_hms(
                    year,
                    u32::try_from(month + 1).unwrap_or(0),
                    u32::try_from(day).unwrap_or(0),
                    u32::try_from(hour).unwrap_or(u32::MAX),
                    u32::try_from(minute).unwrap_or(u32::MAX),
                    u32::try_from(second).unwrap_or(u32::MAX),
                )
                .single()
                .ok_or_else(|| wasmtime::Error::msg("invalid broken-down UTC time"))?;
            write_extern_i32(
                memory.as_extern(),
                &mut caller,
                tm + 24,
                date.weekday().num_days_from_sunday() as i32,
            )?;
            write_extern_i32(
                memory.as_extern(),
                &mut caller,
                tm + 28,
                date.ordinal0() as i32,
            )?;
            write_extern_i32(memory.as_extern(), &mut caller, tm + 32, 0)?;
            write_extern_i32(memory.as_extern(), &mut caller, tm + 36, 0)?;
            Ok(date.timestamp())
        },
    )?;
    linker.func_wrap("env", "exit", |status: i32| -> wasmtime::Result<()> {
        Err(wasmtime_wasi::I32Exit(status).into())
    })?;
    Ok(())
}

pub(crate) fn add_longjmp<T: Send + 'static>(linker: &mut Linker<T>) -> anyhow::Result<()> {
    linker.func_wrap(
        "env",
        "_emscripten_throw_longjmp",
        || -> wasmtime::Result<()> { Err(EmscriptenLongjmp.into()) },
    )?;
    Ok(())
}

pub(crate) fn add_ffi_call(linker: &mut Linker<VmState>) -> anyhow::Result<()> {
    linker.func_wrap(
        "env",
        "ffi_call_js",
        |mut caller: Caller<'_, VmState>,
         cif: i32,
         function_index: i32,
         rvalue: i32,
         avalue: i32| { ffi_call(&mut caller, cif, function_index, rvalue, avalue) },
    )?;
    Ok(())
}

fn ffi_call(
    caller: &mut Caller<'_, VmState>,
    cif: i32,
    function_index: i32,
    rvalue: i32,
    avalue: i32,
) -> wasmtime::Result<()> {
    let memory = caller
        .data()
        .qemu_jit
        .memory
        .clone()
        .ok_or_else(|| wasmtime::Error::msg("missing Emscripten memory import"))?
        .as_extern();
    let nargs = read_extern_i32(memory.clone(), caller, cif + 4)?;
    let arg_types = read_extern_i32(memory.clone(), caller, cif + 8)?;
    let return_type = read_extern_i32(memory.clone(), caller, cif + 12)?;
    let return_id = ffi_type_id(memory.clone(), caller, return_type)?;
    let table = caller
        .get_export("__indirect_function_table")
        .and_then(Extern::into_table)
        .ok_or_else(|| wasmtime::Error::msg("Emscripten function table export is unavailable"))?;
    let function = match table.get(&mut *caller, function_index as u64) {
        Some(Ref::Func(Some(function))) => function,
        _ => {
            return Err(wasmtime::Error::msg(format!(
                "ffi function table entry {function_index} is unavailable"
            )));
        }
    };
    let mut params = Vec::new();
    if return_id == 4 || return_id == 13 {
        params.push(Val::I32(rvalue));
    }
    for index in 0..nargs {
        let value_ptr = read_extern_i32(memory.clone(), caller, avalue + index * 4)?;
        let type_ptr = read_extern_i32(memory.clone(), caller, arg_types + index * 4)?;
        let type_id = ffi_type_id(memory.clone(), caller, type_ptr)?;
        params.push(read_ffi_value(memory.clone(), caller, value_ptr, type_id)?);
    }
    let result_types = function.ty(&*caller).results().collect::<Vec<_>>();
    let mut results = result_types
        .iter()
        .map(zero)
        .collect::<wasmtime::Result<Vec<_>>>()?;
    function.call(&mut *caller, &params, &mut results)?;
    if return_id != 0 && return_id != 4 && return_id != 13 {
        let result = results
            .first()
            .ok_or_else(|| wasmtime::Error::msg("ffi function did not return a value"))?;
        write_ffi_value(memory, caller, rvalue, return_id, result)?;
    }
    Ok(())
}

fn ffi_type_id(
    memory: Extern,
    caller: &mut Caller<'_, VmState>,
    mut type_ptr: i32,
) -> wasmtime::Result<i32> {
    loop {
        let id = read_extern_u16(memory.clone(), caller, type_ptr + 6)? as i32;
        if id != 13 {
            return Ok(id);
        }
        let elements = read_extern_i32(memory.clone(), caller, type_ptr + 8)?;
        let first = read_extern_i32(memory.clone(), caller, elements)?;
        if first == 0 {
            return Ok(0);
        }
        if read_extern_i32(memory.clone(), caller, elements + 4)? != 0 {
            return Ok(id);
        }
        type_ptr = first;
    }
}

fn read_ffi_value(
    memory: Extern,
    caller: &mut Caller<'_, VmState>,
    ptr: i32,
    id: i32,
) -> wasmtime::Result<Val> {
    Ok(match id {
        1 | 5 | 6 | 7 | 8 | 9 | 10 | 14 => Val::I32(read_extern_i32(memory, caller, ptr)?),
        2 => Val::F32(read_extern_u32(memory, caller, ptr)?),
        3 => Val::F64(read_extern_u64(memory, caller, ptr)?),
        11 | 12 => Val::I64(read_extern_u64(memory, caller, ptr)? as i64),
        // Emscripten lowers a multi-field struct argument to a pointer to a
        // stack copy. The libffi-owned value storage remains live for the
        // duration of this synchronous call, so its pointer is equivalent.
        13 => Val::I32(ptr),
        other => {
            return Err(wasmtime::Error::msg(format!(
                "unsupported ffi argument type {other}"
            )));
        }
    })
}

fn write_ffi_value(
    memory: Extern,
    caller: &mut Caller<'_, VmState>,
    ptr: i32,
    id: i32,
    value: &Val,
) -> wasmtime::Result<()> {
    let bytes = match (id, value) {
        (1 | 9 | 10 | 14, Val::I32(v)) => v.to_le_bytes().to_vec(),
        (2, Val::F32(v)) => v.to_le_bytes().to_vec(),
        (3, Val::F64(v)) => v.to_le_bytes().to_vec(),
        (5 | 6, Val::I32(v)) => vec![*v as u8],
        (7 | 8, Val::I32(v)) => (*v as u16).to_le_bytes().to_vec(),
        (11 | 12, Val::I64(v)) => v.to_le_bytes().to_vec(),
        _ => {
            return Err(wasmtime::Error::msg(format!(
                "ffi return type/value mismatch for type {id}"
            )));
        }
    };
    write_extern(memory, caller, ptr, &bytes)
}

pub(crate) fn add_main_thread_init(linker: &mut Linker<VmState>) -> anyhow::Result<()> {
    linker.func_wrap(
        "env",
        "__emscripten_init_main_thread_js",
        |mut caller: Caller<'_, VmState>, pthread: i32| -> wasmtime::Result<()> {
            caller
                .get_export("_emscripten_thread_init")
                .and_then(Extern::into_func)
                .ok_or_else(|| wasmtime::Error::msg("missing _emscripten_thread_init"))?
                .typed::<(i32, i32, i32, i32, i32, i32), ()>(&caller)?
                .call(&mut caller, (pthread, 1, 1, 0, 0, 0))?;
            if let Some(tls) = caller
                .get_export("_emscripten_tls_init")
                .and_then(Extern::into_func)
            {
                let _ = tls.typed::<(), i32>(&caller)?.call(&mut caller, ())?;
            }
            Ok(())
        },
    )?;
    Ok(())
}

pub(crate) fn add_mailbox_notify(linker: &mut Linker<VmState>) -> anyhow::Result<()> {
    linker.func_wrap(
        "env",
        "_emscripten_thread_mailbox_await",
        |mut caller: Caller<'_, VmState>, pthread: i32| -> wasmtime::Result<()> {
            eprintln!("pthread {pthread:#x}: mailbox await requested");
            write_i32(&mut caller, pthread + 128, 1)
        },
    )?;
    linker.func_wrap(
        "env",
        "_emscripten_notify_mailbox_postmessage",
        |caller: Caller<'_, VmState>,
         target: i32,
         _sender: i32,
         _main_thread: i32|
         -> wasmtime::Result<()> {
            eprintln!("pthread {target:#x}: mailbox notification requested");
            let Some(crate::qemu_jit::QemuMemory::Shared(memory)) =
                caller.data().qemu_jit.memory.as_ref()
            else {
                return Err(wasmtime::Error::msg(
                    "Emscripten mailbox notification requires shared memory",
                ));
            };
            memory.atomic_notify(
                u64::try_from(target).map_err(wasmtime::Error::msg)?,
                u32::MAX,
            )?;
            Ok(())
        },
    )?;
    Ok(())
}

/// Keeps QEMU's event loop cooperative when Emscripten has no JavaScript poll
/// service. QEMU rechecks its atomic notifiers, timers, and file handlers after
/// every return, so the bounded wait also permits cross-thread wakeups without
/// a host eventfd.
pub(crate) fn add_poll(linker: &mut Linker<VmState>) -> anyhow::Result<()> {
    linker.func_wrap(
        "env",
        "wasmtime_ppoll_js",
        |mut caller: Caller<'_, VmState>,
         fds: i32,
         count: i32,
         timeout: i32|
         -> wasmtime::Result<i32> {
            if fds < 0 || count < 0 {
                return Ok(-1);
            }
            if let Some(poll) = caller
                .get_export("__syscall_poll")
                .and_then(Extern::into_func)
            {
                let trace_poll = caller.data().poll_calls < 200;
                caller.data_mut().poll_calls += 1;
                let _ = poll
                    .typed::<(i32, i32, i32), i32>(&caller)?
                    .call(&mut caller, (fds, count, 0))?;
                let memory = caller
                    .data()
                    .qemu_jit
                    .memory
                    .clone()
                    .ok_or_else(|| wasmtime::Error::msg("missing Emscripten memory import"))?
                    .as_extern();
                let mut ready = 0;
                for index in 0..count {
                    let address = fds + index * 8;
                    let mut pollfd = [0_u8; 8];
                    read_extern(memory.clone(), &mut caller, address, &mut pollfd)?;
                    let fd = i32::from_le_bytes(pollfd[0..4].try_into().unwrap());
                    let mut revents = i16::from_le_bytes([pollfd[6], pollfd[7]]);
                    if trace_poll {
                        let events = i16::from_le_bytes([pollfd[4], pollfd[5]]);
                        eprintln!(
                            "ppoll syscall fd={fd} events={events:#x} revents={revents:#x} timeout={timeout:#x}"
                        );
                    }
                    // QEMU's Emscripten signalfd fallback is descriptor 8.
                    // WasmFS reports the empty signal pipe as readable, which
                    // makes QEMU spin on zero-byte reads forever. Signals are
                    // delivered by the host runtime, so it stays quiet. Stdin
                    // must remain pollable to release a restored VM's serial
                    // snapshot gate.
                    if fd == 8 {
                        revents = 0;
                        pollfd[6..8].copy_from_slice(&0_i16.to_le_bytes());
                        write_extern(memory.clone(), &mut caller, address, &pollfd)?;
                    }
                    if revents != 0 {
                        ready += 1;
                    }
                }
                if ready == 0 {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                return Ok(ready);
            }
            let memory = caller
                .data()
                .qemu_jit
                .memory
                .clone()
                .ok_or_else(|| wasmtime::Error::msg("missing Emscripten memory import"))?
                .as_extern();
            let mut ready = 0;
            let trace_poll = caller.data().poll_calls < 100;
            caller.data_mut().poll_calls += 1;
            for index in 0..count {
                let address = fds
                    .checked_add(
                        index
                            .checked_mul(8)
                            .ok_or_else(|| wasmtime::Error::msg("pollfd range overflow"))?,
                    )
                    .ok_or_else(|| wasmtime::Error::msg("pollfd range overflow"))?;
                let mut pollfd = [0_u8; 8];
                read_extern(memory.clone(), &mut caller, address, &mut pollfd)?;
                let fd = i32::from_le_bytes(pollfd[0..4].try_into().unwrap());
                let events = i16::from_le_bytes([pollfd[4], pollfd[5]]);
                pollfd[6..8].copy_from_slice(&0_i16.to_le_bytes());
                write_extern(memory.clone(), &mut caller, address, &pollfd)?;
                if trace_poll {
                    eprintln!("ppoll fd={fd} events={events:#x} timeout={timeout:#x}");
                }
                // Synthetic event notifiers are dispatched in-memory by the
                // Wasmtime QEMU fork and appear here as negative descriptors.
                // Only regular WasmFS files are unconditionally pollable.
                if events != 0
                    && fd >= 5
                    && fd != 8
                    && std::env::var_os("OBELISK_QEMU_POLL_NONE").is_none()
                {
                    pollfd[6..8].copy_from_slice(&events.to_le_bytes());
                    write_extern(memory.clone(), &mut caller, address, &pollfd)?;
                    ready += 1;
                }
            }
            if ready == 0 {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Ok(ready)
        },
    )?;
    Ok(())
}

/// Implements the JavaScript half of Emscripten's Asyncify fiber switch.
///
/// The import starts an unwind from inside the guest. `call_asyncify_root`
/// observes the completed unwind, changes stacks, and re-enters the root to
/// rewind the selected fiber.
pub(crate) fn add_fiber_swap(linker: &mut Linker<VmState>) -> anyhow::Result<()> {
    linker.func_wrap(
        "env",
        "emscripten_fiber_swap",
        |mut caller: Caller<'_, VmState>, old_fiber: i32, new_fiber: i32| {
            let state = call_i32_export(&mut caller, "asyncify_get_state", &[])?;
            eprintln!("fiber swap old={old_fiber:#x} new={new_fiber:#x} state={state}");
            match state {
                0 => {
                    let stack = call_i32_export(&mut caller, "stackSave", &[])?;
                    write_i32(&mut caller, old_fiber + 8, stack)?;
                    call_void_export(
                        &mut caller,
                        "asyncify_start_unwind",
                        &[Val::I32(old_fiber + 20)],
                    )?;
                    let entry = caller
                        .data()
                        .active_fiber_entry
                        .ok_or_else(|| wasmtime::Error::msg("fiber swap has no active entry"))?;
                    caller.data_mut().fiber_entries.insert(old_fiber, entry);
                    caller.data_mut().fiber_next = Some(new_fiber);
                    Ok(())
                }
                2 => call_void_export(&mut caller, "asyncify_stop_rewind", &[]),
                other => Err(wasmtime::Error::msg(format!(
                    "emscripten_fiber_swap called in Asyncify state {other}"
                ))),
            }
        },
    )?;
    Ok(())
}

pub(crate) fn call_asyncify_root(
    store: &mut Store<VmState>,
    instance: &Instance,
    _root: &TypedFunc<(), ()>,
) -> wasmtime::Result<()> {
    drive_asyncify(store, instance, FiberEntry::Start)?;
    Ok(())
}

pub(crate) fn call_asyncify_pthread(
    store: &mut Store<VmState>,
    instance: &Instance,
    table_index: i32,
    argument: i32,
) -> wasmtime::Result<i32> {
    let entry = FiberEntry::Table {
        index: table_index,
        argument,
    };
    drive_asyncify(store, instance, entry)?
        .ok_or_else(|| wasmtime::Error::msg("Emscripten pthread returned without an i32 value"))
}

fn drive_asyncify(
    store: &mut Store<VmState>,
    instance: &Instance,
    initial_entry: FiberEntry,
) -> wasmtime::Result<Option<i32>> {
    let mut result = call_fiber_entry(store, instance, initial_entry)?;
    loop {
        let Some(next) = store.data_mut().fiber_next.take() else {
            let state = instance
                .get_typed_func::<(), i32>(&mut *store, "asyncify_get_state")?
                .call(&mut *store, ())?;
            eprintln!("asyncify root returned with state={state} and no next fiber");
            return Ok(result);
        };
        instance
            .get_typed_func::<(), ()>(&mut *store, "asyncify_stop_unwind")?
            .call(&mut *store, ())?;

        let stack_base = read_instance_i32(store, instance, next)?;
        let stack_limit = read_instance_i32(store, instance, next + 4)?;
        let stack_ptr = read_instance_i32(store, instance, next + 8)?;
        let entry = read_instance_i32(store, instance, next + 12)?;
        let user_data = read_instance_i32(store, instance, next + 16)?;
        eprintln!(
            "drive fiber next={next:#x} stack={stack_limit:#x}..{stack_base:#x} ptr={stack_ptr:#x} entry={entry:#x} data={user_data:#x}"
        );
        instance
            .get_typed_func::<(i32, i32), ()>(&mut *store, "emscripten_stack_set_limits")?
            .call(&mut *store, (stack_base, stack_limit))?;
        instance
            .get_typed_func::<i32, ()>(&mut *store, "stackRestore")?
            .call(&mut *store, stack_ptr)?;

        if entry != 0 {
            write_instance_i32(store, instance, next + 12, 0)?;
            let selected = FiberEntry::Table {
                index: entry,
                argument: user_data,
            };
            store.data_mut().fiber_entries.insert(next, selected);
            result = call_fiber_entry(store, instance, selected)?;
        } else {
            instance
                .get_typed_func::<i32, ()>(&mut *store, "asyncify_start_rewind")?
                .call(&mut *store, next + 20)?;
            let selected = store
                .data()
                .fiber_entries
                .get(&next)
                .copied()
                .ok_or_else(|| {
                    wasmtime::Error::msg(format!("fiber {next:#x} has no rewind entry"))
                })?;
            result = call_fiber_entry(store, instance, selected)?;
        }
    }
}

fn call_fiber_entry(
    store: &mut Store<VmState>,
    instance: &Instance,
    entry: FiberEntry,
) -> wasmtime::Result<Option<i32>> {
    store.data_mut().active_fiber_entry = Some(entry);
    let (function, params) = match entry {
        FiberEntry::Start => (
            instance
                .get_func(&mut *store, "_start")
                .ok_or_else(|| wasmtime::Error::msg("missing Emscripten _start export"))?,
            Vec::new(),
        ),
        FiberEntry::Table { index, argument } => {
            let table = instance
                .get_export(&mut *store, "__indirect_function_table")
                .and_then(Extern::into_table)
                .ok_or_else(|| {
                    wasmtime::Error::msg("Emscripten function table export is unavailable")
                })?;
            let function = match table.get(&mut *store, index as u64) {
                Some(Ref::Func(Some(function))) => function,
                _ => {
                    return Err(wasmtime::Error::msg(format!(
                        "Emscripten fiber entry {index} is not a function"
                    )));
                }
            };
            (function, vec![Val::I32(argument)])
        }
    };
    let result_types = function.ty(&*store).results().collect::<Vec<_>>();
    let mut results = result_types
        .iter()
        .map(zero)
        .collect::<wasmtime::Result<Vec<_>>>()?;
    function.call(&mut *store, &params, &mut results)?;
    Ok(results.first().and_then(Val::i32))
}

fn call_i32_export(
    caller: &mut Caller<'_, VmState>,
    name: &str,
    params: &[Val],
) -> wasmtime::Result<i32> {
    let function = caller
        .get_export(name)
        .and_then(Extern::into_func)
        .ok_or_else(|| wasmtime::Error::msg(format!("missing Emscripten export {name}")))?;
    let mut results = [Val::I32(0)];
    function.call(&mut *caller, params, &mut results)?;
    results[0]
        .i32()
        .ok_or_else(|| wasmtime::Error::msg(format!("{name} returned a non-i32 value")))
}

fn call_void_export(
    caller: &mut Caller<'_, VmState>,
    name: &str,
    params: &[Val],
) -> wasmtime::Result<()> {
    let function = caller
        .get_export(name)
        .and_then(Extern::into_func)
        .ok_or_else(|| wasmtime::Error::msg(format!("missing Emscripten export {name}")))?;
    function.call(&mut *caller, params, &mut [])
}

fn write_i32(caller: &mut Caller<'_, VmState>, address: i32, value: i32) -> wasmtime::Result<()> {
    let memory = caller
        .data()
        .qemu_jit
        .memory
        .clone()
        .ok_or_else(|| wasmtime::Error::msg("missing Emscripten memory import"))?;
    write_extern_i32(memory.as_extern(), caller, address, value)
}

fn read_instance_i32(
    store: &mut Store<VmState>,
    instance: &Instance,
    address: i32,
) -> wasmtime::Result<i32> {
    let memory = store
        .data()
        .qemu_jit
        .memory
        .clone()
        .ok_or_else(|| wasmtime::Error::msg("missing Emscripten memory import"))?;
    let _ = instance;
    read_extern_i32(memory.as_extern(), store, address)
}

fn write_instance_i32(
    store: &mut Store<VmState>,
    instance: &Instance,
    address: i32,
    value: i32,
) -> wasmtime::Result<()> {
    let memory = store
        .data()
        .qemu_jit
        .memory
        .clone()
        .ok_or_else(|| wasmtime::Error::msg("missing Emscripten memory import"))?;
    let _ = instance;
    write_extern_i32(memory.as_extern(), store, address, value)
}

fn read_extern_i32<T>(
    memory: Extern,
    store: &mut impl wasmtime::AsContextMut<Data = T>,
    address: i32,
) -> wasmtime::Result<i32> {
    let mut bytes = [0; 4];
    match memory {
        Extern::Memory(memory) => memory.read(store, address as usize, &mut bytes)?,
        Extern::SharedMemory(memory) => read_shared(&memory, address as usize, &mut bytes)?,
        _ => {
            return Err(wasmtime::Error::msg(
                "Emscripten memory export is not a memory",
            ));
        }
    }
    Ok(i32::from_le_bytes(bytes))
}

fn read_extern_u16<T>(
    memory: Extern,
    store: &mut impl wasmtime::AsContextMut<Data = T>,
    address: i32,
) -> wasmtime::Result<u16> {
    let mut bytes = [0; 2];
    read_extern(memory, store, address, &mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_extern_u32<T>(
    memory: Extern,
    store: &mut impl wasmtime::AsContextMut<Data = T>,
    address: i32,
) -> wasmtime::Result<u32> {
    Ok(read_extern_i32(memory, store, address)? as u32)
}

fn read_extern_u64<T>(
    memory: Extern,
    store: &mut impl wasmtime::AsContextMut<Data = T>,
    address: i32,
) -> wasmtime::Result<u64> {
    let mut bytes = [0; 8];
    read_extern(memory, store, address, &mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_extern<T>(
    memory: Extern,
    store: &mut impl wasmtime::AsContextMut<Data = T>,
    address: i32,
    output: &mut [u8],
) -> wasmtime::Result<()> {
    match memory {
        Extern::Memory(memory) => Ok(memory.read(store, address as usize, output)?),
        Extern::SharedMemory(memory) => read_shared(&memory, address as usize, output),
        _ => Err(wasmtime::Error::msg(
            "Emscripten memory export is not a memory",
        )),
    }
}

fn write_extern<T>(
    memory: Extern,
    store: &mut impl wasmtime::AsContextMut<Data = T>,
    address: i32,
    input: &[u8],
) -> wasmtime::Result<()> {
    match memory {
        Extern::Memory(memory) => Ok(memory.write(store, address as usize, input)?),
        Extern::SharedMemory(memory) => write_shared(&memory, address as usize, input),
        _ => Err(wasmtime::Error::msg(
            "Emscripten memory export is not a memory",
        )),
    }
}

fn write_extern_i32<T>(
    memory: Extern,
    store: &mut impl wasmtime::AsContextMut<Data = T>,
    address: i32,
    value: i32,
) -> wasmtime::Result<()> {
    match memory {
        Extern::Memory(memory) => {
            Ok(memory.write(store, address as usize, &value.to_le_bytes())?)
        }
        Extern::SharedMemory(memory) => {
            write_shared(&memory, address as usize, &value.to_le_bytes())
        }
        _ => Err(wasmtime::Error::msg(
            "Emscripten memory export is not a memory",
        )),
    }
}

fn read_shared(
    memory: &wasmtime::SharedMemory,
    address: usize,
    output: &mut [u8],
) -> wasmtime::Result<()> {
    let source = memory
        .data()
        .get(address..address + output.len())
        .ok_or_else(|| wasmtime::Error::msg("Emscripten memory access is out of bounds"))?;
    for (output, source) in output.iter_mut().zip(source) {
        // SAFETY: shared Wasm memory bytes may be read concurrently. Individual
        // byte access cannot tear and fiber metadata is published before use.
        *output = unsafe { source.get().read_volatile() };
    }
    Ok(())
}

fn write_shared(
    memory: &wasmtime::SharedMemory,
    address: usize,
    input: &[u8],
) -> wasmtime::Result<()> {
    let destination = memory
        .data()
        .get(address..address + input.len())
        .ok_or_else(|| wasmtime::Error::msg("Emscripten memory access is out of bounds"))?;
    for (destination, input) in destination.iter().zip(input) {
        // SAFETY: the active fiber owns its metadata while switching and the
        // write completes before the selected fiber is resumed.
        unsafe { destination.get().write_volatile(*input) };
    }
    Ok(())
}

pub(crate) fn add_pthread_create<T, F>(linker: &mut Linker<T>, spawn: F) -> anyhow::Result<()>
where
    T: Send + 'static,
    F: Fn(i32, i32, i32, i32) -> wasmtime::Result<i32> + Send + Sync + 'static,
{
    linker.func_wrap(
        "env",
        "__pthread_create_js",
        move |mut caller: Caller<'_, T>,
              pthread_ptr: i32,
              attr: i32,
              start_routine: i32,
              arg: i32| {
            if let Some(table) = caller
                .get_export("__indirect_function_table")
                .and_then(Extern::into_table)
            {
                eprintln!(
                    "pthread creator table size={} entry6761={}",
                    table.size(&caller),
                    matches!(table.get(&mut caller, 6761), Some(Ref::Func(Some(_))))
                );
            }
            spawn(pthread_ptr, attr, start_routine, arg)
        },
    )?;
    Ok(())
}

fn zero(ty: &ValType) -> wasmtime::Result<Val> {
    Ok(match ty {
        ValType::I32 => Val::I32(0),
        ValType::I64 => Val::I64(0),
        ValType::F32 => Val::F32(0),
        ValType::F64 => Val::F64(0),
        _ => {
            return Err(wasmtime::Error::msg(
                "unsupported Emscripten shim result type",
            ));
        }
    })
}

fn invoke<T>(
    caller: &mut Caller<'_, T>,
    name: &str,
    params: &[Val],
    results: &mut [Val],
) -> anyhow::Result<()> {
    // Emscripten implements setjmp/longjmp by throwing a JavaScript sentinel
    // through an invoke_* wrapper. Reproduce that boundary in the host.
    let saved_stack = caller
        .get_export("stackSave")
        .and_then(Extern::into_func)
        .context("missing Emscripten stack_get_current export")?
        .typed::<(), i32>(&*caller)?
        .call(&mut *caller, ())?;
    let table_index = params
        .first()
        .and_then(Val::i32)
        .context("Emscripten invoke wrapper is missing its table index")?;
    ensure!(
        table_index >= 0,
        "Emscripten invoke table index is negative"
    );
    let table = caller
        .get_export("__indirect_function_table")
        .and_then(Extern::into_table)
        .context("Emscripten function table export is unavailable")?;
    let function = match table.get(&mut *caller, table_index as u64) {
        Some(Ref::Func(Some(function))) => function,
        _ => anyhow::bail!("Emscripten invoke table entry {table_index} is not a function"),
    };
    let result_types = function.ty(&*caller).results().collect::<Vec<_>>();
    match function.call(&mut *caller, &params[1..], results) {
        Ok(()) => Ok(()),
        Err(error) if error.downcast_ref::<EmscriptenLongjmp>().is_some() => {
            caller
                .get_export("stackRestore")
                .and_then(Extern::into_func)
                .context("missing Emscripten stack_restore export")?
                .typed::<i32, ()>(&*caller)?
                .call(&mut *caller, saved_stack)?;
            caller
                .get_export("setThrew")
                .and_then(Extern::into_func)
                .context("missing Emscripten setThrew export")?
                .typed::<(i32, i32), ()>(&*caller)?
                .call(&mut *caller, (1, 0))?;
            for (result, result_type) in results.iter_mut().zip(&result_types) {
                *result = zero(result_type)?;
            }
            Ok(())
        }
        Err(error) => Err(anyhow::anyhow!(
            "calling {name} table entry {table_index}: {error:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::{Engine, Store};

    #[test]
    fn dispatches_invoke_import_through_outer_table() {
        let engine = Engine::default();
        let module = Module::new(
            &engine,
            r#"(module
                (import "env" "invoke_iii" (func $invoke (param i32 i32 i32) (result i32)))
                (table (export "__indirect_function_table") 1 funcref)
                (func $add (param i32 i32) (result i32)
                    local.get 0 local.get 1 i32.add)
                (elem (i32.const 0) $add)
                (func (export "run") (result i32)
                    i32.const 0 i32.const 20 i32.const 22 call $invoke))"#,
        )
        .unwrap();
        let mut linker = Linker::new(&engine);
        add_invoke_wrappers(&mut linker, &module).unwrap();
        let mut store = Store::new(&engine, ());
        let instance = linker.instantiate(&mut store, &module).unwrap();
        let run = instance
            .get_typed_func::<(), i32>(&mut store, "run")
            .unwrap();
        assert_eq!(run.call(&mut store, ()).unwrap(), 42);
    }

    #[test]
    fn links_platform_imports_and_names_unsupported_calls() {
        let engine = Engine::default();
        let module = Module::new(
            &engine,
            r#"(module
                (import "env" "sync_file_range" (func $sync (param i32 i64 i64 i32) (result i32)))
                (import "env" "__pthread_create_js" (func $create (param i32 i32 i32 i32) (result i32)))
                (func (export "sync") (result i32)
                    i32.const 0 i64.const 0 i64.const 0 i32.const 0 call $sync)
                (func (export "create") (result i32)
                    i32.const 0 i32.const 0 i32.const 0 i32.const 0 call $create))"#,
        )
        .unwrap();
        let mut linker = Linker::new(&engine);
        add_platform_shims(&mut linker, &module).unwrap();
        add_pthread_create(&mut linker, |_, _, _, _| {
            Err(wasmtime::Error::msg(
                "unsupported Emscripten runtime import called: env::__pthread_create_js",
            ))
        })
        .unwrap();
        let mut store = Store::new(&engine, ());
        let instance = linker.instantiate(&mut store, &module).unwrap();
        let sync = instance
            .get_typed_func::<(), i32>(&mut store, "sync")
            .unwrap();
        assert_eq!(sync.call(&mut store, ()).unwrap(), 0);
        let create = instance
            .get_typed_func::<(), i32>(&mut store, "create")
            .unwrap();
        let error = create.call(&mut store, ()).unwrap_err();
        assert!(format!("{error:?}").contains("env::__pthread_create_js"));
    }
}
