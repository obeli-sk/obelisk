use crate::VmState;
use std::sync::Arc;
use wasmtime::{Caller, Extern, Linker};
use wasmtime_wasi::p2::{OutputStream, pipe::MemoryOutputPipe};

const ERRNO_BADF: i32 = 8;
pub(crate) fn add_to_linker(
    linker: &mut Linker<VmState>,
    stdout: MemoryOutputPipe,
    stderr: MemoryOutputPipe,
    arguments: Arc<[String]>,
) -> anyhow::Result<()> {
    let size_arguments = arguments.clone();
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "args_sizes_get",
        move |mut caller: Caller<'_, VmState>, count: i32, size: i32| {
            let byte_size = size_arguments.iter().try_fold(0_usize, |size, argument| {
                size.checked_add(argument.len() + 1)
            });
            let byte_size = byte_size
                .and_then(|size| i32::try_from(size).ok())
                .ok_or_else(|| wasmtime::Error::msg("legacy WASI argument size overflow"))?;
            write_i32(&mut caller, count, i32::try_from(size_arguments.len())?)?;
            write_i32(&mut caller, size, byte_size)?;
            Ok(0_i32)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "args_get",
        move |mut caller: Caller<'_, VmState>, pointers: i32, bytes: i32| {
            let mut bytes = bytes;
            for (index, argument) in arguments.iter().enumerate() {
                write_i32(
                    &mut caller,
                    pointers
                        .checked_add(i32::try_from(index * 4)?)
                        .ok_or_else(|| wasmtime::Error::msg("legacy WASI argv overflow"))?,
                    bytes,
                )?;
                write_bytes(&mut caller, bytes, argument.as_bytes())?;
                bytes = bytes
                    .checked_add(i32::try_from(argument.len())?)
                    .ok_or_else(|| wasmtime::Error::msg("legacy WASI argv overflow"))?;
                write_bytes(&mut caller, bytes, &[0])?;
                bytes = bytes
                    .checked_add(1)
                    .ok_or_else(|| wasmtime::Error::msg("legacy WASI argv overflow"))?;
            }
            Ok(0_i32)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "environ_sizes_get",
        |mut caller: Caller<'_, VmState>, count: i32, size: i32| -> wasmtime::Result<i32> {
            write_i32(&mut caller, count, 0)?;
            write_i32(&mut caller, size, 0)?;
            Ok(0)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "environ_get",
        |_caller: Caller<'_, VmState>, _pointers: i32, _bytes: i32| -> i32 { 0 },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_write",
        move |mut caller: Caller<'_, VmState>, fd: i32, iovs: i32, count: i32, written: i32| {
            let result = fd_write(&mut caller, fd, iovs, count, &stdout, &stderr)?;
            write_i32(&mut caller, written, result)?;
            Ok(0_i32)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_read",
        |mut caller: Caller<'_, VmState>, fd: i32, _iovs: i32, _count: i32, read: i32| {
            if fd != 0 {
                return Ok(ERRNO_BADF);
            }
            write_i32(&mut caller, read, 0)?;
            Ok(0_i32)
        },
    )?;
    linker.func_wrap("wasi_snapshot_preview1", "fd_close", |_fd: i32| ERRNO_BADF)?;
    linker.func_wrap("wasi_snapshot_preview1", "fd_sync", |_fd: i32| 0_i32)?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_fdstat_get",
        |mut caller: Caller<'_, VmState>, fd: i32, output: i32| {
            if !(0..=2).contains(&fd) {
                return Ok(ERRNO_BADF);
            }
            write_bytes(
                &mut caller,
                output,
                &[
                    2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ],
            )?;
            Ok(0_i32)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_seek",
        |_fd: i32, _offset: i64, _whence: i32, _output: i32| ERRNO_BADF,
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_pread",
        |_fd: i32, _iovs: i32, _count: i32, _offset: i64, _output: i32| ERRNO_BADF,
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_pwrite",
        |_fd: i32, _iovs: i32, _count: i32, _offset: i64, _output: i32| ERRNO_BADF,
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "proc_exit",
        |status: i32| -> wasmtime::Result<()> { Err(wasmtime_wasi::I32Exit(status).into()) },
    )?;
    Ok(())
}

fn fd_write(
    caller: &mut Caller<'_, VmState>,
    fd: i32,
    iovs: i32,
    count: i32,
    stdout: &MemoryOutputPipe,
    stderr: &MemoryOutputPipe,
) -> wasmtime::Result<i32> {
    let output = match fd {
        1 => stdout,
        2 => stderr,
        _ => {
            return Err(wasmtime::Error::msg(format!(
                "legacy WASI fd_write fd={fd}"
            )));
        }
    };
    let mut total = 0_i32;
    for index in 0..count {
        let pointer = read_i32(caller, iovs + index * 8)?;
        let length = read_i32(caller, iovs + index * 8 + 4)?;
        let bytes = read_bytes(caller, pointer, length)?;
        output
            .clone()
            .write(bytes.into())
            .map_err(wasmtime::Error::msg)?;
        total = total
            .checked_add(length)
            .ok_or_else(|| wasmtime::Error::msg("fd_write overflow"))?;
    }
    Ok(total)
}

fn memory(caller: &Caller<'_, VmState>) -> wasmtime::Result<Extern> {
    caller
        .data()
        .qemu_jit
        .memory
        .as_ref()
        .map(|memory| memory.as_extern())
        .ok_or_else(|| wasmtime::Error::msg("missing Emscripten memory import"))
}

fn read_i32(caller: &mut Caller<'_, VmState>, pointer: i32) -> wasmtime::Result<i32> {
    Ok(i32::from_le_bytes(
        read_bytes(caller, pointer, 4)?.try_into().unwrap(),
    ))
}

fn write_i32(caller: &mut Caller<'_, VmState>, pointer: i32, value: i32) -> wasmtime::Result<()> {
    write_bytes(caller, pointer, &value.to_le_bytes())
}

fn read_bytes(
    caller: &mut Caller<'_, VmState>,
    pointer: i32,
    length: i32,
) -> wasmtime::Result<Vec<u8>> {
    let start = usize::try_from(pointer).map_err(wasmtime::Error::msg)?;
    let length = usize::try_from(length).map_err(wasmtime::Error::msg)?;
    match memory(caller)? {
        Extern::Memory(memory) => {
            let mut bytes = vec![0; length];
            memory.read(caller, start, &mut bytes)?;
            Ok(bytes)
        }
        Extern::SharedMemory(memory) => memory
            .data()
            .get(start..start + length)
            .ok_or_else(|| wasmtime::Error::msg("legacy WASI read is out of bounds"))
            .map(|source| {
                source
                    .iter()
                    .map(|cell| unsafe { cell.get().read_volatile() })
                    .collect()
            }),
        _ => Err(wasmtime::Error::msg("invalid Emscripten memory import")),
    }
}

fn write_bytes(
    caller: &mut Caller<'_, VmState>,
    pointer: i32,
    bytes: &[u8],
) -> wasmtime::Result<()> {
    let start = usize::try_from(pointer).map_err(wasmtime::Error::msg)?;
    match memory(caller)? {
        Extern::Memory(memory) => memory.write(caller, start, bytes)?,
        Extern::SharedMemory(memory) => {
            let destination = memory
                .data()
                .get(start..start + bytes.len())
                .ok_or_else(|| wasmtime::Error::msg("legacy WASI write is out of bounds"))?;
            for (destination, source) in destination.iter().zip(bytes) {
                unsafe { destination.get().write_volatile(*source) };
            }
        }
        _ => return Err(wasmtime::Error::msg("invalid Emscripten memory import")),
    }
    Ok(())
}
