mod cli;
mod clocks;
mod filesystem;
mod random;
mod sockets;

use super::workflow_ctx::WorkflowCtx;
use crate::WasmFileError;
use concepts::time::ClockFn;
use wasmtime::component::Linker;

wasmtime::component::bindgen!({
    path: "host-wit-workflow-wasi/",
    world: "any:any/bindings",
    inline: "package any:any;
    world bindings {
        include wasi:cli/imports@0.2.3;
        }",
    with: {
        "wasi:io": wasmtime_wasi_io::bindings::wasi::io,
    },
    require_store_data_send: true,
    imports: {
        default: trappable
    },
});

pub(crate) fn add_to_linker_async<C: ClockFn>(
    linker: &mut Linker<WorkflowCtx<C>>,
) -> Result<(), WasmFileError> {
    let linking_err =
        |err: wasmtime::Error| WasmFileError::linking_error("cannot link wasi stubs", err);
    let options = LinkOptions::default();

    wasi::clocks::monotonic_clock::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;
    wasi::clocks::wall_clock::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;
    wasi::cli::environment::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;
    wasi::cli::exit::add_to_linker::<_, WorkflowCtx<C>>(linker, &options.clone().into(), |x| x)
        .map_err(linking_err)?;
    wasi::cli::stdin::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x).map_err(linking_err)?;
    wasi::cli::stdout::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x).map_err(linking_err)?;
    wasi::cli::stderr::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x).map_err(linking_err)?;
    wasi::cli::terminal_input::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;
    wasi::cli::terminal_output::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;

    wasi::cli::terminal_stdin::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;
    wasi::cli::terminal_stdout::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;
    wasi::cli::terminal_stderr::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;

    wasi::filesystem::preopens::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;
    wasi::filesystem::types::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;
    wasi::random::random::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x).map_err(linking_err)?;
    wasi::random::insecure::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;
    wasi::random::insecure_seed::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;

    wasi::sockets::network::add_to_linker::<_, WorkflowCtx<C>>(
        linker,
        &options.clone().into(),
        |x| x,
    )
    .map_err(linking_err)?;

    wasi::sockets::instance_network::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;

    wasi::sockets::udp::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x).map_err(linking_err)?;
    wasi::sockets::udp_create_socket::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;

    wasi::sockets::tcp::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x).map_err(linking_err)?;
    wasi::sockets::tcp_create_socket::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;

    wasi::sockets::ip_name_lookup::add_to_linker::<_, WorkflowCtx<C>>(linker, |x| x)
        .map_err(linking_err)?;

    wasmtime_wasi_io::add_to_linker_async(linker).map_err(linking_err)?;
    Ok(())
}
