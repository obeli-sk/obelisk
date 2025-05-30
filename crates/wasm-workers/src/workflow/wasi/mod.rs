mod cli;
mod clocks;
mod filesystem;
mod random;

use super::workflow_ctx::WorkflowCtx;
use crate::WasmFileError;
use concepts::{
    StrVariant,
    storage::{DbConnection, DbPool},
    time::ClockFn,
};
use wasmtime::component::Linker;

wasmtime::component::bindgen!({
    path: "host-wit-workflow-wasi/",
    async: { only_imports: [] },
    world: "any:any/bindings",
    inline: "package any:any;
    world bindings {
        include wasi:cli/imports@0.2.3;
        }",
    trappable_imports: true,
    with: {
        "wasi:io": wasmtime_wasi_io::bindings::wasi::io,
    }
});

pub(crate) fn add_to_linker_async<C: ClockFn, DB: DbConnection, P: DbPool<DB>>(
    linker: &mut Linker<WorkflowCtx<C, DB, P>>,
) -> Result<(), WasmFileError> {
    fn type_annotate<F, C: ClockFn, DB: DbConnection, P: DbPool<DB>>(val: F) -> F
    where
        F: Fn(&mut WorkflowCtx<C, DB, P>) -> &mut WorkflowCtx<C, DB, P>,
    {
        val
    }

    let linking_err = |err: wasmtime::Error| WasmFileError::LinkingError {
        context: StrVariant::Static("linking error"),
        err: err.into(),
    };
    let options = LinkOptions::default();
    let closure = type_annotate(|t| t);
    wasi::clocks::monotonic_clock::add_to_linker_get_host(linker, closure).map_err(linking_err)?;
    wasi::clocks::wall_clock::add_to_linker_get_host(linker, closure).map_err(linking_err)?;
    wasi::cli::environment::add_to_linker_get_host(linker, closure).map_err(linking_err)?;
    wasi::cli::exit::add_to_linker_get_host(linker, &options.into(), closure)
        .map_err(linking_err)?;
    wasi::cli::stdin::add_to_linker_get_host(linker, closure).map_err(linking_err)?;
    wasi::cli::stdout::add_to_linker_get_host(linker, closure).map_err(linking_err)?;
    wasi::cli::stderr::add_to_linker_get_host(linker, closure).map_err(linking_err)?;
    wasi::filesystem::preopens::add_to_linker_get_host(linker, closure).map_err(linking_err)?;
    wasi::filesystem::types::add_to_linker_get_host(linker, closure).map_err(linking_err)?;
    wasi::random::random::add_to_linker_get_host(linker, closure).map_err(linking_err)?;
    wasmtime_wasi_io::add_to_linker_async(linker).map_err(linking_err)?;
    Ok(())
}
