pub(crate) mod process_support_outer {

    pub(crate) mod v1_0_0 {
        wasmtime::component::bindgen!({
            path: "host-wit-activity/",
            async: true,
            inline: "package any:any;
                world bindings {
                    import obelisk:activity/process-support@1.0.0;
                }",
            world: "any:any/bindings",
            trappable_imports: true,
            with: {
                "obelisk:activity/process-support/child-process": crate::activity::process::HostChildProcess,
                "wasi:io": wasmtime_wasi_io::bindings::wasi::io,
            }
        });
    }
}
