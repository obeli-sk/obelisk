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
                "obelisk:activity/process-support/child-process": super::HostChildProcess,
                "wasi:io": wasmtime_wasi_io::bindings::wasi::io,
            }
        });
    }

    #[derive(derive_more::Debug)]
    pub struct HostChildProcess {
        pub id: u64,
        #[expect(dead_code)]
        pub command_str: String, // For logging/debugging purposes
        #[debug(skip)]
        pub child: tokio::process::Child,
        // Store the handles for piped streams before they are converted and taken.
        #[debug(skip)]
        pub stdin: Option<tokio::process::ChildStdin>,
        #[debug(skip)]
        pub stdout: Option<tokio::process::ChildStdout>,
        #[debug(skip)]
        pub stderr: Option<tokio::process::ChildStderr>,
    }
}
