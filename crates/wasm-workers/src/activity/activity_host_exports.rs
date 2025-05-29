pub(crate) mod process_support_outer {
    use std::sync::{Arc, Mutex};

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
            }
        });
    }

    #[allow(dead_code)] // TODO: remove after stdio is implemented.
    #[derive(derive_more::Debug)]
    pub struct HostChildProcess {
        pub id: u64,
        pub command_str: String, // For logging/debugging purposes
        #[debug(skip)]
        pub child: Arc<Mutex<std::process::Child>>,
        // Store the handles for piped streams before they are converted and taken.
        #[debug(skip)]
        pub stdin: Option<std::process::ChildStdin>,
        #[debug(skip)]
        pub stdout: Option<std::process::ChildStdout>,
        #[debug(skip)]
        pub stderr: Option<std::process::ChildStderr>,
    }
}
