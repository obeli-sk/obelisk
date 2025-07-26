use concepts::StrVariant;
use std::{error::Error, fmt::Debug};
use utils::wasm_tools::{self};

pub mod activity;
mod component_logger;
pub mod engines;
pub mod epoch_ticker;
pub mod preopens_cleaner;
pub mod std_output_stream;
pub mod webhook;
pub mod workflow;

#[derive(thiserror::Error, Debug)]
pub enum WasmFileError {
    #[error("cannot read WASM file: {0}")]
    CannotReadComponent(wasmtime::Error),
    #[error("cannot decode: {0}")]
    DecodeError(#[from] wasm_tools::DecodeError),
    #[error("linking error - {context}, details: {err}")]
    LinkingError {
        context: StrVariant,
        err: Box<dyn Error + Send + Sync>,
    },
}

pub mod envvar {
    #[derive(Clone, derive_more::Debug)]
    pub struct EnvVar {
        pub key: String,
        #[debug(skip)]
        pub val: String,
    }
}

#[cfg(test)]
pub(crate) mod tests {

    mod populate_codegen_cache {
        use crate::{
            activity::activity_worker::tests::compile_activity,
            workflow::workflow_worker::tests::compile_workflow,
        };

        #[rstest::rstest(wasm_path => [
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
            test_programs_dir_activity_builder::TEST_PROGRAMS_DIR_ACTIVITY,
            test_programs_process_activity_builder::TEST_PROGRAMS_PROCESS_ACTIVITY,
            ])]
        #[test]
        fn fibo(wasm_path: &str) {
            compile_activity(wasm_path);
        }

        #[rstest::rstest(wasm_path => [
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            ])]
        #[test]
        fn workflow(wasm_path: &str) {
            compile_workflow(wasm_path);
        }

        #[rstest::rstest(wasm_path => [
            test_programs_fibo_webhook_builder::TEST_PROGRAMS_FIBO_WEBHOOK
            ])]
        #[test]
        fn webhook(wasm_path: &str) {
            crate::webhook::webhook_trigger::tests::nosim::compile_webhook(wasm_path);
        }
    }
}
