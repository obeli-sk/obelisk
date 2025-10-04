use crate::exports::testing::serde_workflow::serde_workflow::Guest;
use testing::serde::serde::{self, MyError};
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn expect_trap() -> Result<(), ()> {
        let err = serde::trap().unwrap_err();
        let MyError::ExecutionFailed = err else {
            panic!("wrong error {err:?}");
        };
        Ok(())
    }
}
