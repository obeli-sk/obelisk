use generated::export;
use generated::exports::testing::serde_workflow::serde_workflow::Guest;
use generated::testing::serde::serde::{self, MyError};

mod generated {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn expect_trap() -> Result<(), ()> {
        let err = serde::trap().unwrap_err();
        let MyError::ExecutionFailed = err else {
            panic!("wrong error {err:?}");
        };
        Ok(())
    }

    fn get_stargazers() -> Result<(), ()> {
        serde::get_stargazers().unwrap();
        Ok(())
    }
}
