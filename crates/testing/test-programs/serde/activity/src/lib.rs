use generated::export;
use generated::exports::testing::serde::serde::{Guest, MyError, MyRecord, MyVariant};

mod generated {
    #![allow(clippy::empty_line_after_outer_attr)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn rec(my_record: MyRecord) -> Result<MyRecord, ()> {
        Ok(my_record)
    }

    fn var(my_variant: MyVariant) -> Result<MyVariant, ()> {
        Ok(my_variant)
    }

    fn trap() -> Result<(), MyError> {
        panic!()
    }
}
