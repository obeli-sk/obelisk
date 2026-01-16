use generated::export;
use generated::exports::testing::stub_activity::activity::Guest;

mod generated {
    #![allow(clippy::empty_line_after_outer_attr)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn foo(_arg: String) -> Result<String, ()> {
        unimplemented!("actual implementation is never used")
    }

    fn noret() -> Result<(), ()> {
        unimplemented!("actual implementation is never used")
    }
}
