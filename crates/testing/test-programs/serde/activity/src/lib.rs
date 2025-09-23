use exports::testing::serde::serde::{Guest, MyRecord, MyVariant};
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn rec(my_record: MyRecord) -> Result<MyRecord, ()> {
        Ok(my_record)
    }

    fn var(my_variant: MyVariant) -> Result<MyVariant, ()> {
        Ok(my_variant)
    }
}
