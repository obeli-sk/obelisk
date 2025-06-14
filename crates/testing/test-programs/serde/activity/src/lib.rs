use exports::testing::serde::serde::{Guest, MyRecord, MyVariant};
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn rec(my_record: MyRecord) -> MyRecord {
        my_record
    }

    fn var(my_variant: MyVariant) -> MyVariant {
        my_variant
    }
}
