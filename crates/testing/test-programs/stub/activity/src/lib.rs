use exports::testing::stub_activity::activity::Guest;
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn foo(_arg: String) -> String {
        unimplemented!("actual implementation is never used")
    }

    fn noret() {
        unimplemented!("actual implementation is never used")
    }
}
