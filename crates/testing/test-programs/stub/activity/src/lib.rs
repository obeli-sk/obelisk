use exports::testing::stub_activity::activity::Guest;
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn foo(_arg: String) -> String {
        todo!()
    }

    fn noret() {}
}
