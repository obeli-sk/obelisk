use std::path::PathBuf;

use exports::testing::dir::dir::Guest;
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn io() -> Result<(), String> {
        let mut entries = std::fs::read_dir(".")
            .expect("read_dir failed")
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()
            .expect("iterating over folder failed");
        entries.sort();
        println!("{entries:?}");

        let tempfile = PathBuf::from("test.txt");
        std::fs::write(tempfile, b"Lorem ipsum").expect("writing to a file failed");
        std::fs::create_dir_all("foo/bar/baz").expect("create_dir_all failed");
        if entries.is_empty() {
            Err("error on first try".to_string())
        } else {
            Ok(())
        }
    }
}
