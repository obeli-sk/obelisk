use exports::testing::dir::dir::Guest;
use std::path::PathBuf;
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn io() -> Result<(), String> {
        let entries = ls();
        // Let's put some content idempotently
        let tempfile = PathBuf::from("test.txt");
        std::fs::write(tempfile, b"Lorem ipsum").expect("writing to a file failed");
        std::fs::create_dir_all("foo/bar/baz").expect("create_dir_all failed");

        if entries.is_empty() {
            // Should be retried and succeed..
            return Err(
                "failing on first try, should be automatically fixed on the first retry if `reuse-on-retry` is enabled"
                    .to_string(),
            );
        }

        let entries = ls();
        if entries != vec!["./foo", "./test.txt"] {
            return Err(format!("unexpected file list: {entries:?}"));
        }

        Ok(())
    }
}

fn ls() -> Vec<String> {
    let mut entries = std::fs::read_dir(".")
        .expect("read_dir failed")
        .map(|res| res.map(|e| e.path().to_string_lossy().into_owned()))
        .collect::<Result<Vec<_>, _>>()
        .expect("iterating over folder failed");
    entries.sort();
    println!("ls:");
    println!("{entries:?}");
    entries
}
