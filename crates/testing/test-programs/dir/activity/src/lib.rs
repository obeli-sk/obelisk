use generated::export;
use generated::exports::testing::dir::dir::Guest;
use std::path::PathBuf;

mod generated {
    #![allow(clippy::empty_line_after_outer_attr)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

struct Component;
export!(Component with_types_in generated);

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
                "failing on first try, should be automatically fixed on the first retry if `reuse_on_retry` is enabled"
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
