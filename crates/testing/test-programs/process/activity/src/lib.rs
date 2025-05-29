use exports::testing::process::process::Guest;
use obelisk::activity::process_support::{SpawnOptions, Stdio};
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn spawn() -> Result<(), String> {
        // Idempotently create a file so that the activity works with `reuse-on-retry`.
        let proc = obelisk::activity::process_support::spawn(
            "touch",
            &SpawnOptions {
                args: vec!["touched".to_string()],
                environment: vec![],
                current_working_directory: None,
                stdin: Stdio::Discard,
                stdout: Stdio::Discard,
                stderr: Stdio::Discard,
            },
        )
        .map_err(|err| format!("got spawn error: {err}"))?;
        println!("Waiting for {}", proc.id());
        let exit_code = proc
            .wait()
            .map_err(|err| format!("got wait error: {err}"))?;

        println!("Successfuly launched process, exited with {exit_code:?}");
        let entries = ls();
        if entries != vec!["./touched"] {
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
