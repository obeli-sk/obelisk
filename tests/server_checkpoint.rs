//! The app checkpoint records the deployment and app policy of the last successful activation.
//! Supervisors restart from it after a failed rollout, so only activations may move it.
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::time::{Duration, Instant};

const HOT: &str = "Dep_00000000000000000000000002";
const QUEUED: &str = "Dep_00000000000000000000000003";

struct Fixture {
    dir: tempfile::TempDir,
    api_url: String,
    port: u16,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("app.toml"),
            "app_name = 'checkpoint-test'\n",
        )
        .unwrap();
        let port = free_port();
        let fixture = Self {
            dir,
            api_url: format!("http://127.0.0.1:{port}"),
            port,
        };
        fixture.server_toml("server.toml", "V1_", port);
        fixture
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn server_toml(&self, name: &str, app_env_prefix: &str, port: u16) -> PathBuf {
        let path = self.path(name);
        std::fs::write(
            &path,
            format!(
                r#"app_env_prefix = "{app_env_prefix}"
api.listening_addr = "127.0.0.1:{port}"
webui.enabled = false
http_server.external.enabled = false
http_server.internal.enabled = false
database.sqlite.directory = "{db}"
wasm.cache_directory = "{wasm}"
wasm.codegen_cache.directory = "{codegen}"
"#,
                db = self.path("db").display(),
                wasm = self.path("wasm").display(),
                codegen = self.path("codegen").display(),
            ),
        )
        .unwrap();
        path
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_obelisk"));
        command
            .args(args)
            .current_dir(self.dir.path())
            .env("OBELISK_API_URL", &self.api_url)
            .env("OBELISK_JS_RUNTIME", "v8")
            .env(
                "OBELISK_API_TOKEN",
                "checkpoint-test-api-token-0000000000000000",
            )
            .env_remove("OBELISK_APP_NAME")
            .env_remove("OBELISK_UNSTABLE_ACTIVITY_VM");
        command
    }

    fn run(&self, args: &[&str]) -> Output {
        self.command(args).output().unwrap()
    }

    fn server(&self, server_toml: &Path, extra: &[&str]) -> Server {
        let server_toml = server_toml.to_str().unwrap();
        let mut args = vec![
            "server",
            "run",
            "--dev",
            "-s",
            server_toml,
            "-a",
            "app.toml",
        ];
        args.extend(extra);
        let server = Server(self.command(&args).spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(60);
        // Reading the checkpoint earlier would race the server's migrations.
        while std::net::TcpStream::connect(("127.0.0.1", self.port)).is_err() {
            assert!(Instant::now() < deadline, "server did not start");
            std::thread::sleep(Duration::from_millis(50));
        }
        server
    }

    fn checkpoint(&self) -> Option<serde_json::Value> {
        let output = self.run(&["server", "checkpoint", "-s", "server.toml"]);
        if !output.status.success() {
            return None;
        }
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        (!value.is_null()).then_some(value)
    }

    fn status(&self, deployment_id: &str) -> String {
        rusqlite::Connection::open(self.path("db/obelisk.sqlite"))
            .unwrap()
            .query_row(
                "SELECT status FROM t_deployment WHERE deployment_id = ?1",
                [deployment_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    /// Submit a distinct deployment; identical content is stored once under one ID.
    fn submit(&self, deployment_id: &str) {
        let file = format!("{deployment_id}.toml");
        std::fs::write(
            self.path(&file),
            format!(
                "[[activity_stub]]\nname = 'stub'\nffqn = 'testing:checkpoint/stubs.{}'\nparams = []\nreturn_type = 'result<string, string>'\n",
                deployment_id.to_lowercase().replace('_', "-")
            ),
        )
        .unwrap();
        assert_success(&self.run(&[
            "deployment",
            "submit",
            &file,
            "--deployment-id",
            deployment_id,
        ]));
    }
}

struct Server(Child);

impl Server {
    fn stop(mut self) {
        let pid = self.0.id().to_string();
        assert!(
            Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .unwrap()
                .success()
        );
        assert!(self.0.wait().unwrap().success());
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn checkpoint_moves_only_on_activation() {
    let fixture = Fixture::new();
    let server_toml = fixture.path("server.toml");
    let server = fixture.server(&server_toml, &["--empty"]);
    let initial = fixture.checkpoint().unwrap();
    assert_eq!(initial["app_env_prefix"], "V1_");
    assert!(
        initial["app_toml"]
            .as_str()
            .unwrap()
            .contains("checkpoint-test")
    );
    fixture.submit(HOT);
    fixture.submit(QUEUED);

    // Hot activation commits the checkpoint.
    assert_success(&fixture.run(&["deployment", "apply", HOT]));
    let committed = fixture.checkpoint().unwrap();
    assert_eq!(committed["deployment_id"], HOT);
    assert_success(&fixture.run(&["deployment", "enqueue", QUEUED]));
    assert_eq!(fixture.checkpoint().unwrap(), committed);

    // Verify under another policy never commits.
    let other_policy = fixture.server_toml("other.toml", "V2_", fixture.port);
    let other_policy = other_policy.to_str().unwrap();
    assert_success(&fixture.run(&[
        "server",
        "verify",
        "-s",
        other_policy,
        "-a",
        "app.toml",
        "--deployment-id",
        QUEUED,
    ]));
    assert_eq!(fixture.checkpoint().unwrap(), committed);
    server.stop();

    // A startup failing after compilation, at the API bind, never commits.
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let blocked = fixture.server_toml("blocked.toml", "V2_", occupied.local_addr().unwrap().port());
    let failed = fixture.run(&[
        "server",
        "run",
        "--dev",
        "-s",
        blocked.to_str().unwrap(),
        "-a",
        "app.toml",
        "--deployment-id",
        QUEUED,
    ]);
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("cannot bind"));
    drop(occupied);
    assert_eq!(fixture.checkpoint().unwrap(), committed);
    assert_eq!(fixture.status(QUEUED), "enqueued");

    // Restarting on an explicit deployment ID keeps the unrelated enqueue.
    let server = fixture.server(&server_toml, &["--deployment-id", HOT]);
    assert_eq!(fixture.checkpoint().unwrap(), committed);
    assert_eq!(fixture.status(HOT), "active");
    assert_eq!(fixture.status(QUEUED), "enqueued");
    server.stop();
}

#[test]
fn verify_dev_resolves_internal_http_server_url() {
    let fixture = Fixture::new();
    let config_path = fixture.path("server.toml");
    let config = std::fs::read_to_string(&config_path).unwrap().replace(
        "http_server.internal.enabled = false",
        "http_server.internal.enabled = true",
    );
    std::fs::write(config_path, config).unwrap();
    std::fs::write(
        fixture.path("app.toml"),
        "app_name = 'checkpoint-test'\n[public_env]\nINTERNAL_URL = { type = 'internal', source = 'http-server-url', http_server = 'internal' }\n",
    )
    .unwrap();
    std::fs::write(fixture.path("deployment.toml"), "").unwrap();
    let args = [
        "server",
        "verify",
        "-s",
        "server.toml",
        "--deployment",
        "deployment.toml",
        "--skip-db",
    ];
    let output = fixture.run(&args);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("http-server-url requires an enabled `internal` http_server"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = fixture.command(&args).arg("--dev").output().unwrap();
    assert_success(&output);
}

#[tokio::test]
async fn dev_controls_token_disclosure_and_internal_without_disabling_api_auth() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();
    for (dev, internal_api_auth) in [(false, false), (true, false), (true, true)] {
        let fixture = Fixture::new();
        if dev {
            std::fs::write(fixture.path("app.toml"), "app_name = 'checkpoint-test'\n[public_env]\nINTERNAL_URL = { type = 'internal', source = 'http-server-url', http_server = 'internal' }\n[secrets]\nINTERNAL_TOKEN = { type = 'internal' }\n").unwrap();
        }
        let internal_port = free_port();
        let config_path = fixture.path("server.toml");
        let config = std::fs::read_to_string(&config_path).unwrap().replace(
            "http_server.internal.enabled = false",
            &format!("http_server.internal.listening_addr = '127.0.0.1:{internal_port}'\nhttp_server.internal.api_auth = {internal_api_auth}"),
        );
        std::fs::write(config_path, config).unwrap();
        let stderr_path = fixture.path("stderr.log");
        let mut command = fixture.command(&["server", "run", "--empty", "-s", "server.toml"]);
        if dev {
            command.arg("--dev");
        }
        let mut child = command
            .stderr(std::fs::File::create(&stderr_path).unwrap())
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        while std::net::TcpStream::connect(("127.0.0.1", fixture.port)).is_err() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "server exited: {}",
                std::fs::read_to_string(&stderr_path).unwrap()
            );
            assert!(Instant::now() < deadline, "server did not start");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let server = Server(child);
        let client = reqwest::Client::new();
        let url = format!("{}/v1/functions", fixture.api_url);
        assert_eq!(
            client.get(&url).send().await.unwrap().status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            client
                .get(&url)
                .bearer_auth("wrong")
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        assert!(
            client
                .get(&url)
                .bearer_auth("checkpoint-test-api-token-0000000000000000")
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        let stderr = std::fs::read_to_string(&stderr_path).unwrap();
        let startup_token = stderr
            .lines()
            .find_map(|line| line.strip_prefix("API startup token: "));
        assert_eq!(startup_token.is_some(), dev, "{stderr}");
        assert!(!stderr.contains("This server's startup token:"));
        if let Some(token) = startup_token {
            assert!(
                client
                    .get(&url)
                    .bearer_auth(token)
                    .send()
                    .await
                    .unwrap()
                    .status()
                    .is_success()
            );
        }
        if dev {
            let response = client
                .get(format!("http://127.0.0.1:{internal_port}/"))
                .send()
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                if internal_api_auth {
                    reqwest::StatusCode::UNAUTHORIZED
                } else {
                    reqwest::StatusCode::NOT_FOUND
                }
            );
        } else {
            assert!(std::net::TcpStream::connect(("127.0.0.1", internal_port)).is_err());
            assert!(stderr.contains("use `--dev`"), "{stderr}");
        }
        server.stop();
    }
    let fixture = Fixture::new();
    let output = fixture.run(&["server", "run", "--no-auth"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected argument '--no-auth'"));
}
