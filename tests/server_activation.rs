//! The active deployment records the app policy digest of the last successful activation.
//! Only activations may move it: verification and failed startups leave it untouched.
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
            "app_name = 'activation-test'\n",
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
                "activation-test-api-token-0000000000000000",
            )
            .env_remove("OBELISK_APP_NAME");
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
        // The listener is bound before activation commits; only serving follows it.
        while !responds(self.port) {
            assert!(Instant::now() < deadline, "server did not start");
            std::thread::sleep(Duration::from_millis(50));
        }
        server
    }

    /// The active deployment and the app config digest it was activated under.
    fn active(&self) -> (String, String) {
        rusqlite::Connection::open(self.path("db/obelisk.sqlite"))
            .unwrap()
            .query_row(
                "SELECT deployment_id, last_active_app_config_digest FROM t_deployment WHERE status = 'active'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
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
                "[[activity_stub]]\nname = 'stub'\nffqn = 'testing:activation/stubs.{}'\nparams = []\nreturn_type = 'result<string, string>'\n",
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

fn responds(port: u16) -> bool {
    use std::io::{Read, Write};
    let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut byte = [0];
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .is_ok()
        && matches!(stream.read(&mut byte), Ok(1))
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
fn active_moves_only_on_activation() {
    let fixture = Fixture::new();
    let server_toml = fixture.path("server.toml");
    let server = fixture.server(&server_toml, &["--empty-deployment"]);
    let (_, digest) = fixture.active();
    fixture.submit(HOT);
    fixture.submit(QUEUED);

    // Hot activation moves the active deployment.
    assert_success(&fixture.run(&["deployment", "apply", HOT]));
    let committed = fixture.active();
    assert_eq!(committed, (HOT.to_owned(), digest));
    assert_success(&fixture.run(&["deployment", "enqueue", QUEUED]));
    assert_eq!(fixture.active(), committed);

    // Verify under another policy never activates.
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
    assert_eq!(fixture.active(), committed);
    server.stop();

    // A startup failing after compilation, at the API bind, never activates.
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
    assert_eq!(fixture.active(), committed);
    assert_eq!(fixture.status(QUEUED), "enqueued");

    // Restarting on an explicit deployment ID keeps the unrelated enqueue.
    let server = fixture.server(&server_toml, &["--deployment-id", HOT]);
    assert_eq!(fixture.active(), committed);
    assert_eq!(fixture.status(HOT), "active");
    assert_eq!(fixture.status(QUEUED), "enqueued");
    server.stop();
}

// Supervisors name the empty deployment up front, so its ID is known before the first start.
#[test]
fn empty_deployment_under_selected_id() {
    const EMPTY: &str = "Dep_00000000000000000000000001";
    const OTHER: &str = "Dep_00000000000000000000000004";
    let fixture = Fixture::new();
    let server_toml = fixture.path("server.toml");
    let args = ["--empty-deployment", "--deployment-id", EMPTY];
    let server = fixture.server(&server_toml, &args);
    assert_eq!(fixture.active().0, EMPTY);
    assert_eq!(fixture.status(EMPTY), "active");
    fixture.submit(HOT);
    server.stop();

    // Reused when stored and empty.
    fixture.server(&server_toml, &args).stop();
    assert_eq!(fixture.status(EMPTY), "active");

    let refused = |deployment_id: &str, expected: &str| {
        let output = fixture.run(&[
            "server",
            "run",
            "-s",
            "server.toml",
            "-a",
            "app.toml",
            "--empty-deployment",
            "--deployment-id",
            deployment_id,
        ]);
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected), "{stderr}");
    };
    refused(HOT, &format!("deployment {HOT} is not empty"));
    refused(
        OTHER,
        &format!("the empty deployment is already stored as {EMPTY}, not {OTHER}"),
    );
}

// Freeing space takes a running server, so a full database must not stop it from starting.
#[test]
fn server_starts_on_full_database() {
    let fixture = Fixture::new();
    let server_toml = fixture.path("server.toml");
    fixture.server(&server_toml, &["--empty-deployment"]).stop();

    // A limit below the current page count is clamped to it, so no new page can be allocated.
    let policy = (0..200).fold(
        "app_name = 'activation-test'\n[public_env]\n".to_owned(),
        |acc, i| acc + &format!("VAR_{i} = {{ optional = true }}\n"),
    );
    std::fs::write(fixture.path("app.toml"), policy).unwrap();
    fixture.server_toml("server.toml", "V2_", fixture.port);
    let config = std::fs::read_to_string(&server_toml).unwrap()
        + "database.sqlite.pragma = { max_page_count = '1' }\n";
    std::fs::write(&server_toml, config).unwrap();
    let server = fixture.server(&server_toml, &["--empty-deployment"]);

    let file = "full.toml";
    std::fs::write(
        fixture.path(file),
        "[[activity_stub]]\nname = 'stub'\nffqn = 'testing:activation/stubs.full'\nparams = []\nreturn_type = 'result<string, string>'\n",
    )
    .unwrap();
    let output = fixture.run(&["deployment", "submit", file]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("database is full"), "{stderr}");
    server.stop();
}

// An ID conflict is a `409` with generic error JSON, not a package rejection.
#[test]
fn submit_reports_deployment_id_conflict() {
    let fixture = Fixture::new();
    let server_toml = fixture.path("server.toml");
    let server = fixture.server(&server_toml, &["--empty-deployment"]);
    fixture.submit(HOT);
    // Same ID as `HOT`, different content.
    fixture.submit(QUEUED);
    let output = fixture.run(&[
        "deployment",
        "submit",
        &format!("{QUEUED}.toml"),
        "--deployment-id",
        HOT,
    ]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&format!(
            "deployment {HOT} already exists with a different content digest"
        )),
        "{stderr}"
    );
    server.stop();
}

// A cloud app push submits before the app.toml that opens `external` is applied.
#[test]
fn submit_defers_closed_http_server_to_activation() {
    const WEBHOOK: &str = "Dep_00000000000000000000000005";
    let fixture = Fixture::new();
    let server_toml = fixture.path("server.toml");
    let config = std::fs::read_to_string(&server_toml).unwrap().replace(
        "http_server.external.enabled = false",
        &format!(
            "http_server.external.listening_addr = '127.0.0.1:{}'",
            free_port()
        ),
    );
    std::fs::write(&server_toml, config).unwrap();
    let server = fixture.server(&server_toml, &["--empty-deployment"]);
    let webhook = |file: &str, http_server: &str| {
        std::fs::write(
            fixture.path(file),
            format!("[[webhook_endpoint_js]]\nname = 'hello'\nhttp_server = '{http_server}'\nroutes = ['/hello']\ncontent = 'export default function handle() {{ return new Response(\"hello\"); }}'\n"),
        )
        .unwrap();
    };
    let failure = |args: &[&str], expected: &str| {
        let output = fixture.run(args);
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected), "{stderr}");
    };
    let closed = "No started `http_server` found for some `webhook` configurations";
    webhook("external.toml", "external");
    let submit = [
        "deployment",
        "submit",
        "external.toml",
        "--deployment-id",
        WEBHOOK,
    ];
    failure(&submit, closed);
    let mut deferred = submit.to_vec();
    deferred.push("--allow-unavailable-runtime-config");
    assert_success(&fixture.run(&deferred));
    // Activation under the current app.toml, which does not open `external`, still fails.
    failure(&["deployment", "apply", WEBHOOK], closed);
    // An unknown server is not deferred.
    webhook("unknown.toml", "unknown");
    failure(
        &[
            "deployment",
            "submit",
            "unknown.toml",
            "--allow-unavailable-runtime-config",
        ],
        closed,
    );
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
        "app_name = 'activation-test'\n[public_env]\nINTERNAL_URL = { type = 'internal', source = 'http-server-url', http_server = 'internal' }\n",
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
            std::fs::write(fixture.path("app.toml"), "app_name = 'activation-test'\n[public_env]\nINTERNAL_URL = { type = 'internal', source = 'http-server-url', http_server = 'internal' }\n[secrets]\nINTERNAL_TOKEN = { type = 'internal' }\n").unwrap();
        }
        let internal_port = free_port();
        let config_path = fixture.path("server.toml");
        let config = std::fs::read_to_string(&config_path).unwrap().replace(
            "http_server.internal.enabled = false",
            &format!("http_server.internal.listening_addr = '127.0.0.1:{internal_port}'\nhttp_server.internal.api_auth = {internal_api_auth}"),
        );
        std::fs::write(config_path, config).unwrap();
        let stderr_path = fixture.path("stderr.log");
        let mut command =
            fixture.command(&["server", "run", "--empty-deployment", "-s", "server.toml"]);
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
                .bearer_auth("activation-test-api-token-0000000000000000")
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
