//! `obelisk cloud auth`: browser login and the local credentials file.

use super::{
    Client, DEFAULT_API_URL, DEFAULT_APPS_DOMAIN, Kind, api_origin, human_output, print_json,
    terminal,
};
use crate::args::{CloudArgs, CloudAuth};
use anyhow::{Context as _, bail, ensure};
use reqwest::{Method, Url};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::{
    fmt::Write as _,
    io::{ErrorKind, Write as _},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

const CALLBACK_TIMEOUT: Duration = Duration::from_secs(600);
const CALLBACK_PAGE: &str = "<!doctype html><title>Obelisk Cloud</title><p>Login complete. You can close this tab and return to the terminal.</p>";

#[derive(Default, Serialize, Deserialize)]
struct Store {
    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<String>,
    #[serde(default, rename = "endpoint")]
    endpoints: Vec<Endpoint>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Endpoint {
    api_url: String,
    apps_domain: String,
    api_token: String,
    token_id: String,
    token_name: String,
    expires_at: i64,
}
impl Endpoint {
    fn expired(&self) -> bool {
        self.expires_at <= chrono::Utc::now().timestamp()
    }
    fn expiry(&self) -> String {
        chrono::DateTime::from_timestamp(self.expires_at, 0)
            .map_or_else(|| self.expires_at.to_string(), |date| date.to_rfc3339())
    }
}

fn normalized(api_url: &str) -> &str {
    api_url.trim_end_matches('/')
}

fn store_path() -> Option<PathBuf> {
    crate::project_dirs().map(|dirs| dirs.config_dir().join("cloud-credentials.toml"))
}

impl Store {
    fn load(path: &Path) -> anyhow::Result<Self> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Self::default()),
            Err(err) => {
                return Err(err).with_context(|| format!("Cannot read {}", path.display()));
            }
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let shared = std::fs::metadata(path)?.permissions().mode() & 0o077;
            ensure!(
                shared == 0,
                "{} is readable by other users; run `chmod 600` on it and revoke its tokens if they may have been exposed",
                path.display()
            );
        }
        toml::from_str(&text).with_context(|| format!("Cannot parse {}", path.display()))
    }
    fn save(&self, path: &Path) -> anyhow::Result<()> {
        let directory = path.parent().context("Credentials file has no directory")?;
        std::fs::create_dir_all(directory)?;
        // The temporary file is private from the start, and renaming it never exposes a partial file.
        let mut file = tempfile::NamedTempFile::new_in(directory)?;
        file.write_all(toml::to_string(self)?.as_bytes())?;
        file.persist(path)
            .map_err(|err| err.error)
            .with_context(|| format!("Cannot write {}", path.display()))?;
        Ok(())
    }
    fn by_url(&self, api_url: &str) -> Option<&Endpoint> {
        self.endpoints
            .iter()
            .find(|entry| normalized(&entry.api_url) == normalized(api_url))
    }
    fn by_domain(&self, domain: &str) -> Option<&Endpoint> {
        self.endpoints
            .iter()
            .find(|entry| entry.apps_domain.eq_ignore_ascii_case(domain))
    }
    /// An explicit URL selects by URL, an explicit domain by domain, and neither the default.
    fn select(&self, api_url: Option<&str>, domain: Option<&str>) -> Option<&Endpoint> {
        match (api_url, domain) {
            (Some(api_url), _) => self.by_url(api_url),
            (None, Some(domain)) => self.by_domain(domain),
            (None, None) => self.by_url(self.default.as_deref()?),
        }
    }
    /// Stores the login, returning the entry it replaces. The first login becomes the default.
    fn insert(&mut self, entry: Endpoint) -> Option<Endpoint> {
        let previous = self
            .endpoints
            .iter()
            .position(|old| normalized(&old.api_url) == normalized(&entry.api_url))
            .map(|index| self.endpoints.remove(index));
        if self.endpoints.is_empty() && previous.is_none() {
            self.default = Some(entry.api_url.clone());
        }
        self.endpoints.push(entry);
        previous
    }
    /// Drops the login; a removed default passes to the first remaining entry.
    fn remove(&mut self, api_url: &str) {
        self.endpoints
            .retain(|entry| normalized(&entry.api_url) != normalized(api_url));
        if self
            .default
            .as_deref()
            .is_some_and(|default| normalized(default) == normalized(api_url))
        {
            self.default = self.endpoints.first().map(|entry| entry.api_url.clone());
        }
    }
}

/// Fills what `obelisk cloud` was not told explicitly from the credentials file.
/// An explicit token ignores the file, so it never goes to an endpoint the file makes default.
pub(super) fn resolve(args: &mut CloudArgs, token_required: bool) -> anyhow::Result<()> {
    if args.api_token.is_some() {
        return Ok(());
    }
    if let Some(path) = store_path() {
        let store = Store::load(&path)?;
        if let Some(entry) = store.select(args.api_url.as_deref(), args.apps_domain.as_deref()) {
            args.api_url.get_or_insert_with(|| entry.api_url.clone());
            args.apps_domain
                .get_or_insert_with(|| entry.apps_domain.clone());
            ensure!(
                !token_required || !entry.expired(),
                "Login to {} expired on {}; run `obelisk cloud auth login`",
                entry.api_url,
                entry.expiry()
            );
            args.api_token = Some(SecretString::from(entry.api_token.clone()));
        }
    }
    ensure!(
        !token_required || args.api_token.is_some(),
        "Not logged in to {}; run `obelisk cloud auth login` or set OBELISK_CLOUD_API_TOKEN",
        args.api_url.as_deref().unwrap_or(DEFAULT_API_URL)
    );
    Ok(())
}

pub(super) async fn run(args: &CloudArgs, command: &CloudAuth) -> anyhow::Result<()> {
    let path = store_path().context("Cannot determine the configuration directory")?;
    let value = match command {
        CloudAuth::Login { no_browser } => login(args, &path, *no_browser).await?,
        CloudAuth::Status => status(args, &path).await?,
        CloudAuth::Switch { endpoint } => switch(&path, endpoint)?,
        CloudAuth::Logout => logout(args, &path).await?,
    };
    if args.json {
        print_json(&value)
    } else {
        if value["endpoints"].as_array().is_some_and(Vec::is_empty) {
            eprintln!("Not logged in; run `obelisk cloud auth login`.");
        } else {
            eprint!("{}", human_output(&value));
        }
        Ok(())
    }
}

fn random_hex() -> String {
    hex(&rand::random::<[u8; 32]>())
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}
fn is_code(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

fn hostname() -> String {
    let raw = std::fs::read_to_string("/etc/hostname")
        .ok()
        .filter(|name| !name.trim().is_empty())
        .or_else(|| {
            let output = std::process::Command::new("hostname").output().ok()?;
            String::from_utf8(output.stdout).ok()
        })
        .unwrap_or_default();
    sanitize_hostname(&raw)
}
/// The token name and the consent page accept only this alphabet.
fn sanitize_hostname(raw: &str) -> String {
    let name: String = raw
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect();
    let name = name.trim_start_matches(['.', '_', '-']);
    let name = &name[..name.len().min(64)];
    if name.is_empty() {
        "unknown".to_owned()
    } else {
        name.to_owned()
    }
}

/// The dashboard shares the host with the API, which lives under `/api`.
fn login_page(
    api_url: &str,
    challenge: &str,
    state: &str,
    host: &str,
    port: Option<u16>,
) -> anyhow::Result<Url> {
    let mut url = api_origin(api_url)?;
    let path = url.path().trim_end_matches('/');
    let path = format!("{}/cli-login", path.strip_suffix("/api").unwrap_or(path));
    url.set_path(&path);
    {
        let mut query = url.query_pairs_mut();
        query
            .append_pair("challenge", challenge)
            .append_pair("state", state)
            .append_pair("host", host);
        if let Some(port) = port {
            query.append_pair("port", &port.to_string());
        }
    }
    Ok(url)
}

/// Only a graphical session can show the page on this machine; elsewhere the code is pasted back.
fn open_browser(url: &Url) -> bool {
    let (program, prefix): (&str, &[&str]) = if cfg!(target_os = "macos") {
        ("open", &[])
    } else if cfg!(windows) {
        ("rundll32", &["url.dll,FileProtocolHandler"])
    } else if std::env::var_os("DISPLAY").is_some() || std::env::var_os("WAYLAND_DISPLAY").is_some()
    {
        ("xdg-open", &[])
    } else {
        return false;
    };
    std::process::Command::new(program)
        .args(prefix)
        .arg(url.as_str())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map(|mut child| {
            std::thread::spawn(move || child.wait());
        })
        .is_ok()
}

// Serves one connection; browsers may open idle ones, so each runs on its own task.
async fn callback_connection(
    mut stream: TcpStream,
    state: String,
    codes: mpsc::Sender<String>,
) -> anyhow::Result<()> {
    let mut request = Vec::new();
    let mut buffer = [0; 1024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut buffer).await?;
        ensure!(read > 0 && request.len() < 16 * 1024, "Invalid request");
        request.extend_from_slice(&buffer[..read]);
    }
    let code = callback_code(&String::from_utf8_lossy(&request), &state);
    let response = if code.is_some() {
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/html; charset=utf-8\r\nreferrer-policy: no-referrer\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{CALLBACK_PAGE}",
            CALLBACK_PAGE.len()
        )
    } else {
        "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_owned()
    };
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await?;
    if let Some(code) = code {
        let _ = codes.send(code).await;
    }
    Ok(())
}

// Anything without the right state is ignored, so a page probing local ports cannot end the login.
fn callback_code(request: &str, state: &str) -> Option<String> {
    let target = request.strip_prefix("GET ")?.split(' ').next()?;
    let url = Url::parse(&format!("http://127.0.0.1{target}")).ok()?;
    if url.path() != "/callback" {
        return None;
    }
    let value = |name| {
        url.query_pairs()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.into_owned())
    };
    (value("state")? == state)
        .then(|| value("code"))
        .flatten()
        .filter(|code| is_code(code))
}

async fn wait_for_callback(listener: TcpListener, state: String) -> anyhow::Result<String> {
    let (codes, mut received) = mpsc::channel(1);
    let accept = async {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            tokio::spawn(callback_connection(stream, state.clone(), codes.clone()));
        }
    };
    tokio::time::timeout(CALLBACK_TIMEOUT, async {
        tokio::select! {
            code = received.recv() => code,
            () = accept => None,
        }
    })
    .await
    .ok()
    .flatten()
    .context("Timed out waiting for the browser; run the login again")
}

async fn read_pasted_code() -> anyhow::Result<String> {
    tokio::task::spawn_blocking(|| {
        loop {
            eprint!("Paste the code here: ");
            let mut line = String::new();
            ensure!(
                std::io::stdin().read_line(&mut line)? > 0,
                "No code was entered"
            );
            let code = line.trim();
            if is_code(code) {
                return Ok(code.to_owned());
            }
            eprintln!("That is not a login code.");
        }
    })
    .await?
}

#[derive(Deserialize)]
struct Redeemed {
    token: String,
    token_id: String,
    name: String,
    expires_at: i64,
    apps_domain: String,
}

// The endpoint to log in to: explicit values, then the default entry, then production.
fn login_target(args: &CloudArgs, store: &Store) -> anyhow::Result<String> {
    if let Some(api_url) = &args.api_url {
        return Ok(api_url.clone());
    }
    if let Some(domain) = &args.apps_domain {
        return store.by_domain(domain).map_or_else(
            || {
                if domain.eq_ignore_ascii_case(DEFAULT_APPS_DOMAIN) {
                    Ok(DEFAULT_API_URL.to_owned())
                } else {
                    bail!("No login for apps domain {domain}; set --api-url as well")
                }
            },
            |entry| Ok(entry.api_url.clone()),
        );
    }
    Ok(store
        .select(None, None)
        .map_or(DEFAULT_API_URL, |entry| &entry.api_url)
        .to_owned())
}

async fn login(args: &CloudArgs, path: &Path, no_browser: bool) -> anyhow::Result<Value> {
    ensure!(
        terminal(),
        "`obelisk cloud auth login` needs a terminal; set OBELISK_CLOUD_API_TOKEN instead"
    );
    let api_url = login_target(args, &Store::load(path)?)?;
    let verifier = random_hex();
    let state = random_hex();
    let challenge = hex(&Sha256::digest(verifier.as_bytes()));
    let host = hostname();
    // The redirect only works when the browser runs on this machine.
    let listener = if no_browser {
        None
    } else {
        TcpListener::bind(("127.0.0.1", 0)).await.ok()
    };
    let port = listener
        .as_ref()
        .map(TcpListener::local_addr)
        .transpose()?
        .map(|address| address.port());
    let page = login_page(&api_url, &challenge, &state, &host, port)?;
    let code = match listener {
        Some(listener) if open_browser(&page) => {
            eprintln!("Opening the browser. If it does not open, visit:\n\n{page}\n");
            eprintln!(
                "If the browser is on another machine, cancel and use `obelisk cloud auth login --no-browser`."
            );
            wait_for_callback(listener, state).await?
        }
        _ => {
            let page = login_page(&api_url, &challenge, &state, &host, None)?;
            eprintln!("Visit this page in any browser:\n\n{page}\n");
            read_pasted_code().await?
        }
    };
    finish_login(&api_url, path, &code, &verifier, args.verbose).await
}

async fn finish_login(
    api_url: &str,
    path: &Path,
    code: &str,
    verifier: &str,
    verbose: bool,
) -> anyhow::Result<Value> {
    let redeemed: Redeemed = serde_json::from_value(
        Client::build(api_url, None, verbose)?
            .request(
                Method::POST,
                &["v1", "cli-logins", "redeem"],
                Some(json!({"code": code, "verifier": verifier})),
                Kind::Read,
            )
            .await
            .context("Login failed")?,
    )
    .context("Cannot read the issued token")?;
    let entry = Endpoint {
        api_url: normalized(api_url).to_owned(),
        apps_domain: redeemed.apps_domain,
        api_token: redeemed.token,
        token_id: redeemed.token_id,
        token_name: redeemed.name,
        expires_at: redeemed.expires_at,
    };
    let mut store = Store::load(path)?;
    let previous = store.insert(entry.clone());
    store.save(path)?;
    if let Some(previous) = previous {
        revoke(&previous, verbose).await;
    }
    Ok(json!({
        "logged_in": entry.api_url,
        "apps_domain": entry.apps_domain,
        "token_name": entry.token_name,
        "expires_at": entry.expiry(),
        "default": store.by_url(store.default.as_deref().unwrap_or_default()).is_some_and(|default| default.api_url == entry.api_url),
    }))
}

async fn revoke(entry: &Endpoint, verbose: bool) {
    let result = async {
        let token = SecretString::from(entry.api_token.clone());
        Client::build(&entry.api_url, Some(&token), verbose)?
            .request(
                Method::DELETE,
                &["v1", "tokens", &entry.token_id],
                None,
                Kind::Read,
            )
            .await
    }
    .await;
    // A token that already expired needs no revocation.
    if let Err(err) = result
        && !entry.expired()
    {
        eprintln!(
            "Warning: cannot revoke token {}: {err:#}; revoke it on the dashboard",
            entry.token_name
        );
    }
}

async fn token_state(entry: &Endpoint, verbose: bool) -> String {
    if entry.expired() {
        return "expired".to_owned();
    }
    let token = SecretString::from(entry.api_token.clone());
    let response = async {
        let client = Client::build(&entry.api_url, Some(&token), verbose)?;
        let url = client.url(&["v1", "tokens", &entry.token_id])?;
        Ok::<_, anyhow::Error>(
            tokio::time::timeout(Duration::from_secs(10), client.http.get(url).send())
                .await
                .context("timed out")?
                .map_err(reqwest::Error::without_url)?,
        )
    }
    .await;
    match response {
        Ok(response) if response.status().is_success() => "active".to_owned(),
        Ok(response) if response.status() == reqwest::StatusCode::UNAUTHORIZED => {
            "rejected, run `obelisk cloud auth login`".to_owned()
        }
        Ok(response) => format!("unknown (HTTP {})", response.status()),
        Err(err) => format!("unknown ({err:#})"),
    }
}

async fn status(args: &CloudArgs, path: &Path) -> anyhow::Result<Value> {
    let store = Store::load(path)?;
    if args.api_token.is_some() && !args.json {
        eprintln!("--api-token or OBELISK_CLOUD_API_TOKEN is set and takes precedence.\n");
    }
    let default = store.default.as_deref().map(normalized);
    let mut endpoints = Vec::new();
    for entry in &store.endpoints {
        endpoints.push(json!({
            "api_url": entry.api_url,
            "apps_domain": entry.apps_domain,
            "default": default == Some(normalized(&entry.api_url)),
            "token_name": entry.token_name,
            "expires_at": entry.expiry(),
            "token_state": token_state(entry, args.verbose).await,
        }));
    }
    Ok(json!({ "endpoints": endpoints }))
}

fn switch(path: &Path, endpoint: &str) -> anyhow::Result<Value> {
    let mut store = Store::load(path)?;
    let api_url = store
        .by_url(endpoint)
        .or_else(|| store.by_domain(endpoint))
        .map(|entry| entry.api_url.clone())
        .with_context(|| {
            format!(
                "No login for {endpoint}; `obelisk cloud auth status` lists the logged-in endpoints"
            )
        })?;
    store.default = Some(api_url.clone());
    store.save(path)?;
    Ok(json!({ "default": api_url }))
}

async fn logout(args: &CloudArgs, path: &Path) -> anyhow::Result<Value> {
    let mut store = Store::load(path)?;
    let entry = store
        .select(args.api_url.as_deref(), args.apps_domain.as_deref())
        .cloned()
        .context("Not logged in")?;
    revoke(&entry, args.verbose).await;
    store.remove(&entry.api_url);
    store.save(path)?;
    Ok(json!({ "logged_out": entry.api_url, "default": store.default }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, http::StatusCode, routing::post};
    use std::sync::{Arc, Mutex};

    fn entry(api_url: &str, domain: &str) -> Endpoint {
        Endpoint {
            api_url: api_url.to_owned(),
            apps_domain: domain.to_owned(),
            api_token: format!("token-{domain}"),
            token_id: "1".repeat(32),
            token_name: "cli-test-abcdef".to_owned(),
            expires_at: chrono::Utc::now().timestamp() + 1000,
        }
    }

    fn two_logins() -> Store {
        let mut store = Store::default();
        store.insert(entry("https://cloud.obeli.sk/api", "obelisk.works"));
        store.insert(entry("https://cloud.dev.test/api", "dev.test"));
        store
    }

    #[test]
    fn selection_prefers_url_then_domain_then_default() {
        let store = two_logins();
        let selected = |url, domain| store.select(url, domain).map(|e| e.apps_domain.as_str());
        assert_eq!(
            selected(None, None),
            Some("obelisk.works"),
            "first is default"
        );
        assert_eq!(
            selected(Some("https://cloud.dev.test/api/"), None),
            Some("dev.test")
        );
        assert_eq!(selected(None, Some("DEV.test")), Some("dev.test"));
        assert_eq!(
            selected(Some("https://cloud.obeli.sk/api"), Some("dev.test")),
            Some("obelisk.works")
        );
        assert_eq!(selected(Some("https://other.test"), None), None);
        assert_eq!(selected(None, Some("other.test")), None);
    }

    #[test]
    fn removing_the_default_passes_it_on() {
        let mut store = two_logins();
        store.remove("https://cloud.dev.test/api");
        assert_eq!(store.default.as_deref(), Some("https://cloud.obeli.sk/api"));
        store.insert(entry("https://cloud.dev.test/api", "dev.test"));
        store.remove("https://cloud.obeli.sk/api/");
        assert_eq!(store.default.as_deref(), Some("https://cloud.dev.test/api"));
        store.remove("https://cloud.dev.test/api");
        assert_eq!(store.default, None);
    }

    #[test]
    fn relogin_replaces_the_entry_and_keeps_the_default() {
        let mut store = two_logins();
        let previous = store.insert(entry("https://cloud.dev.test/api/", "dev.test"));
        assert!(previous.is_some());
        assert_eq!(store.endpoints.len(), 2);
        assert_eq!(store.default.as_deref(), Some("https://cloud.obeli.sk/api"));
    }

    #[cfg(unix)]
    #[test]
    fn credentials_are_private_and_shared_files_are_refused() {
        use std::os::unix::fs::PermissionsExt as _;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested/cloud-credentials.toml");
        two_logins().save(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        let loaded = Store::load(&path).unwrap();
        assert_eq!(loaded.endpoints.len(), 2);
        assert_eq!(
            loaded.default.as_deref(),
            Some("https://cloud.obeli.sk/api")
        );
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(Store::load(&path).is_err());
    }

    #[test]
    fn login_page_replaces_the_api_prefix() {
        let page = login_page(
            "https://cloud.obeli.sk/api/",
            "c",
            "s",
            "laptop",
            Some(4567),
        )
        .unwrap();
        assert_eq!(
            page.as_str(),
            "https://cloud.obeli.sk/cli-login?challenge=c&state=s&host=laptop&port=4567"
        );
        let page = login_page("http://127.0.0.1:8080/", "c", "s", "laptop", None).unwrap();
        assert_eq!(
            page.as_str(),
            "http://127.0.0.1:8080/cli-login?challenge=c&state=s&host=laptop"
        );
    }

    #[test]
    fn hostnames_fit_the_token_name_alphabet() {
        assert_eq!(sanitize_hostname(" my laptop\n"), "my-laptop");
        assert_eq!(sanitize_hostname("--a.b_c"), "a.b_c");
        assert_eq!(sanitize_hostname("čš"), "unknown");
        assert_eq!(sanitize_hostname(&"a".repeat(100)).len(), 64);
    }

    #[tokio::test]
    async fn callback_ignores_everything_without_the_state() {
        test_utils::set_up();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let waiting = tokio::spawn(wait_for_callback(listener, "good".to_owned()));
        let code = "c".repeat(64);
        // An idle connection, as browsers preconnect, must not hold up the others.
        let _idle = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        for target in [
            format!("/callback?code={code}&state=bad"),
            format!("/other?code={code}&state=good"),
            "/callback?code=short&state=good".to_owned(),
        ] {
            let response = reqwest::get(format!("http://127.0.0.1:{port}{target}"))
                .await
                .unwrap();
            assert_eq!(response.status(), 404, "{target}");
        }
        assert!(!waiting.is_finished());
        let response = reqwest::get(format!(
            "http://127.0.0.1:{port}/callback?code={code}&state=good"
        ))
        .await
        .unwrap();
        assert_eq!(response.status(), 200);
        assert!(response.text().await.unwrap().contains("Login complete"));
        assert_eq!(waiting.await.unwrap().unwrap(), code);
    }

    #[tokio::test]
    async fn login_saves_the_token_and_revokes_the_previous_one() {
        test_utils::set_up();
        let revoked = Arc::new(Mutex::new(Vec::new()));
        let recorded = revoked.clone();
        let router = Router::new()
            .route(
                "/api/v1/cli-logins/redeem",
                post(|Json(body): Json<Value>| async move {
                    if body != json!({"code": "code", "verifier": "verifier"}) {
                        return Err(StatusCode::BAD_REQUEST);
                    }
                    Ok(Json(json!({
                        "token": "obk_new",
                        "token_id": "2".repeat(32),
                        "name": "cli-laptop-abcdef",
                        "expires_at": 4_000_000_000_i64,
                        "apps_domain": "apps.test",
                    })))
                }),
            )
            .route(
                "/api/v1/tokens/{id}",
                axum::routing::delete(
                    move |axum::extract::Path(id): axum::extract::Path<String>,
                          headers: axum::http::HeaderMap| async move {
                        recorded
                            .lock()
                            .unwrap()
                            .push((id, headers["authorization"].to_str().unwrap().to_owned()));
                        Json(json!({}))
                    },
                ),
            );
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let api_url = format!("http://{}/api", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cloud-credentials.toml");
        let result = finish_login(&api_url, &path, "code", "verifier", false)
            .await
            .unwrap();
        assert_eq!(result["default"], true);
        assert_eq!(result["apps_domain"], "apps.test");
        assert!(revoked.lock().unwrap().is_empty());
        let stored = Store::load(&path).unwrap();
        assert_eq!(stored.endpoints[0].api_token, "obk_new");
        assert_eq!(stored.endpoints[0].apps_domain, "apps.test");
        assert_eq!(stored.default.as_deref(), Some(api_url.as_str()));

        let mut replaced = Store::load(&path).unwrap();
        replaced.endpoints[0].api_token = "obk_old".to_owned();
        replaced.endpoints[0].token_id = "1".repeat(32);
        replaced.save(&path).unwrap();
        assert!(
            finish_login(&api_url, &path, "wrong", "verifier", false)
                .await
                .is_err()
        );
        assert_eq!(
            Store::load(&path).unwrap().endpoints[0].api_token,
            "obk_old"
        );
        finish_login(&api_url, &path, "code", "verifier", false)
            .await
            .unwrap();
        assert_eq!(
            Store::load(&path).unwrap().endpoints[0].api_token,
            "obk_new"
        );
        assert_eq!(
            *revoked.lock().unwrap(),
            [("1".repeat(32), "Bearer obk_old".to_owned())]
        );
    }
}
