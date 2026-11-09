//! `obelisk cloud auth`: browser login and the local credentials file.

use super::{Client, DEFAULT_API_URL, Kind, api_origin, human_output, print_json, terminal};
use crate::args::{CloudArgs, CloudAuth, non_empty_env};
use anyhow::{Context as _, bail, ensure};
use reqwest::{Method, Url};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    io::{ErrorKind, Write as _},
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Default, Serialize, Deserialize)]
struct Store {
    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<String>,
    #[serde(default, rename = "account")]
    accounts: BTreeMap<String, Account>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Account {
    user_slug: String,
    api_url: String,
    apps_domain: String,
    api_token: String,
    token_id: String,
    token_name: String,
    expires_at: i64,
}
impl Account {
    fn simple_name(&self) -> String {
        let endpoint = Url::parse(&self.api_url)
            .map_or_else(|_| self.api_url.clone(), |url| url.authority().to_owned());
        format!("{}@{endpoint}", self.user_slug)
    }
    fn same_login(&self, other: &Self) -> bool {
        self.user_slug == other.user_slug && normalized(&self.api_url) == normalized(&other.api_url)
    }
    fn expired(&self) -> bool {
        self.expires_at <= chrono::Utc::now().timestamp()
    }
    fn expiry(&self) -> String {
        chrono::DateTime::from_timestamp(self.expires_at, 0)
            .map_or_else(|| self.expires_at.to_string(), |date| date.to_rfc3339())
    }
}

fn normalized(api_url: &str) -> String {
    Url::parse(api_url)
        .map_or_else(|_| api_url.to_owned(), |url| url.to_string())
        .trim_end_matches('/')
        .to_owned()
}

fn login_command(api_url: &str) -> String {
    if normalized(api_url) == DEFAULT_API_URL {
        "obelisk cloud auth login".to_owned()
    } else {
        format!("obelisk cloud auth login {api_url}")
    }
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
    fn find(&self, name: &str) -> Option<&Account> {
        self.accounts.get(name)
    }
    fn select(&self, name: Option<&str>) -> Option<(&str, &Account)> {
        self.accounts
            .get_key_value(name.or(self.default.as_deref())?)
            .map(|(name, account)| (name.as_str(), account))
    }
    fn insert(
        &mut self,
        account: Account,
        alias: Option<&str>,
        default: bool,
    ) -> anyhow::Result<(String, Option<Account>)> {
        let existing = self
            .accounts
            .iter()
            .find(|(_, old)| old.same_login(&account))
            .map(|(name, _)| name.clone());
        let name = if let Some(alias) = alias {
            ensure!(!alias.is_empty(), "Account name must not be empty");
            alias.to_owned()
        } else if let Some(existing) = &existing {
            existing.clone()
        } else {
            let simple = account.simple_name();
            if self.accounts.contains_key(&simple) {
                format!("{}@{}", account.user_slug, normalized(&account.api_url))
            } else {
                simple
            }
        };
        if let Some(old) = self.accounts.get(&name) {
            ensure!(
                old.same_login(&account),
                "Account name {name} is already used by another user or backend; choose a different --account"
            );
        }
        let previous = existing
            .as_ref()
            .and_then(|name| self.accounts.remove(name));
        if default || self.accounts.is_empty() || self.default.as_ref() == existing.as_ref() {
            self.default = Some(name.clone());
        }
        self.accounts.insert(name.clone(), account);
        Ok((name, previous))
    }
    /// Drops the login; a removed default passes to the first remaining entry.
    fn remove(&mut self, name: &str) {
        self.accounts.remove(name);
        if self.default.as_deref() == Some(name) {
            self.default = self.accounts.keys().next().cloned();
        }
    }
}

/// Picks what `obelisk cloud` talks to: a token alone means production, all three values a custom
/// deployment, and otherwise a saved account.
pub(super) fn resolve(args: &mut CloudArgs, token_required: bool) -> anyhow::Result<()> {
    select(args, store_path().as_deref(), token_required)
}

fn select(args: &mut CloudArgs, path: Option<&Path>, token_required: bool) -> anyhow::Result<()> {
    let explicit = (
        args.api_token.is_some(),
        args.api_url.is_some(),
        args.apps_domain.is_some(),
    );
    ensure!(
        args.account.is_none() || explicit == (false, false, false),
        "--account or OBELISK_CLOUD_ACCOUNT cannot be combined with an API token, OBELISK_CLOUD_API_URL or OBELISK_CLOUD_APPS_DOMAIN"
    );
    match explicit {
        (true, false, false) | (true, true, true) => return Ok(()),
        (false, false, false) => {}
        (token, api_url, domain) => {
            let missing = [
                (!api_url, "OBELISK_CLOUD_API_URL"),
                (!domain, "OBELISK_CLOUD_APPS_DOMAIN"),
                (!token, "OBELISK_CLOUD_API_TOKEN"),
            ]
            .into_iter()
            .filter_map(|(missing, name)| missing.then_some(name))
            .collect::<Vec<_>>()
            .join(", ");
            bail!(
                "A custom deployment needs OBELISK_CLOUD_API_URL, OBELISK_CLOUD_APPS_DOMAIN and an API token together; missing {missing}. Unset them to use a saved login"
            );
        }
    }
    let store = path.map(Store::load).transpose()?.unwrap_or_default();
    let Some((name, account)) = store.select(args.account.as_deref()) else {
        if let Some(name) = &args.account {
            bail!("No login for {name}; `obelisk cloud auth status` lists the saved accounts");
        }
        ensure!(
            !token_required,
            "Not logged in; run `obelisk cloud auth login` or set OBELISK_CLOUD_API_TOKEN"
        );
        return Ok(());
    };
    ensure!(
        !token_required || !account.expired(),
        "Login of {} expired on {}; run `{}`",
        name,
        account.expiry(),
        login_command(&account.api_url)
    );
    args.api_url = Some(account.api_url.clone());
    args.apps_domain = Some(account.apps_domain.clone());
    args.api_token = Some(SecretString::from(account.api_token.clone()));
    Ok(())
}

pub(super) async fn run(args: &CloudArgs, command: &CloudAuth) -> anyhow::Result<()> {
    let path = store_path().context("Cannot determine the configuration directory")?;
    let value = match command {
        CloudAuth::Login {
            api_url,
            no_browser,
            account,
            default,
        } => {
            login(
                args,
                api_url.as_deref(),
                &path,
                *no_browser,
                account.as_deref(),
                *default,
            )
            .await?
        }
        CloudAuth::Status => status(args, &path).await?,
        CloudAuth::Switch { account } => switch(&path, account)?,
        CloudAuth::Logout { account } => logout(args, account.as_deref(), &path).await?,
    };
    if args.json {
        print_json(&value)
    } else {
        if value["accounts"].as_array().is_some_and(Vec::is_empty) {
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
fn login_page(api_url: &str, challenge: &str, host: &str) -> anyhow::Result<Url> {
    let mut url = api_origin(api_url)?;
    let path = url.path().trim_end_matches('/');
    let path = format!("{}/cli-login", path.strip_suffix("/api").unwrap_or(path));
    url.set_path(&path);
    url.query_pairs_mut()
        .append_pair("challenge", challenge)
        .append_pair("host", host);
    Ok(url)
}

/// Only a graphical session can show the page on this machine.
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
    user_slug: String,
}

async fn login(
    args: &CloudArgs,
    api_url: Option<&str>,
    path: &Path,
    no_browser: bool,
    account: Option<&str>,
    default: bool,
) -> anyhow::Result<Value> {
    ensure!(
        terminal(),
        "`obelisk cloud auth login` needs a terminal; set OBELISK_CLOUD_API_TOKEN instead"
    );
    let api_url = api_url
        .filter(|url| !url.is_empty())
        .unwrap_or(DEFAULT_API_URL);
    let verifier = random_hex();
    let challenge = hex(&Sha256::digest(verifier.as_bytes()));
    let page = login_page(api_url, &challenge, &hostname())?;
    if !no_browser && open_browser(&page) {
        eprintln!("Opening the browser. If it does not open, visit:\n\n{page}\n");
    } else {
        eprintln!("Visit this page in any browser:\n\n{page}\n");
    }
    let code = read_pasted_code().await?;
    finish_login(
        api_url,
        path,
        &code,
        &verifier,
        args.verbose,
        account,
        default,
    )
    .await
}

async fn finish_login(
    api_url: &str,
    path: &Path,
    code: &str,
    verifier: &str,
    verbose: bool,
    alias: Option<&str>,
    default: bool,
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
    let account = Account {
        user_slug: redeemed.user_slug,
        api_url: normalized(api_url),
        apps_domain: redeemed.apps_domain,
        api_token: redeemed.token,
        token_id: redeemed.token_id,
        token_name: redeemed.name,
        expires_at: redeemed.expires_at,
    };
    let mut store = Store::load(path)?;
    let (name, previous) = match store.insert(account.clone(), alias, default) {
        Ok(inserted) => inserted,
        Err(err) => {
            revoke(&account, verbose).await;
            return Err(err);
        }
    };
    store.save(path)?;
    if let Some(previous) = previous {
        revoke(&previous, verbose).await;
    }
    Ok(json!({
        "logged_in": name,
        "apps_domain": account.apps_domain,
        "token_name": account.token_name,
        "expires_at": account.expiry(),
        "default": store.default.as_deref() == Some(name.as_str()),
    }))
}

async fn revoke(entry: &Account, verbose: bool) {
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

async fn token_state(entry: &Account, verbose: bool) -> String {
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
            format!("rejected, run `{}`", login_command(&entry.api_url))
        }
        Ok(response) => format!("unknown (HTTP {})", response.status()),
        Err(err) => format!("unknown ({err:#})"),
    }
}

async fn status(args: &CloudArgs, path: &Path) -> anyhow::Result<Value> {
    if args.verbose {
        eprintln!("Credentials file: {}", path.display());
    }
    let store = Store::load(path)?;
    if non_empty_env("OBELISK_CLOUD_API_TOKEN").is_some() && !args.json {
        eprintln!("OBELISK_CLOUD_API_TOKEN is set and takes precedence.\n");
    }
    let mut accounts = Vec::new();
    for (name, entry) in &store.accounts {
        accounts.push(json!({
            "default": store.default.as_deref() == Some(name.as_str()),
            "account": name,
            "api_url": entry.api_url,
            "apps_domain": entry.apps_domain,
            "token_name": entry.token_name,
            "expires_at": entry.expiry(),
            "token_state": token_state(entry, args.verbose).await,
        }));
    }
    Ok(json!({ "accounts": accounts }))
}

fn switch(path: &Path, name: &str) -> anyhow::Result<Value> {
    let mut store = Store::load(path)?;
    let name = store.find(name).map(|_| name.to_owned()).with_context(|| {
        format!("No login for {name}; `obelisk cloud auth status` lists the saved accounts")
    })?;
    store.default = Some(name.clone());
    store.save(path)?;
    Ok(json!({ "default": name }))
}

async fn logout(args: &CloudArgs, name: Option<&str>, path: &Path) -> anyhow::Result<Value> {
    let mut store = Store::load(path)?;
    let selected = non_empty_env("OBELISK_CLOUD_ACCOUNT");
    let name = name.or(selected.as_deref());
    let (name, entry) = store.select(name).with_context(|| {
        name.map_or_else(
            || "Not logged in".to_owned(),
            |name| format!("No login for {name}"),
        )
    })?;
    let name = name.to_owned();
    revoke(entry, args.verbose).await;
    store.remove(&name);
    store.save(path)?;
    Ok(json!({ "logged_out": name, "default": store.default }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, http::StatusCode, routing::post};
    use secrecy::ExposeSecret as _;
    use std::sync::{Arc, Mutex};

    const ALICE: &str = "alice@cloud.obeli.sk";
    const DEV: &str = "alice@cloud.dev.test";

    fn entry(slug: &str, api_url: &str, domain: &str) -> Account {
        Account {
            user_slug: slug.to_owned(),
            api_url: api_url.to_owned(),
            apps_domain: domain.to_owned(),
            api_token: format!("token-{slug}-{domain}"),
            token_id: "1".repeat(32),
            token_name: "cli-test-abcdef".to_owned(),
            expires_at: chrono::Utc::now().timestamp() + 1000,
        }
    }

    fn two_logins() -> Store {
        let mut store = Store::default();
        store
            .insert(
                entry("alice", "https://cloud.obeli.sk/api", "obelisk.works"),
                None,
                false,
            )
            .unwrap();
        store
            .insert(
                entry("alice", "https://cloud.dev.test/api", "dev.test"),
                None,
                false,
            )
            .unwrap();
        store
    }

    fn parsed(token: Option<&str>, account: Option<&str>) -> CloudArgs {
        CloudArgs {
            json: false,
            verbose: false,
            api_token: token.map(SecretString::from),
            account: account.map(str::to_owned),
            api_url: None,
            apps_domain: None,
            command: crate::args::Cloud::App(crate::args::CloudAppTarget {
                credentials: crate::args::CloudCredentials {
                    api_token: None,
                    account: None,
                },
                command: crate::args::CloudApp::List,
            }),
        }
    }

    #[test]
    fn account_names_use_the_api_host() {
        let name = |url| entry("alice", url, "d").simple_name();
        assert_eq!(name("https://cloud.obeli.sk/api"), ALICE);
        assert_eq!(name("http://127.0.0.1:8080/"), "alice@127.0.0.1:8080");
    }

    #[test]
    fn selection_takes_a_token_alone_all_three_values_or_a_saved_account() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cloud-credentials.toml");
        two_logins().save(&path).unwrap();
        let run = |args: &mut CloudArgs| select(args, Some(&path), true);

        let mut args = parsed(None, None);
        run(&mut args).unwrap();
        assert_eq!(
            args.apps_domain.as_deref(),
            Some("obelisk.works"),
            "default"
        );
        assert!(args.api_token.is_some());

        let mut args = parsed(None, Some(DEV));
        run(&mut args).unwrap();
        assert_eq!(args.api_url.as_deref(), Some("https://cloud.dev.test/api"));
        assert_eq!(args.apps_domain.as_deref(), Some("dev.test"));

        let mut args = parsed(None, Some("bob@cloud.dev.test"));
        assert!(
            run(&mut args)
                .unwrap_err()
                .to_string()
                .contains("No login for")
        );

        let mut args = parsed(Some("t"), None);
        run(&mut args).unwrap();
        assert_eq!(args.api_url, None, "a token alone never uses the file");

        let mut args = parsed(Some("t"), Some(ALICE));
        assert!(
            run(&mut args)
                .unwrap_err()
                .to_string()
                .contains("cannot be combined")
        );

        let mut args = parsed(Some("t"), None);
        args.api_url = Some("https://other.test".to_owned());
        args.apps_domain = Some("other.test".to_owned());
        run(&mut args).unwrap();
        assert_eq!(args.apps_domain.as_deref(), Some("other.test"));
        assert_eq!(args.api_token.as_ref().unwrap().expose_secret(), "t");

        for (token, api_url, domain, missing) in [
            (false, true, true, "missing OBELISK_CLOUD_API_TOKEN."),
            (
                false,
                true,
                false,
                "missing OBELISK_CLOUD_APPS_DOMAIN, OBELISK_CLOUD_API_TOKEN.",
            ),
            (true, true, false, "missing OBELISK_CLOUD_APPS_DOMAIN."),
            (true, false, true, "missing OBELISK_CLOUD_API_URL."),
        ] {
            let mut args = parsed(token.then_some("t"), None);
            args.api_url = api_url.then(|| "https://other.test".to_owned());
            args.apps_domain = domain.then(|| "other.test".to_owned());
            let error = run(&mut args).unwrap_err().to_string();
            assert!(error.contains(missing), "{error}");
        }
    }

    #[test]
    fn offline_commands_need_no_login_but_others_do() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("missing.toml");
        select(&mut parsed(None, None), Some(&path), false).unwrap();
        let error = select(&mut parsed(None, None), Some(&path), true).unwrap_err();
        assert!(error.to_string().contains("Not logged in"));
    }

    #[test]
    fn expired_logins_print_the_command_for_their_endpoint() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cloud-credentials.toml");
        let mut store = Store::default();
        let mut account = entry("alice", "https://cloud.dev.test/api", "dev.test");
        account.expires_at = 1;
        store.insert(account, None, false).unwrap();
        store.save(&path).unwrap();
        let error = select(&mut parsed(None, None), Some(&path), true).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("run `obelisk cloud auth login https://cloud.dev.test/api`"),
            "{error}"
        );
    }

    #[test]
    fn removing_the_default_passes_it_on() {
        let mut store = two_logins();
        store.remove(DEV);
        assert_eq!(store.default.as_deref(), Some(ALICE));
        store
            .insert(
                entry("alice", "https://cloud.dev.test/api", "dev.test"),
                None,
                false,
            )
            .unwrap();
        store.remove(ALICE);
        assert_eq!(store.default.as_deref(), Some(DEV));
        store.remove(DEV);
        assert_eq!(store.default, None);
    }

    #[test]
    fn relogin_replaces_only_the_same_account() {
        let mut store = two_logins();
        let (_, previous) = store
            .insert(
                entry("alice", "https://cloud.dev.test/api/", "dev.test"),
                None,
                false,
            )
            .unwrap();
        assert!(previous.is_some());
        assert_eq!(store.accounts.len(), 2);
        assert_eq!(store.default.as_deref(), Some(ALICE));
        let (_, other) = store
            .insert(
                entry("bob", "https://cloud.dev.test/api", "dev.test"),
                None,
                false,
            )
            .unwrap();
        assert!(other.is_none(), "another account is not replaced");
        assert_eq!(store.accounts.len(), 3);
        assert_eq!(store.default.as_deref(), Some(ALICE));
    }

    #[test]
    fn generated_names_preserve_backend_scheme_port_and_path() {
        let mut store = Store::default();
        for (url, name) in [
            ("https://example.test/prod/api", "alice@example.test"),
            (
                "https://example.test/staging/api",
                "alice@https://example.test/staging/api",
            ),
            (
                "http://example.test/prod/api",
                "alice@http://example.test/prod/api",
            ),
            (
                "https://example.test/Prod/api",
                "alice@https://example.test/Prod/api",
            ),
            (
                "https://example.test:8443/prod/api",
                "alice@example.test:8443",
            ),
        ] {
            let (actual, previous) = store
                .insert(entry("alice", url, "apps.test"), None, false)
                .unwrap();
            assert_eq!(actual, name);
            assert!(previous.is_none());
            assert_eq!(store.find(name).unwrap().api_url, url);
        }
        let (name, previous) = store
            .insert(
                entry(
                    "alice",
                    "https://EXAMPLE.test:443/staging/api/",
                    "apps.test",
                ),
                None,
                false,
            )
            .unwrap();
        assert_eq!(name, "alice@https://example.test/staging/api");
        assert!(previous.is_some());
        assert_eq!(store.accounts.len(), 5);
        assert_eq!(store.default.as_deref(), Some("alice@example.test"));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cloud-credentials.toml");
        store.save(&path).unwrap();
        let mut args = parsed(None, Some("alice@https://example.test/Prod/api"));
        select(&mut args, Some(&path), true).unwrap();
        assert_eq!(
            args.api_url.as_deref(),
            Some("https://example.test/Prod/api")
        );
    }

    #[test]
    fn aliases_survive_refresh_and_explicit_rename_updates_the_default() {
        let mut store = two_logins();
        let account = entry("alice", "https://staging.test/api", "apps.test");
        store
            .insert(account.clone(), Some("staging"), false)
            .unwrap();
        assert_eq!(store.default.as_deref(), Some(ALICE));
        let (name, previous) = store.insert(account.clone(), None, true).unwrap();
        assert_eq!(name, "staging");
        assert!(previous.is_some());
        assert_eq!(store.default.as_deref(), Some("staging"));
        store.insert(account, Some("preview"), false).unwrap();
        assert_eq!(store.default.as_deref(), Some("preview"));
        assert!(!store.accounts.contains_key("staging"));
        assert_eq!(store.accounts.len(), 3);
    }

    #[test]
    fn aliases_cannot_replace_another_user_or_backend() {
        let mut store = two_logins();
        for account in [
            entry("bob", "https://cloud.obeli.sk/api", "obelisk.works"),
            entry("alice", "https://cloud.obeli.sk/other/api", "obelisk.works"),
        ] {
            assert!(
                store
                    .insert(account, Some(ALICE), true)
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("already used")
            );
            assert_eq!(store.accounts.len(), 2);
            assert_eq!(store.default.as_deref(), Some(ALICE));
            assert_eq!(
                store.find(ALICE).unwrap().api_url,
                "https://cloud.obeli.sk/api"
            );
        }
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
        assert_eq!(loaded.accounts.len(), 2);
        assert_eq!(loaded.default.as_deref(), Some(ALICE));
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(Store::load(&path).is_err());
    }

    #[test]
    fn login_page_replaces_the_api_prefix() {
        let page = login_page("https://cloud.obeli.sk/api/", "c", "laptop").unwrap();
        assert_eq!(
            page.as_str(),
            "https://cloud.obeli.sk/cli-login?challenge=c&host=laptop"
        );
        let page = login_page("http://127.0.0.1:8080/", "c", "laptop").unwrap();
        assert_eq!(
            page.as_str(),
            "http://127.0.0.1:8080/cli-login?challenge=c&host=laptop"
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
                        "user_slug": "alice",
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
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let api_url = format!("http://{}/api", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cloud-credentials.toml");
        two_logins().save(&path).unwrap();
        let result = finish_login(
            &api_url,
            &path,
            "code",
            "verifier",
            false,
            Some("local"),
            true,
        )
        .await
        .unwrap();
        assert_eq!(result["default"], true);
        assert_eq!(result["apps_domain"], "apps.test");
        assert!(revoked.lock().unwrap().is_empty());
        let stored = Store::load(&path).unwrap();
        assert_eq!(stored.accounts["local"].api_token, "obk_new");
        assert_eq!(stored.accounts["local"].apps_domain, "apps.test");
        assert_eq!(stored.default.as_deref(), Some("local"));
        assert_eq!(result["logged_in"], "local");

        let mut replaced = Store::load(&path).unwrap();
        replaced.accounts.get_mut("local").unwrap().api_token = "obk_old".to_owned();
        replaced.accounts.get_mut("local").unwrap().token_id = "1".repeat(32);
        replaced.save(&path).unwrap();
        assert!(
            finish_login(&api_url, &path, "wrong", "verifier", false, None, false)
                .await
                .is_err()
        );
        assert_eq!(
            Store::load(&path).unwrap().accounts["local"].api_token,
            "obk_old"
        );
        finish_login(&api_url, &path, "code", "verifier", false, None, false)
            .await
            .unwrap();
        assert_eq!(
            Store::load(&path).unwrap().accounts["local"].api_token,
            "obk_new"
        );
        assert_eq!(
            *revoked.lock().unwrap(),
            [("1".repeat(32), "Bearer obk_old".to_owned())]
        );

        switch(&path, ALICE).unwrap();
        let refreshed = finish_login(&api_url, &path, "code", "verifier", false, None, false)
            .await
            .unwrap();
        assert_eq!(refreshed["logged_in"], "local");
        assert_eq!(refreshed["default"], false);
        assert_eq!(Store::load(&path).unwrap().default.as_deref(), Some(ALICE));
        let refreshed = finish_login(&api_url, &path, "code", "verifier", false, None, true)
            .await
            .unwrap();
        assert_eq!(refreshed["default"], true);
        assert_eq!(
            Store::load(&path).unwrap().default.as_deref(),
            Some("local")
        );

        let before = std::fs::read(&path).unwrap();
        let error = finish_login(
            &api_url,
            &path,
            "code",
            "verifier",
            false,
            Some(ALICE),
            true,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("already used"));
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(
            revoked.lock().unwrap().last().unwrap(),
            &("2".repeat(32), "Bearer obk_new".to_owned())
        );

        let renamed = finish_login(
            &api_url,
            &path,
            "code",
            "verifier",
            false,
            Some("renamed"),
            false,
        )
        .await
        .unwrap();
        assert_eq!(renamed["logged_in"], "renamed");
        assert_eq!(renamed["default"], true);
        let stored = Store::load(&path).unwrap();
        assert_eq!(stored.accounts.len(), 3);
        assert!(!stored.accounts.contains_key("local"));
        let logged_out = logout(&parsed(None, None), Some("renamed"), &path)
            .await
            .unwrap();
        assert_eq!(logged_out["logged_out"], "renamed");
        assert!(!Store::load(&path).unwrap().accounts.contains_key("renamed"));
    }
}
