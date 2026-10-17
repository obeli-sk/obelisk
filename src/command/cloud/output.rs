use crate::args::{Cloud, CloudApp, CloudAppTarget, CloudEnv, CloudSecrets, CloudToken};
use serde_json::Value;
use std::fmt::Write as _;

pub(super) enum Summary {
    App(Option<&'static str>),
    Apps,
    Status,
    Token(Option<&'static str>),
    Tokens,
    Env,
    Secrets,
    Message(&'static str),
    Staged(&'static str, String),
    Fill(String),
}

impl From<&Cloud> for Summary {
    fn from(command: &Cloud) -> Self {
        match command {
            Cloud::Apps(_) => Self::Apps,
            Cloud::Token(CloudToken::List) => Self::Tokens,
            Cloud::Token(CloudToken::New { .. }) => Self::Token(Some("Token registered.")),
            Cloud::Token(CloudToken::Revoke { .. }) => Self::Token(Some("Token revoked.")),
            Cloud::Token(CloudToken::Get { .. }) => Self::Token(None),
            Cloud::App(CloudAppTarget { app, command }) => {
                let apply = app.as_ref().map_or_else(
                    || "obelisk cloud app apply".to_owned(),
                    |app| format!("obelisk cloud app --app {app} apply"),
                );
                match command {
                    CloudApp::Get => Self::App(None),
                    CloudApp::Status => Self::Status,
                    CloudApp::New { .. } => Self::App(Some("App created.")),
                    CloudApp::Apply { .. } => Self::App(Some("App applied.")),
                    CloudApp::Start { .. } => Self::App(Some("App started.")),
                    CloudApp::Stop { .. } => Self::App(Some("App stopped.")),
                    CloudApp::Delete { .. } => Self::Message("App deleted."),
                    CloudApp::Stage { .. } => Self::Staged("Configuration staged.", apply),
                    CloudApp::Env(CloudEnv::List) => Self::Env,
                    CloudApp::Env(CloudEnv::Set { .. } | CloudEnv::Unset { .. }) => {
                        Self::Staged("Environment staged.", apply)
                    }
                    CloudApp::Secrets(CloudSecrets::List) => Self::Secrets,
                    CloudApp::Secrets(CloudSecrets::Set { .. } | CloudSecrets::Unset { .. }) => {
                        Self::Staged("Secret change staged.", apply)
                    }
                    CloudApp::Fill { .. } => Self::Fill(apply),
                    CloudApp::Endpoint { .. }
                    | CloudApp::Deploy { .. }
                    | CloudApp::Deployment(_)
                    | CloudApp::Execution(_)
                    | CloudApp::Component(_)
                    | CloudApp::Admin(_) => unreachable!("command has its own output"),
                }
            }
        }
    }
}

impl Summary {
    pub(super) fn render(&self, value: &Value) -> String {
        let mut out = String::new();
        match self {
            Self::App(message) => {
                if let Some(message) = message {
                    let _ = writeln!(out, "{message}");
                }
                app(&mut out, value);
            }
            Self::Apps => {
                if let Some(apps) = value["apps"].as_array().filter(|apps| !apps.is_empty()) {
                    for value in apps {
                        app(&mut out, value);
                    }
                } else {
                    out.push_str("No apps. Run `obelisk cloud app new` to create one.\n");
                }
            }
            Self::Status => {
                app(&mut out, &value["app"]);
                if let Some(memory) = value["memory_mb"].as_u64() {
                    let _ = writeln!(out, "Memory: {memory} MB");
                }
                let _ = writeln!(
                    out,
                    "API: {}",
                    if value["api_ready"] == true {
                        "ready"
                    } else {
                        "unavailable"
                    }
                );
            }
            Self::Token(message) => {
                if let Some(message) = message {
                    let _ = writeln!(out, "{message}");
                }
                token(&mut out, value);
                if let Some(path) = value["token_file"].as_str() {
                    let _ = writeln!(out, "Token saved in {path}");
                }
                if value["sync"]["status"] == "pending"
                    && let Some(apps) = value["sync"]["pending_apps"].as_array()
                {
                    for app in apps {
                        if app["status"] == "waiting_for_start" {
                            let name = text(app, "app_slug", "app");
                            let _ = writeln!(out, "{name}: changes take effect on next start.");
                        }
                    }
                }
            }
            Self::Tokens => {
                if let Some(tokens) = value["tokens"]
                    .as_array()
                    .filter(|tokens| !tokens.is_empty())
                {
                    for value in tokens {
                        token(&mut out, value);
                    }
                } else {
                    out.push_str("No tokens.\n");
                }
            }
            Self::Env => {
                if let Some(env) = value["env"].as_object().filter(|env| !env.is_empty()) {
                    for (name, value) in env {
                        let _ = writeln!(out, "{name}={}", value.as_str().unwrap_or(""));
                    }
                } else {
                    out.push_str("No environment variables.\n");
                }
            }
            Self::Secrets => {
                if let Some(secrets) = value["secrets"]
                    .as_array()
                    .filter(|secrets| !secrets.is_empty())
                {
                    for secret in secrets {
                        let _ = writeln!(out, "{}", text(secret, "name", "secret"));
                    }
                } else {
                    out.push_str("No secrets.\n");
                }
            }
            Self::Message(message) => {
                let _ = writeln!(out, "{message}");
            }
            Self::Staged(message, apply) => {
                let _ = writeln!(out, "{message} Run `{apply}` to apply changes.");
            }
            Self::Fill(apply) => {
                let count = value["staged"].as_array().map_or(0, Vec::len);
                let _ = writeln!(out, "Staged {count} configuration values.");
                if count > 0 {
                    let _ = writeln!(out, "Run `{apply}` to apply changes.");
                }
            }
        }
        out
    }
}

fn text<'a>(value: &'a Value, key: &str, fallback: &'a str) -> &'a str {
    value[key].as_str().unwrap_or(fallback)
}

fn app(out: &mut String, value: &Value) {
    let _ = writeln!(
        out,
        "{}: {}",
        text(value, "app_slug", "App"),
        text(value, "status", "unknown")
    );
    if let Some(url) = value["webhook_url"].as_str() {
        let _ = writeln!(out, "URL: {url}");
    }
    if value["potentially_stale"] == true {
        let name = text(value, "app_slug", "");
        let _ = writeln!(
            out,
            "Pending changes. Run `obelisk cloud app --app {name} apply` to apply them."
        );
    }
    if let Some(failure) = value["state"]["failure"].as_str() {
        let _ = writeln!(out, "Error: {failure}");
    }
    if let Some(reason) = value["state"]["startup_failure"]["reason"].as_str() {
        let _ = writeln!(
            out,
            "Startup failed: {reason}. Use --verbose for startup details."
        );
    }
}

fn token(out: &mut String, value: &Value) {
    let scope = &value["creation"];
    let scope = if scope["global"] == true {
        "account-wide".to_owned()
    } else {
        scope["apps"]
            .as_array()
            .map(|apps| {
                apps.iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default()
    };
    let state = if value["revoked"] == true {
        "revoked"
    } else if value["expires_at"]
        .as_i64()
        .is_some_and(|expiry| expiry <= chrono::Utc::now().timestamp())
    {
        "expired"
    } else {
        "active"
    };
    let _ = writeln!(
        out,
        "{}  {}  {state}  scope: {scope}",
        text(value, "token_id", "Token"),
        text(value, "name", "")
    );
    if value["sync"]["status"] == "pending" {
        out.push_str("Access changes pending.\n");
    }
    if let Some(expiry) = value["expires_at"]
        .as_i64()
        .and_then(|expiry| chrono::DateTime::from_timestamp(expiry, 0))
    {
        let _ = writeln!(out, "Expires: {}", expiry.to_rfc3339());
    }
}

pub(super) fn error_message(body: &str) -> String {
    if let Ok(value) = serde_json::from_str::<Value>(body) {
        let mut message = text(&value, "error", "Cloud request failed").to_owned();
        if let Some(missing) = value["missing"].as_array() {
            let _ = write!(
                message,
                ": {}",
                missing
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if let Some(reason) = value["startup_failure"]["reason"].as_str() {
            let _ = write!(message, ": {reason}. Use --verbose for startup details");
        }
        message
    } else {
        "Cloud request failed".to_owned()
    }
}
