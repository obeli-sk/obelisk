use crate::args::{Cloud, CloudApp, CloudAppTarget, CloudEnv, CloudSecrets, CloudToken};
use serde_json::Value;
use std::fmt::Write as _;

pub(super) enum Summary {
    App(Option<&'static str>),
    Apps,
    Lifecycle,
    Status,
    Events,
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
                    CloudApp::Status { .. } => Self::Status,
                    CloudApp::Events { .. } => Self::Events,
                    CloudApp::New { .. } => Self::App(Some("App created.")),
                    CloudApp::Apply { .. } | CloudApp::Enable { .. } | CloudApp::Disable { .. } => {
                        Self::Lifecycle
                    }
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
                    | CloudApp::Admin(_)
                    | CloudApp::Pull(_) => unreachable!("command has its own output"),
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
            Self::Lifecycle => {
                let _ = writeln!(out, "{}", lifecycle_outcome(value));
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
                let status = &value["app"]["status"];
                let machine = text(value, "machine_state", "");
                let up = machine == "started";
                // An idle running app is suspended by design and resumes on the next request.
                let idle = status == "running" && matches!(machine, "suspended" | "suspending");
                app_with_note(&mut out, &value["app"], idle.then_some("suspended"));
                if !idle
                    && matches!(machine, "started" | "stopped" | "suspended")
                    && ((status == "running" && !up) || (status == "stopped" && up))
                {
                    let _ = writeln!(
                        out,
                        "VM is {machine} although the app is {status}. Run `obelisk cloud app apply` to reconcile it.",
                        status = text(&value["app"], "status", "")
                    );
                }
                config_revisions(&mut out, value);
                if let Some(memory) = value["memory_mb"].as_u64() {
                    let _ = writeln!(out, "Memory: {memory} MB");
                }
                health(&mut out, value);
                let _ = writeln!(
                    out,
                    "API: {}",
                    match &value["api_ready"] {
                        Value::Bool(true) => "ready",
                        // Not checked: probing would wake the suspended Machine.
                        Value::Null if idle => "resumes on request",
                        _ => "unavailable",
                    }
                );
            }
            Self::Events => {
                if let Some(events) = value["events"]
                    .as_array()
                    .filter(|events| !events.is_empty())
                {
                    for event in events {
                        let _ = writeln!(
                            out,
                            "{}  {:<5}  {}  ({})",
                            time(&event["created_at"]),
                            text(event, "level", ""),
                            text(event, "message", ""),
                            text(event, "code", "")
                        );
                    }
                } else {
                    out.push_str("No events.\n");
                }
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
                            let name = text(app, "app_name", "app");
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
                if value["changed"] == false {
                    let _ = writeln!(
                        out,
                        "Configuration unchanged at staged revision {}.",
                        value["revision"]
                    );
                } else if let Some(revision) = value["revision"].as_u64()
                    && value.get("changed").is_some()
                {
                    let _ = writeln!(
                        out,
                        "Configuration staged as revision {revision}. Run `{apply}` to apply changes."
                    );
                } else {
                    let _ = writeln!(out, "{message} Run `{apply}` to apply changes.");
                }
            }
            Self::Fill(apply) => {
                for declaration in value["declarations"].as_array().into_iter().flatten() {
                    let kind = if declaration["kind"] == "secrets" {
                        "secret"
                    } else {
                        "env"
                    };
                    let action = declaration["action"].as_str().unwrap_or("not set");
                    let _ = writeln!(out, "{kind} {}: {action}", text(declaration, "name", ""));
                }
                if value["apply_required"] == true {
                    let _ = writeln!(out, "Run `{apply}` to apply changes.");
                }
            }
        }
        out
    }
}

pub(super) fn lifecycle_outcome(value: &Value) -> String {
    let configuration = match value["outcome"]["configuration"].as_str() {
        Some("applied") => "Configuration applied. ",
        Some("unchanged") => "Configuration already applied. ",
        _ => "",
    };
    let vm = match value["outcome"]["vm"].as_str() {
        Some("started") => "App started.",
        Some("restarted") => "App restarted.",
        Some("stopped") => "App stopped.",
        Some("remained_stopped") => "App remains stopped.",
        Some("remained_running") => "App remains running.",
        // backcompat: 0.43.0-rc.4 cloud responses did not include lifecycle outcomes.
        _ => "Cloud operation completed.",
    };
    format!("{configuration}{vm}")
}

fn text<'a>(value: &'a Value, key: &str, fallback: &'a str) -> &'a str {
    value[key].as_str().unwrap_or(fallback)
}

/// Unix milliseconds as UTC time.
fn time(ms: &Value) -> String {
    ms.as_i64()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map_or_else(
            || "unknown time".to_owned(),
            |time| time.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        )
}

/// Health check of a started Machine and a failing scheduled wake-up.
fn health(out: &mut String, value: &Value) {
    let health = &value["health"];
    if let Some(status) = health["status"].as_str() {
        if health["critical_since"].is_null() {
            let _ = writeln!(out, "Health: {status}");
        } else {
            let _ = writeln!(
                out,
                "Health: {status} since {}",
                time(&health["critical_since"])
            );
        }
    }
    let wake = &value["wake_failure"];
    if wake.is_object() {
        let _ = writeln!(
            out,
            "Wake-up failing: {} attempts since {}, next retry at {}",
            wake["attempts"],
            time(&wake["wake_at"]),
            time(&wake["retry_after"])
        );
        if let Some(error) = wake["error"].as_str() {
            let _ = writeln!(out, "Wake-up error: {error}");
        }
    }
}

fn app(out: &mut String, value: &Value) {
    app_with_note(out, value, None);
}

fn app_with_note(out: &mut String, value: &Value, note: Option<&str>) {
    let _ = write!(
        out,
        "{}: {}",
        text(value, "app_name", "App"),
        text(value, "status", "unknown")
    );
    let _ = match note {
        Some(note) => writeln!(out, " ({note})"),
        None => writeln!(out),
    };
    if let Some(desired) = value["desired_state"].as_str()
        && value["status"] != desired
    {
        if matches!(value["status"].as_str(), Some("running" | "stopped")) {
            let _ = writeln!(
                out,
                "Intended state: {desired}. Run `obelisk cloud app --app {} apply` to reach it.",
                text(value, "app_name", "")
            );
        } else {
            let _ = writeln!(out, "Intended state: {desired}");
        }
    }
    if let Some(url) = value["webhook_url"].as_str() {
        let _ = writeln!(out, "URL: {url}");
    }
    if value["potentially_stale"] == true {
        let name = text(value, "app_name", "");
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

fn config_revisions(out: &mut String, value: &Value) {
    let app = &value["app"];
    if let Some(revision) = app["revision"].as_u64() {
        let applied = app["applied_revision"].as_u64();
        let _ = writeln!(
            out,
            "Configuration: staged revision {revision}, {}",
            match applied {
                Some(applied) if applied == revision => format!("applied revision {applied}"),
                Some(applied) => format!("applied revision {applied} (apply pending)"),
                None => "not applied yet".to_owned(),
            }
        );
    }
    let local = &value["local_config"];
    let path = text(local, "path", "app.toml");
    if let Some(error) = local["error"].as_str() {
        let _ = writeln!(out, "{path}: cannot compare ({error})");
    } else if let Some(changes) = local["comparison"]["changes"].as_array() {
        if changes.is_empty() {
            let _ = writeln!(out, "{path}: matches the staged configuration");
        } else {
            let changes = changes
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            let _ = writeln!(
                out,
                "{path}: differs from the staged configuration: {changes}. Run `obelisk cloud app --app {} stage` to stage it.",
                text(app, "app_name", "")
            );
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn status_shows_revisions_and_local_differences() {
        let status = |applied: Value, local: Value| {
            Summary::Status.render(&json!({
                "app":{"app_name":"demo","status":"running","revision":3,"applied_revision":applied},
                "local_config":local,
            }))
        };
        let out = status(
            json!(2),
            json!({"path":"app.toml","comparison":{"changes":["/cloud/memory_mb"]}}),
        );
        assert!(
            out.contains("staged revision 3, applied revision 2 (apply pending)"),
            "{out}"
        );
        assert!(
            out.contains("app.toml: differs from the staged configuration: /cloud/memory_mb"),
            "{out}"
        );
        let out = status(
            json!(3),
            json!({"path":"app.toml","comparison":{"changes":[]}}),
        );
        assert!(
            out.contains("staged revision 3, applied revision 3\n"),
            "{out}"
        );
        assert!(
            out.contains("app.toml: matches the staged configuration"),
            "{out}"
        );
        let out = status(Value::Null, Value::Null);
        assert!(out.contains("not applied yet"), "{out}");
        assert!(!out.contains("app.toml"), "{out}");
    }

    #[test]
    fn status_reports_idle_suspension() {
        let status = |app_status: &str, machine: &str, api_ready: Value| {
            Summary::Status.render(&json!({
                "app":{"app_name":"demo","status":app_status},
                "machine_state":machine,
                "api_ready":api_ready,
            }))
        };
        let out = status("running", "suspended", Value::Null);
        assert!(out.starts_with("demo: running (suspended)\n"), "{out}");
        assert!(out.contains("API: resumes on request\n"), "{out}");
        assert!(!out.contains("reconcile"), "{out}");
        let out = status("running", "started", json!(true));
        assert!(out.starts_with("demo: running\n"), "{out}");
        assert!(out.contains("API: ready\n"), "{out}");
        let out = status("running", "stopped", json!(false));
        assert!(
            out.contains("VM is stopped although the app is running."),
            "{out}"
        );
        let out = status("stopped", "suspended", Value::Null);
        assert!(out.starts_with("demo: stopped\n"), "{out}");
        assert!(out.contains("API: unavailable\n"), "{out}");
    }

    #[test]
    fn status_reports_health_and_wake_failure() {
        let out = Summary::Status.render(&json!({
            "app":{"app_name":"demo","status":"running"},
            "machine_state":"started",
            "api_ready":false,
            "health":{"status":"critical","critical_since":1_762_180_000_000_u64},
            "wake_failure":{"wake_at":1_762_180_000_000_u64,"attempts":3,"retry_after":1_762_180_060_000_u64,"error":"host unavailable"},
        }));
        assert!(
            out.contains("Health: critical since 2025-11-03 14:26:40 UTC\n"),
            "{out}"
        );
        assert!(
            out.contains("Wake-up failing: 3 attempts since 2025-11-03 14:26:40 UTC, next retry at 2025-11-03 14:27:40 UTC\nWake-up error: host unavailable\n"),
            "{out}"
        );
        let out = Summary::Status.render(&json!({
            "app":{"app_name":"demo","status":"running"},
            "machine_state":"started",
            "health":{"status":"passing","critical_since":null},
            "wake_failure":null,
        }));
        assert!(out.contains("Health: passing\n"), "{out}");
        assert!(!out.contains("Wake-up"), "{out}");
        let out = Summary::Status.render(&json!({
            "app":{"app_name":"demo","status":"running"},
            "machine_state":"suspended",
            "health":null,
        }));
        assert!(!out.contains("Health"), "{out}");
    }

    #[test]
    fn events_list_newest_first() {
        let out = Summary::Events.render(&json!({"events":[
            {"event_id":2,"created_at":1_762_180_060_000_u64,"level":"info","code":"machine.health.recovered","message":"Health check recovered after 60 s","details":null},
            {"event_id":1,"created_at":1_762_180_000_000_u64,"level":"warn","code":"machine.health.critical","message":"Health check is critical","details":{}},
        ]}));
        assert_eq!(
            out,
            "2025-11-03 14:27:40 UTC  info   Health check recovered after 60 s  (machine.health.recovered)\n\
             2025-11-03 14:26:40 UTC  warn   Health check is critical  (machine.health.critical)\n"
        );
        assert_eq!(
            Summary::Events.render(&json!({"events":[]})),
            "No events.\n"
        );
    }

    #[test]
    fn stage_reports_unchanged_revision() {
        let staged = Summary::Staged("Configuration staged.", "apply".into());
        assert_eq!(
            staged.render(&json!({"revision":4,"changed":false})),
            "Configuration unchanged at staged revision 4.\n"
        );
        assert!(
            staged
                .render(&json!({"revision":5,"changed":true}))
                .starts_with("Configuration staged as revision 5.")
        );
        assert!(
            staged
                .render(&json!({}))
                .starts_with("Configuration staged.")
        );
    }
}
