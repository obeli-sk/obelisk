use crate::args::{
    Cloud, CloudApp, CloudAppTarget, CloudEnv, CloudSecrets, CloudToken, CloudTokenArgs,
};
use serde_json::Value;
use std::fmt::Write as _;

pub(super) enum Summary {
    App(&'static str),
    Apps,
    Lifecycle,
    Status,
    Account,
    Events,
    /// The limit when paging forward from `--since`.
    Logs(Option<u16>),
    Token(Option<&'static str>),
    Tokens,
    Env,
    Secrets,
    Message(&'static str),
    Staged(&'static str, String),
    Unstaged(String),
}

impl From<&Cloud> for Summary {
    fn from(command: &Cloud) -> Self {
        match command {
            Cloud::Auth(_) => unreachable!("command has its own output"),
            Cloud::Status(_) => Self::Account,
            Cloud::Events(_) => Self::Events,
            Cloud::Token(CloudTokenArgs {
                command: CloudToken::List,
                ..
            }) => Self::Tokens,
            Cloud::Token(CloudTokenArgs {
                command: CloudToken::New { .. },
                ..
            }) => Self::Token(Some("Token registered.")),
            Cloud::Token(CloudTokenArgs {
                command: CloudToken::Revoke { .. },
                ..
            }) => Self::Token(Some("Token revoked.")),
            Cloud::Token(CloudTokenArgs {
                command: CloudToken::Get { .. },
                ..
            }) => Self::Token(None),
            Cloud::App(CloudAppTarget {
                command: CloudApp::List,
                ..
            }) => Self::Apps,
            Cloud::App(CloudAppTarget { command, .. }) => {
                let key = match command {
                    CloudApp::Unstage { draft, .. }
                    | CloudApp::Env {
                        command: CloudEnv::Set { draft, .. } | CloudEnv::Unset { draft, .. },
                        ..
                    }
                    | CloudApp::Secrets {
                        command: CloudSecrets::Set { draft, .. } | CloudSecrets::Unset { draft, .. },
                        ..
                    } => draft.key.as_str(),
                    _ => "default",
                };
                let apply = format!(
                    "obelisk cloud app apply{}{}",
                    command
                        .selected_app()
                        .map_or_else(String::new, |app| format!(" --app {app}")),
                    draft_arg(key)
                );
                match command {
                    CloudApp::Status { .. } => Self::Status,
                    CloudApp::Events { .. } => Self::Events,
                    CloudApp::Logs { limit, since, .. } => Self::Logs(since.map(|_| *limit)),
                    CloudApp::New { .. } => Self::App("App created."),
                    CloudApp::Apply { .. } | CloudApp::Enable { .. } | CloudApp::Disable { .. } => {
                        Self::Lifecycle
                    }
                    CloudApp::Delete { .. } => Self::Message("App deleted."),
                    CloudApp::Unstage { .. } => Self::Unstaged(key.to_owned()),
                    CloudApp::Env {
                        command: CloudEnv::List,
                        ..
                    } => Self::Env,
                    CloudApp::Env {
                        command: CloudEnv::Set { .. } | CloudEnv::Unset { .. },
                        ..
                    } => Self::Staged("Environment staged.", apply),
                    CloudApp::Secrets {
                        command: CloudSecrets::List,
                        ..
                    } => Self::Secrets,
                    CloudApp::Secrets {
                        command: CloudSecrets::Set { .. } | CloudSecrets::Unset { .. },
                        ..
                    } => Self::Staged("Secret change staged.", apply),
                    CloudApp::Push(_)
                    | CloudApp::Stage(_)
                    | CloudApp::Deployment { .. }
                    | CloudApp::Execution { .. }
                    | CloudApp::Component { .. }
                    | CloudApp::Admin { .. }
                    | CloudApp::Pull { .. }
                    | CloudApp::Db { .. } => unreachable!("command has its own output"),
                    CloudApp::List => unreachable!("matched above"),
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
                let _ = writeln!(out, "{message}");
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
                        "VM is {machine} although the app is {status}.",
                        status = text(&value["app"], "status", "")
                    );
                }
                config_revisions(&mut out, value);
                if let Some(memory) = value["memory_mb"].as_u64() {
                    let _ = writeln!(out, "Memory: {memory} MB");
                }
                health(&mut out, value);
                let api = match &value["api_ready"] {
                    Value::Bool(true) => "ready",
                    // Not checked: probing would wake the suspended Machine.
                    Value::Null if idle => "resumes on request",
                    _ => "unavailable",
                };
                endpoints(&mut out, &value["app"]["endpoints"], api);
            }
            Self::Account => account(&mut out, value),
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
            Self::Logs(forward) => {
                let logs = value["logs"].as_array().map_or(&[][..], Vec::as_slice);
                for line in logs {
                    let _ = writeln!(
                        out,
                        "{}  {:<5}  {}  {:<6}  {}",
                        text(line, "timestamp", ""),
                        text(line, "level", "-"),
                        text(line, "machine_id", "-"),
                        text(line, "source", "-"),
                        text(line, "message", "")
                    );
                }
                match (logs.last(), forward) {
                    (None, _) => out.push_str("No logs.\n"),
                    (Some(last), Some(limit)) if logs.len() >= usize::from(*limit) => {
                        let _ = writeln!(
                            out,
                            "More lines may follow; continue with `--since {}`.",
                            text(last, "timestamp", "")
                        );
                    }
                    _ => {}
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
                } else if let Some(revision) = value["revision"].as_u64() {
                    let _ = writeln!(
                        out,
                        "{message} Staged revision {revision}; run `{apply}` to apply changes."
                    );
                } else {
                    let _ = writeln!(out, "{message} Run `{apply}` to apply changes.");
                }
                draft_notes(&mut out, value);
            }
            Self::Unstaged(key) => {
                if value["discarded"] == true {
                    let _ = writeln!(out, "Draft `{key}` discarded.");
                } else {
                    let _ = writeln!(out, "No draft `{key}`.");
                }
            }
        }
        out
    }
}

/// ` --draft KEY` for a key other than the default.
pub(super) fn draft_arg(key: &str) -> String {
    if key == "default" {
        String::new()
    } else {
        format!(" --draft {key}")
    }
}

/// `missing` or `unused` values of a draft, as `env NAME` and `secret NAME`.
pub(super) fn draft_values(draft: &Value, which: &str) -> Vec<String> {
    let names = |kind: &str| {
        draft["values"][format!("{which}_{kind}")]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(|name| format!("{} {name}", if kind == "env" { "env" } else { "secret" }))
            .collect::<Vec<_>>()
    };
    let mut values = names("env");
    values.extend(names("secrets"));
    values
}

/// A draft's changes against the applied revision, by name only.
pub(super) fn draft_changes(draft: &Value) -> String {
    let summary = &draft["summary"];
    let mut changes = Vec::new();
    for (key, label) in [
        ("app_toml", "app.toml"),
        ("policy", "policy"),
        ("cloud", "cloud"),
    ] {
        if summary[key] == true {
            changes.push(label.to_owned());
        }
    }
    if let Some(deployment) = draft["deployment_id"].as_str() {
        changes.push(format!("deployment {deployment}"));
    }
    for (kind, label) in [("env", "env"), ("secrets", "secret")] {
        for (diff, sign) in [("added", '+'), ("removed", '-'), ("changed", '~')] {
            for name in summary[kind][diff].as_array().into_iter().flatten() {
                changes.push(format!(
                    "{label} {sign}{}",
                    name.as_str().unwrap_or_default()
                ));
            }
        }
    }
    if changes.is_empty() {
        "no changes".to_owned()
    } else {
        changes.join(", ")
    }
}

fn draft_notes(out: &mut String, draft: &Value) {
    let missing = draft_values(draft, "missing");
    if !missing.is_empty() {
        let _ = writeln!(out, "Missing values: {}", missing.join(", "));
    }
    let unused = draft_values(draft, "unused");
    if !unused.is_empty() {
        let _ = writeln!(out, "Not declared in app.toml: {}", unused.join(", "));
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
        .and_then(|millis| jiff::Timestamp::from_millisecond(millis).ok())
        .map_or_else(
            || "unknown time".to_owned(),
            |time| time.strftime("%Y-%m-%d %H:%M:%S UTC").to_string(),
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

/// Endpoints served by the applied configuration, with the API readiness.
pub(super) fn endpoints(out: &mut String, endpoints: &Value, api: &str) {
    if endpoints.is_null() {
        let _ = writeln!(
            out,
            "API: {api}\nEndpoints are available after the first apply."
        );
        return;
    }
    for (key, label) in [
        ("api", "API"),
        ("webui", "Web UI"),
        ("internal", "Internal server"),
        ("external", "External server"),
    ] {
        if let Some(url) = endpoints[key].as_str() {
            let _ = match key {
                "api" if !api.is_empty() => writeln!(out, "{label}: {url} ({api})"),
                _ => writeln!(out, "{label}: {url}"),
            };
        }
    }
}

/// Plan, hold, apps, this month's allowances and the latest events.
fn account(out: &mut String, value: &Value) {
    let plan = &value["plan"];
    let _ = writeln!(
        out,
        "{} plan, ${} per month",
        text(plan, "name", "Unknown"),
        plan["price_usd"]
    );
    if !value["hold"].is_null() {
        let _ = writeln!(
            out,
            "Apps stopped: {}",
            text(&value["hold"], "message", "the account is on hold")
        );
    }
    let apps = value["apps"].as_array().map_or(&[][..], Vec::as_slice);
    let _ = writeln!(out, "\nApps ({} of {}):", apps.len(), value["apps_allowed"]);
    if apps.is_empty() {
        out.push_str("  none\n");
    }
    for app in apps {
        let status = if app["enabled"] == false && app["status"] != "deleting" {
            "disabled"
        } else {
            text(app, "status", "unknown")
        };
        let _ = writeln!(out, "  {}: {status}", text(app, "app_name", "?"));
    }
    out.push_str("\nAllowances this month:\n");
    for budget in value["budgets"].as_array().map_or(&[][..], Vec::as_slice) {
        let kind = text(budget, "kind", "");
        let (used, allowance) = (
            budget["used"].as_f64().unwrap_or(0.0),
            budget["allowance"].as_f64().unwrap_or(0.0),
        );
        let percent = if allowance > 0.0 {
            used / allowance * 100.0
        } else {
            0.0
        };
        let _ = write!(
            out,
            "  {:<16} {} of {} ({percent:.0}%",
            allowance_label(kind),
            quantity(kind, used),
            quantity(kind, allowance)
        );
        let _ = match text(budget, "state", "ok") {
            "ok" => writeln!(out, ")"),
            state => writeln!(out, ", {state})"),
        };
    }
    if let Some(events) = value["events"].as_array().filter(|e| !e.is_empty()) {
        out.push_str("\nRecent events:\n");
        for event in events {
            let _ = writeln!(
                out,
                "  {}  {:<5}  {}",
                time(&event["created_at"]),
                text(event, "level", ""),
                text(event, "message", "")
            );
        }
    }
}

fn allowance_label(kind: &str) -> &str {
    match kind {
        "memory_gb_hours" => "Memory",
        "vcpu_hours" => "vCPU",
        "llm" => "LLM",
        "storage_class_a" => "Writes and lists",
        "storage_class_b" => "Reads",
        "stored_bytes" => "Stored data",
        "transfer_bytes" => "Transfer",
        other => other,
    }
}

fn quantity(kind: &str, value: f64) -> String {
    match kind {
        "memory_gb_hours" => format!("{value:.1} GB·h"),
        "vcpu_hours" => format!("{value:.1} vCPU·h"),
        "llm" => format!("${value:.2}"),
        "stored_bytes" | "transfer_bytes" => {
            let gib = value / f64::from(1u32 << 30);
            if gib >= 1.0 {
                format!("{gib:.2} GiB")
            } else {
                format!("{:.1} MiB", value / f64::from(1u32 << 20))
            }
        }
        _ => format!("{value:.0}"),
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
        let _ = writeln!(out, "Intended state: {desired}");
    }
    // The provisioner retries reaching the intended state on its own.
    let reconciliation = &value["reconciliation"];
    if let Some(attempts) = reconciliation["attempts"].as_u64().filter(|a| *a > 0) {
        if value["status"] == "failed" {
            // The failure, reported below, carries the last error.
            let _ = writeln!(out, "Reconciliation gave up after {attempts} attempts");
        } else {
            let _ = writeln!(
                out,
                "Reconciling: attempt {attempts}, next at {}",
                time(&reconciliation["next_attempt_at"])
            );
            // A startup failure is reported below with its reason.
            if let Some(error) = reconciliation["error"].as_str()
                && value["state"]["startup_failure"].is_null()
            {
                let _ = writeln!(out, "Reconciliation error: {error}");
            }
        }
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
    let _ = writeln!(
        out,
        "Configuration: applied revision {}",
        app["applied_revision"]
    );
    for draft in value["drafts"].as_array().into_iter().flatten() {
        let key = text(draft, "key", "default");
        if draft["stale"] == true {
            let _ = writeln!(
                out,
                "Draft `{key}`: opened on revision {}, superseded",
                draft["parent_revision"]
            );
        } else if draft["summary"]["changed"] == true {
            let _ = writeln!(
                out,
                "Draft `{key}`: revision {}: {}. Run `obelisk cloud app apply --app {}{}` to apply it.",
                draft["revision"],
                draft_changes(draft),
                text(app, "app_name", ""),
                draft_arg(key)
            );
            draft_notes(out, draft);
        }
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
                "{path}: differs from the staged configuration: {changes}. Run `obelisk cloud app stage --app {}` to stage it.",
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
        .is_some_and(|expiry| expiry <= jiff::Timestamp::now().as_second())
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
        .and_then(|expiry| jiff::Timestamp::from_second(expiry).ok())
    {
        let _ = writeln!(out, "Expires: {expiry}");
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
        let status = |local: Value| {
            Summary::Status.render(&json!({
                "app":{"app_name":"demo","status":"running","revision":3,"applied_revision":2},
                "drafts":[],
                "local_config":local,
            }))
        };
        let out = status(json!({"path":"app.toml","comparison":{"changes":["/cloud/memory_mb"]}}));
        assert!(out.contains("Configuration: applied revision 2\n"), "{out}");
        assert!(
            out.contains("app.toml: differs from the staged configuration: /cloud/memory_mb"),
            "{out}"
        );
        let out = status(json!({"path":"app.toml","comparison":{"changes":[]}}));
        assert!(
            out.contains("app.toml: matches the staged configuration"),
            "{out}"
        );
        let out = status(Value::Null);
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
    fn account_status_lists_hold_apps_and_allowances() {
        let out = Summary::Account.render(&json!({
            "plan":{"name":"Hobby","price_usd":15},"apps_allowed":4,
            "hold":{"reason":"budget.storage_class_a","message":"Writes and lists used up.","created_at":1},
            "from":0,"to":0,
            "budgets":[
                {"kind":"storage_class_a","allowance":400_000,"used":400_100,"state":"exhausted"},
                {"kind":"llm","allowance":4.5,"used":1.2,"state":"ok"},
                {"kind":"stored_bytes","allowance":5_368_709_120_u64,"used":104_857_600,"state":"ok"}],
            "apps":[{"app_name":"demo","status":"stopped","enabled":true},{"app_name":"old","status":"stopped","enabled":false}],
            "events":[{"event_id":1,"created_at":1_762_180_000_000_u64,"level":"info","code":"account.hold.applied","message":"Apps stopped"}]
        }));
        assert_eq!(
            out,
            "Hobby plan, $15 per month\n\
             Apps stopped: Writes and lists used up.\n\
             \n\
             Apps (2 of 4):\n  demo: stopped\n  old: disabled\n\
             \n\
             Allowances this month:\n\
             \x20 Writes and lists 400100 of 400000 (100%, exhausted)\n\
             \x20 LLM              $1.20 of $4.50 (27%)\n\
             \x20 Stored data      100.0 MiB of 5.00 GiB (2%)\n\
             \n\
             Recent events:\n\
             \x20 2025-11-03 14:26:40 UTC  info   Apps stopped\n"
        );
    }

    #[test]
    fn logs_hint_continuation_when_paging_forward() {
        let logs = json!({"logs":[
            {"timestamp":"2026-11-22T15:28:01.468044322Z","level":"info","message":"Machine started in 778ms","machine_id":"84ed09eb490928","region":"ams","source":"runner"},
            {"timestamp":"2026-11-22T15:28:01.548908094Z","level":"info","message":"Server started","machine_id":"84ed09eb490928","region":"ams","source":"app"},
        ],"next":"1795361281548908094"});
        let lines = "2026-11-22T15:28:01.468044322Z  info   84ed09eb490928  runner  Machine started in 778ms\n\
                     2026-11-22T15:28:01.548908094Z  info   84ed09eb490928  app     Server started\n";
        assert_eq!(Summary::Logs(None).render(&logs), lines);
        assert_eq!(Summary::Logs(Some(3)).render(&logs), lines);
        assert_eq!(
            Summary::Logs(Some(2)).render(&logs),
            format!(
                "{lines}More lines may follow; continue with `--since 2026-11-22T15:28:01.548908094Z`.\n"
            )
        );
        assert_eq!(
            Summary::Logs(None).render(&json!({"logs":[],"next":null})),
            "No logs.\n"
        );
    }

    #[test]
    fn stage_reports_unchanged_revision() {
        let staged = Summary::Staged("Configuration staged.", "apply".into());
        assert_eq!(
            staged.render(&json!({"revision":4,"changed":false})),
            "Configuration unchanged at staged revision 4.\n"
        );
        assert_eq!(
            staged.render(&json!({
                "revision":5,"changed":true,
                "values":{"missing_env":["REGION"],"missing_secrets":[],"unused_env":[],"unused_secrets":["OLD"]},
            })),
            "Configuration staged. Staged revision 5; run `apply` to apply changes.\nMissing values: env REGION\nNot declared in app.toml: secret OLD\n"
        );
        assert!(
            staged
                .render(&json!({}))
                .starts_with("Configuration staged.")
        );
    }
}
