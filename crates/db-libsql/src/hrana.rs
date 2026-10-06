//! Client of the Hrana over HTTP `/v3/pipeline` endpoint. Every request closes its stream, so no
//! baton ever leaves this module and a request loss cannot strand a server-side transaction.
use crate::LibsqlConfig;
use base64::Engine as _;
use db_sqlite::sql_connection::{
    BatchStep, Condition, PipelineError, QueryResult, SqlParams, SqlPipeline, StepOutcome,
};
use rusqlite::types::Value;
use secrecy::{ExposeSecret as _, SecretString};
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;

const BASE64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD_NO_PAD;

pub struct HranaClient {
    runtime: tokio::runtime::Runtime,
    http: reqwest::Client,
    endpoint: String,
    auth_token: SecretString,
    timeout: Duration,
}

#[derive(Deserialize)]
struct PipelineResponse {
    results: Vec<PipelineResult>,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum PipelineResult {
    Ok { response: serde_json::Value },
    Error { error: StreamError },
}
#[derive(Deserialize)]
struct StreamError {
    message: String,
    code: Option<String>,
}
#[derive(Deserialize)]
struct BatchResult {
    step_results: Vec<Option<StatementResult>>,
    step_errors: Vec<Option<StreamError>>,
}
#[derive(Deserialize)]
struct StatementResult {
    cols: Vec<Column>,
    rows: Vec<Vec<HranaValue>>,
    affected_row_count: usize,
}
#[derive(Deserialize)]
struct Column {
    name: Option<String>,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum HranaValue {
    Null,
    Integer { value: String },
    Float { value: f64 },
    Text { value: String },
    Blob { base64: String },
}
impl HranaValue {
    fn into_value(self) -> Result<Value, PipelineError> {
        Ok(match self {
            Self::Null => Value::Null,
            Self::Integer { value } => Value::Integer(value.parse().map_err(|err| {
                PipelineError::Failed(format!("invalid integer `{value}` - {err}"))
            })?),
            Self::Float { value } => Value::Real(value),
            Self::Text { value } => Value::Text(value),
            Self::Blob { base64 } => Value::Blob(
                BASE64
                    .decode(base64.trim_end_matches('='))
                    .map_err(|err| PipelineError::Failed(format!("invalid base64 - {err}")))?,
            ),
        })
    }
}

fn value_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => json!({"type": "null"}),
        Value::Integer(v) => json!({"type": "integer", "value": v.to_string()}),
        Value::Real(v) => json!({"type": "float", "value": v}),
        Value::Text(v) => json!({"type": "text", "value": v}),
        Value::Blob(v) => json!({"type": "blob", "base64": BASE64.encode(v)}),
    }
}
fn condition_json(condition: &Condition) -> serde_json::Value {
    match condition {
        Condition::Ok(step) => json!({"type": "ok", "step": step}),
        Condition::Not(cond) => json!({"type": "not", "cond": condition_json(cond)}),
        Condition::And(conds) => {
            json!({"type": "and", "conds": conds.iter().map(condition_json).collect::<Vec<_>>()})
        }
        Condition::IsAutocommit => json!({"type": "is_autocommit"}),
    }
}
fn stmt_json(sql: &str, params: &SqlParams, want_rows: bool) -> serde_json::Value {
    let mut stmt = json!({"sql": sql, "want_rows": want_rows});
    match params {
        SqlParams::None => {}
        SqlParams::Positional(values) => {
            stmt["args"] = values.iter().map(value_json).collect();
        }
        SqlParams::Named(values) => {
            stmt["named_args"] = values
                .iter()
                .map(|(name, value)| json!({"name": name, "value": value_json(value)}))
                .collect();
        }
    }
    stmt
}

impl HranaClient {
    pub fn new(config: &LibsqlConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // `reqwest` is built without a default crypto provider.
        let _ = rustls::crypto::ring::default_provider().install_default();
        // Poll HTTP connection tasks even while the SQL worker waits for commands.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let base = config.url.trim_end_matches('/');
        let base = base
            .strip_prefix("libsql://")
            .map_or_else(|| base.to_owned(), |host| format!("https://{host}"));
        Ok(Self {
            runtime,
            http: reqwest::Client::builder().build()?,
            endpoint: format!("{base}/v3/pipeline"),
            auth_token: config.auth_token.clone(),
            timeout: config.request_timeout,
        })
    }

    fn pipeline(&self, request: &serde_json::Value) -> Result<PipelineResult, PipelineError> {
        let mut builder = self.http.post(&self.endpoint).timeout(self.timeout);
        let token = self.auth_token.expose_secret();
        if !token.is_empty() {
            builder = builder.bearer_auth(token);
        }
        let body = serde_json::to_vec(&json!({"requests": [request, {"type": "close"}]}))
            .map_err(|err| PipelineError::Failed(format!("cannot serialize request - {err}")))?;
        let response = self.runtime.block_on(async {
            let response = builder
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await?;
            let status = response.status();
            Ok::<_, reqwest::Error>((status, response.bytes().await?))
        });
        let (status, body) = response
            .map_err(|err| PipelineError::Failed(format!("libSQL request failed - {err}")))?;
        if !status.is_success() {
            return Err(PipelineError::Failed(format!(
                "libSQL responded with {status}: {}",
                String::from_utf8_lossy(&body)
            )));
        }
        let response: PipelineResponse = serde_json::from_slice(&body)
            .map_err(|err| PipelineError::Failed(format!("invalid libSQL response - {err}")))?;
        response
            .results
            .into_iter()
            .next()
            .ok_or_else(|| PipelineError::Failed("libSQL response has no results".to_string()))
    }

    /// Runs a script of statements, stopping at the first failure. An unfinished transaction is
    /// discarded when the stream closes, so `BEGIN` without `COMMIT` is safe here.
    pub(crate) fn sequence(&self, sql: &str) -> Result<(), PipelineError> {
        match self.pipeline(&json!({"type": "sequence", "sql": sql}))? {
            PipelineResult::Ok { .. } => Ok(()),
            PipelineResult::Error { error } => Err(PipelineError::Failed(error.message)),
        }
    }

    pub(crate) fn query(&self, sql: &str) -> Result<QueryResult, PipelineError> {
        let steps = [BatchStep {
            sql: sql.to_owned(),
            params: SqlParams::None,
            want_rows: true,
            condition: None,
        }];
        match self.batch(&steps)?.pop() {
            Some(StepOutcome::Done(result)) => Ok(result),
            Some(StepOutcome::Error { message, .. }) => Err(PipelineError::Failed(message)),
            _ => Err(PipelineError::Failed(
                "libSQL did not run the query".to_string(),
            )),
        }
    }
}

impl SqlPipeline for HranaClient {
    fn batch(&self, steps: &[BatchStep]) -> Result<Vec<StepOutcome>, PipelineError> {
        let steps_json = steps
            .iter()
            .map(|step| {
                let mut json = json!({"stmt": stmt_json(&step.sql, &step.params, step.want_rows)});
                if let Some(condition) = &step.condition {
                    json["condition"] = condition_json(condition);
                }
                json
            })
            .collect::<Vec<_>>();
        let request = json!({"type": "batch", "batch": {"steps": steps_json}});
        let response = match self.pipeline(&request)? {
            PipelineResult::Ok { response } => response,
            PipelineResult::Error { error }
                if error.code.as_deref() == Some("RESPONSE_TOO_LARGE") =>
            {
                return Err(PipelineError::ResponseTooLarge);
            }
            PipelineResult::Error { error } => {
                return Err(PipelineError::Failed(format!(
                    "libSQL rejected the batch: {} ({})",
                    error.message,
                    error.code.unwrap_or_default()
                )));
            }
        };
        let result: BatchResult = serde_json::from_value(response["result"].clone())
            .map_err(|err| PipelineError::Failed(format!("invalid libSQL batch result - {err}")))?;
        result
            .step_results
            .into_iter()
            .zip(result.step_errors)
            .map(|(result, error)| match (result, error) {
                (_, Some(error)) => Ok(StepOutcome::Error {
                    code: error.code.unwrap_or_default(),
                    message: error.message,
                }),
                (Some(result), None) => Ok(StepOutcome::Done(QueryResult {
                    columns: result
                        .cols
                        .into_iter()
                        .map(|column| column.name.unwrap_or_default())
                        .collect(),
                    rows: result
                        .rows
                        .into_iter()
                        .map(|row| row.into_iter().map(HranaValue::into_value).collect())
                        .collect::<Result<_, _>>()?,
                    affected: result.affected_row_count,
                })),
                (None, None) => Ok(StepOutcome::Skipped),
            })
            .collect()
    }
}
