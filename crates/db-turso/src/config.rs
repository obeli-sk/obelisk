use secrecy::SecretString;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransactionMode {
    #[default]
    Immediate,
    Concurrent,
}

#[derive(Debug, Clone)]
pub struct TursoConfig {
    pub url: String,
    pub auth_token: SecretString,
    pub queue_capacity: usize,
    pub transaction_mode: TransactionMode,
    pub request_timeout: Duration,
    pub metrics_threshold: Option<Duration>,
    /// Table prefix for isolated integration-test databases; empty in production.
    pub namespace: String,
}
