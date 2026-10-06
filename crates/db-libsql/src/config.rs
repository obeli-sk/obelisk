use secrecy::SecretString;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct LibsqlConfig {
    pub url: String,
    pub auth_token: SecretString,
    pub queue_capacity: usize,
    pub request_timeout: Duration,
    pub metrics_threshold: Option<Duration>,
}
