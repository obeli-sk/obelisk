use secrecy::SecretString;
use std::sync::Arc;

pub trait SecretResolver: Send + Sync + std::fmt::Debug {
    fn secret_lookup(&self, name: &str) -> Option<SecretString>;
}

/// Declared names and the component-scoped resolver that supplies their values at spawn time.
#[derive(Debug, Clone)]
pub struct ExecSecrets {
    pub names: Vec<String>,
    pub resolver: Arc<dyn SecretResolver>,
}

/// Serializable HTTP-policy boundary for helper processes; it contains no secret values.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ProcessHttpPolicySpec {
    pub component: Vec<ProcessAllowedHostSpec>,
    pub global: Vec<ProcessAllowedHostSpec>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ProcessAllowedHostSpec {
    pub pattern: String,
    pub methods: Vec<String>,
    pub all_methods: bool,
    pub request_url_regex: Option<String>,
    pub secret_names: Vec<String>,
    pub replace_in: Vec<String>,
}
