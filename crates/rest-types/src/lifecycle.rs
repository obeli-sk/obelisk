//! Wire types generated from the authoritative lifecycle `OpenAPI` contract.

pub use api_types::*;

pub const OPENAPI: &str = include_str!("../../../assets/openapi/lifecycle.json");

#[must_use]
pub fn openapi() -> utoipa::openapi::OpenApi {
    serde_json::from_str(OPENAPI).expect("valid lifecycle OpenAPI contract")
}

pub struct AppPolicySchema;

impl utoipa::PartialSchema for AppPolicySchema {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        openapi()
            .components
            .expect("lifecycle components")
            .schemas
            .remove("AppPolicyV1")
            .expect("app policy schema")
    }
}

impl utoipa::ToSchema for AppPolicySchema {
    fn name() -> std::borrow::Cow<'static, str> {
        "AppPolicyV1".into()
    }
}
