//! Wire types generated from the authoritative lifecycle `OpenAPI` contract.

#[allow(clippy::all, clippy::pedantic)]
mod generated {
    include!("lifecycle_generated.rs");
}

pub use generated::*;

pub const OPENAPI: &str = include_str!("../../../assets/openapi/lifecycle.json");

#[must_use]
pub fn openapi() -> utoipa::openapi::OpenApi {
    serde_json::from_str(OPENAPI).expect("valid lifecycle OpenAPI contract")
}

impl utoipa::PartialSchema for AppPolicyV1 {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        openapi()
            .components
            .expect("lifecycle components")
            .schemas
            .remove("AppPolicyV1")
            .expect("app policy schema")
    }
}

impl utoipa::ToSchema for AppPolicyV1 {}
