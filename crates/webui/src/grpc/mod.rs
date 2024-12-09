use grpc_client::{result_detail, ComponentType};

pub mod execution_id;
pub mod ffqn;
pub mod function_detail;
pub mod grpc_client;
pub mod ifc_fqn;
pub mod join_set_id;

pub const NAMESPACE_OBELISK: &str = "obelisk"; // TODO: unify with concepts
pub const SUFFIX_PKG_EXT: &str = "-obelisk-ext"; // TODO: unify with concepts

pub trait ResultValueExt {
    fn is_ok(&self) -> bool;
    fn is_err(&self) -> bool;
}

impl ResultValueExt for result_detail::Value {
    fn is_ok(&self) -> bool {
        matches!(self, result_detail::Value::Ok(_))
    }

    fn is_err(&self) -> bool {
        !self.is_ok()
    }
}

impl yew::ToHtml for ComponentType {
    fn to_html(&self) -> yew::Html {
        match self {
            ComponentType::Workflow => "Workflow",
            ComponentType::ActivityWasm => "Activity",
            ComponentType::WebhookWasm => "Webhook",
        }
        .to_html()
    }
}
