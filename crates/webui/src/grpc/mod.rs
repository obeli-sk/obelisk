use grpc_client::{result_detail, ComponentType};

pub mod execution_id;
pub mod ffqn;
pub mod function_detail;
pub mod grpc_client;
pub mod ifc_fqn;
pub mod join_set_id;
pub mod pkg_fqn;
pub mod version;

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

impl grpc_client::Component {
    pub fn as_type(&self) -> ComponentType {
        ComponentType::try_from(self.r#type)
            .expect("generated ComponentType must contain all types")
    }
}

impl grpc_client::ComponentId {
    pub fn as_type(&self) -> ComponentType {
        let ty = self
            .id
            .split_once(":")
            .expect("ComponentId must contain `:` infix")
            .0
            .to_uppercase();
        ComponentType::from_str_name(&ty)
            .unwrap_or_else(|| panic!("ComponentType must be found for {ty}"))
    }
}

impl yew::ToHtml for ComponentType {
    fn to_html(&self) -> yew::Html {
        match self {
            ComponentType::Workflow => "Workflow",
            ComponentType::ActivityWasm => "Activity",
            ComponentType::WebhookEndpoint => "Webhook Endpoint",
        }
        .to_html()
    }
}

impl ComponentType {
    pub fn as_icon(&self) -> yewprint::Icon {
        match self {
            ComponentType::Workflow => yewprint::Icon::GanttChart,
            ComponentType::ActivityWasm => yewprint::Icon::CodeBlock,
            ComponentType::WebhookEndpoint => yewprint::Icon::GlobeNetwork,
        }
    }
}
