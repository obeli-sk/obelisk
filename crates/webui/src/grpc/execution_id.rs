use super::grpc_client;
use std::{fmt::Display, str::FromStr};
use yew::{html, ToHtml};

impl Display for grpc_client::ExecutionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl ToHtml for grpc_client::ExecutionId {
    fn to_html(&self) -> yew::Html {
        html! { &self.id }
    }
}

impl FromStr for grpc_client::ExecutionId {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(grpc_client::ExecutionId { id: s.to_string() })
    }
}
