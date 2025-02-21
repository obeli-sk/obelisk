use super::grpc_client::{self};
use std::{fmt::Display, str::FromStr};
use yew::{html, ToHtml};

pub trait ExecutionIdExt {
    fn generate() -> grpc_client::ExecutionId {
        let ulid = ulid::Ulid::new();
        grpc_client::ExecutionId {
            id: format!("E_{ulid}"),
        }
    }
    // For the top-level ExecutionId return [(execution_id.to_string(), execution_id)].
    // For a derived ExecutionId, return [(grandparent_id.to_string(), grandparent_id), (parent_index, parent_id), .. (child_index, child_id)].
    fn as_hierarchy(&self) -> Vec<(String, grpc_client::ExecutionId)>;
}

pub const EXECUTION_ID_INFIX: &str = ".";

impl ExecutionIdExt for grpc_client::ExecutionId {
    fn as_hierarchy(&self) -> Vec<(String, grpc_client::ExecutionId)> {
        let mut execution_id = String::new();
        let mut vec = Vec::new();
        for part in self.id.split(EXECUTION_ID_INFIX) {
            execution_id = if execution_id.is_empty() {
                part.to_string()
            } else {
                format!("{execution_id}{EXECUTION_ID_INFIX}{part}")
            };
            vec.push((
                part.to_string(),
                grpc_client::ExecutionId {
                    id: execution_id.clone(),
                },
            ));
        }
        vec
    }
}

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
