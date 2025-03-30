use crate::app::Route;

use super::grpc_client::{self, ExecutionId};
use std::{fmt::Display, hash::Hash, str::FromStr};
use yew::{html, Html, ToHtml};
use yew_router::prelude::Link;

impl Eq for grpc_client::ExecutionId {}

impl Hash for grpc_client::ExecutionId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

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

    fn render_execution_parts(
        &self,
        hide_parents: bool,
        route_fn: fn(ExecutionId) -> Route,
    ) -> Html;
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

    fn render_execution_parts(
        &self,
        hide_parents: bool,
        route_fn: fn(ExecutionId) -> Route,
    ) -> Html {
        let mut execution_id_vec = self.as_hierarchy();
        if hide_parents {
            execution_id_vec.drain(..execution_id_vec.len() - 1);
        }
        execution_id_vec
            .into_iter()
            .enumerate()
            .map(|(idx, (part, execution_id))| {
                html! {<>
                    if idx > 0 {
                        {EXECUTION_ID_INFIX}
                    }
                    <Link<Route> to={route_fn(execution_id)}>
                        {part}
                    </Link<Route>>
                </>}
            })
            .collect::<Vec<_>>()
            .to_html()
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
