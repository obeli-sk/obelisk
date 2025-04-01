use crate::{
    components::execution_detail::tree_component::TreeComponent,
    grpc::{grpc_client, version::VersionType},
};
use chrono::DateTime;
use yew::prelude::*;
use yewprint::{
    id_tree::{InsertBehavior, Node, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq, Clone)]
pub struct TemporarilyFailedEventProps {
    pub event: grpc_client::execution_event::TemporarilyFailed,
    pub version: VersionType,
}

impl TemporarilyFailedEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(Node::new(NodeData::default()), InsertBehavior::AsRoot)
            .unwrap();

        let failed_node = tree
            .insert(
                Node::new(NodeData {
                    icon: Icon::Error,
                    label: format!("{}. Temporarily Failed", self.version).into_html(),
                    has_caret: true,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&root_id),
            )
            .unwrap();

        // Add reason node
        tree.insert(
            Node::new(NodeData {
                icon: Icon::Error,
                label: self.event.reason.as_str().into_html(),
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&failed_node),
        )
        .unwrap();

        // Add detail
        if let Some(detail) = &self.event.detail {
            tree.insert(
                Node::new(NodeData {
                    icon: Icon::List,
                    label: format!("detail: {detail}").into_html(),
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&failed_node),
            )
            .unwrap();
        }

        // Add backoff expiration
        let backoff_expires_at = DateTime::from(
            self.event
                .backoff_expires_at
                .expect("`backoff_expires_at` is sent by the server"),
        );
        tree.insert(
            Node::new(NodeData {
                icon: Icon::Time,
                label: format!("Backoff Expires At: {}", backoff_expires_at).into_html(),
                has_caret: false,
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&failed_node),
        )
        .unwrap();

        TreeData::from(tree)
    }
}

#[function_component(TemporarilyFailedEvent)]
pub fn temporarily_failed_event(props: &TemporarilyFailedEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
