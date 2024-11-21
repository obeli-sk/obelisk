use crate::{components::execution_detail::tree_component::TreeComponent, grpc::grpc_client};
use chrono::DateTime;
use yew::prelude::*;
use yewprint::{
    id_tree::{InsertBehavior, Node, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq, Clone)]
pub struct IntermittentlyFailedEventProps {
    pub event: grpc_client::execution_event::IntermittentlyFailed,
}

impl IntermittentlyFailedEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(Node::new(NodeData::default()), InsertBehavior::AsRoot)
            .unwrap();

        let failed_node = tree
            .insert(
                Node::new(NodeData {
                    icon: Icon::Error,
                    label: "Intermittently Failed".into_html(),
                    has_caret: true,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&root_id),
            )
            .unwrap();

        // Add reason node
        tree.insert(
            Node::new(NodeData {
                icon: Icon::Comment,
                label: format!("Reason: {}", self.event.reason).into_html(),
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&failed_node),
        )
        .unwrap();

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

#[function_component(IntermittentlyFailedEvent)]
pub fn intermittently_failed_event(props: &IntermittentlyFailedEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
