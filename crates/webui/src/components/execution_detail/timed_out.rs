use crate::{components::execution_detail::tree_component::TreeComponent, grpc::grpc_client};
use chrono::DateTime;
use yew::prelude::*;
use yewprint::{
    id_tree::{InsertBehavior, Node, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq, Clone)]
pub struct IntermittentlyTimedOutEventProps {
    pub event: grpc_client::execution_event::IntermittentlyTimedOut,
}

impl IntermittentlyTimedOutEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(
                Node::new(NodeData {
                    data: 0_u32,
                    ..Default::default()
                }),
                InsertBehavior::AsRoot,
            )
            .unwrap();

        let timed_out_node = tree
            .insert(
                Node::new(NodeData {
                    icon: Icon::Time,
                    label: "Intermittently Timed Out".into_html(),
                    has_caret: self.event.backoff_expires_at.is_some(),
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&root_id),
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
                icon: Icon::Calendar,
                label: format!("Backoff Expires At: {}", backoff_expires_at).into_html(),
                has_caret: false,
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&timed_out_node),
        )
        .unwrap();

        TreeData::from(tree)
    }
}

#[function_component(IntermittentlyTimedOutEvent)]
pub fn intermittently_timed_out_event(props: &IntermittentlyTimedOutEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}