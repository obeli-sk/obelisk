use crate::{components::execution_detail::tree_component::TreeComponent, grpc::grpc_client};
use chrono::DateTime;
use yew::prelude::*;
use yewprint::{
    id_tree::{InsertBehavior, Node, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq, Clone)]
pub struct HistoryJoinNextEventProps {
    pub event: grpc_client::execution_event::history_event::JoinNext,
}

impl HistoryJoinNextEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(Node::new(NodeData::default()), InsertBehavior::AsRoot)
            .unwrap();

        // Add node for JoinSet ID and details
        if let Some(join_set_id) = &self.event.join_set_id {
            let join_next_node = tree
                .insert(
                    Node::new(NodeData {
                        icon: Icon::Box,
                        label: html! {
                            <>
                                {"Join Next: "}
                                {&join_set_id.id}
                            </>
                        },
                        has_caret: true,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&root_id),
                )
                .unwrap();

            // Add run expiration details
            let expires_at = DateTime::from(
                self.event
                    .run_expires_at
                    .expect("`run_expires_at` is sent by the server"),
            );
            tree.insert(
                Node::new(NodeData {
                    icon: Icon::Time,
                    label: format!("Workflow run expires at: {}", expires_at).into_html(),
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&join_next_node),
            )
            .unwrap();

            // Add closing status
            tree.insert(
                Node::new(NodeData {
                    icon: Icon::Lock,
                    label: format!("Closing: {}", self.event.closing).into_html(),
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&join_next_node),
            )
            .unwrap();
        }

        TreeData::from(tree)
    }
}

#[function_component(HistoryJoinNextEvent)]
pub fn history_join_next_event(props: &HistoryJoinNextEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
