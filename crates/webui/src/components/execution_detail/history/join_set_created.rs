use crate::{components::execution_detail::tree_component::TreeComponent, grpc::grpc_client};
use yew::prelude::*;
use yewprint::{
    id_tree::{InsertBehavior, Node, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq, Clone)]
pub struct HistoryJoinSetCreatedEventProps {
    pub event: grpc_client::execution_event::history_event::JoinSetCreated,
}

impl HistoryJoinSetCreatedEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(Node::new(NodeData::default()), InsertBehavior::AsRoot)
            .unwrap();

        // Add node for JoinSet ID
        if let Some(join_set_id) = &self.event.join_set_id {
            tree.insert(
                Node::new(NodeData {
                    icon: Icon::Box,
                    label: html! {
                        <>
                            {"Join Set Created: "}
                            {&join_set_id.id}
                        </>
                    },
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&root_id),
            )
            .unwrap();
        }

        TreeData::from(tree)
    }
}

#[function_component(HistoryJoinSetCreatedEvent)]
pub fn history_join_set_created_event(props: &HistoryJoinSetCreatedEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
