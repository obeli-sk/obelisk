use crate::{
    components::execution_detail::tree_component::TreeComponent,
    grpc::{grpc_client, version::VersionType},
};
use yew::prelude::*;
use yewprint::{
    id_tree::{InsertBehavior, Node, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq, Clone)]
pub struct UnlockedEventProps {
    pub event: grpc_client::execution_event::Unlocked,
    pub version: VersionType,
    pub is_selected: bool,
}

impl UnlockedEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(Node::new(NodeData::default()), InsertBehavior::AsRoot)
            .unwrap();

        let unlocked_node_id = tree
            .insert(
                Node::new(NodeData {
                    icon: Icon::Unlock,
                    label: format!("{}. Execution Unlocked", self.version).into_html(),
                    is_selected: self.is_selected,
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
            InsertBehavior::UnderNode(&unlocked_node_id),
        )
        .unwrap();

        TreeData::from(tree)
    }
}

#[function_component(UnlockedEvent)]
pub fn unlocked_event(props: &UnlockedEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
