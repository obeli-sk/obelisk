use crate::{components::execution_detail::tree_component::TreeComponent, grpc::grpc_client};
use yew::prelude::*;
use yewprint::{
    id_tree::{InsertBehavior, Node, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq, Clone)]
pub struct LockedEventProps {
    pub locked: grpc_client::execution_event::Locked,
}

impl LockedEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let locked = &self.locked;
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
        let event_type = tree
            .insert(
                Node::new(NodeData {
                    icon: Icon::FolderClose,
                    label: "Locked".into(),
                    has_caret: true,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&root_id),
            )
            .unwrap();
        tree.insert(
            Node::new(NodeData {
                icon: Icon::Time,
                label: html! { {format!("Expires at {}", locked.lock_expires_at.expect("`lock_expires_at` is sent by the server"))} },
                has_caret: false,
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&event_type),
        )
        .unwrap();
        TreeData::from(tree)
    }
}

#[function_component(LockedEvent)]
pub fn locked_event(props: &LockedEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
