use crate::grpc::grpc_client;
use log::debug;
use std::ops::Deref;
use yew::prelude::*;
use yewprint::{
    id_tree::{InsertBehavior, Node, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq)]
pub struct LockedProps {
    pub locked: grpc_client::execution_event::Locked,
}

#[function_component(LockedEvent)]
pub fn locked_event(LockedProps { locked }: &LockedProps) -> Html {
    debug!("<LockedEvent /> render");
    let tree_state = use_state(|| {
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
    });

    let expand_collapse = {
        let tree_state = tree_state.clone();
        Callback::from(move |(node_id, _)| {
            let mut tree = tree_state.deref().clone();
            {
                let mut tree = tree.borrow_mut();
                let node = tree.get_mut(&node_id).unwrap();
                let data = node.data_mut();
                data.is_expanded ^= true;
            }
            tree_state.set(tree); // Setter must be called to notify yew that we request render
        })
    };
    html! {
        <yewprint::Tree<u32>
            tree={tree_state.deref()}
            on_collapse={Some(expand_collapse.clone())}
            on_expand={Some(expand_collapse.clone())}
            onclick={Some(expand_collapse.clone())}
        />
    }
}
