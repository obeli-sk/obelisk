use super::tree_component::{TreeComponent, TreeComponentAction, TreeComponentInner, TreeFactory};
use crate::grpc::grpc_client;
use yew::prelude::*;
use yewprint::{
    id_tree::{InsertBehavior, Node, NodeId, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq, Clone)]
pub struct LockedEventProps {
    pub locked: grpc_client::execution_event::Locked,
}

impl TreeFactory for LockedEventProps {
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

pub struct LockedEvent {
    inner: TreeComponentInner<LockedEvent>,
}

impl Component for LockedEvent {
    type Message = TreeComponentAction;
    type Properties = LockedEventProps;

    fn create(ctx: &Context<Self>) -> Self {
        Self {
            inner: TreeComponentInner::create(ctx),
        }
    }

    fn update(&mut self, ctx: &Context<Self>, msg: Self::Message) -> bool {
        TreeComponent::update(self, ctx, msg)
    }

    fn changed(&mut self, ctx: &Context<Self>, old_props: &Self::Properties) -> bool {
        TreeComponent::changed(self, ctx, old_props)
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        TreeComponent::view(self, ctx)
    }
}

impl TreeComponent for LockedEvent {
    fn tree_get(&self) -> &TreeData<u32> {
        &self.inner.tree
    }

    fn tree_mut(&mut self) -> &mut TreeData<u32> {
        &mut self.inner.tree
    }

    fn tree_set(&mut self, tree: TreeData<u32>) {
        self.inner.tree = tree;
    }

    fn on_expand_node(&self) -> Callback<(NodeId, MouseEvent)> {
        self.inner.on_expand_node.clone()
    }
}
