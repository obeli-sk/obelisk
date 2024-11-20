use super::super::tree_component::{
    TreeComponent, TreeComponentAction, TreeComponentInner, TreeFactory,
};
use crate::{app::Route, grpc::grpc_client};
use yew::prelude::*;
use yew_router::prelude::Link;
use yewprint::{
    id_tree::{InsertBehavior, Node, NodeId, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq, Clone)]
pub struct HistoryScheduleEventProps {
    pub event: grpc_client::execution_event::history_event::Schedule,
}

impl TreeFactory for HistoryScheduleEventProps {
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
        let scheduled_execution_id = self
            .event
            .execution_id
            .clone()
            .expect("`execution_id` is sent by the server");
        let event_type = tree
            .insert(
                Node::new(NodeData {
                    icon: Icon::FolderClose,
                    label: html!{<>
                        {"Scheduled execution "}
                        <Link<Route> to={Route::ExecutionDetail { execution_id: scheduled_execution_id.clone() } }>{scheduled_execution_id}</Link<Route>>
                    </>},
                    has_caret: true,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&root_id),
            )
            .unwrap();

        let scheduled_at = self
            .event
            .scheduled_at
            .expect("`scheduled_at` is sent by the server");
        tree.insert(
            Node::new(NodeData {
                icon: Icon::Time,
                label: format!("At: {scheduled_at:?}").into_html(),
                has_caret: false,
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&event_type),
        )
        .unwrap();
        TreeData::from(tree)
    }
}

pub struct HistoryScheduleEvent {
    inner: TreeComponentInner<HistoryScheduleEvent>,
}

impl Component for HistoryScheduleEvent {
    type Message = TreeComponentAction;
    type Properties = HistoryScheduleEventProps;

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

impl TreeComponent for HistoryScheduleEvent {
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
