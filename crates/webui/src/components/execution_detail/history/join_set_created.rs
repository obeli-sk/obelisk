use crate::{
    app::{BacktraceVersions, Route},
    components::execution_detail::tree_component::TreeComponent,
    grpc::{
        grpc_client::{self, ExecutionId},
        version::VersionType,
    },
};
use yew::prelude::*;
use yew_router::prelude::Link;
use yewprint::{
    id_tree::{InsertBehavior, Node, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq, Clone)]
pub struct HistoryJoinSetCreatedEventProps {
    pub event: grpc_client::execution_event::history_event::JoinSetCreated,
    pub version: VersionType,
    pub is_selected: bool,
    pub execution_id: ExecutionId,
    pub backtrace_id: Option<VersionType>,
}

impl HistoryJoinSetCreatedEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(Node::new(NodeData::default()), InsertBehavior::AsRoot)
            .unwrap();

        // Add node for JoinSet ID
        let join_set_id = &self
            .event
            .join_set_id
            .as_ref()
            .expect("join_set_id must be sent");
        let join_set_created_node_id = tree
            .insert(
                Node::new(NodeData {
                    icon: Icon::History,
                    label: html! {
                        <>
                            {self.version}
                            {". Join Set Created: `"}
                            {join_set_id}
                            {"`"}
                        </>
                    },
                    has_caret: self.backtrace_id.is_some(),
                    is_selected: self.is_selected,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&root_id),
            )
            .unwrap();
        // TODO: Add closing strategy when cancellation is supported.

        if let Some(backtrace_id) = self.backtrace_id {
            tree.insert(
                Node::new(NodeData {
                    icon: Icon::Flows,
                    label: html! {
                        <Link<Route> to={Route::ExecutionDebuggerWithVersions { execution_id: self.execution_id.clone(), versions: BacktraceVersions::from(backtrace_id) } }>
                            {"Backtrace"}
                        </Link<Route>>
                    },
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&join_set_created_node_id),
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
