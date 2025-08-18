use crate::app::BacktraceVersions;
use crate::grpc::grpc_client::ExecutionId;
use crate::grpc::version::VersionType;
use crate::{
    app::Route, components::execution_detail::tree_component::TreeComponent, grpc::grpc_client,
};
use yew::prelude::*;
use yew_router::prelude::Link;
use yewprint::{
    Icon, NodeData, TreeData,
    id_tree::{InsertBehavior, Node, TreeBuilder},
};

#[derive(Properties, PartialEq, Clone)]
pub struct HistoryJoinNextTooManyEventProps {
    pub event: grpc_client::execution_event::history_event::JoinNextTooMany,
    pub execution_id: ExecutionId,
    pub backtrace_id: Option<VersionType>,
    pub version: VersionType,
    pub is_selected: bool,
}

impl HistoryJoinNextTooManyEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(Node::new(NodeData::default()), InsertBehavior::AsRoot)
            .unwrap();

        // Add node for JoinSet ID and details
        let join_set_id = self
            .event
            .join_set_id
            .as_ref()
            .expect("JoinNextTooMany.join_set_id is sent");

        let icon = Icon::Error;

        let join_next_node = tree
            .insert(
                Node::new(NodeData {
                    icon,
                    label: html! {
                        <>
                            {self.version}
                            {". Join Next (more than submissions) : `"}
                            {join_set_id}
                            {"`"}
                        </>
                    },
                    has_caret: true,
                    is_selected: self.is_selected,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&root_id),
            )
            .unwrap();

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
                InsertBehavior::UnderNode(&join_next_node),
            )
            .unwrap();
        }
        TreeData::from(tree)
    }
}

#[function_component(HistoryJoinNextTooManyEvent)]
pub fn history_join_next_too_many_event(props: &HistoryJoinNextTooManyEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
