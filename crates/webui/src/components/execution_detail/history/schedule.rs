use crate::{
    components::{
        execution_detail::tree_component::TreeComponent, execution_header::ExecutionLink,
    },
    grpc::{grpc_client, version::VersionType},
};
use yew::prelude::*;
use yewprint::{
    Icon, NodeData, TreeData,
    id_tree::{InsertBehavior, Node, TreeBuilder},
};

#[derive(Properties, PartialEq, Clone)]
pub struct HistoryScheduleEventProps {
    pub event: grpc_client::execution_event::history_event::Schedule,
    pub version: VersionType,
    pub link: ExecutionLink,
    pub is_selected: bool,
}

impl HistoryScheduleEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(Node::new(NodeData::default()), InsertBehavior::AsRoot)
            .unwrap();
        let scheduled_execution_id = self
            .event
            .execution_id
            .clone()
            .expect("`execution_id` is sent by the server");
        let event_type = tree
            .insert(
                Node::new(NodeData {
                    icon: Icon::History,
                    label: html!{<>
                        { self.version }
                        {". Scheduled execution "}
                        { self.link.link(scheduled_execution_id.clone(), &scheduled_execution_id.id) }
                    </>},
                    has_caret: true,
                    is_selected: self.is_selected,
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
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&event_type),
        )
        .unwrap();
        TreeData::from(tree)
    }
}

#[function_component(HistoryScheduleEvent)]
pub fn history_schedule_event(props: &HistoryScheduleEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
