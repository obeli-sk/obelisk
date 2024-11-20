use crate::{
    app::Route, components::execution_detail::tree_component::TreeComponent, grpc::grpc_client,
};
use yew::prelude::*;
use yew_router::prelude::Link;
use yewprint::{
    id_tree::{InsertBehavior, Node, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq, Clone)]
pub struct HistoryScheduleEventProps {
    pub event: grpc_client::execution_event::history_event::Schedule,
}

impl HistoryScheduleEventProps {
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
                        <Link<Route> to={Route::ExecutionDetail { execution_id: scheduled_execution_id.clone() } }>
                            {scheduled_execution_id}
                        </Link<Route>>
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

#[function_component(HistoryScheduleEvent)]
pub fn history_schedule_event(props: &HistoryScheduleEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
