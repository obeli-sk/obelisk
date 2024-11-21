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
pub struct HistoryJoinSetRequestEventProps {
    pub event: grpc_client::execution_event::history_event::JoinSetRequest,
}

impl HistoryJoinSetRequestEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(Node::new(NodeData::default()), InsertBehavior::AsRoot)
            .unwrap();

        // Add node for JoinSet ID
        if let Some(join_set_id) = &self.event.join_set_id {
            let join_set_node = tree
                .insert(
                    Node::new(NodeData {
                        icon: Icon::Box,
                        label: html! {
                            <>
                                {"Join Set Request: "}
                                {&join_set_id.id}
                            </>
                        },
                        has_caret: true,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&root_id),
                )
                .unwrap();

            // Handle different types of join set requests
            match &self.event.join_set_request {
                Some(grpc_client::execution_event::history_event::join_set_request::JoinSetRequest::DelayRequest(delay_req)) => {
                    if let (Some(delay_id), Some(expires_at)) = (&delay_req.delay_id, &delay_req.expires_at) {
                        tree.insert(
                            Node::new(NodeData {
                                icon: Icon::Time,
                                label: html! {
                                    <>
                                        {"Delay Request: "}
                                        {&delay_id.id}
                                        {" expires at "}
                                        {expires_at.clone()}
                                    </>
                                },
                                ..Default::default()
                            }),
                            InsertBehavior::UnderNode(&join_set_node),
                        )
                        .unwrap();
                    }
                },
                Some(grpc_client::execution_event::history_event::join_set_request::JoinSetRequest::ChildExecutionRequest(child_req)) => {
                    if let Some(child_execution_id) = &child_req.child_execution_id {
                        tree.insert(
                            Node::new(NodeData {
                                icon: Icon::Flows,
                                label: html! {
                                    <>
                                        {"Child Execution Request: "}
                                        <Link<Route> to={Route::ExecutionDetail { execution_id: child_execution_id.clone() } }>
                                            {child_execution_id}
                                        </Link<Route>>
                                    </>
                                },
                                ..Default::default()
                            }),
                            InsertBehavior::UnderNode(&join_set_node),
                        )
                        .unwrap();
                    }
                },
                None => {}
            }
        }

        TreeData::from(tree)
    }
}

#[function_component(HistoryJoinSetRequestEvent)]
pub fn history_join_set_request_event(props: &HistoryJoinSetRequestEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
