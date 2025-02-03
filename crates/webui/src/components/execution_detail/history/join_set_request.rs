use crate::{
    app::Route,
    components::execution_detail::tree_component::TreeComponent,
    grpc::grpc_client::{self, execution_event::history_event::join_set_request},
};
use chrono::DateTime;
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
                        icon: Icon::History,
                        label: html! {
                            <>
                                {"Join Set Request: `"}
                                {&join_set_id.name}
                                {"`"}
                            </>
                        },
                        has_caret: true,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&root_id),
                )
                .unwrap();

            // Handle different types of join set requests
            match self
                .event
                .join_set_request
                .as_ref()
                .expect("`join_set_request` is sent in `JoinSetRequest`")
            {
                join_set_request::JoinSetRequest::DelayRequest(delay_req) => {
                    let (Some(delay_id), Some(expires_at)) =
                        (&delay_req.delay_id, &delay_req.expires_at)
                    else {
                        panic!("`delay_id` and `expires_at` are sent in `DelayRequest` message");
                    };
                    let expires_at = DateTime::from(*expires_at);
                    tree.insert(
                        Node::new(NodeData {
                            icon: Icon::Time,
                            label: html! {
                                <>
                                    {"Delay Request: "}
                                    {&delay_id.id}
                                </>
                            },
                            ..Default::default()
                        }),
                        InsertBehavior::UnderNode(&join_set_node),
                    )
                    .unwrap();
                    tree.insert(
                        Node::new(NodeData {
                            icon: Icon::Time,
                            label: html! {
                                <>
                                    {"Expires At: "}
                                    {expires_at}
                                </>
                            },
                            ..Default::default()
                        }),
                        InsertBehavior::UnderNode(&join_set_node),
                    )
                    .unwrap();
                }
                join_set_request::JoinSetRequest::ChildExecutionRequest(child_req) => {
                    let child_execution_id = child_req
                        .child_execution_id
                        .as_ref()
                        .expect("`child_execution_id` is sent in `ChildExecutionRequest`");
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
