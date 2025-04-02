use crate::app::BacktraceVersions;
use crate::components::execution_header::ExecutionLink;
use crate::grpc::grpc_client::ExecutionId;
use crate::grpc::version::VersionType;
use crate::grpc::ResultValueExt;
use crate::{
    app::Route,
    components::execution_detail::{finished::attach_result_detail, tree_component::TreeComponent},
    grpc::grpc_client::{self, join_set_response_event, JoinSetResponseEvent, ResultDetail},
};
use chrono::DateTime;
use log::error;
use yew::prelude::*;
use yew_router::prelude::Link;
use yewprint::{
    id_tree::{InsertBehavior, Node, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq, Clone)]
pub struct HistoryJoinNextEventProps {
    pub event: grpc_client::execution_event::history_event::JoinNext,
    pub response: Option<JoinSetResponseEvent>,
    pub execution_id: ExecutionId,
    pub backtrace_id: Option<VersionType>,
    pub version: VersionType,
    pub link: ExecutionLink,
}

impl HistoryJoinNextEventProps {
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
            .expect("JoinSetRequest.join_set_id is sent");

        let icon = match &self.response {
            Some(JoinSetResponseEvent {
                response:
                    Some(join_set_response_event::Response::ChildExecutionFinished(
                        join_set_response_event::ChildExecutionFinished {
                            result_detail: Some(ResultDetail { value: Some(value) }),
                            ..
                        },
                    )),
                ..
            }) if value.is_err() => Icon::Error,
            Some(_) => Icon::Tick,
            None => Icon::Search,
        };

        let join_next_node = tree
            .insert(
                Node::new(NodeData {
                    icon,
                    label: html! {
                        <>
                            {self.version}
                            {". Join Next: `"}
                            {join_set_id}
                            {"`"}
                        </>
                    },
                    has_caret: true,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&root_id),
            )
            .unwrap();

        match &self.response {
            Some(JoinSetResponseEvent {
                created_at: Some(finished_at),
                join_set_id: _,
                response:
                    Some(join_set_response_event::Response::ChildExecutionFinished(child_finished)),
            }) => {
                let child_execution_id = child_finished
                    .child_execution_id
                    .as_ref()
                    .expect("`child_execution_id` of `ChildExecutionFinished` must be sent");
                let child_node = tree.insert(
                        Node::new(NodeData {
                            icon: Icon::Flows,
                            label: html! {
                                <>
                                    {"Matched Child Execution Finished: "}
                                    { self.link.link(child_execution_id.clone(), &child_execution_id.id) }
                                </>
                            },
                            has_caret: true,
                            ..Default::default()
                        }),
                        InsertBehavior::UnderNode(&join_next_node),
                    )
                    .unwrap();

                let result_detail = child_finished
                    .result_detail
                    .as_ref()
                    .expect("`child_execution_id` of `ChildExecutionFinished` must be sent");
                attach_result_detail(&mut tree, &child_node, result_detail, None, self.link);

                let finished_at = DateTime::from(*finished_at);
                tree.insert(
                    Node::new(NodeData {
                        icon: Icon::Time,
                        label: format!("Finished At: {finished_at}").into_html(),
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&child_node),
                )
                .unwrap();
            }
            Some(JoinSetResponseEvent {
                created_at: Some(finished_at),
                join_set_id: _,
                response: Some(join_set_response_event::Response::DelayFinished(delay_finished)),
            }) => {
                let delay_id = delay_finished
                    .delay_id
                    .as_ref()
                    .expect("`delay_id` of `DelayFinished` must be sent");
                let delay_node = tree
                    .insert(
                        Node::new(NodeData {
                            icon: Icon::Time,
                            label: html! {
                                <>
                                    {"Matched Delay Finished: "}
                                    {&delay_id.id}
                                </>
                            },
                            has_caret: true,
                            ..Default::default()
                        }),
                        InsertBehavior::UnderNode(&join_next_node),
                    )
                    .unwrap();

                let finished_at = DateTime::from(*finished_at);
                tree.insert(
                    Node::new(NodeData {
                        icon: Icon::Time,
                        label: format!("Finished At: {finished_at}").into_html(),
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&delay_node),
                )
                .unwrap();
            }
            other => {
                error!("Unknown format {other:?}");
            }
        }

        // Add closing status
        tree.insert(
            Node::new(NodeData {
                icon: Icon::Lock,
                label: format!("Closing: {}", self.event.closing).into_html(),
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&join_next_node),
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

#[function_component(HistoryJoinNextEvent)]
pub fn history_join_next_event(props: &HistoryJoinNextEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
