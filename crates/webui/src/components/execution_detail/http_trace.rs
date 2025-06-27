use crate::{components::execution_detail::tree_component::TreeComponent, grpc::grpc_client};
use chrono::DateTime;
use yew::prelude::*;
use yewprint::{
    Icon, NodeData, TreeData,
    id_tree::{InsertBehavior, Node, NodeId, Tree, TreeBuilder},
};

#[derive(Properties, PartialEq, Clone)]
pub struct HttpTraceEventProps {
    pub http_client_traces: Vec<grpc_client::HttpClientTrace>,
}

fn attach_http_traces(
    tree: &mut Tree<NodeData<u32>>,
    root_id: &NodeId,
    traces: &[grpc_client::HttpClientTrace],
) {
    if traces.is_empty() {
        return;
    }

    let traces_node = tree
        .insert(
            Node::new(NodeData {
                icon: Icon::Exchange,
                label: "HTTP Traces".into_html(),
                has_caret: true,
                ..Default::default()
            }),
            InsertBehavior::UnderNode(root_id),
        )
        .unwrap();

    for trace in traces {
        let sent_at = DateTime::from(
            trace
                .sent_at
                .expect("HttpClientTrace.sent_at is always sent"),
        );
        let finished_at = trace
            .finished_at
            .as_ref()
            .map(|finished_at| DateTime::from(*finished_at));
        let trace_node_label = format!(
            "{} {} ({})",
            trace.method,
            trace.uri,
            if let Some(finished_at) = finished_at {
                let duration = (finished_at - sent_at)
                    .to_std()
                    .expect("duration should never be negative");
                format!("{duration:?}")
            } else {
                "No response".to_string()
            }
        );

        let trace_node = tree
            .insert(
                Node::new(NodeData {
                    icon: Icon::Exchange,
                    label: trace_node_label.into_html(),
                    has_caret: true,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&traces_node),
            )
            .unwrap();

        tree.insert(
            Node::new(NodeData {
                icon: Icon::Time,
                label: format!("Sent at: {sent_at}").into_html(),
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&trace_node),
        )
        .unwrap();
        if let Some(finished_at) = finished_at {
            tree.insert(
                Node::new(NodeData {
                    icon: Icon::Time,
                    label: format!("Finished at: {finished_at}").into_html(),
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&trace_node),
            )
            .unwrap();
        }

        match &trace.result {
            Some(grpc_client::http_client_trace::Result::Status(status)) => {
                tree.insert(
                    Node::new(NodeData {
                        icon: Icon::Tag,
                        label: format!("Status: {status}").into_html(),
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&trace_node),
                )
                .unwrap();
            }
            Some(grpc_client::http_client_trace::Result::Error(error)) => {
                tree.insert(
                    Node::new(NodeData {
                        icon: Icon::Error,
                        label: format!("Error: {error}").into_html(),
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&trace_node),
                )
                .unwrap();
            }
            None => {}
        }
    }
}

impl HttpTraceEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(Node::new(NodeData::default()), InsertBehavior::AsRoot)
            .unwrap();
        attach_http_traces(&mut tree, &root_id, &self.http_client_traces);
        TreeData::from(tree)
    }
}

#[function_component(HttpTraceEvent)]
pub fn http_trace(props: &HttpTraceEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
