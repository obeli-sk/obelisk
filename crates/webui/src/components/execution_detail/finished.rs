use crate::{
    app::Route,
    components::{
        execution_detail::tree_component::TreeComponent,
        json_tree::{insert_json_into_tree, JsonValue},
    },
    grpc::grpc_client,
};
use yew::prelude::*;
use yew_router::prelude::Link;
use yewprint::{
    id_tree::{InsertBehavior, Node, NodeId, Tree, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq, Clone)]
pub struct FinishedEventProps {
    pub result_detail: grpc_client::ResultDetail,
}

pub fn attach_result_detail(
    tree: &mut Tree<NodeData<u32>>,
    root_id: &NodeId,
    result_detail: &grpc_client::ResultDetail,
) {
    match &result_detail
        .value
        .as_ref()
        .expect("`value` is sent in `ResultDetail` message")
    {
        grpc_client::result_detail::Value::Ok(ok) => {
            let ok_node = tree
                .insert(
                    Node::new(NodeData {
                        icon: Icon::Tick,
                        label: "Execution Successful".into_html(),
                        has_caret: ok.return_value.is_some(),
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(root_id),
                )
                .unwrap();

            if let Some(any) = &ok.return_value {
                let _ = insert_json_into_tree(tree, ok_node, JsonValue::Serialized(&any.value));
            }
        }
        grpc_client::result_detail::Value::FallibleError(fallible) => {
            let error_node = tree
                .insert(
                    Node::new(NodeData {
                        icon: Icon::Error,
                        label: "Fallible Error".into_html(),
                        has_caret: fallible.return_value.is_some(),
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(root_id),
                )
                .unwrap();
            if let Some(any) = &fallible.return_value {
                let _ = insert_json_into_tree(tree, error_node, JsonValue::Serialized(&any.value));
            }
        }
        grpc_client::result_detail::Value::Timeout(_) => {
            tree.insert(
                Node::new(NodeData {
                    icon: Icon::Time,
                    label: "Execution Timed Out".into_html(),
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(root_id),
            )
            .unwrap();
        }
        grpc_client::result_detail::Value::UnhandledChildExecutionError(
            grpc_client::result_detail::UnhandledChildExecutionError {
                child_execution_id,
                root_cause_id,
            },
        ) => {
            let child_execution_id = child_execution_id
                .as_ref()
                .expect("`child_execution_id` is sent in `UnhandledChildExecutionError` message");
            let root_cause_id = root_cause_id
                .as_ref()
                .expect("`root_cause_id` is sent in `UnhandledChildExecutionError` mesage");
            let error_node = tree
                .insert(
                    Node::new(NodeData {
                        icon: Icon::Error,
                        label: "Unhandled child execution error".into_html(),
                        has_caret: true,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(root_id),
                )
                .unwrap();
            tree.insert(
                Node::new(NodeData {
                    icon: Icon::Error,
                    label: html! {
                        <>
                            {"Child Execution: "}
                            <Link<Route> to={Route::ExecutionDetail { execution_id: child_execution_id.clone() } }>
                                {child_execution_id}
                            </Link<Route>>
                        </>
                    },
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&error_node),
            )
            .unwrap();
            tree.insert(
                Node::new(NodeData {
                    icon: Icon::Error,
                    label: html! {
                        <>
                            {"Root cause: "}
                            <Link<Route> to={Route::ExecutionDetail { execution_id: root_cause_id.clone() } }>
                                {root_cause_id}
                            </Link<Route>>
                        </>
                    },
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&error_node),
            )
            .unwrap();
        }
        grpc_client::result_detail::Value::ExecutionFailure(failure) => {
            let error_node = tree
                .insert(
                    Node::new(NodeData {
                        icon: Icon::Error,
                        label: "Execution Failure".into_html(),
                        has_caret: true,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(root_id),
                )
                .unwrap();

            tree.insert(
                Node::new(NodeData {
                    icon: Icon::Error,
                    label: failure.reason.as_str().into_html(),
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&error_node),
            )
            .unwrap();
            if let Some(detail) = &failure.detail {
                tree.insert(
                    Node::new(NodeData {
                        icon: Icon::List,
                        label: format!("detail: {detail}").into_html(),
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&error_node),
                )
                .unwrap();
            }
        }
    }
}

impl FinishedEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(Node::new(NodeData::default()), InsertBehavior::AsRoot)
            .unwrap();
        attach_result_detail(&mut tree, &root_id, &self.result_detail);
        TreeData::from(tree)
    }
}

#[function_component(FinishedEvent)]
pub fn finished_event(props: &FinishedEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
