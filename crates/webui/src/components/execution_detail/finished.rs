use crate::{
    components::{
        execution_detail::tree_component::TreeComponent, json_tree::insert_json_into_tree,
    },
    grpc::grpc_client,
};
use yew::prelude::*;
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
                let json_tree_parent = tree
                    .insert(
                        Node::new(NodeData {
                            icon: Icon::Database,
                            label: "Value".into_html(),
                            has_caret: true,
                            ..Default::default()
                        }),
                        InsertBehavior::UnderNode(&ok_node),
                    )
                    .unwrap();

                let _ = insert_json_into_tree(tree, json_tree_parent, &any.value);

                let serialized = tree
                    .insert(
                        Node::new(NodeData {
                            icon: Icon::Database,
                            label: "Value: (serialized)".into_html(),
                            has_caret: true,
                            ..Default::default()
                        }),
                        InsertBehavior::UnderNode(&ok_node),
                    )
                    .unwrap();
                tree.insert(
                    Node::new(NodeData {
                        icon: Icon::Database,
                        label: String::from_utf8_lossy(&any.value).into_html(),
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&serialized),
                )
                .unwrap();
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
                let json_tree_parent = tree
                    .insert(
                        Node::new(NodeData {
                            icon: Icon::Database,
                            label: "Value".into_html(),
                            has_caret: true,
                            ..Default::default()
                        }),
                        InsertBehavior::UnderNode(&error_node),
                    )
                    .unwrap();
                let _ = insert_json_into_tree(tree, json_tree_parent, &any.value);

                let serialized = tree
                    .insert(
                        Node::new(NodeData {
                            icon: Icon::Database,
                            label: "Value: (serialized)".into_html(),
                            has_caret: true,
                            ..Default::default()
                        }),
                        InsertBehavior::UnderNode(&error_node),
                    )
                    .unwrap();
                tree.insert(
                    Node::new(NodeData {
                        icon: Icon::Database,
                        label: String::from_utf8_lossy(&any.value).into_html(),
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&serialized),
                )
                .unwrap();
            }
        }
        grpc_client::result_detail::Value::Timeout(_) => {
            tree.insert(
                Node::new(NodeData {
                    icon: Icon::Time,
                    label: "Execution Timed Out".into_html(),
                    has_caret: false,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(root_id),
            )
            .unwrap();
        }
        grpc_client::result_detail::Value::NondeterminismDetected(nondeterminism) => {
            tree.insert(
                Node::new(NodeData {
                    icon: Icon::Error,
                    label: format!("Nondeterminism Detected: {}", nondeterminism.reason)
                        .into_html(),
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(root_id),
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
