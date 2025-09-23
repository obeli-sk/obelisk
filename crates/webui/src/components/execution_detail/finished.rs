use crate::{
    components::{
        execution_detail::tree_component::TreeComponent,
        json_tree::{JsonValue, insert_json_into_tree},
    },
    grpc::{grpc_client, version::VersionType},
};
use yew::prelude::*;
use yewprint::{
    Icon, NodeData, TreeData,
    id_tree::{InsertBehavior, Node, NodeId, Tree, TreeBuilder},
};

#[derive(Properties, PartialEq, Clone)]
pub struct FinishedEventProps {
    pub result_detail: grpc_client::ResultDetail,
    pub version: Option<VersionType>,
    pub is_selected: bool,
}

fn with_version(version: Option<VersionType>, label: &'static str) -> Html {
    if let Some(version) = version {
        format!("{version}. {label}").to_html()
    } else {
        label.to_html()
    }
}

pub fn attach_result_detail(
    tree: &mut Tree<NodeData<u32>>,
    root_id: &NodeId,
    result_detail: &grpc_client::ResultDetail,
    version: Option<VersionType>,
    is_selected: bool,
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
                        label: with_version(version, "Execution Successful"),
                        has_caret: ok.return_value.is_some(),
                        is_selected,
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
                        label: with_version(version, "Fallible Error"),
                        has_caret: fallible.return_value.is_some(),
                        is_selected,
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
                    label: with_version(version, "Execution Timed Out"),
                    is_selected,
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
                        label: with_version(version, "Execution Failure"),
                        has_caret: true,
                        is_selected,
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
        attach_result_detail(
            &mut tree,
            &root_id,
            &self.result_detail,
            self.version,
            self.is_selected,
        );
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
