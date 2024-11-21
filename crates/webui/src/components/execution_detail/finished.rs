use crate::{components::execution_detail::tree_component::TreeComponent, grpc::grpc_client};
use yew::prelude::*;
use yewprint::{
    id_tree::{InsertBehavior, Node, TreeBuilder},
    Icon, NodeData, TreeData,
};

#[derive(Properties, PartialEq, Clone)]
pub struct FinishedEventProps {
    pub event: grpc_client::execution_event::Finished,
}

impl FinishedEventProps {
    fn construct_result_detail_tree(
        &self,
        result_detail: &grpc_client::ResultDetail,
    ) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(Node::new(NodeData::default()), InsertBehavior::AsRoot)
            .unwrap();

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
                        InsertBehavior::UnderNode(&root_id),
                    )
                    .unwrap();

                if let Some(any) = &ok.return_value {
                    tree.insert(
                        Node::new(NodeData {
                            icon: Icon::Database,
                            label: "Return Value: (Serialized)".into_html(),
                            has_caret: false,
                            ..Default::default()
                        }),
                        InsertBehavior::UnderNode(&ok_node),
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
                        InsertBehavior::UnderNode(&root_id),
                    )
                    .unwrap();

                if let Some(any) = &fallible.return_value {
                    tree.insert(
                        Node::new(NodeData {
                            icon: Icon::Database,
                            label: "Error Value: (Serialized)".into_html(),
                            has_caret: false,
                            ..Default::default()
                        }),
                        InsertBehavior::UnderNode(&error_node),
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
                    InsertBehavior::UnderNode(&root_id),
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
                    InsertBehavior::UnderNode(&root_id),
                )
                .unwrap();
            }
            grpc_client::result_detail::Value::ExecutionFailure(failure) => {
                tree.insert(
                    Node::new(NodeData {
                        icon: Icon::Error,
                        label: format!("Execution Failure: {}", failure.reason).into_html(),
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&root_id),
                )
                .unwrap();
            }
        }

        TreeData::from(tree)
    }

    fn construct_tree(&self) -> TreeData<u32> {
        self.construct_result_detail_tree(
            self.event
                .result_detail
                .as_ref()
                .expect("`result_detail` is sent in the `Finished` message"),
        )
    }
}

#[function_component(FinishedEvent)]
pub fn finished_event(props: &FinishedEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
