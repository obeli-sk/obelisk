use crate::app::Route;
use crate::grpc::ffqn::FunctionFqn;
use crate::grpc::grpc_client::ExecutionId;
use serde_json::Value;
use std::cell::RefCell;
use std::ops::Deref;
use std::rc::Rc;
use yew::prelude::*;
use yew::{function_component, Html};
use yew_router::prelude::Link;
use yewprint::id_tree::{InsertBehavior, Node, NodeId, TreeBuilder};
use yewprint::{Icon, NodeData, TreeData};

#[derive(Properties, PartialEq)]
pub struct CreateProps {
    pub ffqn: FunctionFqn,
    pub params: Vec<Value>,
    pub scheduled_by: Option<ExecutionId>,
}

#[function_component(Create)]
pub fn create(
    CreateProps {
        ffqn,
        params,
        scheduled_by,
    }: &CreateProps,
) -> Html {
    let tree_state = use_state(|| {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(
                Node::new(NodeData {
                    data: 0_i32,
                    ..Default::default()
                }),
                InsertBehavior::AsRoot,
            )
            .unwrap();
        let event_type = tree
            .insert(
                Node::new(NodeData {
                    icon: Icon::FolderClose,
                    label: "Created".into(),
                    has_caret: true,
                    data: 1,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&root_id),
            )
            .unwrap();
        // ffqn
        tree
                .insert(
                    Node::new(NodeData {
                        icon: Icon::Function,
                        label: html!{ <Link<Route> to={Route::ExecutionListByFfqn { ffqn: ffqn.clone() } }>{ffqn}</Link<Route>> },
                        has_caret: false,
                        data: 1,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&event_type),
                )
                .unwrap();
        // params
        let params_node_id = tree
            .insert(
                Node::new(NodeData {
                    icon: Icon::Function,
                    label: "Parameters".into_html(),
                    has_caret: true,
                    data: 1,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&event_type),
            )
            .unwrap();
        for param in params {
            tree.insert(
                Node::new(NodeData {
                    icon: Icon::Function,
                    label: param.to_string().into_html(),
                    has_caret: false,
                    data: 1,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&params_node_id),
            )
            .unwrap();
        }
        // scheduled by
        if let Some(scheduled_by) = scheduled_by {
            tree
                .insert(
                    Node::new(NodeData {
                        icon: Icon::Time,
                        label: html!{ <Link<Route> to={Route::ExecutionDetail { execution_id: scheduled_by.clone() } }>{"Scheduled by"}</Link<Route>> },
                        has_caret: false,
                        data: 1,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&event_type),
                )
                .unwrap();
        }
        Rc::new(RefCell::new(TreeData::from(tree)))
    });

    let on_expand_node = {
        let tree_state = tree_state.clone();
        Callback::from(move |(node_id, _): (NodeId, MouseEvent)| {
            let tree = tree_state.deref().clone();
            {
                let mut tree = tree.borrow_mut();
                let mut tree = tree.borrow_mut();
                let node = tree.get_mut(&node_id).unwrap();
                let data = node.data_mut();
                data.is_expanded ^= true;
            }
            tree_state.set(tree);
        })
    };
    html! {
        <yewprint::Tree<i32>
        tree={tree_state.deref().borrow().deref()}
            on_collapse={Some(on_expand_node.clone())}
            on_expand={Some(on_expand_node)}

        />
    }
}
