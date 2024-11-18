use crate::app::Route;
use crate::grpc::ffqn::FunctionFqn;
use crate::grpc::grpc_client::ExecutionId;
use chrono::{DateTime, Utc};
use log::debug;
use serde_json::Value;
use std::cell::RefCell;
use std::ops::Deref;
use std::rc::Rc;
use yew::prelude::*;
use yew::{function_component, Html};
use yew_router::prelude::Link;
use yewprint::id_tree::{InsertBehavior, Node, NodeId, TreeBuilder};
use yewprint::{Icon, NodeData, TreeData};

#[derive(Properties, PartialEq, Clone)]
pub struct CreateProps {
    pub created_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
    pub ffqn: FunctionFqn,
    pub params: Vec<Value>,
    pub scheduled_by: Option<ExecutionId>,
}

fn construct_tree(props: &CreateProps) -> Rc<RefCell<TreeData<u32>>> {
    debug!("<Create /> construct_tree");
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
    let event_type = tree
        .insert(
            Node::new(NodeData {
                icon: Icon::FolderClose,
                label: "Created".into(),
                has_caret: true,
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&root_id),
        )
        .unwrap();
    // created at
    tree.insert(
        Node::new(NodeData {
            icon: Icon::Time,
            label: html! { {format!("Created at {}", props.created_at)} },
            has_caret: false,
            ..Default::default()
        }),
        InsertBehavior::UnderNode(&event_type),
    )
    .unwrap();
    tree.insert(
        Node::new(NodeData {
            icon: Icon::Time,
            label: html! { {format!("Scheduled at {}", props.scheduled_at)} },
            has_caret: false,
            ..Default::default()
        }),
        InsertBehavior::UnderNode(&event_type),
    )
    .unwrap();
    // ffqn
    tree
        .insert(
            Node::new(NodeData {
                icon: Icon::Function,
                label: html!{ <Link<Route> to={Route::ExecutionListByFfqn { ffqn: props.ffqn.clone() } }>{&props.ffqn}</Link<Route>> },
                has_caret: false,
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
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&event_type),
        )
        .unwrap();
    for param in &props.params {
        tree.insert(
            Node::new(NodeData {
                icon: Icon::Function,
                label: param.to_string().into_html(),
                has_caret: false,
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&params_node_id),
        )
        .unwrap();
    }
    // scheduled by
    if let Some(scheduled_by) = &props.scheduled_by {
        tree
        .insert(
            Node::new(NodeData {
                icon: Icon::Time,
                label: html!{ <> {"Scheduled by "} <Link<Route> to={Route::ExecutionDetail { execution_id: scheduled_by.clone() } }>{scheduled_by}</Link<Route>> </>},
                has_caret: false,
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&event_type),
        )
        .unwrap();
    }
    Rc::new(RefCell::new(TreeData::from(tree)))
}

#[function_component(Create)]
pub fn create(props: &CreateProps) -> Html {
    debug!("<Create /> Rendering");
    // TODO: called unnecessary second time with `use_effect_with`
    let tree_state = use_state(|| construct_tree(props));
    // Must ensure that the tree is re-constructed when props change :(
    use_effect_with(props.clone(), {
        let tree_state = tree_state.clone();
        move |props| tree_state.set(construct_tree(props))
    });

    let on_expand_node = {
        let tree_state = tree_state.clone();
        Callback::from(move |(node_id, _): (NodeId, MouseEvent)| {
            debug!("<Create /> on_expand_node");
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
        <yewprint::Tree<u32>
            tree={tree_state.deref().borrow().deref()}
            on_collapse={Some(on_expand_node.clone())}
            on_expand={Some(on_expand_node.clone())}
            onclick={Some(on_expand_node)}
        />
    }
}
