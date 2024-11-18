use crate::app::Route;
use crate::grpc::ffqn::FunctionFqn;
use crate::grpc::grpc_client::ExecutionId;
use chrono::{DateTime, Utc};
use log::debug;
use serde_json::Value;
use yew::prelude::*;
use yew::Html;
use yew_router::prelude::Link;
use yewprint::id_tree::{InsertBehavior, Node, NodeId, TreeBuilder};
use yewprint::{Icon, NodeData, TreeData};

#[derive(Properties, PartialEq, Clone)]
pub struct CreateEventProps {
    pub created_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
    pub ffqn: FunctionFqn,
    pub params: Vec<Value>,
    pub scheduled_by: Option<ExecutionId>,
}

pub struct CreateEvent {
    tree: TreeData<u32>,
    on_expand_node: Callback<(NodeId, MouseEvent)>,
}

#[derive(Debug)]
pub enum Action {
    ExpandNode(NodeId),
}

impl Component for CreateEvent {
    type Message = Action;
    type Properties = CreateEventProps;

    fn create(ctx: &Context<Self>) -> Self {
        debug!("<CreateEvent /> create");
        let props = ctx.props();
        let tree = construct_tree(props);
        Self {
            tree,
            on_expand_node: ctx
                .link()
                .callback(|(node_id, _)| Action::ExpandNode(node_id)),
        }
    }

    fn update(&mut self, _ctx: &Context<Self>, msg: Self::Message) -> bool {
        log::debug!("<CreateEvent /> update");
        match msg {
            Self::Message::ExpandNode(node_id) => {
                let mut tree = self.tree.borrow_mut();
                let node = tree.get_mut(&node_id).unwrap();
                let data = node.data_mut();
                data.is_expanded ^= true;
            }
        }
        true
    }

    fn changed(&mut self, ctx: &Context<Self>, _old_props: &Self::Properties) -> bool {
        log::debug!("<CreateEvent /> changed");
        let props = ctx.props();
        let tree = construct_tree(props);
        self.tree = tree;
        true
    }

    fn view(&self, _ctx: &Context<Self>) -> Html {
        debug!("<CreateEvent /> view");
        html! {
            <yewprint::Tree<u32>
                tree={&self.tree}
                on_collapse={Some(self.on_expand_node.clone())}
                on_expand={Some(self.on_expand_node.clone())}
                onclick={Some(self.on_expand_node.clone())}
            />
        }
    }
}

fn construct_tree(props: &CreateEventProps) -> TreeData<u32> {
    debug!("<CreateEvent /> construct_tree");
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
    TreeData::from(tree)
}
