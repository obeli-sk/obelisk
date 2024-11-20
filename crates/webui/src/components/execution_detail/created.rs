use crate::app::AppState;
use crate::app::Route;
use crate::components::ffqn_with_links::FfqnWithLinks;
use crate::grpc::ffqn::FunctionFqn;
use crate::grpc::grpc_client;
use crate::grpc::grpc_client::ExecutionId;
use chrono::{DateTime, Utc};
use grpc_client::execution_event::Created;
use indexmap::IndexMap;
use log::debug;
use serde_json::Value;
use yew::prelude::*;
use yew::Html;
use yew_router::prelude::Link;
use yewprint::id_tree::{InsertBehavior, Node, NodeId, TreeBuilder};
use yewprint::{Icon, NodeData, TreeData};

#[derive(Properties, PartialEq, Clone)]
pub struct CreatedEventProps {
    pub created: Created,
}
impl CreatedEventProps {
    fn process(&self, app_state: &AppState) -> ProcessedProps {
        let Created {
            function_name: Some(function_name),
            params: Some(params),
            scheduled_at,
            config_id: _,
            scheduled_by,
        } = &self.created
        else {
            panic!()
        };
        let ffqn = FunctionFqn::from(function_name.clone());
        let params: Vec<serde_json::Value> =
            serde_json::from_slice(&params.value).expect("`params` must be a JSON array");
        let scheduled_at =
            DateTime::from(scheduled_at.expect("`scheduled_at` is sent by the server"));
        let scheduled_by = scheduled_by.clone();
        let params = match app_state.submittable_ffqns_to_details.get(&ffqn) {
            Some(function_detail) if function_detail.params.len() == params.len() => {
                let param_tuples = function_detail.params.iter().zip(params.iter()).map(
                    |(fn_param, param_value)| {
                        (
                            fn_param.name.as_deref().unwrap_or("(unknown)").to_string(),
                            param_value.clone(),
                        )
                    },
                );
                IndexMap::from_iter(param_tuples)
            }
            _ => IndexMap::from_iter(
                params
                    .iter()
                    .map(|param_value| ("(unknown)".to_string(), param_value.clone())),
            ),
        };
        ProcessedProps {
            params,
            scheduled_at,
            ffqn,
            scheduled_by,
        }
    }
}

struct ProcessedProps {
    params: IndexMap<String, Value>,
    scheduled_at: DateTime<Utc>,
    ffqn: FunctionFqn,
    scheduled_by: Option<ExecutionId>,
}
impl ProcessedProps {
    fn construct_tree(self) -> TreeData<u32> {
        debug!("<CreatedEvent /> construct_tree");
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
        // Scheduled at
        tree.insert(
            Node::new(NodeData {
                icon: Icon::Time,
                label: html! { {format!("Scheduled at {}", self.scheduled_at)} },
                has_caret: false,
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&event_type),
        )
        .unwrap();
        // ffqn
        tree.insert(
            Node::new(NodeData {
                icon: Icon::Function,
                label: html! { <FfqnWithLinks ffqn={self.ffqn} fully_qualified={true} /> },
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
                    icon: Icon::FolderClose,
                    label: "Parameters".into_html(),
                    has_caret: true,
                    is_expanded: true,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&event_type),
            )
            .unwrap();
        for (param_name, param_value) in self.params {
            tree.insert(
                Node::new(NodeData {
                    icon: Icon::Function,
                    label: format!("{param_name}: {param_value}").into_html(),
                    has_caret: false,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&params_node_id),
            )
            .unwrap();
        }
        // scheduled by
        if let Some(scheduled_by) = self.scheduled_by {
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
}

#[derive(Debug)]
pub enum Action {
    ExpandNode(NodeId),
}

pub struct CreatedEvent {
    tree: TreeData<u32>,
    on_expand_node: Callback<(NodeId, MouseEvent)>,
}
impl Component for CreatedEvent {
    type Message = Action;
    type Properties = CreatedEventProps;

    fn create(ctx: &Context<Self>) -> Self {
        debug!("<CreatedEvent /> create");
        let app_state = ctx
            .link()
            .context::<AppState>(Callback::noop())
            .expect("AppState context is set when starting the App")
            .0;
        let tree = ctx.props().process(&app_state).construct_tree();
        Self {
            tree,
            on_expand_node: ctx
                .link()
                .callback(|(node_id, _)| Action::ExpandNode(node_id)),
        }
    }

    fn update(&mut self, _ctx: &Context<Self>, msg: Self::Message) -> bool {
        log::debug!("<CreatedEvent /> update");
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
        log::debug!("<CreatedEvent /> changed");
        let app_state = ctx
            .link()
            .context::<AppState>(Callback::noop())
            .expect("AppState context is set when starting the App")
            .0;
        let tree = ctx.props().process(&app_state).construct_tree();
        self.tree = tree;
        true
    }

    fn view(&self, _ctx: &Context<Self>) -> Html {
        debug!("<CreatedEvent /> view");
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
