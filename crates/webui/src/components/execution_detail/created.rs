use crate::app::AppState;
use crate::app::Route;
use crate::components::execution_detail::tree_component::TreeComponent;
use crate::components::execution_header::ExecutionLink;
use crate::components::ffqn_with_links::FfqnWithLinks;
use crate::components::json_tree::JsonValue;
use crate::components::json_tree::insert_json_into_tree;
use crate::grpc::ffqn::FunctionFqn;
use crate::grpc::grpc_client;
use crate::grpc::grpc_client::ComponentId;
use crate::grpc::grpc_client::ExecutionId;
use crate::grpc::version::VersionType;
use chrono::{DateTime, Utc};
use grpc_client::execution_event::Created;
use serde_json::Value;
use yew::Html;
use yew::prelude::*;
use yew_router::prelude::Link;
use yewprint::id_tree::{InsertBehavior, Node, TreeBuilder};
use yewprint::{Icon, NodeData, TreeData};

#[derive(Properties, PartialEq, Clone)]
pub struct CreatedEventProps {
    pub created: Created,
    pub version: VersionType,
    pub link: ExecutionLink,
    pub is_selected: bool,
}
impl CreatedEventProps {
    fn process(&self, app_state: &AppState) -> TreeData<u32> {
        let Created {
            function_name: Some(function_name),
            params: Some(params),
            scheduled_at,
            component_id: Some(component_id),
            scheduled_by,
        } = &self.created
        else {
            panic!("created must contain required fields - {:?}", self.created)
        };
        let ffqn = FunctionFqn::from(function_name.clone());
        let params: Vec<serde_json::Value> =
            serde_json::from_slice(&params.value).expect("`params` must be a JSON array");
        let scheduled_at =
            DateTime::from(scheduled_at.expect("`scheduled_at` is sent by the server"));
        let scheduled_by = scheduled_by.clone();
        let params = match app_state.ffqns_to_details.get(&ffqn) {
            Some((function_detail, _)) if function_detail.params.len() == params.len() => {
                function_detail
                    .params
                    .iter()
                    .zip(params.iter())
                    .map(|(fn_param, param_value)| (fn_param.name.clone(), param_value.clone()))
                    .collect()
            }
            _ => params
                .iter()
                .map(|param_value| ("(unknown)".to_string(), param_value.clone()))
                .collect(),
        };

        let props = ProcessedProps {
            params,
            scheduled_at,
            ffqn,
            scheduled_by,
            component_id: component_id.clone(),
            component_exists: app_state.components_by_id.contains_key(component_id),
            version: self.version,
            link: self.link,
            is_selected: self.is_selected,
        };
        props.construct_tree()
    }
}

struct ProcessedProps {
    params: Vec<(String, Value)>,
    scheduled_at: DateTime<Utc>,
    ffqn: FunctionFqn,
    scheduled_by: Option<ExecutionId>,
    component_id: ComponentId,
    component_exists: bool,
    version: VersionType,
    link: ExecutionLink,
    is_selected: bool,
}
impl ProcessedProps {
    fn construct_tree(self) -> TreeData<u32> {
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
                    icon: Icon::NewObject,
                    label: format!("{}. Created `{}`", self.version, self.ffqn.function_name)
                        .to_html(),
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
                is_selected: self.is_selected,
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
            let param_name_node = tree
                .insert(
                    Node::new(NodeData {
                        icon: Icon::Function,
                        label: format!("{param_name}: {param_value}").into_html(),
                        has_caret: true,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&params_node_id),
                )
                .unwrap();
            let _ =
                insert_json_into_tree(&mut tree, param_name_node, JsonValue::Parsed(&param_value));
        }
        // component id
        tree.insert(
            Node::new(NodeData {
                icon: self.component_id.as_type().as_icon(),
                label: html! {
                    if self.component_exists {
                        <Link<Route> to={Route::Component { component_id: self.component_id.clone() } }>
                        { self.component_id.id }
                        </Link<Route>>
                    } else {
                        { self.component_id.id }
                    }
                },
                has_caret: false,
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&event_type),
        )
        .unwrap();
        // scheduled by
        if let Some(scheduled_by) = self.scheduled_by {
            tree.insert(
                Node::new(NodeData {
                    icon: Icon::Time,
                    label: html! { <>
                        {"Scheduled by "}
                        { self.link.link(scheduled_by.clone(), &scheduled_by.to_string()) }
                    </>},
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

#[function_component(CreatedEvent)]
pub fn created_event(props: &CreatedEventProps) -> Html {
    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");
    let tree = props.process(&app_state);
    html! {
        <TreeComponent {tree} />
    }
}
