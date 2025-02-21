use crate::app::AppState;
use crate::components::ffqn_with_links::FfqnWithLinks;
use crate::grpc::ffqn::FunctionFqn;
use crate::grpc::function_detail::{map_interfaces_to_fn_details, InterfaceFilter};
use crate::grpc::grpc_client::{self, ComponentId, ComponentType};
use crate::grpc::ifc_fqn::IfcFqn;
use hashbrown::HashMap;
use indexmap::IndexMap;
use std::fmt::Debug;
use std::rc::Rc;
use yew::prelude::*;
use yewprint::id_tree::{InsertBehavior, Node, NodeId, TreeBuilder};
use yewprint::{Icon, NodeData, TreeData};

#[derive(Properties, PartialEq)]
pub struct ComponentTreeProps {
    pub config: ComponentTreeConfig,
}

#[derive(Clone)]
pub enum ComponentTreeConfig {
    ComponentsOnly {
        on_component_selected: Callback<ComponentId>,
    },
    ExecutionListFiltering, // No extensions, no imports
}

impl PartialEq for ComponentTreeConfig {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            // Ignore the fact that `on_component_selected` is differrent
            (
                Self::ComponentsOnly {
                    on_component_selected: _
                },
                Self::ComponentsOnly {
                    on_component_selected: _
                }
            ) | (Self::ExecutionListFiltering, Self::ExecutionListFiltering)
        )
    }
}

pub struct ComponentTree {
    tree: TreeData<NodeDataType>,
    callback_expand_node: Callback<(NodeId, MouseEvent)>,
    config: ComponentTreeConfig,
}

#[derive(Debug)]
pub enum Msg {
    ExpandNode(NodeId),
}

type NodeDataType = Option<grpc_client::ComponentId>;

impl ComponentTree {
    fn fill_interfaces_and_fns(
        tree: &mut yewprint::id_tree::Tree<NodeData<NodeDataType>>,
        exports_or_imports: IndexMap<IfcFqn, Vec<grpc_client::FunctionDetail>>,
        parent_node_id: &NodeId,
    ) {
        for (interface, function_detail_vec) in exports_or_imports {
            let ifc_node_id = tree
                .insert(
                    Node::new(NodeData {
                        icon: Icon::Export,
                        label: interface.into(),
                        has_caret: true,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(parent_node_id),
                )
                .unwrap();
            for fn_detail in function_detail_vec {
                let ffqn =
                    FunctionFqn::from_fn_detail(&fn_detail).expect("ffqn should be parseable");
                tree.insert(
                    Node::new(NodeData {
                        icon: Icon::Function,
                        label: html! {<FfqnWithLinks {ffqn} hide_submit={true}/> },
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&ifc_node_id),
                )
                .unwrap();
            }
        }
    }

    fn attach_components_to_tree<'a>(
        tree: &mut yewprint::id_tree::Tree<NodeData<NodeDataType>>,
        root_id: &NodeId,
        config: &ComponentTreeConfig,
        label: Html,
        icon: Icon,
        components: impl Iterator<Item = (&'a grpc_client::ComponentId, Rc<grpc_client::Component>)>,
    ) {
        let group_dir_node_id = tree
            .insert(
                Node::new(NodeData {
                    icon: icon.clone(),
                    label,
                    has_caret: true,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(root_id),
            )
            .unwrap();
        for (id, component) in components {
            let execution_list_filtering =
                matches!(config, ComponentTreeConfig::ExecutionListFiltering);
            let component_node_id = tree
                .insert(
                    Node::new(NodeData {
                        icon: icon.clone(),
                        label: component.name.clone().into(),
                        has_caret: execution_list_filtering,
                        data: Some(id.clone()),
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&group_dir_node_id),
                )
                .unwrap();
            if execution_list_filtering {
                // Add children - export functions, without extensions as they are not to be found in the database.
                Self::fill_interfaces_and_fns(
                    tree,
                    map_interfaces_to_fn_details(
                        &component.exports,
                        InterfaceFilter::WithoutExtensions,
                    ),
                    &component_node_id,
                );
            }
        }
    }

    fn construct_tree(
        components: &HashMap<grpc_client::ComponentId, Rc<grpc_client::Component>>,
        config: &ComponentTreeConfig,
    ) -> TreeData<NodeDataType> {
        let workflows =
            filter_component_list_by_type(components, grpc_client::ComponentType::Workflow);
        let activities =
            filter_component_list_by_type(components, grpc_client::ComponentType::ActivityWasm);
        let webhooks =
            filter_component_list_by_type(components, grpc_client::ComponentType::WebhookEndpoint);
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(
                Node::new(NodeData {
                    ..Default::default()
                }),
                InsertBehavior::AsRoot,
            )
            .unwrap();

        // Workflows
        Self::attach_components_to_tree(
            &mut tree,
            &root_id,
            config,
            "Workflows".into(),
            ComponentType::Workflow.as_icon(),
            workflows,
        );
        // Activities
        Self::attach_components_to_tree(
            &mut tree,
            &root_id,
            config,
            "Activities".into(),
            ComponentType::ActivityWasm.as_icon(),
            activities,
        );
        // Webhook endpoints
        if matches!(config, ComponentTreeConfig::ComponentsOnly { .. }) {
            Self::attach_components_to_tree(
                &mut tree,
                &root_id,
                config,
                "Webhook Endpoints".into(),
                ComponentType::WebhookEndpoint.as_icon(),
                webhooks,
            );
        }
        tree.into()
    }
}

impl Component for ComponentTree {
    type Message = Msg;
    type Properties = ComponentTreeProps;

    fn create(ctx: &Context<Self>) -> Self {
        log::debug!("<ComponentTree /> create");
        let ComponentTreeProps { config } = ctx.props();
        let (app_state, _) = ctx
            .link()
            .context::<AppState>(Callback::noop())
            .expect("AppState context must be set");
        let tree = Self::construct_tree(&app_state.components_by_id, config);

        Self {
            tree,
            callback_expand_node: ctx.link().callback(|(node_id, _)| Msg::ExpandNode(node_id)),
            config: config.clone(),
        }
    }

    fn update(&mut self, _ctx: &Context<Self>, msg: Self::Message) -> bool {
        match msg {
            Self::Message::ExpandNode(node_id) => {
                let mut tree = self.tree.borrow_mut();
                let node = tree.get_mut(&node_id).unwrap();
                let data = node.data_mut();
                log::debug!("<ComponentTree /> update, data: {:?}", data.data);
                data.is_expanded ^= true;
                if let (
                    ComponentTreeConfig::ComponentsOnly {
                        on_component_selected,
                    },
                    Some(data),
                ) = (&self.config, &data.data)
                {
                    on_component_selected.emit(data.clone());
                }
            }
        }
        true
    }

    fn changed(&mut self, ctx: &Context<Self>, _old_props: &Self::Properties) -> bool {
        log::debug!("<ComponentTree /> changed");
        let ComponentTreeProps { config } = ctx.props();
        let (app_state, _) = ctx
            .link()
            .context::<AppState>(Callback::noop())
            .expect("AppState context must be set");

        let tree = Self::construct_tree(&app_state.components_by_id, config);
        self.tree = tree;
        true
    }

    fn view(&self, _ctx: &Context<Self>) -> Html {
        log::debug!("<ComponentTree /> view");
        html! {
            <yewprint::Tree<NodeDataType>
                tree={&self.tree}
                on_collapse={ Some(self.callback_expand_node.clone()) }
                on_expand={ Some(self.callback_expand_node.clone()) }
                onclick={ Some(self.callback_expand_node.clone()) }
            />
        }
    }
}

fn filter_component_list_by_type(
    components: &HashMap<grpc_client::ComponentId, Rc<grpc_client::Component>>,
    r#type: grpc_client::ComponentType,
) -> impl Iterator<Item = (&grpc_client::ComponentId, Rc<grpc_client::Component>)> {
    components
        .iter()
        .filter(move |(_idx, component)| component.as_type() == r#type)
        .map(|(id, component)| (id, component.clone()))
}
