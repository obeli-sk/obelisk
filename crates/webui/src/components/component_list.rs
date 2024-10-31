use crate::grpc_client;
use indexmap::IndexMap;
use std::fmt::Debug;
use yew::prelude::*;
use yewprint::id_tree::{InsertBehavior, Node, NodeId, TreeBuilder};
use yewprint::{Icon, NodeData, TreeData};

#[derive(Properties, PartialEq)]
pub struct ComponentTreeProps {
    pub components: Vec<grpc_client::Component>,
}

pub struct ComponentTree {
    tree: TreeData<i32>,
    callback_expand_node: Callback<(NodeId, MouseEvent)>,
    callback_select_node: Callback<(NodeId, MouseEvent)>,
}

#[derive(Debug)]
pub enum Msg {
    ExpandNode(NodeId),
    SelectNode(NodeId),
}

fn render_group<'a>(
    tree: &mut yewprint::id_tree::Tree<NodeData<i32>>,
    root_id: &NodeId,
    label: Html,
    components: impl Iterator<Item = &'a grpc_client::Component>,
) {
    let group_dir = tree
        .insert(
            Node::new(NodeData {
                icon: Icon::FolderClose,
                label,
                has_caret: true,
                data: 0,
                ..Default::default()
            }),
            InsertBehavior::UnderNode(root_id),
        )
        .unwrap();

    let fill_interfaces_and_fns = |tree: &mut yewprint::id_tree::Tree<NodeData<i32>>,
                                   exports_or_imports,
                                   parent_node_id,
                                   is_expanded| {
        for (interface, function_detail_vec) in map_interfaces_to_fn_details(exports_or_imports) {
            let ifc_node_id = tree
                .insert(
                    Node::new(NodeData {
                        icon: Icon::FolderClose,
                        label: interface.into(),
                        has_caret: true,
                        is_expanded,
                        data: 0,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&parent_node_id),
                )
                .unwrap();
            for function_detail in function_detail_vec {
                let function_name = function_detail
                    .function
                    .expect("`.function` is sent by the server");
                let fn_node_id = tree
                    .insert(
                        Node::new(NodeData {
                            icon: Icon::Function,
                            label: html! {<>
                                {format!("{} ", function_name.function_name)}
                                if function_detail.submittable {
                                    <button>{"Submit"}</button>
                                }
                            </>},
                            has_caret: true,
                            data: 0,
                            ..Default::default()
                        }),
                        InsertBehavior::UnderNode(&ifc_node_id),
                    )
                    .unwrap();
                // insert fn details
                tree.insert(
                    Node::new(NodeData {
                        icon: Icon::Document,
                        label: html! {<>
                            {"func ("}
                                <FunctionParameterList params = {function_detail.params} />
                            {")"}
                            if let Some(return_type) = function_detail.return_type {

                                {" -> "}
                                if let Some(wit_type) = return_type.wit_type {
                                    { wit_type }
                                } else {
                                    { "<unknown type>" }
                                }
                            }
                        </>},
                        data: 0,
                        disabled: true,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&fn_node_id),
                )
                .unwrap();
            }
        }
    };

    for component in components {
        let component_node_id = tree
            .insert(
                Node::new(NodeData {
                    icon: Icon::FolderClose,
                    label: component.name.clone().into(),
                    has_caret: true,
                    data: 0,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&group_dir),
            )
            .unwrap();
        if !component.exports.is_empty() {
            let exports_node_id = tree
                .insert(
                    Node::new(NodeData {
                        icon: Icon::FolderClose,
                        label: "Exports".into(),
                        has_caret: true,
                        is_expanded: true,
                        data: 0,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&component_node_id),
                )
                .unwrap();
            fill_interfaces_and_fns(tree, &component.exports, exports_node_id, true);
        }
        if !component.imports.is_empty() {
            let imports_node_id = tree
                .insert(
                    Node::new(NodeData {
                        icon: Icon::FolderClose,
                        label: "Imports".into(),
                        has_caret: true,
                        data: 0,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&component_node_id),
                )
                .unwrap();
            fill_interfaces_and_fns(tree, &component.imports, imports_node_id, false);
        }
    }
}

fn map_interfaces_to_fn_details(
    functions: &[grpc_client::FunctionDetails],
) -> IndexMap<String, Vec<grpc_client::FunctionDetails>> {
    let mut interfaces_to_fn_details: IndexMap<String, Vec<grpc_client::FunctionDetails>> =
        IndexMap::new();
    for function_detail in functions {
        let function_name = function_detail
            .function
            .clone()
            .expect("function and its name is sent by the server");
        interfaces_to_fn_details
            .entry(function_name.interface_name.clone())
            .or_default()
            .push(function_detail.clone());
    }
    interfaces_to_fn_details
}

impl Component for ComponentTree {
    type Message = Msg;
    type Properties = ComponentTreeProps;

    fn create(ctx: &Context<Self>) -> Self {
        let ComponentTreeProps { components } = ctx.props();
        log::debug!("Create: {} components", components.len());
        let workflows =
            filter_component_list_by_type(components, grpc_client::ComponentType::Workflow);
        let activities =
            filter_component_list_by_type(components, grpc_client::ComponentType::ActivityWasm);
        let webhooks =
            filter_component_list_by_type(components, grpc_client::ComponentType::WebhookWasm);
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

        render_group(&mut tree, &root_id, "Workflows".into(), workflows);
        render_group(&mut tree, &root_id, "Activities".into(), activities);
        render_group(&mut tree, &root_id, "Webhooks".into(), webhooks);

        Self {
            tree: tree.into(),
            callback_expand_node: ctx.link().callback(|(node_id, _)| Msg::ExpandNode(node_id)),
            callback_select_node: ctx.link().callback(|(node_id, _)| Msg::SelectNode(node_id)),
        }
    }

    fn update(&mut self, ctx: &Context<Self>, msg: Self::Message) -> bool {
        let ComponentTreeProps { components } = ctx.props();
        log::debug!("Update: {} components", components.len());

        log::debug!("update {msg:?}");
        match msg {
            Msg::ExpandNode(node_id) => {
                let mut tree = self.tree.borrow_mut();
                let node = tree.get_mut(&node_id).unwrap();
                let data = node.data_mut();
                data.is_expanded ^= true;
                data.icon = if data.is_expanded {
                    Icon::FolderOpen
                } else {
                    Icon::FolderClose
                };
            }
            Msg::SelectNode(_node_id) => {
                // let mut tree = self.tree.borrow_mut();
                // let node = tree.get_mut(&node_id).unwrap();
                // node.data_mut().is_selected ^= true;
            }
        }

        true
    }

    fn view(&self, _ctx: &Context<Self>) -> Html {
        html! {
            <yewprint::Tree<i32>
                tree={self.tree.clone()}
                on_collapse={Some(self.callback_expand_node.clone())}
                on_expand={Some(self.callback_expand_node.clone())}
                onclick={Some(self.callback_select_node.clone())}
            />
        }
    }
}

#[derive(Properties, PartialEq)]
pub struct FunctionParameterListProps {
    pub params: Vec<grpc_client::FunctionParameter>,
}
#[function_component(FunctionParameterList)]
pub fn function_parameter_list(
    FunctionParameterListProps { params }: &FunctionParameterListProps,
) -> Html {
    params
        .iter()
        .enumerate()
        .map(|(idx, param)| {
            let name = param.name.as_deref().unwrap_or("unknown_param_name");
            let r#type = param
                .r#type
                .as_ref()
                .and_then(|wit_type| wit_type.wit_type.as_deref())
                .unwrap_or("<unknown_type>");
            html! {<>
                if idx > 0 {
                    {", "}
                }
                { format!("{}: {}", name, r#type) }
            </>}
        })
        .collect()
}

#[derive(Properties, PartialEq)]
pub struct ComponentListProps {
    pub components: Vec<grpc_client::Component>,
    pub on_click: Callback<grpc_client::Component>,
}
#[function_component(ComponentList)]
pub fn component_list(
    ComponentListProps {
        components,
        on_click,
    }: &ComponentListProps,
) -> Html {
    let workflows = filter_component_list_by_type_html(
        components,
        grpc_client::ComponentType::Workflow,
        on_click,
    );
    let activities = filter_component_list_by_type_html(
        components,
        grpc_client::ComponentType::ActivityWasm,
        on_click,
    );
    let webhooks = filter_component_list_by_type_html(
        components,
        grpc_client::ComponentType::WebhookWasm,
        on_click,
    );

    html! {
        <div key={"workflows"}>
        <h3>{"Workflows"}</h3>
        {workflows}
        <h3>{"Activities"}</h3>
        {activities}
        <h3>{"Webhooks"}</h3>
        {webhooks}
        </div>
    }
}

fn filter_component_list_by_type_html(
    components: &[grpc_client::Component],
    r#type: grpc_client::ComponentType,
    on_click: &Callback<grpc_client::Component>,
) -> Vec<Html> {
    components
        .iter()
        .filter(|component| component.r#type == r#type as i32)
        .map(|component| {
            let on_select = {
                let on_click = on_click.clone();
                let component = component.clone();
                Callback::from(move |_| on_click.emit(component.clone()))
            };
            html! {
                    <p key={component.config_id.as_ref().unwrap().id.as_str()}
                        onclick={on_select}>{format!("{}", component.name)}</p>
            }
        })
        .collect::<Vec<_>>()
}

fn filter_component_list_by_type(
    components: &[grpc_client::Component],
    r#type: grpc_client::ComponentType,
) -> impl Iterator<Item = &grpc_client::Component> {
    components
        .iter()
        .filter(move |component| component.r#type == r#type as i32)
}
