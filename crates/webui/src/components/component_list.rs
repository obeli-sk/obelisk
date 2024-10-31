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

fn fill_interfaces_and_fns(
    tree: &mut yewprint::id_tree::Tree<NodeData<i32>>,
    exports_or_imports: &[grpc_client::FunctionDetails],
    parent_node_id: &NodeId,
    is_exports: bool,
) {
    for (interface, function_detail_vec) in map_interfaces_to_fn_details(exports_or_imports) {
        let ifc_node_id = tree
            .insert(
                Node::new(NodeData {
                    icon: if is_exports {
                        Icon::Export
                    } else {
                        Icon::Import
                    },
                    label: interface.into(),
                    has_caret: true,
                    is_expanded: is_exports,
                    data: 0,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(parent_node_id),
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
                                <button>
                                    <yewprint::Icon icon = { Icon::Play }/>
                                </button>
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
                    label: html! {
                        <FunctionSignature params = {function_detail.params} return_type = {function_detail.return_type} />
                    },
                    data: 0,
                    disabled: true,
                    ..Default::default()
                }),
                InsertBehavior::UnderNode(&fn_node_id),
            )
            .unwrap();
        }
    }
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
            fill_interfaces_and_fns(tree, &component.exports, &component_node_id, true);
        }
        if !component.imports.is_empty() {
            let imports_node_id = tree
                .insert(
                    Node::new(NodeData {
                        icon: Icon::Import,
                        label: "Imports".into(),
                        has_caret: true,
                        data: 0,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&component_node_id),
                )
                .unwrap();
            fill_interfaces_and_fns(tree, &component.imports, &imports_node_id, false);
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
pub struct FunctionSignatureProps {
    pub params: Vec<grpc_client::FunctionParameter>,
    pub return_type: Option<grpc_client::WitType>,
}
#[function_component(FunctionSignature)]
pub fn function_signature(
    FunctionSignatureProps {
        params,
        return_type,
    }: &FunctionSignatureProps,
) -> Html {
    html! {<>
        {"func ("}
            <FunctionParameterList params = {params.clone()} />
        {")"}
        if let Some(return_type) = return_type {

            {" -> "}
            if let Some(wit_type) = &return_type.wit_type {
                { wit_type }
            } else {
                { "<unknown type>" }
            }
        }
    </>}
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

fn filter_component_list_by_type(
    components: &[grpc_client::Component],
    r#type: grpc_client::ComponentType,
) -> impl Iterator<Item = &grpc_client::Component> {
    components
        .iter()
        .filter(move |component| component.r#type == r#type as i32)
}
