use super::function_signature::FunctionSignature;
use crate::grpc::ffqn::FunctionFqn;
use crate::grpc::grpc_client;
use indexmap::IndexMap;
use std::fmt::Debug;
use yew::prelude::*;
use yewprint::id_tree::{InsertBehavior, Node, NodeId, TreeBuilder};
use yewprint::{Icon, NodeData, TreeData};

#[derive(Properties, PartialEq)]
pub struct ComponentTreeProps {
    pub components: Vec<grpc_client::Component>,
    pub show_extensions: bool,
    pub submittable_link_fn: SubmittableLinkFn,
    pub show_submittable_only: bool,
}

pub type SubmittableLinkFn = Callback<FunctionFqn, Html>;

pub struct ComponentTree {
    tree: TreeData<i32>,
    callback_expand_node: Callback<(NodeId, MouseEvent)>,
}

#[derive(Debug)]
pub enum Msg {
    ExpandNode(NodeId),
}

impl ComponentTree {
    fn is_extension_interface(interface: &str) -> bool {
        interface.contains("-obelisk-ext/") // TODO: Use `concepts::SUFFIX_PKG_EXT`
    }

    fn fill_interfaces_and_fns(
        tree: &mut yewprint::id_tree::Tree<NodeData<i32>>,
        exports_or_imports: IndexMap<String, Vec<grpc_client::FunctionDetail>>,
        parent_node_id: &NodeId,
        submittable_link_on_exports: Option<&SubmittableLinkFn>,
    ) {
        let is_exports = submittable_link_on_exports.is_some();
        for (interface, function_detail_vec) in exports_or_imports {
            let is_extension_ifc = Self::is_extension_interface(&interface);
            let ifc_node_id = tree
                .insert(
                    Node::new(NodeData {
                        icon: if is_exports {
                            Icon::Export
                        } else {
                            Icon::Import
                        },
                        is_expanded: is_exports && !is_extension_ifc,
                        label: interface.into(),
                        has_caret: true,
                        data: 0,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(parent_node_id),
                )
                .unwrap();
            for function_detail in function_detail_vec {
                let ffqn = FunctionFqn::from_fn_detail(&function_detail);
                let fn_node_id = tree
                    .insert(
                        Node::new(NodeData {
                            icon: Icon::Function,
                            label: if let (true, Some(submittable_link)) =
                                (function_detail.submittable, submittable_link_on_exports)
                            {
                                submittable_link.emit(ffqn)
                            } else {
                                html! {
                                    {format!("{} ", ffqn.function_name)}
                                }
                            },
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
                    icon: Icon::Blank,
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

    #[expect(clippy::too_many_arguments)]
    fn attach_components_to_tree<'a>(
        tree: &mut yewprint::id_tree::Tree<NodeData<i32>>,
        root_id: &NodeId,
        show_extensions: bool,
        label: Html,
        icon: Icon,
        components: impl Iterator<Item = &'a grpc_client::Component>,
        submittable_link_fn: &SubmittableLinkFn,
        show_submittable_only: bool,
    ) {
        let group_dir_node_id = tree
            .insert(
                Node::new(NodeData {
                    icon: icon.clone(),
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
                        icon: icon.clone(),
                        label: component.name.clone().into(),
                        has_caret: true,
                        data: 0,
                        ..Default::default()
                    }),
                    InsertBehavior::UnderNode(&group_dir_node_id),
                )
                .unwrap();
            Self::fill_interfaces_and_fns(
                tree,
                Self::map_interfaces_to_fn_details(&component.exports, show_extensions),
                &component_node_id,
                Some(submittable_link_fn),
            );
            if !component.imports.is_empty() && !show_submittable_only {
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
                Self::fill_interfaces_and_fns(
                    tree,
                    Self::map_interfaces_to_fn_details(&component.imports, show_extensions),
                    &imports_node_id,
                    None,
                );
            }
        }
    }

    fn map_interfaces_to_fn_details(
        functions: &[grpc_client::FunctionDetail],
        show_extensions: bool,
    ) -> IndexMap<String, Vec<grpc_client::FunctionDetail>> {
        let mut interfaces_to_fn_details: IndexMap<String, Vec<grpc_client::FunctionDetail>> =
            IndexMap::new();
        let mut extensions: IndexMap<String, Vec<grpc_client::FunctionDetail>> = IndexMap::new();
        for function_detail in functions {
            let function_name = function_detail
                .function
                .clone()
                .expect("function and its name is sent by the server");
            let is_extension_ifc = Self::is_extension_interface(&function_name.interface_name);
            if !is_extension_ifc {
                interfaces_to_fn_details
                    .entry(function_name.interface_name.clone())
                    .or_default()
                    .push(function_detail.clone());
            } else if show_extensions {
                extensions
                    .entry(function_name.interface_name.clone())
                    .or_default()
                    .push(function_detail.clone());
            }
        }
        interfaces_to_fn_details.sort_keys();
        extensions.sort_keys();
        interfaces_to_fn_details.append(&mut extensions);
        // sort functions in each interface
        for (_, fns) in interfaces_to_fn_details.iter_mut() {
            fns.sort_by(|a, b| {
                a.function
                    .as_ref()
                    .map(|f| &f.function_name)
                    .cmp(&b.function.as_ref().map(|f| &f.function_name))
            });
        }
        interfaces_to_fn_details
    }

    fn construct_tree(
        components: &[grpc_client::Component],
        show_extensions: bool,
        submittable_link_fn: &SubmittableLinkFn,
        show_submittable_only: bool,
    ) -> TreeData<i32> {
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

        Self::attach_components_to_tree(
            &mut tree,
            &root_id,
            show_extensions,
            "Workflows".into(),
            Icon::GanttChart,
            workflows,
            submittable_link_fn,
            show_submittable_only,
        );
        Self::attach_components_to_tree(
            &mut tree,
            &root_id,
            show_extensions,
            "Activities".into(),
            Icon::CodeBlock,
            activities,
            submittable_link_fn,
            show_submittable_only,
        );
        if !show_submittable_only {
            Self::attach_components_to_tree(
                &mut tree,
                &root_id,
                show_extensions,
                "Webhooks".into(),
                Icon::GlobeNetwork,
                webhooks,
                submittable_link_fn,
                show_submittable_only,
            );
        }
        tree.into()
    }
}

impl Component for ComponentTree {
    type Message = Msg;
    type Properties = ComponentTreeProps;

    fn create(ctx: &Context<Self>) -> Self {
        log::debug!("create");
        let ComponentTreeProps {
            components,
            show_extensions,
            submittable_link_fn,
            show_submittable_only,
        } = ctx.props();
        let tree = Self::construct_tree(
            components,
            *show_extensions,
            submittable_link_fn,
            *show_submittable_only,
        );

        Self {
            tree,
            callback_expand_node: ctx.link().callback(|(node_id, _)| Msg::ExpandNode(node_id)),
        }
    }

    fn update(&mut self, _ctx: &Context<Self>, msg: Self::Message) -> bool {
        log::debug!("update");
        match msg {
            Msg::ExpandNode(node_id) => {
                let mut tree = self.tree.borrow_mut();
                let node = tree.get_mut(&node_id).unwrap();
                let data = node.data_mut();
                data.is_expanded ^= true;
            }
        }
        true
    }

    fn changed(&mut self, ctx: &Context<Self>, _old_props: &Self::Properties) -> bool {
        log::debug!("changed");
        let ComponentTreeProps {
            components,
            show_extensions: extensions,
            submittable_link_fn,
            show_submittable_only,
        } = ctx.props();
        let tree = Self::construct_tree(
            components,
            *extensions,
            submittable_link_fn,
            *show_submittable_only,
        );
        self.tree = tree;
        true
    }

    fn view(&self, _ctx: &Context<Self>) -> Html {
        log::debug!("view");
        html! {
            <yewprint::Tree<i32>
                tree={self.tree.clone()}
                on_collapse={ Some(self.callback_expand_node.clone()) }
                on_expand={ Some(self.callback_expand_node.clone()) }
                onclick={ Some(self.callback_expand_node.clone()) }
            />
        }
    }
}

fn filter_component_list_by_type(
    components: &[grpc_client::Component],
    r#type: grpc_client::ComponentType,
) -> impl Iterator<Item = &grpc_client::Component> {
    components
        .iter()
        .filter(move |component| component.r#type == r#type as i32)
}
