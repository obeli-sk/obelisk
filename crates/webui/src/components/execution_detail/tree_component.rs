use yew::prelude::*;
use yewprint::{TreeData, id_tree::NodeId};

#[derive(Debug)]
pub enum TreeComponentAction {
    ExpandNode(NodeId),
}

#[derive(Properties, PartialEq)]
pub struct TreeComponentProps {
    pub tree: TreeData<u32>,
}

pub struct TreeComponent {
    pub tree: TreeData<u32>,
    pub on_expand_node: Callback<(NodeId, MouseEvent)>,
}

impl Component for TreeComponent {
    type Message = TreeComponentAction;

    type Properties = TreeComponentProps;

    fn create(ctx: &Context<Self>) -> Self {
        let props = ctx.props();
        let tree = props.tree.clone();
        Self {
            tree,
            on_expand_node: ctx
                .link()
                .callback(|(node_id, _)| TreeComponentAction::ExpandNode(node_id)),
        }
    }

    fn update(&mut self, _ctx: &Context<Self>, msg: Self::Message) -> bool {
        match msg {
            TreeComponentAction::ExpandNode(node_id) => {
                let mut tree = self.tree.borrow_mut();
                let node = tree.get_mut(&node_id).unwrap();
                let data = node.data_mut();
                data.is_expanded ^= true;
            }
        }
        true
    }

    fn changed(&mut self, ctx: &Context<Self>, _old_props: &Self::Properties) -> bool {
        self.tree = ctx.props().tree.clone();
        true
    }

    fn view(&self, _ctx: &Context<Self>) -> Html {
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
