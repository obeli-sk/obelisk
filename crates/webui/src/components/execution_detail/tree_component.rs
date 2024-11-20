use std::marker::PhantomData;
use yew::prelude::*;
use yewprint::{id_tree::NodeId, TreeData};

pub trait TreeFactory {
    fn construct_tree(&self) -> TreeData<u32>;
}

#[derive(Debug)]
pub enum TreeComponentAction {
    ExpandNode(NodeId),
}

pub trait TreeComponent: BaseComponent
where
    Self::Message:
        std::convert::Into<TreeComponentAction> + std::convert::From<TreeComponentAction>,
    Self::Properties: TreeFactory,
{
    fn tree_get(&self) -> &TreeData<u32>;
    fn tree_mut(&mut self) -> &mut TreeData<u32>;
    fn tree_set(&mut self, tree: TreeData<u32>);
    fn on_expand_node(&self) -> Callback<(NodeId, MouseEvent)>;

    fn update(&mut self, _ctx: &Context<Self>, msg: Self::Message) -> bool {
        match msg.into() {
            TreeComponentAction::ExpandNode(node_id) => {
                let tree = self.tree_mut();
                let mut tree = tree.borrow_mut();
                let node = tree.get_mut(&node_id).unwrap();
                let data = node.data_mut();
                data.is_expanded ^= true;
            }
        }
        true
    }

    fn changed(&mut self, ctx: &Context<Self>, _old_props: &Self::Properties) -> bool {
        let tree = ctx.props().construct_tree();
        self.tree_set(tree);
        true
    }

    fn view(&self, _ctx: &Context<Self>) -> Html {
        html! {
            <yewprint::Tree<u32>
                tree={self.tree_get()}
                on_collapse={Some(self.on_expand_node())}
                on_expand={Some(self.on_expand_node())}
                onclick={Some(self.on_expand_node())}
            />
        }
    }
}

pub struct TreeComponentInner<COMP: BaseComponent> {
    pub tree: TreeData<u32>,
    pub on_expand_node: Callback<(NodeId, MouseEvent)>,
    phantom: PhantomData<COMP>,
}
impl<COMP: BaseComponent> TreeComponentInner<COMP>
where
    COMP::Message:
        std::convert::Into<TreeComponentAction> + std::convert::From<TreeComponentAction>,
    COMP::Properties: TreeFactory,
{
    pub fn create(ctx: &Context<COMP>) -> TreeComponentInner<COMP> {
        let props = ctx.props();
        let tree = props.construct_tree();
        TreeComponentInner {
            tree,
            on_expand_node: ctx
                .link()
                .callback(|(node_id, _)| TreeComponentAction::ExpandNode(node_id)),
            phantom: PhantomData,
        }
    }
}
