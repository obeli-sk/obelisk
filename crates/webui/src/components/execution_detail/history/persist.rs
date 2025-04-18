use crate::{
    components::execution_detail::tree_component::TreeComponent,
    grpc::{grpc_client, version::VersionType},
};
use yew::prelude::*;
use yewprint::{
    Icon, NodeData, TreeData,
    id_tree::{InsertBehavior, Node, TreeBuilder},
};

#[derive(Properties, PartialEq, Clone)]
pub struct HistoryPersistEventProps {
    pub event: grpc_client::execution_event::history_event::Persist,
    pub version: VersionType,
    pub is_selected: bool,
}

impl HistoryPersistEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(Node::new(NodeData::default()), InsertBehavior::AsRoot)
            .unwrap();

        // Add node for Persist event
        tree.insert(
            Node::new(NodeData {
                icon: Icon::History,
                label: format!("{}. Persist Event", self.version).into_html(),
                is_selected: self.is_selected,
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&root_id),
        )
        .unwrap();
        // Not showing the data
        TreeData::from(tree)
    }
}

#[function_component(HistoryPersistEvent)]
pub fn history_persist_event(props: &HistoryPersistEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
