use crate::{
    components::{
        execution_detail::tree_component::TreeComponent, execution_header::ExecutionLink,
    },
    grpc::{grpc_client, version::VersionType},
};
use yew::prelude::*;
use yewprint::{
    Icon, NodeData, TreeData,
    id_tree::{InsertBehavior, Node, TreeBuilder},
};

#[derive(Properties, PartialEq, Clone)]
pub struct HistoryStubEventProps {
    pub event: grpc_client::execution_event::history_event::Stub,
    pub version: VersionType,
    pub link: ExecutionLink,
    pub is_selected: bool,
}

impl HistoryStubEventProps {
    fn construct_tree(&self) -> TreeData<u32> {
        let mut tree = TreeBuilder::new().build();
        let root_id = tree
            .insert(Node::new(NodeData::default()), InsertBehavior::AsRoot)
            .unwrap();

        let stubbed_execution_id = self
            .event
            .execution_id
            .clone()
            .expect("`execution_id` is sent by the server");

        // Add node for Stub event
        tree.insert(
            Node::new(NodeData {
                icon: if self.event.success {
                    Icon::Tick
                } else {
                    Icon::Error
                },
                label: html! {<>
                    { self.version }
                    {". Stubbed execution "}
                    { self.link.link(stubbed_execution_id.clone(), &stubbed_execution_id.id) }
                </>},
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

#[function_component(HistoryStubEvent)]
pub fn history_stub_event(props: &HistoryStubEventProps) -> Html {
    let tree = props.construct_tree();
    html! {
        <TreeComponent {tree} />
    }
}
