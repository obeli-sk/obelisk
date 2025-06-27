use serde_json::Value;
use std::{borrow::Cow, collections::BTreeMap, ops::Deref};
use yew::prelude::*;
use yewprint::{
    Icon, NodeData,
    id_tree::{self, InsertBehavior, Node, NodeId},
};

fn render_json_value(
    tree: &mut id_tree::Tree<NodeData<u32>>,
    parent: &NodeId,
    key: Option<&str>,
    value: &Value,
) -> NodeId {
    // Create a display label for the node
    let label = match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("\"{s}\""),
        Value::Array(arr) => format!("[{} items]", arr.len()),
        Value::Object(obj) => format!("{{{}}} items", obj.len()),
    };

    // Determine icon and whether node has children
    let (icon, has_caret) = match value {
        Value::Null => (Icon::Circle, false),
        Value::Bool(true) => (Icon::Tick, false),
        Value::Bool(false) => (Icon::Cross, false),
        Value::Number(_) => (Icon::Numerical, false),
        Value::String(_) => (Icon::Citation, false),
        Value::Array(arr) => (Icon::Array, !arr.is_empty()),
        Value::Object(obj) => (Icon::DiagramTree, !obj.is_empty()),
    };

    // Prepare the label with optional key prefix
    let display_label = match key {
        Some(k) => format!("{k}: {label}"),
        None => label,
    };

    // Insert the node
    let node_id = tree
        .insert(
            Node::new(NodeData {
                icon,
                label: display_label.into_html(),
                has_caret,
                ..Default::default()
            }),
            InsertBehavior::UnderNode(parent),
        )
        .unwrap();
    // Recursively add children for complex types
    match value {
        Value::Array(arr) => {
            for (idx, item) in arr.iter().enumerate() {
                render_json_value(tree, &node_id, Some(&idx.to_string()), item);
            }
        }
        Value::Object(obj) => {
            // Sort keys for consistent display
            let sorted_keys: BTreeMap<_, _> = obj.iter().collect();
            for (k, v) in sorted_keys {
                render_json_value(tree, &node_id, Some(k), v);
            }
        }
        _ => {} // Primitive types have no children
    }
    node_id
}

pub enum JsonValue<'a> {
    Serialized(&'a [u8]),
    Parsed(&'a Value),
}
impl JsonValue<'_> {
    #[expect(clippy::inherent_to_string)]
    fn to_string(&self) -> String {
        match self {
            JsonValue::Serialized(slice) => String::from_utf8_lossy(slice).into_owned(),
            JsonValue::Parsed(value) => value.to_string(),
        }
    }
    fn to_value(&self) -> Result<Cow<'_, Value>, serde_json::Error> {
        match self {
            JsonValue::Serialized(slice) => serde_json::from_slice::<Value>(slice).map(Cow::Owned),
            JsonValue::Parsed(value) => Ok(Cow::Borrowed(*value)),
        }
    }
}

pub fn insert_json_into_tree(
    tree: &mut id_tree::Tree<NodeData<u32>>,
    parent_node: NodeId,
    json_data: JsonValue<'_>,
) -> Result<(), serde_json::Error> {
    let value_node = tree
        .insert(
            Node::new(NodeData {
                icon: Icon::Database,
                label: "Value".into_html(),
                has_caret: true,
                ..Default::default()
            }),
            InsertBehavior::UnderNode(&parent_node),
        )
        .unwrap();

    let json_string = json_data.to_string();
    tree.insert(
        Node::new(NodeData {
            icon: Icon::Database,
            label: html! {<> {"Serialized: "} <input type="text" value={json_string} /> </>},
            ..Default::default()
        }),
        InsertBehavior::UnderNode(&parent_node),
    )
    .unwrap();

    // Try to parse the JSON
    let json_value = json_data.to_value()?;
    // Render the entire JSON structure
    match json_value.deref() {
        Value::Object(obj) => {
            // Sort keys for consistent display
            let sorted_keys: BTreeMap<_, _> = obj.iter().collect();
            for (k, v) in sorted_keys {
                render_json_value(tree, &value_node, Some(k), v);
            }
        }
        Value::Array(arr) => {
            for (idx, item) in arr.iter().enumerate() {
                render_json_value(tree, &value_node, Some(&idx.to_string()), item);
            }
        }
        _ => {
            // For primitive root values
            render_json_value(tree, &value_node, None, &json_value);
        }
    }
    Ok(())
}
