use serde_json::Value;
use std::collections::BTreeMap;
use yew::prelude::*;
use yewprint::{
    Icon, NodeData,
    id_tree::{self, InsertBehavior, Node, NodeId},
};

#[derive(Properties, PartialEq, Clone)]
pub struct JsonTreeViewerProps {
    pub json_data: Vec<u8>,
}

pub fn render_json_value(
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
        Value::String(s) => format!("\"{}\"", s),
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
        Some(k) => format!("{}: {}", k, label),
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

pub fn insert_json_into_tree(
    tree: &mut id_tree::Tree<NodeData<u32>>,
    root_id: NodeId,
    json_data: &[u8],
) -> Result<(), serde_json::Error> {
    // Try to parse the JSON
    let json_value = serde_json::from_slice::<Value>(json_data)?;

    // Render the entire JSON structure
    match json_value {
        Value::Object(obj) => {
            // Sort keys for consistent display
            let sorted_keys: BTreeMap<_, _> = obj.iter().collect();
            for (k, v) in sorted_keys {
                render_json_value(tree, &root_id, Some(k), v);
            }
        }
        Value::Array(arr) => {
            for (idx, item) in arr.iter().enumerate() {
                render_json_value(tree, &root_id, Some(&idx.to_string()), item);
            }
        }
        _ => {
            // For primitive root values
            render_json_value(tree, &root_id, None, &json_value);
        }
    }
    Ok(())
}
