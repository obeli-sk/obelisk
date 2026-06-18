#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeploymentComponentType {
    WorkflowWasm,
    WorkflowJs,
    ActivityWasm,
    ActivityJs,
    ActivityExec,
    ActivityStub,
    ActivityExternal,
    WebhookEndpointWasm,
    WebhookEndpointJs,
    Cron,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
pub(crate) struct DeploymentComponentCount {
    pub(crate) component_type: DeploymentComponentType,
    pub(crate) count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
pub(crate) struct DeploymentComponentSummary {
    pub(crate) components: Vec<DeploymentComponentCount>,
}

const COMPONENT_SECTIONS: &[(&str, DeploymentComponentType)] = &[
    ("workflow_wasm", DeploymentComponentType::WorkflowWasm),
    ("workflow_js", DeploymentComponentType::WorkflowJs),
    ("activity_wasm", DeploymentComponentType::ActivityWasm),
    ("activity_js", DeploymentComponentType::ActivityJs),
    ("activity_exec", DeploymentComponentType::ActivityExec),
    ("activity_stub", DeploymentComponentType::ActivityStub),
    (
        "activity_external",
        DeploymentComponentType::ActivityExternal,
    ),
    (
        "webhook_endpoint_wasm",
        DeploymentComponentType::WebhookEndpointWasm,
    ),
    (
        "webhook_endpoint_js",
        DeploymentComponentType::WebhookEndpointJs,
    ),
    ("cron", DeploymentComponentType::Cron),
];

pub(crate) fn deployment_component_summary(
    deployment_toml: &str,
) -> Option<DeploymentComponentSummary> {
    let manifest: toml::Value = toml::from_str(deployment_toml).ok()?;
    let manifest = manifest.as_table()?;

    let components = COMPONENT_SECTIONS
        .iter()
        .filter_map(|(section, component_type)| {
            let count = manifest
                .get(*section)
                .and_then(toml::Value::as_array)?
                .len();
            if count == 0 {
                return None;
            }
            Some(DeploymentComponentCount {
                component_type: *component_type,
                count: u32::try_from(count).unwrap_or(u32::MAX),
            })
        })
        .collect();

    Some(DeploymentComponentSummary { components })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_components_by_manifest_section() {
        let summary = deployment_component_summary(
            r#"
[[activity_exec]]
name = "danger-1"
location = "run.sh"

[[activity_exec]]
name = "danger-2"
location = "run-other.sh"

[[activity_js]]
name = "sandboxed"
location = "run.js"

[[workflow_wasm]]
name = "workflow"
location = "workflow.wasm"
"#,
        )
        .expect("valid manifest");

        assert_eq!(
            summary.components,
            vec![
                DeploymentComponentCount {
                    component_type: DeploymentComponentType::WorkflowWasm,
                    count: 1,
                },
                DeploymentComponentCount {
                    component_type: DeploymentComponentType::ActivityJs,
                    count: 1,
                },
                DeploymentComponentCount {
                    component_type: DeploymentComponentType::ActivityExec,
                    count: 2,
                },
            ]
        );
    }

    #[test]
    fn empty_manifest_has_present_empty_summary() {
        let summary = deployment_component_summary("").expect("empty TOML is valid");
        assert!(summary.components.is_empty());
    }
}
