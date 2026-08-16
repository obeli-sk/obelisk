include!(concat!(env!("OUT_DIR"), "/gen.rs"));

pub const ACTIVITY_JS_RUNTIME_LOCATION: &str = include_str!("../activity-js-runtime-version.txt");
pub const WORKFLOW_JS_RUNTIME_LOCATION: &str = include_str!("../workflow-js-runtime-version.txt");
pub const WEBHOOK_JS_RUNTIME_LOCATION: &str = include_str!("../webhook-js-runtime-version.txt");
pub const WEBUI_LOCATION: &str = include_str!("../webui-version.txt");
