//! Multi-file JS components (activity, workflow, webhook), each loaded from an `index.js`
//! that imports sibling modules under `lib/`. The workflow additionally imports the `add`
//! activity, so the manifest bundles it too.

use super::*;

const DEPLOYMENT: &str = r#"[[activity_js]]
name = "test_add_activity"
location = "./add.js"
ffqn = "testing:integration/activity.add"
params = [
  { name = "a", type = "u32" },
  { name = "b", type = "u32" },
]
return_type = "result<u32, string>"

[[activity_js]]
name = "test_multifile_activity"
location = "./multifile-activity/index.js"
ffqn = "testing:integration/activity-multifile.greet"
params = [{ name = "name", type = "string" }]
return_type = "result<string, string>"

[[workflow_js]]
name = "test_multifile_workflow"
location = "./multifile-workflow/index.js"
ffqn = "testing:integration/workflow-multifile.add-three"
params = [
  { name = "a", type = "u32" },
  { name = "b", type = "u32" },
  { name = "c", type = "u32" },
]
return_type = "result<u32, string>"

[[webhook_endpoint_js]]
name = "test_multifile_webhook"
location = "./multifile-webhook/index.js"
routes = [{ methods = ["GET"], route = "/multifile" }]
"#;

const ADD_JS: &str = r#"export default function add(a, b) {
    return a + b;
}
"#;

const ACTIVITY_INDEX_JS: &str = r#"import { greet } from './lib/greeter.js';
import { exclaim } from './lib/util.js';

export default function multifileActivity(name) {
    return exclaim(greet(name));
}
"#;

const ACTIVITY_GREETER_JS: &str = r#"import { exclaim } from './util.js';

export function greet(name) {
    return exclaim(`hello, ${name}`);
}
"#;

const ACTIVITY_UTIL_JS: &str = r#"export function exclaim(message) {
    return message + '!';
}
"#;

const WORKFLOW_INDEX_JS: &str = r#"import * as activity from 'testing:integration/activity';
import { computeTotal } from './lib/math.js';

export default function multifileWorkflow(a, b, c) {
    return computeTotal(activity.add(a, b), c);
}
"#;

const WORKFLOW_MATH_JS: &str = r#"import * as activity from 'testing:integration/activity';

export function computeTotal(partial, c) {
    return activity.add(partial, c);
}
"#;

const WEBHOOK_INDEX_JS: &str = r#"import { renderJson } from './lib/render.js';

export default function multifileWebhook(_request) {
    return renderJson({ ok: true, message: 'multifile webhook works' });
}
"#;

const WEBHOOK_RENDER_JS: &str = r#"export function renderJson(payload) {
    return Response.json(payload);
}
"#;

async fn start(ip: String) -> TestServer {
    let files = [
        ("add.js", ADD_JS),
        ("multifile-activity/index.js", ACTIVITY_INDEX_JS),
        ("multifile-activity/lib/greeter.js", ACTIVITY_GREETER_JS),
        ("multifile-activity/lib/util.js", ACTIVITY_UTIL_JS),
        ("multifile-workflow/index.js", WORKFLOW_INDEX_JS),
        ("multifile-workflow/lib/math.js", WORKFLOW_MATH_JS),
        ("multifile-webhook/index.js", WEBHOOK_INDEX_JS),
        ("multifile-webhook/lib/render.js", WEBHOOK_RENDER_JS),
    ];
    TestServer::start_inline_deployment(ip, "", DEPLOYMENT, &files).await
}

#[tokio::test]
async fn activity() {
    let server = start(test_addr!(120)).await;
    let resp = server
        .submit_follow(
            "testing:integration/activity-multifile.greet",
            vec![json!("world")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!({ "ok": "hello, world!!" })
    );
    server.shutdown().await;
}

#[tokio::test]
async fn workflow() {
    let server = start(test_addr!(121)).await;
    let resp = server
        .submit_follow(
            "testing:integration/workflow-multifile.add-three",
            vec![json!(2), json!(3), json!(5)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    assert_eq!(resp.json::<Value>().await.unwrap(), json!({ "ok": 10 }));
    server.shutdown().await;
}

#[tokio::test]
async fn webhook() {
    let server = start(test_addr!(122)).await;
    let resp = server
        .client
        .get(format!("{}/multifile", server.webhook_base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!({ "ok": true, "message": "multifile webhook works" })
    );
    server.shutdown().await;
}
