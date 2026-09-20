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

const ADD_JS: &str = r"export default function add(a, b) {
    return a + b;
}
";

const ACTIVITY_INDEX_JS: &str = r"import { greet } from './lib/greeter.js';
import { exclaim } from './lib/util.js';

export default function multifileActivity(name) {
    return exclaim(greet(name));
}
";

const ACTIVITY_GREETER_JS: &str = r"import { exclaim } from './util.js';

export function greet(name) {
    return exclaim(`hello, ${name}`);
}
";

const ACTIVITY_UTIL_JS: &str = r"export function exclaim(message) {
    return message + '!';
}
";

const WORKFLOW_INDEX_JS: &str = r"import * as activity from 'testing:integration/activity';
import { computeTotal } from './lib/math.js';
import { core } from './lib/reexport.js';

export default function multifileWorkflow(a, b, c) {
    const values = core.export(null, [activity.add(a, b), c]);
    return computeTotal(Number(values[0]), Number(values[1]));
}
";

const WORKFLOW_MATH_JS: &str = r"import * as activity from 'testing:integration/activity';

export function computeTotal(partial, c) {
    return activity.add(partial, c);
}
";

// These are reduced versions of syntax found in workflow-agent's just-bash graph. In
// particular, `export(...)` is an object method, not an ESM declaration, and the regular
// expression contains braces that must not affect module-declaration detection.
const WORKFLOW_CORE_JS: &str = r"export const core = {
    export(_interp, args) {
        return args.map(String);
    },
};

export function containsBrace(value) {
    return /[{}]/.test(value);
}
";

// Keep import and export on one line. The production bundle contains this compact form.
const WORKFLOW_REEXPORT_JS: &str = r"import { core } from './core.js'; export { core };";

const WEBHOOK_INDEX_JS: &str = r"import { renderJson } from './lib/render.js';

export default function multifileWebhook(_request) {
    return renderJson({ ok: true, message: 'multifile webhook works' });
}
";

const WEBHOOK_RENDER_JS: &str = r#"const embeddedModuleExample = `
import mermaid from "https://example.invalid/mermaid.mjs";
export default function renderGraph() { return "not an actual module declaration"; }
`;

export function renderJson(payload) {
    if (!embeddedModuleExample.includes("export default")) throw new Error("template changed");
    return Response.json(payload);
}
"#;

async fn start(ip: String, server_toml: &str) -> TestServer {
    let files = [
        ("add.js", ADD_JS),
        ("multifile-activity/index.js", ACTIVITY_INDEX_JS),
        ("multifile-activity/lib/greeter.js", ACTIVITY_GREETER_JS),
        ("multifile-activity/lib/util.js", ACTIVITY_UTIL_JS),
        ("multifile-workflow/index.js", WORKFLOW_INDEX_JS),
        ("multifile-workflow/lib/math.js", WORKFLOW_MATH_JS),
        ("multifile-workflow/lib/core.js", WORKFLOW_CORE_JS),
        ("multifile-workflow/lib/reexport.js", WORKFLOW_REEXPORT_JS),
        ("multifile-webhook/index.js", WEBHOOK_INDEX_JS),
        ("multifile-webhook/lib/render.js", WEBHOOK_RENDER_JS),
    ];
    TestServer::start_inline_deployment(ip, server_toml, DEPLOYMENT, &files).await
}

#[rstest::rstest]
#[case::boa_wasm(JsRuntime::BoaWasm)]
#[case::v8(JsRuntime::V8)]
#[tokio::test]
async fn activity(#[case] runtime: JsRuntime) {
    let ip = match runtime {
        JsRuntime::BoaWasm => test_addr!(120),
        JsRuntime::V8 => test_addr!(163),
    };
    let server = start(ip, &runtime.server_toml(&["activities"])).await;
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

#[rstest::rstest]
#[case::boa_wasm(JsRuntime::BoaWasm)]
#[case::v8(JsRuntime::V8)]
#[tokio::test]
async fn workflow(#[case] runtime: JsRuntime) {
    let ip = match runtime {
        JsRuntime::BoaWasm => test_addr!(121),
        JsRuntime::V8 => test_addr!(173),
    };
    let server = start(ip, &runtime.server_toml(&["workflows"])).await;
    let execution_id = server.generate_execution_id().await;
    let resp = server
        .submit_follow_with_id(
            &execution_id,
            "testing:integration/workflow-multifile.add-three",
            vec![json!(2), json!(3), json!(5)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201, "runtime: {}", runtime.name());
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!({ "ok": 10 }),
        "runtime: {}",
        runtime.name()
    );

    let events_before = server.get_events(&execution_id).await;
    let replay = server.replay(&execution_id).await;
    assert_eq!(
        replay.status().as_u16(),
        200,
        "multifile JS replay failed using {}: {}",
        runtime.name(),
        replay.text().await.unwrap()
    );
    assert_eq!(
        events_before,
        server.get_events(&execution_id).await,
        "replay using {} must not mutate the execution history",
        runtime.name()
    );
    server.shutdown().await;
}

#[rstest::rstest]
#[case::boa_wasm(JsRuntime::BoaWasm)]
#[case::v8(JsRuntime::V8)]
#[tokio::test]
async fn webhook(#[case] runtime: JsRuntime) {
    let ip = match runtime {
        JsRuntime::BoaWasm => test_addr!(122),
        JsRuntime::V8 => test_addr!(164),
    };
    let server = start(ip, &runtime.server_toml(&["webhooks"])).await;
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
