//! Workflow child-error and external-cancellation semantics: a re-thrown child `err`, and
//! `ChildError`s surfaced when a child execution, a durable sleep, or a join-set delay is
//! cancelled out-of-band. The manifest bundles the `my-stub` stub (imported by the rethrow
//! workflow) and a `sleep-cancellable` child workflow (submitted by the cancel-child workflow).

use super::*;

const DEPLOYMENT: &str = r#"[[workflow_js]]
name = "test_rethrow_child_error_workflow"
ffqn = "testing:integration/workflow-rethrow-child-error.rethrow-child-error"
content = '''
// Catch a child execution's string err as a ChildError and re-throw it.
// The transparent re-propagation must reproduce the original err payload as the
// workflow's own err value (i.e. `throw e` behaves like `throw e.value`).
import { myStubSubmit } from 'testing:integration-obelisk-ext/stubs';
import { myStubStub } from 'testing:integration-obelisk-stub/stubs';

export default function rethrow_child_error(id) {
    const js = obelisk.createJoinSet();
    const execId = myStubSubmit(js, id);
    myStubStub(execId, { err: 'boom' });
    try {
        js.joinNext();
    } catch (e) {
        if (!(e instanceof obelisk.ChildError)) {
            throw `expected ChildError, got: ${e}`;
        }
        throw e; // transparent re-propagation -> workflow err 'boom'
    }
    return 'unreachable';
}
import * as obelisk from "obelisk:workflow@1.0.0";
'''
params = [
  { name = "id", type = "u64" },
]
return_type = "result<string, string>"

[[workflow_js]]
name = "test_cancel_child_error_workflow"
ffqn = "testing:integration/workflow-cancel-child-error.cancel-child-error"
content = '''
// Await a child execution that is cancelled out-of-band. The child's cancellation
// is a platform failure (not a business err), so `joinNext` throws a
// `ChildError` whose `.cancelled` is true, `.failureKind` is `cancelled`
// and `.value` is projected onto the child's string err type.
export default function cancel_child_error() {
    // A named join set gives the child a well-known id the test can reconstruct.
    const js = obelisk.createJoinSet({ name: 'cancel-set' });
    const childId = js.submit('testing:integration/workflow-sleep.sleep-cancellable', []);
    try {
        js.joinNext();
    } catch (e) {
        if (!(e instanceof obelisk.ChildError)) {
            throw `expected ChildError, got: ${e}`;
        }
        if (e.cancelled !== true) {
            throw `expected cancelled=true, got: ${e.cancelled}`;
        }
        if (e.failureKind !== 'cancelled') {
            throw `expected failureKind=cancelled, got: ${e.failureKind}`;
        }
        if (e.childId !== childId) {
            throw `expected childId=${childId}, got: ${e.childId}`;
        }
        if (e.value !== 'execution_failed') {
            throw `expected value=execution_failed, got: ${JSON.stringify(e.value)}`;
        }
        return 'cancelled-child-observed';
    }
    throw 'joinNext did not throw for a cancelled child';
}
import * as obelisk from "obelisk:workflow@1.0.0";
'''
params = []
return_type = "result<string, string>"

[[workflow_js]]
name = "test_cancel_sleep_error_workflow"
ffqn = "testing:integration/workflow-cancel-sleep-error.cancel-sleep-error"
content = '''
// Validate the ChildError thrown when a named durable sleep is cancelled externally,
// then re-throw it to exercise transparent propagation of its undefined value.
export default function cancel_sleep_error() {
    try {
        obelisk.sleep({ milliseconds: 100000 }, 'cancel-sleep');
    } catch (e) {
        if (!(e instanceof obelisk.ChildError) || !(e instanceof Error)) {
            throw `expected ChildError, got: ${e}`;
        }
        if (e.name !== 'ChildError' || e.message !== 'Sleep was cancelled') {
            throw `unexpected error identity: ${e.name}: ${e.message}`;
        }
        if (e.cancelled !== true || e.failureKind !== 'cancelled') {
            throw `unexpected cancellation metadata: ${e.cancelled}, ${e.failureKind}`;
        }
        if (e.value !== undefined || e.childId !== undefined) {
            throw `unexpected payload metadata: ${e.value}, ${e.childId}`;
        }
        throw e;
    }
    throw 'sleep was not cancelled';
}
import * as obelisk from "obelisk:workflow@1.0.0";
'''
params = []
return_type = "result<string>"

[[workflow_js]]
name = "test_cancel_delay_error_workflow"
ffqn = "testing:integration/workflow-cancel-delay-error.cancel-delay-error"
content = '''
// Validate the ChildError thrown by joinNext when a submitted delay is cancelled externally.
export default function cancel_delay_error() {
    const js = obelisk.createJoinSet({ name: 'cancel-delay' });
    const delayId = js.submitDelay({ milliseconds: 100000 });
    try {
        js.joinNext();
    } catch (e) {
        if (!(e instanceof obelisk.ChildError) || !(e instanceof Error)) {
            throw `expected ChildError, got: ${e}`;
        }
        if (e.name !== 'ChildError' || e.message !== `delay ${delayId} cancelled`) {
            throw `unexpected error identity: ${e.name}: ${e.message}`;
        }
        if (e.cancelled !== true || e.failureKind !== 'cancelled') {
            throw `unexpected cancellation metadata: ${e.cancelled}, ${e.failureKind}`;
        }
        if (e.value !== undefined || e.childId !== undefined) {
            throw `unexpected payload metadata: ${e.value}, ${e.childId}`;
        }
        return 'cancelled-delay-observed';
    }
    throw 'delay was not cancelled';
}
import * as obelisk from "obelisk:workflow@1.0.0";
'''
params = []
return_type = "result<string, string>"

[[workflow_js]]
name = "test_sleep_cancellable_workflow"
ffqn = "testing:integration/workflow-sleep.sleep-cancellable"
content = '''
// Cancellable workflow that blocks on a long durable sleep, submitted as a child by
// the cancel-child workflow so the test can cancel it out-of-band.
export default function sleep_cancellable() {
    obelisk.sleep({ milliseconds: 100000 });
    return "ok";
}
import * as obelisk from "obelisk:workflow@1.0.0";
'''
params = []
return_type = "result<string, string>"

[[activity_stub]]
ffqn = "testing:integration/stubs.my-stub"
params = [
  { name = "id", type = "u64" },
]
return_type = "result<string, string>"
"#;

async fn start(ip: String, runtime: WorkflowJsTestRuntime) -> TestServer {
    TestServer::start_inline_deployment_with_js_runtime(ip, "", DEPLOYMENT, &[], runtime.mode())
        .await
}

/// A caught `ChildError` re-thrown with `throw e` transparently
/// reproduces the child's original err payload as the workflow's err.
#[rstest::rstest]
#[case::boa_wasm(WorkflowJsTestRuntime::BoaWasm)]
#[case::v8(WorkflowJsTestRuntime::V8)]
#[tokio::test]
async fn rethrow_child_error(#[case] runtime: WorkflowJsTestRuntime) {
    let server = start(runtime.ip(test_addr!(87)), runtime).await;
    let resp = server
        .submit_follow(
            "testing:integration/workflow-rethrow-child-error.rethrow-child-error",
            vec![json!(7u64)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "err": "boom" }));

    server.shutdown().await;
}

/// A child execution cancelled out-of-band surfaces to an awaiting JS parent as a
/// `ChildError` with `.cancelled === true` and `.failureKind === "cancelled"`
/// whose `.value` is projected onto the child's string err type.
#[rstest::rstest]
#[case::boa_wasm(WorkflowJsTestRuntime::BoaWasm)]
#[case::v8(WorkflowJsTestRuntime::V8)]
#[tokio::test]
async fn child_cancelled(#[case] runtime: WorkflowJsTestRuntime) {
    use concepts::{ExecutionId, JoinSetId, JoinSetKind, StrVariant};

    let server = start(runtime.ip(test_addr!(88)), runtime).await;
    let parent_id = server.generate_execution_id().await;

    // The child id is deterministic either way, but the parent's named join set makes it
    // a well-known string (first child of `n:cancel-set`) we can reconstruct here instead
    // of replaying the parent's generated join-set id.
    let join_set_id = JoinSetId::new(JoinSetKind::Named, StrVariant::from("cancel-set")).unwrap();
    let child_id = ExecutionId::Derived(
        parent_id
            .parse::<ExecutionId>()
            .unwrap()
            .next_level(&join_set_id),
    )
    .to_string();

    // Cancel concurrently with the follow so the parent is still blocked on joinNext.
    let follow = server.submit_follow_with_id(
        &parent_id,
        "testing:integration/workflow-cancel-child-error.cancel-child-error",
        vec![],
    );
    let cancel = server.cancel_execution_with_retries(&child_id);
    let (resp, ()) = tokio::join!(follow, cancel);

    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "cancelled-child-observed" }));

    server.shutdown().await;
}

/// A cancelled named sleep throws a payload-less `ChildError`; re-throwing it
/// serializes its undefined `.value` as a null workflow err payload.
#[rstest::rstest]
#[case::boa_wasm(WorkflowJsTestRuntime::BoaWasm)]
#[case::v8(WorkflowJsTestRuntime::V8)]
#[tokio::test]
async fn cancelled_sleep_rethrows_null(#[case] runtime: WorkflowJsTestRuntime) {
    use concepts::prefixed_ulid::DelayId;
    use concepts::{ExecutionId, JoinSetId, JoinSetKind, StrVariant};

    let server = start(runtime.ip(test_addr!(90)), runtime).await;
    let execution_id = server.generate_execution_id().await;
    let execution_id = execution_id.parse::<ExecutionId>().unwrap();
    let join_set_id =
        JoinSetId::new(JoinSetKind::OneOff, StrVariant::from("1-cancel-sleep")).unwrap();
    let delay_id = DelayId::new(&execution_id, &join_set_id).to_string();
    let execution_id = execution_id.to_string();

    let follow = server.submit_follow_with_id(
        &execution_id,
        "testing:integration/workflow-cancel-sleep-error.cancel-sleep-error",
        vec![],
    );
    let cancel = server.cancel_delay_with_retries(&delay_id);
    let (resp, ()) = tokio::join!(follow, cancel);

    assert_eq!(resp.status().as_u16(), 201);
    assert_eq!(resp.json::<Value>().await.unwrap(), json!({ "err": null }));

    server.shutdown().await;
}

/// A cancelled join-set delay throws a `ChildError` with cancellation metadata
/// and no business error payload.
#[rstest::rstest]
#[case::boa_wasm(WorkflowJsTestRuntime::BoaWasm)]
#[case::v8(WorkflowJsTestRuntime::V8)]
#[tokio::test]
async fn cancelled_delay_surfaces_child_error(#[case] runtime: WorkflowJsTestRuntime) {
    use concepts::prefixed_ulid::DelayId;
    use concepts::{ExecutionId, JoinSetId, JoinSetKind, StrVariant};

    let server = start(runtime.ip(test_addr!(91)), runtime).await;
    let execution_id = server.generate_execution_id().await;
    let execution_id = execution_id.parse::<ExecutionId>().unwrap();
    let join_set_id = JoinSetId::new(JoinSetKind::Named, StrVariant::from("cancel-delay")).unwrap();
    let delay_id = DelayId::new(&execution_id, &join_set_id).to_string();
    let execution_id = execution_id.to_string();

    let follow = server.submit_follow_with_id(
        &execution_id,
        "testing:integration/workflow-cancel-delay-error.cancel-delay-error",
        vec![],
    );
    let cancel = server.cancel_delay_with_retries(&delay_id);
    let (resp, ()) = tokio::join!(follow, cancel);

    assert_eq!(resp.status().as_u16(), 201);
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!({ "ok": "cancelled-delay-observed" })
    );

    server.shutdown().await;
}
