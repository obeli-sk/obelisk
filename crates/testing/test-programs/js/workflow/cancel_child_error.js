// Await a child execution that is cancelled out-of-band. The child's cancellation
// is a platform failure (not a business err), so `joinNext` throws a
// `ChildExecutionError` whose `.cancelled` is true, `.failureKind` is `cancelled`
// and `.value` is undefined (no err payload to carry).
export default function cancel_child_error() {
    // A named join set gives the child a well-known id the test can reconstruct.
    const js = obelisk.createJoinSet({ name: 'cancel-set' });
    const childId = js.submit('testing:integration/workflow-sleep.sleep-cancellable', []);
    try {
        js.joinNext();
    } catch (e) {
        if (!(e instanceof obelisk.ChildExecutionError)) {
            throw `expected ChildExecutionError, got: ${e}`;
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
        if (e.value !== undefined) {
            throw `expected value=undefined, got: ${JSON.stringify(e.value)}`;
        }
        return 'cancelled-child-observed';
    }
    throw 'joinNext did not throw for a cancelled child';
}
