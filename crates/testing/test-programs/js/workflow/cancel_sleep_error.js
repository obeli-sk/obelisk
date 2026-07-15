// Validate the ChildError thrown when a named durable sleep is cancelled externally,
// then re-throw it to exercise transparent propagation of its undefined value.
export default function cancel_sleep_error() {
    try {
        obelisk.sleep({ milliseconds: 100000 }, 'cancel-sleep');
    } catch (e) {
        if (!(e instanceof obelisk.ChildError) || !(e instanceof Error)) {
            throw `expected ChildError, got: ${e}`;
        }
        if (obelisk.ChildExecutionError !== obelisk.ChildError) {
            throw 'expected deprecated ChildExecutionError alias';
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
