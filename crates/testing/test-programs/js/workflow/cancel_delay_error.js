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
