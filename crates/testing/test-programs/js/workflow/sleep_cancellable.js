// Cancellable workflow that blocks on a long durable sleep. The cancellation
// driver can then cancel it while it is running, cancel the pending delay, and
// finish it as Cancelled without any worker running WASM.
export default function sleep_cancellable() {
    obelisk.sleep({ milliseconds: 100000 });
    return "ok";
}
