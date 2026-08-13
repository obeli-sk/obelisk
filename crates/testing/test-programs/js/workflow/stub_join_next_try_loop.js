// Submit a stub whose result is never injected, then poll forever with
// joinNextTry(). The response stays pending, so joinNextTry() returns undefined
// on every iteration and the workflow never terminates.
import { myStubSubmit } from 'testing:integration-obelisk-ext/stubs';

export default function stub_join_next_try_loop(id) {
    const js = obelisk.createJoinSet();
    myStubSubmit(js, id);
    for (;;) {
        const result = js.joinNextTry();
        if (result !== undefined) {
            throw `stub result was injected unexpectedly: ${JSON.stringify(result)}`;
        }
        // obelisk.sleep({ seconds: 1 });
    }
}
