// Catch a child execution's string err as a ChildExecutionError and re-throw it.
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
        if (!(e instanceof obelisk.ChildExecutionError)) {
            throw `expected ChildExecutionError, got: ${e}`;
        }
        throw e; // transparent re-propagation -> workflow err 'boom'
    }
    return 'unreachable';
}
