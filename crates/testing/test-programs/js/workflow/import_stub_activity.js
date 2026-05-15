// Use ES module ext + stub imports instead of raw obelisk.* API
import { myStubSubmit, myStubAwaitNext } from 'testing:integration-obelisk-ext/stubs';
import { myStubStub } from 'testing:integration-obelisk-stub/stubs';

export default function call_stub(id) {
    const js = obelisk.createJoinSet();
    const execId = myStubSubmit(js, id);
    myStubStub(execId, { 'ok': 'stub-ok' });
    const [, result] = myStubAwaitNext(js);
    return result.val;
}
