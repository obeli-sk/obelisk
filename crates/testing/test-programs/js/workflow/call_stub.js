import * as dynamic from "obelisk:workflow-dynamic@1.0.0";
// Call inline stub activity and provide its result via obelisk.stub().
export default function call_stub(id) {
    const js = obelisk.createJoinSet();
    const execId = dynamic.submit(js, 'testing:integration/stubs.my-stub', [id]);
    console.log("stubbed id", execId);
    obelisk.stub(execId, { 'ok': 'stub-ok' });
    return js.joinNext();
}
import * as obelisk from "obelisk:workflow@1.0.0";
