// Use ES module ext imports to submit and await an activity via join set
import { addSubmit, addAwaitNext } from 'testing:integration-obelisk-ext/activity';

export default function add_via_activity(a, b) {
    const js = obelisk.createJoinSet();
    addSubmit(js, a, b);
    const [execId, result] = addAwaitNext(js);
    console.log('Got execId:', execId, 'result:', JSON.stringify(result));
    return result.val;
}
