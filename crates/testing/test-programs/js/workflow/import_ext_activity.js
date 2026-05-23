// Use ES module ext imports to submit and await an activity via join set
import { addSubmit, addAwaitNext } from 'testing:integration-obelisk-ext/activity';

export default function add_via_activity(a, b) {
    const js = obelisk.createJoinSet();
    addSubmit(js, a, b);
    const result = addAwaitNext(js);
    console.log('Got execId:', js.lastId, 'result:', JSON.stringify(result));
    return result;
}
