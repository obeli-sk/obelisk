import * as dynamic from "obelisk:workflow-dynamic@1.0.0";
// Use createJoinSet + submit + joinNext to call an activity
export default function add_via_activity(a, b) {
    const js = createJoinSet();
    const execId = dynamic.submit(js, 'testing:integration/activity.add', [a, b]);
    console.log('Submitted add activity, execId:', execId);
    const result = js.joinNext();
    if (js.lastId !== execId) {
        throw 'unexpected completed execution';
    }
    console.log('Got result:', JSON.stringify(result));
    return result;
}

function createJoinSet() {
    return obelisk.createJoinSet();
}
import * as obelisk from "obelisk:workflow@1.0.0";
