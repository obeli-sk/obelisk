// Use createJoinSet + submit + joinNext to call an activity
export default function add_via_activity(a, b) {
    const js = createJoinSet();
    const execId = js.submit('testing:integration/activity.add', [a, b]);
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
