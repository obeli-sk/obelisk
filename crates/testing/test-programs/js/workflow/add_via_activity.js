// Use createJoinSet + submit + joinNext to call an activity
export default function add_via_activity(a, b) {
    const js = createJoinSet();
    const execId = js.submit('testing:integration/activity-add.add', [a, b]);
    console.log('Submitted add activity, execId:', execId);
    const response = js.joinNext();
    if (!response.ok) {
        throw 'activity failed';
    }
    const result = obelisk.getResult(response.id);
    console.log('Got result:', JSON.stringify(result));
    return result.ok;
}

function createJoinSet() {
    return obelisk.createJoinSet();
}
