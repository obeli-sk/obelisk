function add_via_activity(a, b) {
    const js = obelisk.createJoinSet();
    const execId = js.submit('testing:integration/activities.add', [a, b]);
    console.log('Submitted add activity, execId:', execId);
    const response = js.joinNext();
    if (!response.ok) {
        throw 'activity failed';
    }
    const result = obelisk.getResult(response.id);
    console.log('Got result:', JSON.stringify(result));
    return result.ok;
}
