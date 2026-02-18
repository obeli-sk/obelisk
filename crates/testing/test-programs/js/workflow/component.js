function fetch_components() {
    const js = obelisk.createJoinSet();
    const activityFfqn = 'testing:js/activity.fetch-get';
    const execId = js.submit(activityFfqn, ["http://localhost:5005/v1/components"]);
    console.log('Submitted execId:', execId);
    const response = js.joinNext();
    if (response.err !== undefined) {
        throw 'activity failed';
    }
    console.log('response:', JSON.stringify(response));
    return "ok";
}
