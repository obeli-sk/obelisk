function fetch_components() {
    const activityFfqn = 'testing:js/activity.fetch-get';
    const result = obelisk.call(activityFfqn,
        ["http://localhost:5005/v1/components", [["accept", "application/json"]]]);
    console.log('child result:', result);
    return result;
}
