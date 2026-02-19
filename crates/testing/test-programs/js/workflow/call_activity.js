function call_activity(a, b) {
    const result = obelisk.call('testing:integration/activities.add', [a, b]);
    console.log('Got result:', result);
    return result;
}
