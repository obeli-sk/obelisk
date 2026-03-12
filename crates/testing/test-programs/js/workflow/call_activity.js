// Use obelisk.call to submit and await an activity
export default function call_activity(a, b) {
    const result = obelisk.call('testing:integration/activity-add.add', [a, b]);
    console.log('Got result:', result);
    return result;
}
