import * as dynamic from "obelisk:workflow-dynamic@1.0.0";

// Use dynamic.call to submit and await an activity
export default function call_activity(a, b) {
    const result = dynamic.call('testing:integration/activity.add', [a, b]);
    console.log('Got result:', result);
    return result;
}
