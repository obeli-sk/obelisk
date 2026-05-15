// Use ES module namespace import to call an activity (import * as ns)
import * as activity from 'testing:integration/activity';

export default function call_activity(a, b) {
    const result = activity.add(a, b);
    console.log('Got result:', result);
    return result;
}
