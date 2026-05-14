// Use ES module import to call an activity (instead of obelisk.call)
import { add } from 'testing:integration/activity';

export default function call_activity(a, b) {
    const result = add(a, b);
    console.log('Got result:', result);
    return result;
}
