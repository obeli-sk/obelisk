// Webhook that calls an activity using ES module import
import { add } from 'testing:integration/activity';

export default function handle(request) {
    const result = add(5, 7);
    return Response.json({ result });
}
