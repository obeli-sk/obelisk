
import * as dynamic from "obelisk:webhook-dynamic@1.0.0";

// Webhook that calls an activity dynamically
export default function handle(request) {
    const a = Number(process.env['a']);
    const b = Number(process.env['b']);

    // Call the add activity and wait for result
    const result = call(a, b);

    return Response.json({ result });
}

function call(a, b) {
    return function (a, b) {
        return dynamic.call("testing:integration/activity.add", [a, b]);
    }(a, b)
}
