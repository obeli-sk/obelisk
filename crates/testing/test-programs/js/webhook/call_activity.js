// Webhook that calls an activity using obelisk.call
export default function handle(request) {
    const a = Number(process.env['a']);
    const b = Number(process.env['b']);

    // Call the add activity and wait for result
    const result = call(a, b);

    return Response.json({ result });
}

function call(a, b) {
    return function (a, b) {
        return obelisk.call("testing:integration/activities.add", [a, b]);
    }(a, b)
}
