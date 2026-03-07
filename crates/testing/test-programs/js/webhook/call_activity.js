// Webhook that calls an activity using obelisk.call
export default function handle(request) {
    const a = Number(obelisk.env('a'));
    const b = Number(obelisk.env('b'));

    // Call the add activity and wait for result
    const result = obelisk.call("testing:integration/activities.add", [a, b]);

    return Response.json({ result });
}
