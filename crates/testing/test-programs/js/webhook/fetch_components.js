export default async function handle(request) {
    const addr = request.headers["x-target-addr"]?.[0] || "127.0.0.1:5005";
    const resp = await fetch(`http://${addr}/v1/components`, {
        headers: {
            "accept": "application/json"
        }
    });
    const text = await resp.text();
    return {
        status: 200,
        headers: [],
        body: text
    };
}
