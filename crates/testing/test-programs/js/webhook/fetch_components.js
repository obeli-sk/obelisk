export default async function handle(request) {
    const port = request.headers["x-target-port"]?.[0] || "5005";
    const resp = await fetch(`http://127.0.0.1:${port}/v1/components`, {
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
