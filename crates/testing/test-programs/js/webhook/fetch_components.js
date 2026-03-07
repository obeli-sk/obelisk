export default async function handle(request) {
    const addr = request.headers.get("x-target-addr") || "127.0.0.1:5005";
    const resp = await fetch(`http://${addr}/v1/components`, {
        headers: {
            "accept": "application/json"
        }
    });
    return resp;
}
