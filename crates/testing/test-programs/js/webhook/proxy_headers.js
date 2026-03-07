export default async function handle(request) {
    const targetAddr = request.headers.get("x-target-addr");
    return fetch(`http://${targetAddr}/`, { headers: request.headers });
}
