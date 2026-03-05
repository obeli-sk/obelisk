export default function handle(request) {
    const customHeaders = request.headers["x-custom"] || [];
    return {
        status: 200,
        headers: { "content-type": "application/json" },
        body: JSON.stringify(customHeaders)
    };
}
