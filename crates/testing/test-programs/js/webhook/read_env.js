export default function handle(_request) {
    const value = process.env["WEBHOOK_TEST_ENV_VAR"];
    if (value === undefined) {
        return new Response("env var not found", { status: 500 });
    }
    return new Response(value, {
        status: 200,
        headers: { "content-type": "text/plain" },
    });
}
