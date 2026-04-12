export default async function handle(request) {
    const text = await request.text();
    return new Response(text, {
        status: 200,
        headers: { "content-type": "text/plain" },
    });
}
