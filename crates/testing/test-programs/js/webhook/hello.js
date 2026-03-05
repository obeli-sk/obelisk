export default function handle(request) {
    console.info(request);
    return new Response("Hello from JS webhook!", {
        status: 200,
        headers: { "content-type": "text/plain" },
    });
}
