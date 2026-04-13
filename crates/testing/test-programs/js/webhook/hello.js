export default function handle(request) {
    console.info(request);
    const execId = obelisk.executionIdCurrent();
    return new Response("Hello from JS webhook!", {
        status: 200,
        headers: { "content-type": "text/plain", "x-execution-id": execId },
    });
}
