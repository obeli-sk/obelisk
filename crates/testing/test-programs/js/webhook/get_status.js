export default function handle(request) {
    const execId = request.headers.get("x-execution-id");
    const status = obelisk.getStatus(execId);
    return Response.json({ executionStatus: status });
}
