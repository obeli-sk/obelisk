export default function handle(request) {
    const id1 = obelisk.executionIdGenerate();
    const id2 = obelisk.executionIdGenerate();
    return Response.json({
        id1,
        id2,
        different: id1 !== id2,
        hasPrefix: id1.startsWith("E_"),
    });
}
