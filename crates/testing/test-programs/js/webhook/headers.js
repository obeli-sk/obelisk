export default function handle(request) {
    const value = request.headers.get("x-custom");
    return Response.json(value !== null ? value.split(",") : []);
}
