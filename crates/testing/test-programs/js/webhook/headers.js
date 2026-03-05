export default function handle(request) {
    const customHeaders = request.headers["x-custom"] || [];
    return Response.json(customHeaders);
}
