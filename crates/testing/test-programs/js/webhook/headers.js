export default function handle(request) {
    const value = request.headers.get("x-custom");
    console.log("header value:`" + value + "`");
    return Response.json(value !== null ? value.split(", ") : []);
}
