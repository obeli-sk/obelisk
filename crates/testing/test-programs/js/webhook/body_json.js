export default async function handle(request) {
    const data = await request.json();
    return Response.json({ received: data });
}
