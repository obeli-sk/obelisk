export default async function handle(request) {
    const form = await request.formData();
    return Response.json(form);
}
