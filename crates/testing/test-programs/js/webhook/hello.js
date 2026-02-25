function handle(request) {
    return {
        status: 200,
        headers: [["content-type", "text/plain"]],
        body: "Hello from JS webhook!"
    };
}
