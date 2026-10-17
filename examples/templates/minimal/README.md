# JavaScript HTTP starter

This app has one webhook, one durable workflow, and one HTTP activity. The webhook calls the
workflow, which asks the activity to GET `https://example.com/` and returns its status code.

Run it from this directory:

```sh
obelisk server run --app-config app.toml --deployment deployment.toml
```

Then, in another terminal:

```sh
curl http://localhost:9090/run
```

The HTTP destination is allowed in both `app.toml` and `deployment.toml`. Review both files when
changing the destination. `app.toml` defines the app administrator's maximum allowance;
`deployment.toml` selects what this deployment uses.
