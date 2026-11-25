# JavaScript HTTP starter

The webhook in `src/webhook/handle.js` calls the durable workflow in `src/workflow/run.js`.
The workflow uses the activity in `src/activity/fetch-status.js` to fetch `https://example.com/`
and return its HTTP status code.

## Deploy to Obelisk Cloud

Push from this directory.

```sh
obelisk cloud app push
```

Call `/run` on the external server URL printed by the push. Replace the example
hostname with your app's hostname.

```sh
curl https://my-app.obelisk.works/run
```

## Run locally

```sh
obelisk server run --app-config app.toml --deployment deployment.toml
```

Call the webhook from another terminal.

```sh
curl http://localhost:9090/run
```

The HTTP destination is allowed in both `app.toml` and `deployment.toml`. Review both files when
changing the destination. `app.toml` defines the app administrator's maximum allowance;
`deployment.toml` selects what this deployment uses.
