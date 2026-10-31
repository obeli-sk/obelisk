# Site health dashboard

A cron schedule probes one website every 5 minutes, and a web page shows its uptime, latency
history and the log of recent probes. It is built from three components:

- `activity/check-url.js` performs one GET request. A network error throws, so Obelisk retries the
  activity (`max_retries` in `deployment.toml`) before the workflow sees the error.
- `workflow/check-site.js` is a durable workflow that runs the activity and returns the probed URL with
  its outcome, recording an outage once the retries are exhausted. The `[[cron]]` entry in `deployment.toml` schedules it.
- `webhook/dashboard.js` serves the page. `GET /api/status` lists recent workflow runs through the
  Obelisk API and reads their results; `POST /api/check` schedules a probe right away.

Run it from this directory:

```sh
obelisk server run --app-config app.toml --deployment deployment.toml
```

Then open <http://localhost:9090/>.

## Monitoring another site

The site is `https://example.com/`. To change it, edit `deployment.toml` (the cron `params`, the
activity's `allowed_host` and the dashboard's `TARGET_URL`) and the matching `allowed_host` in
`app.toml`. `app.toml` defines the app administrator's maximum allowance; `deployment.toml` selects
what this deployment uses.

## Obelisk API access

The dashboard calls the Obelisk API with `INTERNAL_TOKEN`, an internal secret that the server
supplies. It grants full API access, so `app.toml` and `deployment.toml` allow sending it only to
`GET /v1/executions`. The component sees an opaque placeholder; Obelisk substitutes the real token
in the request headers. `OBELISK_API_URL` and `OBELISK_UI_URL` are likewise supplied by the server.
