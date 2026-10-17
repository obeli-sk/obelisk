# URL health dashboard

A web page where you paste a list of URLs and see each one's HTTP status, latency, size and page
title. It is built from three components:

- `webhook/dashboard.js` serves the page and a small JSON API. `POST /api/check` schedules the
  workflow and returns its execution ID; `GET /api/result/<id>` reports progress or the results.
- `workflow/check-batch.js` is a durable workflow. It submits one `check-url` activity per URL so
  the checks run in parallel, then collects the results in input order.
- `activity/check-url.js` performs one GET request. A network error throws, so Obelisk retries the
  activity (`max_retries` in `deployment.toml`) before the workflow records the error.

Run it from this directory:

```sh
obelisk server run --app-config app.toml --deployment deployment.toml
```

Then open <http://localhost:9090/> or use the API directly:

```sh
curl -X POST http://localhost:9090/api/check -d '{"urls": ["https://example.com"]}'
curl http://localhost:9090/api/result/<execution_id>
```

## Outbound HTTP

The dashboard fetches whatever URLs its visitors submit, so the allowed destinations matter. Both
`app.toml` and `deployment.toml` allow `GET` to any HTTPS host on port 443. `app.toml` defines the
app administrator's maximum allowance; `deployment.toml` selects what this deployment uses.
Before exposing the dashboard beyond your machine, narrow both patterns to the hosts you want to
monitor, e.g. `https://*.example.com`.

## Web UI link

After scheduling a check, the page links the execution in the Obelisk Web UI. By default the link
points to port 8080 on the dashboard's own host, which matches a local server and Obelisk cloud. Set
`WEBUI_URL` (e.g. `WEBUI_URL=https://webui.example.com`) before starting the server to override it.
