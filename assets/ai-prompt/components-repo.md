# Obelisk Components

Reusable components for [Obelisk](https://obeli.sk/).
Each component folder contains a README.md and a `*-oci.toml` deployment file with the component's OCI reference.
Components can be added to a local deployment.toml copying `*-oci.toml` sections or by using `obelisk component add`.

## Components

### Activities

Activities are components that perform side effects (HTTP calls, database operations, etc.).

| Component | Description |
|-----------|-------------|
| [activity-docker](docker/activity-docker) | Docker container management using Process API |
| [activity-fly-http](fly/activity-fly-http) | Fly.io API (apps, machines, secrets, volumes) |
| [activity-github-graphql](github/activity-github-graphql) | GitHub GraphQL API (account info, stargazers) |
| [activity-http-generic](http/activity-http-generic) | Generic HTTP client |
| [activity-obelisk-client-http](obelisk/activity-obelisk-client-http) | Obelisk API client |
| [activity-openai-responses](openai/activity-openai-responses) | OpenAI Responses API |
| [activity-postmark-email](postmark/activity-postmark-email) | Postmark email API |
| [activity-sendgrid-email](sendgrid/activity-sendgrid-email) | SendGrid email API |

### Webhooks

Webhooks are HTTP endpoint handlers that receive external events.

| Component | Description |
|-----------|-------------|
| [webhook-fly-secrets-updater](fly/webhook-fly-secrets-updater) | WASM webhook that directly calls fly's secrets endpoint |

## Adding components
Use
```sh
obelisk component add oci://docker.io/repo/image:tag new_name --locked -d deployment.toml
```
to add a component to a local deployment file.

Export WITs of all external components:
```sh
obelisk generate wit-deps -d deployment.toml --skip-local ./wit
```
