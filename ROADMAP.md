# Immediate goals

# Upcoming goals

## Activities
* External activity executor gRPC API, streaming connection ends on hot redeploy
* Lock extension - define max duration, but lock is extended periodically every `lock_duration`, makes for a heartbeat.

## Security
* Dynamic secrets loaded from Vault, missing dynamic secrets should be a warning
* Secure API endpoint: JS webhook "around" function defined in server.toml

## Workflows
workflow.legacy - not part of fn registry, can execute old execs without upgrades

## Extend /execution/list
ExecutionsListParams needs to be able to search by pending status (finished, !finished),
also fetch only pending in the future / range, also in webUI. WebUI should show pending at relative to now.

Add finished column to execution/list, allow sorting by created, first scheduled, finished

## feat: Retention
Perform a cascading delete of top-level executions that finished more than a certain number of days ago.

## feat: Allow setting retry config, timeouts during execution creation using an extension function
Change `-submit` extension signature to accept a config record.

## Future ideas
* Optional caching of activity executions with a TTL - serve cached response if parameters are the same
* [Deadline propagation](https://sre.google/sre-book/addressing-cascading-failures)
* [Cancellation propagation](https://sre.google/sre-book/addressing-cascading-failures)
* Queue capacity setting, adding backpressure to execution submission
* Smart dependency routing from a caller via an interface import to one of many components that export it.
* Smart retries - Retry budget, disabling retries when the activity is failing certain % of requests
* Configurable jitter added to retries
