# Immediate goals

# Upcoming goals

## Activities
* External activity executor gRPC API

## Security
* Dynamic secrets loaded from Vault, missing dynamic secrets should be a warning

## Workflows
Ability to drive old workflows to completion when auto upgrade fails.

## Future ideas
* Perform a cascading delete of top-level executions, deployments, CAS entries that finished more than a certain number of days ago.
* Optional caching of activity executions with a TTL - serve cached response if parameters are the same
* [Deadline propagation](https://sre.google/sre-book/addressing-cascading-failures)
* Queue capacity setting, adding backpressure to execution submission
* Smart dependency routing from a caller via an interface import to one of many components that export it.
* Smart retries - Retry budget, disabling retries when the activity is failing certain % of requests
* Configurable jitter added to retries
