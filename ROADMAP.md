# Immediate goals

# Upcoming goals

## Activities
* External activity executor gRPC API

## Security
* Dynamic secrets loaded from Vault, missing dynamic secrets should be a warning

## Workflows
Ability to drive old workflows to completion when auto upgrade fails - dormant deployments.

## Scale to zero: Less CPU,IO
slow down executor poll, watcher should wake them up
Explore all notifications going through db transparently (sometimes using triggers, inmem or NOTIFY)
Have a single waker per app deployment. No more executors, but internal or external signal run this webhook / lock and run other. Maybe waker hands an already locked exe?
- waker can be external and launch the container
- shutdown when no workers are running - coordinate with waker. webhooks with long connections would prevent shutdown
- Replace executors with a per-app select

## Future ideas
* GC: Perform a cascading delete of top-level executions, deployments, CAS entries that finished more than a certain number of days ago.
* Optional caching of activity executions with a TTL - serve cached response if parameters are the same
* [Deadline propagation](https://sre.google/sre-book/addressing-cascading-failures)
* Queue capacity setting, adding backpressure to execution submission
