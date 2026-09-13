# Workflow snapshotting experiment findings

## Outcome

The Asyncify/Wizer-based workflow snapshotting prototype should not be merged or
deployed in its current form. Snapshotting must remain disabled by default.

The prototype did successfully persist workflow state in CAS and later select a
snapshot belonging to the currently prepared component. However, it did not
improve end-to-end replay latency in the tested workload and it broke durable
workflow replay semantics.

## Performance

Persisting a snapshot was expensive. Observed snapshots grew beyond 116 MiB,
and individual persistence operations took several seconds (for example, about
3.5 seconds at history version 401 and 7.8 seconds at version 1202).

Snapshot deletion and CAS garbage collection also imposed material SQLite work.
Individual GC transactions took hundreds of milliseconds and occasionally more
than one second. Because SQLite serializes writes, these operations delayed
unrelated execution, timer, and maintenance transactions. The delays could
cascade: a slow snapshot or GC operation delayed lock and timer processing,
which caused retries and still more database work.

Bounded GC batches reduced the size of an individual GC transaction, but did
not remove the underlying cost or write contention.

Snapshot replay did not break even in the tested slow JavaScript workflow, even
after the history exceeded 2,000 replayed events. Representative measurements
at history version 1995 were:

| Replay mode | Events replayed | Total time |
| --- | ---: | ---: |
| Snapshot from version 1202 | 678 | 2.79-2.84 s |
| Full replay | 1,848 | 2.24-2.30 s |

A later snapshot reduced the number of replayed events further, but snapshot
restoration became slower as the snapshot grew. The fixed and size-dependent
cost of restoring the Wizer-produced component outweighed the event replay that
was skipped.

## Correctness

Snapshot restoration broke replay transparency. After restoring a snapshot,
the JavaScript workflow failed to consume an already-recorded result from the
static `webapi.callTarget(...)` durable import. It instead created a new one-off
join set and submitted the same child call again. This repeated hundreds of
times and created genuine duplicate durable events.

Disabling snapshotting and restarting restored correct normal replay. The
already-corrupted execution then reported nondeterminism because its history
contained the duplicate events produced while snapshotting was active. A fresh
execution with snapshotting disabled replayed normally. This isolates the
regression to the snapshotting preparation/restoration path rather than ordinary
workflow replay.

The likely class of bug is disagreement at the checkpoint boundary: guest state,
the saved lowered host-call result, and the host event-history cursor do not all
describe exactly the same point in execution. The precise mechanism was not
proven.

## Error propagation and Wizer failures

The transformed component also changed how a host failure surfaced. After a
slow SQLite `get_expired_timers` transaction took 591 ms, the workflow reached
its epoch deadline. The resulting `lock expired` execution error crossed the
Wizer callback boundary and was converted into a panic by an internal
`Result::unwrap()` in `wasmtime-wizer`'s component adapter:

```text
called `Result::unwrap()` on an `Err` value: error while executing at wasm backtrace:
    0: 0x42d809 - <unknown>!<wasm function 1>

Caused by:
    lock expired

panic.location=".../wasmtime-wizer-48.0.2/src/component/wasmtime.rs:146:63"
```

Host-call fuel exhaustion while copying data between host and guest was also
surfaced through an internal `Result::unwrap()` at the same location. This
suggests that the Asyncify/Wizer execution path does not transparently preserve
the surrounding worker's error propagation. The exact ownership between
Asyncify, Wizer, and the Wasmtime component adapter remains unresolved.

Wizer also panicked while producing a snapshot whose rewritten component
exceeded Wasmtime's default data-segment validation limit:

```text
data segments count exceeds limit of 100000
```

Allowing larger generated components avoided that specific limit but did not
address replay correctness or performance.

## Storage and lifecycle complexity

The prototype adds significant permanent machinery:

- a workflow-snapshot metadata table and oversized-snapshot marker state;
- potentially very large CAS objects;
- snapshot compatibility keyed by both input component and prepared-component
  digests;
- persistent suppression after an oversized snapshot;
- special CAS reachability and GC rules for running and finished executions;
- additional component-preparation stages for Asyncify instrumentation,
  durable-import wrappers, componentization, and Wizer restoration;
- checkpoint stack reservation, rewind startup, saved lowered results, and host
  event-cursor coordination.

The experiment also found a retention bug where a numerically newer snapshot
from an incompatible prepared-component digest caused GC to delete every newer
compatible snapshot produced at a lower history version. That was corrected by
atomically replacing all prior snapshot metadata when a snapshot is persisted,
but it illustrates the lifecycle complexity introduced by the feature.

## Recommendation

Stop the Asyncify/Wizer snapshotting experiment and keep it out of deployment.
The current approach adds substantial transformation, storage, database, and GC
complexity; increases contention; does not improve measured replay latency; and,
most importantly, violates durable replay semantics.

Any future attempt should first demonstrate transparent durable-host-call replay
in an isolated integration test and should benchmark snapshot creation,
restoration, and GC independently before being integrated with execution
storage.
