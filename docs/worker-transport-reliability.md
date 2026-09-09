# Worker transport reliability

Version v1.2.0 ports the early-resolution, completion-token, and
stream-recovery fixes from Dapr onto the synchronous mafilus executor. The
public Backend and Executor interfaces remain synchronous and unchanged.

## Sources and integration boundary

- Early task resolution: Dapr
  `baf7de98299d3c1835604acc4e8fb314eda4dcb4`.
- Completion tokens: Dapr
  `ada260f468011eba20abf4664ac474f79383aa87`.
- Disconnected stream recovery: Dapr
  `18cd4b5a26a5d53f127a12dd7aa2d496314f3de6`.

These are manual ports. They do not introduce asynchronous Backend callbacks,
pipelined execution, or a new public TaskProcessor interface.

## Required invariants

A resolution received before its task is registered is retained for that
workflow replay, keyed by resolution kind and task ID. Registering the matching
task consumes that resolution. The first resolution wins, and one task kind
must not satisfy another task kind with the same numeric ID. A new replay or
ContinueAsNew generation must not inherit an old replay's pending resolutions.

Older histories can omit the synthetic timer now created by an indefinite
external-event wait. Until a historical scheduling event confirms whether that
timer occupies an ID, later task IDs are provisional. Early resolutions at that
boundary must remain buffered, and affected actions (including the provisional
timer) must not be dispatched. A later TimerCreated or legacy scheduling event
resolves the ambiguity. If no such evidence ever arrives, the ambiguous replay
remains blocked with a warning; absence of evidence is not proof of the newer
numbering scheme.

Each dispatched workflow or activity attempt has a fresh, nonempty completion
token. Its backend waiter is registered before the item is exposed to a worker.
The completion path checks the task identity and token against the same pending
attempt whose state it transitions. Cleanup from a predecessor must not remove
or cancel a successor that uses the same instance or activity key.

A disconnected stream is removed from routing and invalidates only the attempts
it owns. The synchronous Execute call returns an error; the existing worker then
abandons the leased backend work item. Subsequent backend acquisition dispatches
a new attempt with a new token. The transport must not requeue the previously
sent protobuf: stateful-history delivery may already have removed its full
history prefix. A replacement stream starts with full history.

Shutdown must wake queued producers and pending waiters without racing sends
against a closed work queue. External callbacks and potentially blocking backend
operations must not run under a global registry lock.

A custom backend that ignores context cancellation can still delay an Execute
call or an already-started backend completion. Its registry reservation remains
held until that completion returns, preventing it from reaching a successor.
Such a backend must not block Shutdown or unrelated stream cleanup.

## Compatibility and rollout

The bundled Go worker echoes the WorkItem completion token on both successful
and failed workflow/activity responses. The token identifies a dispatch attempt;
it is not the workflow ExecutionId or the logical activity TaskExecutionId.

By default, empty completion tokens remain accepted for compatibility with
older workers. Nonempty mismatched tokens are rejected. Empty-token responses
cannot be distinguished from stale responses, so this compatibility mode does
not provide stale-response protection for legacy workers.

For a deployment whose workers all echo tokens, enable
`backend.WithRequireCompletionTokens()` when creating the gRPC executor. This
rejects empty tokens as well. Upgrade workers before enabling this option.
An upgraded worker remains compatible with an older server that sends no token.

Completion tokens do not prevent duplicate external activity side effects.
Activities remain subject to at-least-once execution and must use appropriate
idempotency at their external effect boundary. The persistent backend remains
responsible for durable lease ownership and history commits.

## Proof boundary

Transport unit tests must exercise completion before waiting, stale and duplicate
responses, cancellation, stream ownership, and concurrent shutdown. Race checks
are needed for registry and completion transitions. Durable recovery additionally
requires observing backend abandonment, a new acquisition, rejection of the old
token, and successful completion of the new attempt through a real backend.

The [v1.2.0 release note](releases/v1.2.0.md) records the completed local and CI
checks. PostgreSQL restart-boundary chaos passed separately in CI on the exact
release commit; this does not establish deployment-level network partition
recovery or exactly-once external activity effects.
