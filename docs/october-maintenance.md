# October maintenance changes

Version v1.2.2 addresses repeated activity execution, oversized gRPC
work items, overwritten completion waiters, and incomplete scheduling history.
They adapt the upstream fixes to this fork's synchronous executor and preserve
its early-resolution and execution-isolation protections.

## Retry chains and replay compatibility

A patched retry chain observes execution errors separately from decoding a
successful result. A decoding error returns to the workflow without running the
activity or child again. Repeated `Await` calls retain the terminal attempt and
can decode its result into a different destination without restarting retries.
Unobserved tasks do not advance in the background. Backoff timers are allocated
where workflow code observes failure, preserving action ordering in fan-out
workflows awaited out of scheduling order. The UUID generated for a new dispatch
is private to scheduling. `TaskExecutionId()` exposes an observed historical ID,
or the stable empty value when no such identity is available, including a first
successful attempt and child workflow results. It never exposes the random
construction UUID as a replay-visible fallback.

The internal patch name `durabletask-go/retry-chain-v1` is reserved for the engine.
It uses the existing `WorkflowResponse.Version.Patches` mechanism. At the first
retry-chain scheduling point, a recorded marker selects the corrected behavior.
With no marker and remaining history, the engine conservatively keeps the old
behavior for that execution, including historical decode-induced retries and
repeated-Await retries. At the history frontier it selects the corrected behavior
and records the marker. The decision is shared by subsequent retry chains in
the execution.

This avoids requiring a forced migration of existing histories, but old
executions can retain the old defects until they drain or start a new execution.
Do not add the marker to an existing history by hand: that history may contain
actions produced by the old behavior. `ContinueAsNew` provides a new history in
which the decision can be made again.

The bundled workflow processor persists `WorkflowResponse.Version` on the turn's
`WorkflowStarted` event. Custom hosts calling `task.NewTaskExecutor` directly
must persist and replay this metadata too, as they already must for workflow
patching. An older worker cannot honor the new internal marker; route marked
histories to upgraded workers rather than alternating engine versions for one
execution. This change adds neither `Select` nor `WhenAny`.

## Bounded gRPC work items

The worker now receives work items up to 16 MiB by default. Hosts can set an
explicit per-message budget with `client.WithWorkItemMaxReceiveBytes(bytes)`.
The option applies to `GetWorkItems` and overrides the connection's default
receive limit for that call. Non-positive option values are ignored.

`ResourceExhausted` can mean a locally oversized message or a temporary server
quota. The listener does not infer which one occurred from that status. It
allows two reconnects, after 100 ms and 200 ms, and stops on the third resource
receive error without useful work in between. Receiving an activity or workflow
work item resets this budget. Other stream errors retain their existing retry
policy. This permits recovery from a transient quota while bounding repeated
redelivery of an oversized item.

A terminal failure cancels the listener resources and invokes the optional
`client.WithWorkItemListenerErrorHandler(func(error))` exactly once, before
executor shutdown. The handler must return promptly and runs without library
locks. Resource-budget exhaustion wraps `client.ErrWorkItemStreamExhausted` and
the original gRPC error, so the host can inspect both. Startup errors are still
returned by `StartWorkItemListener`; caller cancellation does not trigger the
handler. Without a handler, terminal errors are logged.

A host receiving a terminal error must inspect the payload, receive budget or
server quota before restarting a listener. This budget does not reject or
quarantine a stored workflow; repeated oversized work can exhaust listeners on
other workers too.

For a host that intentionally accepts messages up to 16 MiB:

```go
server := grpc.NewServer(grpc.MaxRecvMsgSize(16 << 20))
listenerFailures := make(chan error, 1)
workerClient := client.NewTaskHubGrpcClient(conn, logger,
    client.WithWorkItemMaxReceiveBytes(16 << 20),
    client.WithWorkItemListenerErrorHandler(func(err error) {
        listenerFailures <- err
    }))
// The host monitors listenerFailures and decides whether to restart.
```

Server receive limits govern inputs and activity/workflow responses. Other
client RPCs, such as metadata or history fetches, retain their connection-level
limits; hosts needing larger responses can configure
`grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16 << 20))` on the connection.
The standalone server's receive policy is unchanged. History-cache budgets
bound retained history, not individual incoming messages. No unbounded gRPC
receive setting is introduced.

## Completion registrations

The local tasks backend now rejects duplicate pending keys with
`local.ErrTaskAlreadyPending` instead of overwriting the original waiter. The
original owner can still complete or cancel; a rejected waiter owns no entry
and cannot remove the original registration. A new owner can register after
the previous entry is retired.

The optional `backend.CompletionRegistrationBackend` capability reports this
error before dispatch. SQLite and PostgreSQL expose it through their embedded
local task backend. The gRPC executor uses it when available; custom backends
without it retain the existing `WaitFor...Completion` contract. Backend wrappers
must forward the capability to retain early rejection. Direct users of the
legacy wait methods receive registration errors from the returned wait function;
callers that dispatch work themselves should use the new registration methods.

The existing per-executor reservation, stream ownership, completion-token
validation and strict-token option remain in force. This does not add parallel
attempts of the same logical task or exactly-once external effects.

## Early resolutions

The SDK keeps the early-resolution buffer and the guards for provisional IDs
in pre-synthetic-timer histories. Once a resolution can be matched, its schedule
is emitted so the applier can record `TaskScheduled`, `TimerCreated` or
`ChildWorkflowInstanceCreated`. The applier suppresses dispatch when state
already contains the matching resolution family and ID. For an early activity
success or failure, the re-emitted schedule takes `TaskExecutionId` from the
resolution, including an empty historical value, rather than its generated
scheduling UUID. This preserves correlation between the schedule, resolution
and retry timer without changing legacy public success-ID semantics.

Both halves must be upgraded together: a custom host consuming workflow actions
must implement the same record-without-dispatch rule before accepting the new
actions. The bundled applier does so. Existing histories lacking a scheduling
event can still use the buffer. The Dapr-only removal of that buffer and the
`ActivityInvocation.parentExecutionId` protobuf change are not included.

## Sources and validation scope

- Retry correctness: [upstream PR 131](https://github.com/dapr/durabletask-go/pull/131),
  limited to retries rather than the new selection API.
- Receive limit: [upstream PR 141](https://github.com/dapr/durabletask-go/pull/141),
  adapted to a bounded, configurable policy.
- Pending registrations: [upstream PR 137](https://github.com/dapr/durabletask-go/pull/137),
  adapted to synchronous rejection rather than multiple asynchronous callbacks.
- Scheduling history: [upstream PR 133](https://github.com/dapr/durabletask-go/pull/133),
  retaining the fork's additional replay safeguards.

Regression tests cover historical retry shapes, stable terminal results,
fan-out observation order, child retry identity, real worker turns with SQLite,
real gRPC messages over 4 MiB, refusal before dispatch, concurrent duplicate
registrations, and serialized replay of early activity/timer/child results.
Local test results qualify source behavior, not an upgrade of downstream
consumers or a production deployment.
