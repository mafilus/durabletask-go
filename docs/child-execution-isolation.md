# Child workflow execution isolation

New child results target the parent execution captured in the child's persisted
`ExecutionStarted.ParentInstance.WorkflowInstance.ExecutionId`. SQL backends
persist this identity in `NewEvents.ExecutionID`. Admission and dequeue compare
it with the destination execution under the same database transaction. An absent
or replaced parent does not receive the obsolete result; the child can complete.
The existing ContinueAsNew cleanup also removes results committed before the
transition. External events retain their existing instance-scoped behavior.

Automatically generated child IDs include the parent's execution and the complete
action ID. Historical child IDs remain authoritative during replay, including
retry timer grouping when a failure arrives before its scheduling event. An
explicit collision with an active child fails the transaction rather than
acknowledging a scheduling event without creating the intended child.

## Upgrade requirements

The existing SQL schema already contains the required columns. Before starting
workers after this upgrade, drain children while the old writers are still
running, or migrate their captured parent identity and queued child-result
identity offline using authoritative creation records. Upgrade SQL writers
together: an older writer can still enqueue an unqualified child result.

Legacy child starts without a captured parent execution, queued child results
without `ExecutionID`, and parent instance/history execution mismatches return
`backend.ErrChildExecutionProvenanceRequired`. The backend neither guesses the
current execution nor silently deletes an ambiguous result. Inspect and resolve
these cases before restarting workers. The error rolls back acquisition; an
ambiguous earliest queued instance can therefore repeatedly fail acquisition and
prevent later instances from progressing until its data is resolved.

This policy is scoped to child results. Existing legacy timer and activity queue
entries do not acquire a new migration requirement from this change. Both generic
event ingress APIs reject child results without captured execution provenance;
qualified results are written by child completion transactions.

Do not fill missing identities with the current parent execution or infer them
from action IDs, timestamps, or the parent instance ID. Those values are reused
across ContinueAsNew and purge/recreation boundaries.
