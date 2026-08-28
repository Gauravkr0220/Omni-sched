# Durability and IPC Contracts

## WAL ordering

The `TaskEngine` accepts an optional `IJobJournal`. When configured, every task
must include a `DurableTaskSpec` containing a factory type and payload.

The ordering rule is:

1. Append the submission or next state transition.
2. Flush it with `fsync`.
3. Publish the corresponding state in the in-memory registry.
4. Enqueue or execute the work when applicable.

An append failure prevents the requested transition. The affected in-memory job
becomes `FAILED` with a WAL error, but that failure itself cannot be promised as
durable when the journal is unavailable.

## Record format and replay

Records are newline-delimited and begin with `K1`. Submission records preserve
job ID, sequence, priority, estimate, display name, task type, and task payload.
Transition records preserve the latest lifecycle state, total consumed work, and
message. Variable text is hex encoded. A CRC32 over all preceding fields detects
complete-record corruption.

Replay processes records in order and builds the latest state for each job:

- Duplicate submissions, unknown transitions, invalid checksums, and malformed
  complete lines stop startup with a precise line number.
- A final line without a newline is treated as an interrupted append and ignored.
- Terminal jobs remain available to `history` but are not requeued.
- `READY` and `RUNNING` jobs are rebuilt from the factory specification, assigned
  their original IDs, and rerun from the beginning.
- ID and submission-sequence counters advance past every historical job,
  including terminal entries.

This is an at-least-once model. A crash after task side effects but before its
`COMPLETED` record can cause the task to run again.

## FIFO ownership

The engine creates `requests.fifo` and `responses.fifo` with user-only access in
a user-owned directory. It refuses to replace a regular file, symlink, or FIFO
owned by another user. Stale user-owned FIFOs are replaced on startup and
removed during graceful shutdown; the WAL remains.

An advisory lock file allows only one engine to own a given IPC directory at a
time, preventing FIFO replacement or concurrent writers against the same WAL.

The current transport intentionally supports one active shell. This keeps the
request/response topology honest; multi-client routing would require per-client
reply endpoints or a Unix-domain socket.

## Message framing

Wire messages use this logical shape:

```text
KIPC1<TAB>request-id<TAB>kind<TAB>hex-field...<NEWLINE>
```

Kinds are `REQUEST`, `OK`, `ERROR`, and `EVENT`. Hex encoding prevents tabs,
newlines, or arbitrary task messages from breaking framing. Messages are limited
to one MiB. The shell assigns request IDs and has one listener thread that routes
responses to waiting commands while printing asynchronous events.

## Process shutdown

- `quit` terminates only the shell.
- `shutdown` returns an acknowledgement, stops new engine submissions, drains
  accepted work, joins completion observers, removes FIFOs, and exits.
- `SIGINT` and `SIGTERM` set an async-signal-safe flag; the daemon observes it in
  its polling loop and follows the same drain path.
- `SIGKILL` cannot be handled. On the next startup, WAL replay restores every
  accepted non-terminal job.
