# Sandboxed code-execution roadmap

## Status

This document defines a planned extension. Kronos currently executes only the
compiled-in Prime and Step tasks. It does not accept, compile, or sandbox
submitted source code, and no resume or portfolio description should imply
otherwise.

## Target use case

A client submits source code together with priority, eligibility delay,
execution timeout, and resource limits. Kronos compiles the submission, queues
the executable, runs it outside the engine, captures its output and exit status,
and retains enough metadata to explain or recover the job after a restart.

## Security boundary

Untrusted code must never execute inside a worker thread or share the engine's
address space. The sandbox backend must provide:

- a non-root process identity;
- a private, size-limited working directory;
- read-only runtime files and no host-directory access;
- disabled network access by default;
- CPU, memory, process-count, output-size, and wall-time limits;
- complete child-process-group termination on cancellation or timeout; and
- explicit cleanup of temporary files after result publication.

The first portable backend should use a container runtime on Linux and macOS.
An interface such as `ISandboxRunner` should keep engine scheduling independent
of the chosen isolation implementation.

## Proposed components

- `CodeSubmission`: language, source reference, priority, delay, timeout, and
  resource limits.
- `ICompiler`: converts a validated submission into an immutable executable
  artifact and returns diagnostics.
- `ISandboxRunner`: starts, observes, cancels, and cleans an isolated process.
- `CodeTask`: adapts compilation and execution phases to the existing task
  interface and lifecycle.
- `TaskFactory` reconstruction: recreates a durable code job from stored,
  integrity-checked metadata.

These interfaces extend the existing Strategy, Factory, Producer-Consumer,
RAII, and dependency-inversion boundaries without embedding container commands
inside `TaskEngine`.

## Scheduling semantics

FIFO, Priority, and SJF can choose which eligible code job starts next. An
eligibility delay prevents a job from entering the ready queue before its
configured time.

The current Round-Robin implementation is cooperative and cannot interrupt
arbitrary submitted programs. True process-level Round Robin would require the
sandbox backend to pause and resume the complete process group at quantum
boundaries. Until that controller exists and is tested, code jobs must not be
described as preemptively scheduled.

## Durability and recovery

The engine must persist the source or immutable artifact reference, content
hash, compiler configuration, scheduling metadata, and resource limits before
acknowledging a submission. A restarted engine may execute an unfinished job
again, preserving the existing at-least-once recovery contract. External side
effects therefore require idempotency.

## Required verification

Completion requires automated tests for:

- successful compilation and captured standard output/error;
- syntax errors and non-zero exit codes;
- wall-time, CPU, memory, process-count, and output-size enforcement;
- blocked filesystem and network access;
- forked-child cleanup on cancellation and engine shutdown;
- crash recovery without duplicate job identities; and
- malformed submissions and unsafe path rejection.

Separate Release benchmarks must measure compilation latency, sandbox startup
overhead, timeout accuracy, concurrent isolated-job throughput, and recovery.
Existing in-memory task benchmarks must not be reused as sandbox-performance
claims.
