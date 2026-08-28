# Kronos

Kronos is a concurrent, crash-recoverable C++20 background task engine. It
turns operating-system ideas—worker pools, scheduling, process communication,
signals, and durable state—into a small system that can be controlled from its
own shell.

The complete PRD path is implemented: the execution engine and schedulers,
write-ahead logging and recovery, a separate engine process, POSIX named-pipe
communication, and an interactive command shell.

## What is implemented

- Fixed-size `std::jthread` worker pool with condition-variable wakeups.
- FIFO, stable Priority, stable Shortest Job First, and custom cooperative
  Round Robin scheduling.
- Runtime-safe scheduler switching, observable job state, result futures,
  exception isolation, cancellation, and graceful shutdown.
- Checksummed, versioned, append-only WAL records with a synchronized writer
  and `fsync` after every submission or transition.
- At-least-once recovery of accepted jobs that were `READY` or `RUNNING` when
  the engine stopped; original job IDs remain stable after restart.
- A task factory that can reconstruct built-in Prime and Step jobs from their
  durable specifications.
- Separate `kronos_engine` and `kronos_shell` processes communicating through
  user-owned POSIX FIFOs and a framed protocol.
- Shell commands for submission, active jobs, cancellation, history, scheduler
  changes, health checks, and graceful engine shutdown.
- Asynchronous completion/failure notifications while the shell is connected.
- GoogleTest coverage, a real crash/restart integration test, Linux/macOS CI,
  and address, undefined-behavior, and thread sanitizer presets.

## Build and test

Requirements:

- CMake 3.24 or newer
- A C++20 compiler (Apple Clang, Clang, or GCC)
- POSIX APIs available on Linux or macOS
- Git and network access on the first build if GoogleTest is not installed

```sh
cmake --preset dev
cmake --build --preset dev --parallel
ctest --preset dev

cmake --preset release
cmake --build --preset release --parallel
ctest --preset release
```

The build first looks for an installed GoogleTest package. Otherwise it fetches
a pinned upstream commit. CTest includes unit, concurrency, FIFO, WAL, and real
multi-process crash-recovery coverage.

Useful hardening presets:

```sh
cmake --preset asan
cmake --build --preset asan --parallel
ctest --preset asan

cmake --preset tsan
cmake --build --preset tsan --parallel
ctest --preset tsan
```

## Benchmark

Build and run the dependency-free benchmark harness in Release mode:

```sh
cmake --preset release
cmake --build --preset release --parallel
./build/release/kronos_benchmark \
  --scheduler-jobs 100000 \
  --cpu-jobs 128 \
  --cpu-operations 2000000 \
  --wal-jobs 2000 \
  --samples 7
```

The harness reports median scheduler throughput, worker-pool scaling, mean
queue wait under FIFO versus SJF, durable append cost, and recovery parsing
time. Results are machine-specific and must be quoted with their workload and
environment. See [Benchmark methodology and reference run](docs/benchmarks.md).

## Run the engine and shell

Start the durable engine in one terminal:

```sh
./build/dev/kronos_engine --workers 4 --scheduler rr --quantum 250
```

Connect the shell from another terminal:

```sh
./build/dev/kronos_shell
```

By default both processes use `/tmp/kronos-<user-id>`. The directory contains
`requests.fifo`, `responses.fifo`, and the persistent `kronos.wal`. Override the
locations when needed:

```sh
./build/dev/kronos_engine \
  --ipc-dir /tmp/my-kronos --wal ./data/jobs.wal --workers 2
./build/dev/kronos_shell --ipc-dir /tmp/my-kronos
```

The shell also supports one-shot commands for scripts:

```sh
./build/dev/kronos_shell --command "submit prime 100000 8"
./build/dev/kronos_shell --command "ps"
```

## Shell commands

```text
submit prime <upper-bound> [priority]
submit step <steps> <delay-ms> [priority]
ps
kill <job-id>
history
scheduler <fifo|priority|sjf|rr> [quantum]
ping
shutdown
quit
```

`quit` closes only the shell. `shutdown` asks the engine to stop accepting new
work, drain accepted jobs, close its FIFOs, and exit. `SIGINT` and `SIGTERM` use
the same graceful drain behavior.

## Durability contract

Kronos records a reconstructible task definition before acknowledging a
submission. It then logs and synchronously flushes every lifecycle transition
before exposing that state in memory.

Each WAL line includes a version, record type, job metadata or transition,
hex-encoded text fields, and CRC32 checksum. Recovery rejects corrupted complete
records and safely ignores only an incomplete final line caused by a torn write.

Jobs last recorded as `READY` or `RUNNING` are recreated from their original
task specification and submitted with the same ID. They restart from the
beginning, so recovery provides **at-least-once execution**, not exactly-once
side effects. The built-in tasks are deterministic and safe to repeat. Any
future side-effecting task must provide its own idempotency rule.

## Library example

```cpp
#include <memory>

#include "kronos/task_factory.hpp"
#include "kronos/task_engine.hpp"
#include "kronos/wal.hpp"

auto wal = std::make_shared<kronos::WalManager>("./data/jobs.wal");
kronos::TaskEngine engine({
    .worker_count = 4,
    .scheduler = {kronos::SchedulerKind::RoundRobin, 500},
    .journal = wal,
});

auto created =
    kronos::TaskFactory::create_for_submission("prime", {"100000"}, 8);
auto job = engine.submit(std::move(created.task), created.options);

const kronos::JobResult result = job.result().get();
engine.shutdown(kronos::ShutdownMode::Drain);
```

Custom durable tasks must implement `ITask::run_slice` and add a reconstruction
rule to `TaskFactory`. Tasks must stop after the supplied work-unit budget,
return `Yielded` when work remains, and poll the stop token often enough for
their required cancellation latency.

## Scheduling contracts

| Policy | Ordering | Slice budget |
| --- | --- | --- |
| FIFO | Queue insertion order | Unlimited |
| Priority | Higher integer first; FIFO for ties | Unlimited |
| SJF | Lower declared estimate first; FIFO for ties | Unlimited |
| Round Robin | FIFO rotation after each yielded slice | Configured quantum |

Work units are defined by each task rather than wall-clock milliseconds.
Portable C++ cannot safely preempt arbitrary task code at a deadline, so Round
Robin is explicitly cooperative and deterministic.

See [Architecture](docs/architecture.md) and
[Durability and IPC](docs/durability-and-ipc.md) for the internal contracts.

## Planned sandboxed code execution

Kronos currently executes only the compiled-in Prime and Step task types. It
does not yet compile or run submitted source code. The planned extension will
execute untrusted programs outside the engine with CPU, memory, process,
filesystem, network, output, and wall-time restrictions. See the
[sandbox roadmap](docs/sandbox-roadmap.md) for the security boundary, proposed
components, scheduling semantics, persistence requirements, and verification
plan. Sandbox claims must not be used as completed-project claims until that
roadmap is implemented and tested.

## Future extensions

- Sandboxed source-code compilation and execution described in the roadmap.
- DAG dependencies and blocked-job state.
- Web dashboard with live worker and queue visualization.
- Resource quotas and execution-time accounting.
- Memory-pool experiments for high-volume short tasks.
- Remote workers behind an RPC transport.
