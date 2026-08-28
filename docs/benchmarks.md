# Benchmark methodology and reference run

Kronos includes a dependency-free benchmark executable so performance claims
can be reproduced instead of estimated. Always use a Release build and retain
the workload parameters alongside any result.

## Command

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

## What is measured

- **Scheduler throughput:** submission-to-completion time for 100,000 minimal
  in-memory jobs using eight workers. Journaling and task payload work are
  intentionally excluded to isolate engine dispatch overhead.
- **Worker scaling:** 128 deterministic CPU jobs, each performing 2,000,000
  integer operations. The median elapsed time is compared with one worker.
- **Queue waiting:** one worker receives ten 20 ms jobs followed by thirty 1 ms
  jobs while a gate ensures the full workload is queued. The benchmark compares
  each job's recorded submission-to-start interval under FIFO and SJF.
- **Durability and recovery:** 2,000 jobs generate 6,000 synchronously flushed
  records. Half finish and half remain restartable. Recovery validates, parses,
  orders, and reconstructs the complete history.

Each timed scenario is repeated seven times and the median is reported. The
durable append phase is a single sequential run because it creates the input
used by the repeated recovery measurements.

## Reference environment

- Date: 2026-08-23
- Hardware: Apple M2 MacBook Air, 8 CPU cores, 8 GB memory
- Operating system: macOS 26.6.2, arm64
- Compiler: Apple Clang 21.0.0
- Build: CMake Release

## Reference results

| Measurement | Median result |
| --- | ---: |
| FIFO short-job throughput | 243,830 jobs/second |
| Priority short-job throughput | 233,233 jobs/second |
| SJF short-job throughput | 238,182 jobs/second |
| Round-Robin short-job throughput | 242,922 jobs/second |
| CPU workload, one worker | 516.81 ms |
| CPU workload, eight workers | 83.98 ms |
| Eight-worker CPU speedup | 6.15x |
| FIFO mean queue wait | 225.78 ms |
| SJF mean queue wait | 50.51 ms |
| SJF mean-wait reduction | 77.63% |
| Durable append rate | 29,710 records/second |
| Recovery of 6,000 records | 6.33 ms |
| Recovery parsing rate | 948,460 records/second |
| Restartable jobs identified | 1,000 of 2,000 |

These numbers characterize the engine on the reference machine; they are not
cross-platform guarantees. The scheduler test uses minimal tasks, so its rate
must not be presented as source-code compilation or sandbox throughput. The
wait-time result applies only to the documented mixed workload. The CPU speedup
is workload-specific and is not a general claim that every task scales 6.15x.
