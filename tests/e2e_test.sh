#!/usr/bin/env bash
set -euo pipefail

engine_binary=$1
shell_binary=$2
test_directory=$(mktemp -d "${TMPDIR:-/tmp}/kronos-e2e.XXXXXX")
ipc_directory="$test_directory/ipc"
wal_path="$test_directory/jobs.wal"
engine_pid=""

cleanup() {
  local status=$?
  if [[ $status -ne 0 ]]; then
    echo "Kronos end-to-end test failed; daemon logs follow:" >&2
    for log_path in "$test_directory"/*.log; do
      if [[ -f "$log_path" ]]; then
        echo "--- $log_path" >&2
        cat "$log_path" >&2
      fi
    done
  fi
  if [[ -n "$engine_pid" ]] && kill -0 "$engine_pid" 2>/dev/null; then
    kill "$engine_pid" 2>/dev/null || true
    wait "$engine_pid" 2>/dev/null || true
  fi
  rm -rf "$test_directory"
  return "$status"
}
trap cleanup EXIT

start_engine() {
  local log_path=$1
  "$engine_binary" \
    --ipc-dir "$ipc_directory" \
    --wal "$wal_path" \
    --workers 2 \
    --scheduler rr \
    --quantum 5 >"$log_path" 2>&1 &
  engine_pid=$!

  for _ in $(seq 1 100); do
    if "$shell_binary" --ipc-dir "$ipc_directory" --command ping \
        >/dev/null 2>&1; then
      return
    fi
    if ! kill -0 "$engine_pid" 2>/dev/null; then
      cat "$log_path"
      return 1
    fi
    sleep 0.02
  done
  cat "$log_path"
  return 1
}

run_shell() {
  "$shell_binary" --ipc-dir "$ipc_directory" --command "$1"
}

first_log="$test_directory/engine-first.log"
start_engine "$first_log"

[[ "$(run_shell ping)" == "pong" ]]
submit_output=$(run_shell "submit step 2000 2 4")
job_id=${submit_output##* }
[[ "$job_id" =~ ^[0-9]+$ ]]
kill_output=$(run_shell "kill $job_id")
[[ "$kill_output" == *"cancellation requested"* ]]
scheduler_output=$(run_shell "scheduler priority")
[[ "$scheduler_output" == *"scheduler changed to priority"* ]]

for _ in $(seq 1 100); do
  history_output=$(run_shell history)
  if [[ "$history_output" == *"$job_id"*"CANCELLED"* ]]; then
    break
  fi
  sleep 0.02
done
history_output=$(run_shell history)
[[ "$history_output" == *"$job_id"*"CANCELLED"* ]]
shutdown_output=$(run_shell shutdown)
[[ "$shutdown_output" == *"shutdown started"* ]]
wait "$engine_pid"
engine_pid=""

second_log="$test_directory/engine-before-crash.log"
start_engine "$second_log"
submit_output=$(run_shell "submit step 1000 2 1")
recovery_id=${submit_output##* }
[[ "$recovery_id" =~ ^[0-9]+$ ]]

kill -9 "$engine_pid"
wait "$engine_pid" 2>/dev/null || true
engine_pid=""

recovery_log="$test_directory/engine-recovered.log"
start_engine "$recovery_log"
grep -q "Recovered jobs: 1" "$recovery_log"

for _ in $(seq 1 200); do
  history_output=$(run_shell history)
  if [[ "$history_output" == *"$recovery_id"*"COMPLETED"* ]]; then
    break
  fi
  sleep 0.02
done
history_output=$(run_shell history)
[[ "$history_output" == *"$recovery_id"*"COMPLETED"* ]]
shutdown_output=$(run_shell shutdown)
[[ "$shutdown_output" == *"shutdown started"* ]]
wait "$engine_pid"
engine_pid=""
