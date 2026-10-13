# Benchmarking

The `bench` binary drives the spec §9.1 matrix against this crate's uring
server, the tokio twin that uses the same `serve_session` over the runtime
seam, and mosquitto as the third competitor. A smoke profile runs the
pipeline end to end on this host without `perf` or mosquitto. A recorded
profile runs the fixed matrix, counts syscalls over the measurement
window, and renders `BENCHMARKS.md` after the human sign-off.

Last verified: WSL2 kernel 6.18.33.2-microsoft-standard-WSL2 with rustc
1.89.0 on the host's stock `taskset`, `perf`, and `strace`.

## Purpose

Spec §9 and §9.1 (`features/uring-ingest.md`) define the comparison:
same MQTT 3.1.1 codec, same callback, same in-process event channel,
only the I/O runtime differs. F4.3 delivers the harness that records
the raw measurements and renders the document — the attended follow-up
(builder keys, sudo preflight, recorded runs, owner sign-off) starts
from this file. The smoke profile is the gate the runner proves the
end-to-end pipeline locally.

## Build

Build from the repository root. The bench crate is its own workspace
(like `fuzz/`), so every command passes `--manifest-path bench/Cargo.toml`.

```sh
cargo build --release --manifest-path bench/Cargo.toml
```

That command writes `bench/target/release/bench`. Nothing installs it,
so a bare `bench` is not on `PATH`: every command below runs from the
repository root and names the built executable by path. Export it once
per shell to keep the commands short:

```sh
bench=bench/target/release/bench
```

The release build is the recorded profile's required build mode
(`RunRecord::build == Release`). Debug runs read stack traces and short
histograms; the runner records `cfg!(debug_assertions)` and
`"$bench" doc` treats a debug record in a recorded document as a
refusal.

## Operator preparation

### perf (counted runs only)

The Linux kernel source's `tools/perf` directory must match the running
kernel; prebuilt `perf` from a mismatched build refuses to open
`raw_syscalls:sys_enter`. Match `uname -r`:

```sh
cd /usr/src/linux-$(uname -r)/tools/perf
make
sudo cp perf /usr/local/bin/perf
```

`perf` needs to read tracefs and open kernel counters as a non-root
user:

```sh
sudo sysctl kernel.perf_event_paranoid=-1
sudo chmod a+rx /sys/kernel/tracing
```

If the host does not grant that access, run `bench run` as root.

### mosquitto

```sh
sudo apt install mosquitto              # version 2.0.18
sudo systemctl stop mosquitto.service   # free 1883 for the benchmark
```

The recorded matrix pins mosquitto to one worker only; `matrix::servers_for`
drops it from the four-worker rows automatically.

### Environment

- AC power (no battery), fixed power plan (`performance`), idle machine.
- The two CPU sets the runner uses by default — `0,2,4,6` for the server,
  `8-17` for the generator — match an 8-core / 16-thread hybrid CPU with
  the P-cores exposed by WSL2. Override with `--server-cpus` and
  `--generator-cpus` when the host layout differs.

## Preflight

Each check below is a gate — stop and report on failure.

1. **Mosquitto** — start the broker on the exact config the runner
   renders (`bench::runner::mosquitto_conf`, seven lines) and confirm
   it accepts a connection. With mosquitto on `PATH`:

   ```sh
   dir=$(mktemp -d)
   printf '%s\n' 'listener 1883 127.0.0.1' 'allow_anonymous true' \
     'persistence false' 'max_queued_messages 0' \
     'max_inflight_messages 0' 'set_tcp_nodelay true' 'log_dest none' \
     > "$dir/mosquitto.conf"
   mosquitto -c "$dir/mosquitto.conf" &
   pid=$!
   sleep 1
   nc -z 127.0.0.1 1883
   status=$?
   if [ "$status" -eq 0 ]; then
     echo OK
   else
     echo 'FAIL: no connection accepted on 127.0.0.1:1883'
   fi
   kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null || true
   rm -rf "$dir"
   ( exit "$status" )   # gate status: 0 passes, nonzero stops the preflight
   ```

   The config lines are the runner's own, so this gate is also the
   option check: a line this mosquitto build rejects makes it exit
   during startup, and `run_one` reports that as a harness error on
   every mosquitto row. Keep the lines in sync with `mosquitto_conf`.

2. **perf** — run the exact `perf stat` invocation the runner uses on a
   known pid and compare the line shape against `bench::analysis::parse_perf_count`:

   ```sh
   perf_bin=/usr/local/bin/perf
   dir=$(mktemp -d)
   mkfifo "$dir/ctl" "$dir/ack"
   sleep 30 &
   pid=$!
   "$perf_bin" stat --delay=-1 --control=fifo:"$dir/ctl","$dir/ack" \
     -x , -e raw_syscalls:sys_enter -p "$pid" 2>"$dir/perf.out" &
   perf_pid=$!
   exec 3<>"$dir/ctl" 4<>"$dir/ack"
   echo enable >&3
   read -r ack <&4
   sleep 0.2
   echo disable >&3
   read -r ack <&4
   kill -INT "$perf_pid"; wait "$perf_pid" || true
   kill "$pid" 2>/dev/null || true
   exec 3>&- 4>&-
   # `parse_perf_count`'s own acceptance: field 3 names the event and
   # field 1 must be a count. A `<not supported>` or `<not counted>`
   # field 1 is an unopened counter, not a zero count, and every counted
   # run of the matrix would fail on it.
   awk -F, '$3 == "raw_syscalls:sys_enter" && $1 ~ /^[0-9]+$/ { ok = 1 }
            END { exit !ok }' "$dir/perf.out"
   status=$?
   if [ "$status" -eq 0 ]; then
     echo OK
   else
     echo 'FAIL: no counted raw_syscalls:sys_enter line'
     cat "$dir/perf.out"
   fi
   rm -rf "$dir"
   ( exit "$status" )   # gate status: 0 passes, 1 stops the preflight
   ```

   The gate requires a numeric count, because that is what
   `parse_perf_count` returns a value for: a `<not supported>` line
   carries the event name and no count, so a name-only check approves a
   counter the recorded runs cannot use. The line ends in
   `enable`/`disable` ack-exchange only and exits by SIGINT (or 0) after
   printing its counts — anything else fails the recorded profile's
   exit-status rule.

3. **strace** — equivalent preflight for the fallback:

   ```sh
   strace_bin=strace
   sleep 30 &
   pid=$!
   "$strace_bin" -c -f -p "$pid" -o /tmp/strace.out 2>/tmp/strace.err &
   strace_pid=$!
   sleep 1
   kill -INT "$strace_pid"; wait "$strace_pid" || true
   grep -q ' total' /tmp/strace.out
   status=$?
   if [ "$status" -eq 0 ]; then
     echo OK
   else
     echo 'FAIL: no total row in the strace summary'
     cat /tmp/strace.out /tmp/strace.err
   fi
   kill "$pid" 2>/dev/null || true
   ( exit "$status" )   # gate status: 0 passes, nonzero stops the preflight
   ```

   `analysis::parse_strace_total` reads the call count from the ` total`
   row, so a summary without that row fails every counted run. An
   attach refusal (`ptrace(`) lands in `/tmp/strace.err`.

4. **Env probe** — a smoke record's `Env.tools` lists each supplied
   tool's first non-blank `--version` line:

   ```sh
   "$bench" run --profile smoke --out /tmp/tools-check \
     --servers uring --workers 1 --qos 1 --syscalls none
   jq '.env.tools' /tmp/tools-check/*.json
   ```

   The captured versions are empty for smoke (no tools required).

## Recorded run

```sh
"$bench" run --profile recorded \
  --out "bench/results/$(date +%F)-$(hostname)" \
  --syscalls perf \
  --perf-bin /usr/local/bin/perf \
  --mosquitto-bin /usr/sbin/mosquitto
```

Exit codes:

- `0` — every configuration passed; `bench doc` can render.
- `2` — at least one configuration failed; records were written, the
  failed keys are named on stderr.
- `1` — refusal (missing `perf-bin`, missing `mosquitto-bin`, output
  directory already holds `*.json`, dirty / mismatched build when
  `--resume` is set, missing provenance) or SIGINT/SIGTERM during the
  preflight.

The recorded profile runs only the full fixed matrix; `check_request`
refuses `--servers`/`--workers`/`--qos` subsets.

`--resume` continues an interrupted run in the same directory: it
refuses records from another build or host (`check_resume` runs
`analysis::recompute` on every existing file), skips configurations
whose highest attempt passed, and continues attempt numbering for
the rest. Interruptions during the preflight exit 1 with `cancelled`
on stderr.

## Document generation

```sh
"$bench" doc bench/results/2026-10-09-host \
  --out BENCHMARKS.md
```

`bench doc` refusal errors print the `DocError` variant verbatim and
exit 1:

- `NoRecords` — directory holds no `*.json` runs.
- `UnsupportedSchema(n)` — a record declares a different `schema`.
- `SmokeRecordInRecordedDoc(file)` — a `Profile::Smoke` record in a
  non-smoke render.
- `DebugBuild` — any record was produced by a debug build.
- `DirtyTree` — any record's source tree was uncommitted at build.
- `MixedEnvironment(field)` — records disagree on `git_sha`,
  `kernel`, `host`, `cpu_model` or `tools`.
- `BlankEnvironment(field)` — the shared `git_sha`, `kernel`, `host`
  or `cpu_model` is blank. `bench run` captures these once per run
  (from `/proc` and the build); fix the host and record a fresh run —
  `--resume` refuses a changed value, and an edited record no longer
  shows what was captured.
- `NoToolVersions` — the shared `env.tools` lists no tool.
- `BlankToolPath(index)` — the `env.tools` entry at that zero-based
  position has a blank path.
- `BlankToolVersion(path)` — the `env.tools` entry for that path has
  a blank version.
- `InconsistentRecord(file: mismatches)` — `recompute` rejected at
  least one record.
- `ProtocolViolation(text)` — recorded constants diverge, repetition
  set is missing or non-`1..=5`, an 80 % latency run lacks a counter,
  or a latency rate does not match the throughput medians.
- `FailedConfigurations(names)` — the fixed matrix is missing or
  failed configurations, listed by kebab-case label
  (`uring-w1-q1-throughput`).

The document ends with five sign-off items that the operator ticks
by hand after checking each one.

## Sign-off

After the recorded run:

- [ ] Driver proof: every uring run shows io_uring fds = workers and
      eventpoll fds = 0.
- [ ] Spread: every configuration within 10 % over 5 interleaved
      repetitions.
- [ ] Reconciliation: every run reconciled (unresolved 0, duplicates 0,
      dropped 0 in throughput runs).
- [ ] Environment: host, kernel, CPU sets, AC power and idle state
      match this disclosure.
- [ ] Saturation: in every throughput row the server CPU set, not the
      generator, is the saturated side.

`bench doc` does not tick these — that is the owner's review.

## Smoke

The smoke profile proves the pipeline locally. It runs eight records
(throughput, latency 50 %, latency 80 % and connections, two
repetitions per kind, for the 1-worker uring row), exits 0 only when every
record reconciled, and `bench doc --smoke` then renders the document
with the `SMOKE — not publishable` header instead of the recorded
checks.

```sh
rm -rf /tmp/bench-smoke
"$bench" run --profile smoke --out /tmp/bench-smoke \
  --servers uring,tokio --workers 1 --qos 1 --syscalls none
test -z "$(grep -L '"valid": true' /tmp/bench-smoke/*.json)"
"$bench" doc /tmp/bench-smoke --out /tmp/bench-smoke-doc.md --smoke
grep -q 'SMOKE — not publishable' /tmp/bench-smoke-doc.md
grep -q '| uring |' /tmp/bench-smoke-doc.md
grep -q '| tokio |' /tmp/bench-smoke-doc.md
```

Do not commit anything under `/tmp/bench-smoke`. The smoke output
directory is local scratch. The committed `BENCHMARKS.md` is produced
by a recorded run, not by `bench doc --smoke`.

## Constants

`bench::runner::ProtocolConstants::for_profile` chooses the set.
`CONFIG_KEY` packs `server × workers × qos × kind × rate_fraction_pct`
so one run fills one cell.

| Field | Recorded | Smoke | Source |
|---|---|---|---|
| `CONNECTIONS` | 100 | 100 | `runner.rs` |
| `PAYLOAD_BYTES` | 64 | 64 | `payload.rs` |
| `QOS1_WINDOW` | 32 | 32 | `runner.rs` |
| `warmup_secs` | 10 | 2 | `ProtocolConstants::for_profile` |
| `window_secs` | 60 | 5 | `ProtocolConstants::for_profile` |
| `repetitions` | 5 | 2 | `ProtocolConstants::for_profile` |
| `drain_grace_secs` | 5 | 2 | `ProtocolConstants::for_profile` |
| `max_config_attempts` | 3 | 1 | `ProtocolConstants::for_profile` |
| `enforce_spread` | true | false | `ProtocolConstants::for_profile` |
| `LATENCY_FRACTIONS_PCT` | `[50, 80]` | `[50, 80]` | `runner.rs` |
| `SYSCALL_FRACTION_PCT` | 80 | 80 | `runner.rs` |
| `SPREAD_LIMIT` | 0.10 | 0.10 | `analysis.rs` |
| `GENERATOR_LATENESS_FRACTION` | 0.10 | 0.10 | `analysis.rs` |
| `SUSTAINED_TAIL_SECS` | 10 | 10 | `runner.rs` |
| `GEN_LEAD_SECS` | 1 | 1 | `runner.rs` |
| `COUNTER_BOUNDARY_TOLERANCE_NS` | 10 ms | 10 ms | `runner.rs` |
| `COUNTER_CONTROL_TIMEOUT_SECS` | 5 | 5 | `runner.rs` |
| `CHILD_EXIT_SLACK_SECS` | 30 | 30 | `runner.rs` |
| `STARTUP_TIMEOUT_SECS` | 10 | 10 | `runner.rs` |
| `WAIT_POLL_MS` | 50 | 50 | `runner.rs` |
| `DEFAULT_SERVER_CPUS` | `0,2,4,6` | `0,2,4,6` | `runner.rs` |
| `DEFAULT_GENERATOR_CPUS` | `8-17` | `8-17` | `runner.rs` |

The runner refuses a `--profile recorded` invocation whose `--servers`,
`--workers` or `--qos` differs from the recorded triple above; the
smoke profile accepts any subset.

## File naming

One JSON per run; naming uses `bench::run_record::RunRecord::file_name`:

```
<server>-w<workers>[-q<qos>]-<kind>[-p<rate_fraction_pct>]-a<attempt>-r<repetition>.json
```

Examples:

- `uring-w1-q1-throughput-a1-r1.json`
- `tokio-w4-q0-latency-p80-a2-r3.json`
- `mosquitto-w1-connections-a1-r1.json`

Worker is mandatory; QoS appears only on throughput and latency;
`rate_fraction_pct` appears only on latency. The runner writes failed
attempts under their own attempt number, so a configuration that
succeeds on attempt 2 leaves both `…-a1-…` and `…-a2-…` on disk.

## Recorded raw JSON

`bench run` is the committed raw artefact: the `*.json` files under
`<out>/` are what `BENCHMARKS.md` is generated from. Do not delete
them between runs; `--resume` reads them, and the owner sign-off
cross-references them.

## Reading this file

If a later nightly `rustc` fails to build the bench crate, install the
toolchain named on the "Last verified" line and rebuild from the
manifest path above. If `bench doc` refuses with a constant
mismatch, re-read the constants table; the runner enforces it
verbatim.
