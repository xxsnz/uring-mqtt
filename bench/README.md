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

Checks 1 (mosquitto) and 4 (env probe) are gates — stop and report on
failure. Checks 2 (perf) and 3 (strace) select the syscall counter
(spec §9.1 Counter selection): run check 2; if it prints `OK`, record
with `--syscalls perf --perf-bin <path>`. Otherwise run check 3; if it
prints `OK`, record with `--syscalls strace --strace-bin <path>`. If
neither prints `OK`, stop. Only the selected counter's check blocks
recording, and `bench run` refuses the path of a counter `--syscalls`
did not select, so pass only the selected counter's `--*-bin`.

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
   # 60 s > COUNTER_CONTROL_TIMEOUT_SECS + CHILD_EXIT_SLACK_SECS: the probe
   # target has to outlive the supervisor's SIGKILL deadline, and is killed
   # below only after the verdict's inputs are read. `reap_counter` waits for
   # the counter with the server still running, so a perf that ignores SIGINT
   # and exits only when its target exits must fail here too — a target that
   # died inside the slack would let that perf write `exit 0`.
   sleep 60 &
   pid=$!
   # The supervisor starts perf with SIGINT at its default disposition, as
   # `bench run` does, forwards this check's SIGINT to it, and writes how
   # perf ended to perf.end: `exit N` or `signal N`. A shell `wait` reports
   # 130 both for a death by SIGINT and for a plain `exit 130`, and reconcile
   # row 23 accepts only the first. 30 s after the SIGINT
   # (CHILD_EXIT_SLACK_SECS) it SIGKILLs perf, so the `wait` below is bounded.
   python3 -c '
   import signal, subprocess, sys, time
   deadline = []
   def forward(signum, frame):
       perf.send_signal(signal.SIGINT)
       deadline.append(time.monotonic() + 30)
   signal.signal(signal.SIGINT, forward)
   perf = subprocess.Popen(sys.argv[2:])
   while True:
       try:
           code = perf.wait(timeout=0.1)
           break
       except subprocess.TimeoutExpired:
           if deadline and time.monotonic() > deadline[0]:
               perf.kill()
               code = perf.wait()
               break
   with open(sys.argv[1], "w") as end:
       end.write(f"signal {-code}\n" if code < 0 else f"exit {code}\n")
   ' "$dir/perf.end" "$perf_bin" stat --delay=-1 \
     --control=fifo:"$dir/ctl","$dir/ack" \
     -x , -e raw_syscalls:sys_enter -p "$pid" 2>"$dir/perf.out" &
   perf_pid=$!
   exec 3<>"$dir/ctl" 4<>"$dir/ack"
   # 5 s = COUNTER_CONTROL_TIMEOUT_SECS: a perf that is missing or never
   # acknowledges fails this check instead of hanging it.
   # Both responses must contain `ack`: `wait_for_ack` accepts only those
   # bytes, so an empty or error line fails here as it fails a counted run.
   acked=0
   if echo enable >&3 && read -r -t 5 ack <&4 && [ "${ack#*ack}" != "$ack" ]; then
     sleep 0.2
     if echo disable >&3 && read -r -t 5 ack <&4 && [ "${ack#*ack}" != "$ack" ]; then
       acked=1
     fi
   fi
   kill -INT "$perf_pid" 2>/dev/null
   wait "$perf_pid"
   perf_end=$(cat "$dir/perf.end" 2>/dev/null)
   kill "$pid" 2>/dev/null
   wait "$pid" 2>/dev/null
   exec 3>&- 4>&-
   # Reconcile row 23: perf must end by exit 0 or by SIGINT.
   status=1
   if [ "$acked" -eq 1 ] && { [ "$perf_end" = "exit 0" ] || [ "$perf_end" = "signal 2" ]; }; then
     # `parse_perf_count`'s own acceptance: field 3 names the event and
     # field 1 must be a count. A `<not supported>` or `<not counted>`
     # field 1 is an unopened counter, not a zero count, and every counted
     # run of the matrix would fail on it. Only the first matching line
     # counts, because that is the one the parser returns.
     awk -F, 'seen == 0 && $3 == "raw_syscalls:sys_enter" {
                seen = 1; ok = ($1 ~ /^[0-9]+$/)
              }
              END { exit !ok }' "$dir/perf.out"
     status=$?
   fi
   if [ "$status" -eq 0 ]; then
     echo OK
   else
     echo "FAIL: acked=$acked, perf ended '$perf_end', or no counted raw_syscalls:sys_enter line"
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
   exit-status rule. Every wait is bounded, and the check fails on a
   missing perf, a missing acknowledgement, a perf that ignores SIGINT,
   a perf that exits only when its target exits, and any ending but exit
   0 or death by SIGINT. It needs `python3` for the supervisor that
   tells those endings apart.

3. **strace** — equivalent preflight for the fallback:

   ```sh
   strace_bin=strace
   dir=$(mktemp -d)
   # A multithreaded probe, because every counted run traces a
   # multithreaded server. `check_strace_attachment` compares the thread
   # count strace reports with the server's own `/proc/<pid>/task` count and
   # invalidates the counted run on a shortfall, and a per-thread attach
   # failure is otherwise silent. A single-threaded probe cannot tell a host
   # that attaches to every thread from one that attaches to the main thread
   # alone, so it would select a strace the recorded runs reject.
   # The 60 s lifetime is check 2's: 60 s > COUNTER_CONTROL_TIMEOUT_SECS +
   # CHILD_EXIT_SLACK_SECS, so the probe target outlives the supervisor's
   # SIGKILL deadline and a tracer that ignores SIGINT and exits only with
   # its target fails here as it fails a counted run. The probe is killed
   # below only after the verdict's inputs are read.
   probe_threads_expected=4   # the main thread plus the 3 started below
   python3 -c '
   import sys, threading, time
   idle = threading.Event()
   for _ in range(3):
       threading.Thread(target=idle.wait, daemon=True).start()
   open(sys.argv[1], "w").close()
   time.sleep(60)
   ' "$dir/probe.ready" &
   pid=$!
   # `spawn_strace_counter` reads the thread count before it spawns strace,
   # off a server whose threads all exist by `READY`. Wait for the probe's
   # own ready file for the same reason — bounded, so a missing python3
   # fails this check instead of hanging it.
   ready_deadline=$((SECONDS + 5))   # 5 s = COUNTER_CONTROL_TIMEOUT_SECS
   while [ ! -e "$dir/probe.ready" ] && [ "$SECONDS" -lt "$ready_deadline" ]; do
     sleep 0.1
   done
   # `read_thread_count`'s own source: one `/proc/<pid>/task` entry per
   # thread. A probe that never reached its thread count fails the verdict
   # below, so this check never approves strace off a single-threaded trace.
   probe_threads=$(ls "/proc/$pid/task" 2>/dev/null | wc -l)
   # The same supervisor check 2 uses: it starts strace with SIGINT at its
   # default disposition, as `bench run` does, forwards this check's SIGINT to
   # it, and writes how strace ended to strace.end: `exit N` or `signal N`. A
   # shell `wait` reports 130 both for a death by SIGINT and for a plain
   # `exit 130`, and reconcile row 23 accepts only the first. 30 s after the
   # SIGINT (CHILD_EXIT_SLACK_SECS) it SIGKILLs strace, so the `wait` below is
   # bounded and a tracer that ignores SIGINT fails this check instead of
   # hanging it.
   python3 -c '
   import signal, subprocess, sys, time
   deadline = []
   def forward(signum, frame):
       strace.send_signal(signal.SIGINT)
       deadline.append(time.monotonic() + 30)
   signal.signal(signal.SIGINT, forward)
   strace = subprocess.Popen(sys.argv[2:])
   while True:
       try:
           code = strace.wait(timeout=0.1)
           break
       except subprocess.TimeoutExpired:
           if deadline and time.monotonic() > deadline[0]:
               strace.kill()
               code = strace.wait()
               break
   with open(sys.argv[1], "w") as end:
       end.write(f"signal {-code}\n" if code < 0 else f"exit {code}\n")
   ' "$dir/strace.end" "$strace_bin" -c -f -p "$pid" \
     -o "$dir/strace.out" 2>"$dir/strace.err" &
   strace_pid=$!
   # `arm_counter` waits for strace's `attached` stderr marker and gives up
   # after COUNTER_CONTROL_TIMEOUT_SECS, so wait for the same marker on the
   # same deadline instead of a fixed sleep: a tracer that counts but never
   # announces the attachment fails every counted run. The marker carries
   # the attachment count, so the line also has to be complete — the
   # `tail -c 1` test below is empty only when the file ends on a newline.
   attach_deadline=$((SECONDS + 5))   # 5 s = COUNTER_CONTROL_TIMEOUT_SECS
   attached=0
   while [ "$SECONDS" -lt "$attach_deadline" ]; do
     if grep -q attached "$dir/strace.err" 2>/dev/null \
        && [ -z "$(tail -c 1 "$dir/strace.err")" ]; then
       # `parse_strace_attached_threads`'s own reading: the first line
       # carrying the marker, the number after `attached with `, and 1 for
       # the short `attached` wording — which is what strace prints both for
       # a single thread and when every sibling thread's attach failed.
       attached=$(awk '/attached/ {
                         n = 1
                         if (match($0, /attached with [0-9]+/)) {
                           n = substr($0, RSTART + 14, RLENGTH - 14)
                         }
                         print n
                         exit
                       }' "$dir/strace.err")
       break
     fi
     sleep 0.1
   done
   kill -INT "$strace_pid" 2>/dev/null
   wait "$strace_pid"
   strace_end=$(cat "$dir/strace.end" 2>/dev/null)
   kill "$pid" 2>/dev/null
   wait "$pid" 2>/dev/null
   # Reconcile row 23: strace must end by exit 0 or by SIGINT. The summary
   # goes to a fresh file, so no earlier run's ` total` row can pass this
   # check. The thread test is `check_strace_attachment`'s own: the reported
   # count must not fall below the traced process's thread count, and the
   # probe must have reached its own thread count for that test to mean
   # anything.
   status=1
   if [ "$probe_threads" -eq "$probe_threads_expected" ] \
      && [ "$attached" -ge "$probe_threads" ] \
      && { [ "$strace_end" = "exit 0" ] || [ "$strace_end" = "signal 2" ]; }; then
     # `parse_strace_total`'s own acceptance: the row's last field is
     # `total` and its fourth field is the call count. A ` total` row whose
     # calls field is not a number carries no count, and every counted run
     # of the matrix would fail on it.
     awk '$NF == "total" && $4 ~ /^[0-9]+$/ { ok = 1 }
          END { exit !ok }' "$dir/strace.out"
     status=$?
   fi
   if [ "$status" -eq 0 ]; then
     echo OK
   else
     echo "FAIL: attached=$attached of $probe_threads probe threads" \
          "(expected $probe_threads_expected), strace ended '$strace_end'," \
          "or no counted total row in the summary"
     cat "$dir/strace.out" "$dir/strace.err" 2>/dev/null
   fi
   rm -rf "$dir"
   ( exit "$status" )   # gate status: 0 passes, nonzero stops the preflight
   ```

   `analysis::parse_strace_total` reads the call count from the fourth
   field of the ` total` row, so a summary without that row, or with a
   nonnumeric calls field, fails every counted run — the gate requires a
   numeric count for the same reason check 2 does. An attach refusal
   (`ptrace(`) lands in `$dir/strace.err`, which this check prints on
   failure. The probe is multithreaded and the gate compares the reported
   attachment count with its thread count, because
   `runner::check_strace_attachment` does the same against the server's
   `/proc/<pid>/task` count: a host that attaches to the main thread but
   misses a sibling thread passes a single-threaded probe and then
   invalidates every counted run, after the counted attempts are spent.
   Every wait is bounded, and the check fails on a missing strace, a probe
   that never reached its thread count, a tracer that never prints the
   `attached` marker within `COUNTER_CONTROL_TIMEOUT_SECS`, an attachment
   short of the probe's threads, a strace that ignores SIGINT, a strace
   that exits only when its target exits, and any ending but exit 0 or
   death by SIGINT. Like check 2, it needs `python3`
   for the supervisor that tells those endings apart, and for the probe.

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

For a strace counted run, swap the two perf lines for `--syscalls strace --strace-bin "$(command -v strace)"`.

### Counted runs

`syscalls/msg` comes from a dedicated counted run per server, worker count and QoS: open-loop at the matched 80 % rate, 5 repetitions interleaved with the latency runs (spec §9.1 Counted run). No latency run carries the counter, so both published latency columns are uncounted. The counted runs add 10 configurations — about 50 runs, roughly an hour — to a recorded session.

### Strace exhaustion recovery

A strace counted run that cannot reconcile at the matched rate is re-run at that same rate, never a lower one. When a counted configuration still fails after its last attempt (`bench run` exits 2 naming a `Counted` key), the strace session cannot be published: repair the perf build, pass preflight check 2, and record a fresh dataset into a new `--out` directory with `--syscalls perf`. Do not `--resume` the strace directory — `check_resume` refuses the changed method and tool set — and do not copy, commit or publish any of its records, counted or not: one document carries one method and one environment.

Before every `--resume`, run the selected counter's preflight check again (check 2 for perf, check 3 for strace) and stop if it does not print `OK`. The `perf_event_paranoid` and tracefs settings it relies on do not survive a WSL restart, and `check_resume` does not re-test them, so a resumed session without them uses up its counted attempts.

Exit codes:

- `0` — every configuration passed; `bench doc` can render.
- `2` — at least one configuration failed; records were written, the
  failed keys are named on stderr.
- `1` — refusal (missing `perf-bin`, missing `mosquitto-bin`, output
  directory already holds `*.json`, dirty / mismatched build when
  `--resume` is set, a `--perf-bin`/`--strace-bin` the selected
  `--syscalls` does not use, missing provenance, a provenance
  `git rev-parse` that fails or does not answer within
  `GIT_HEAD_TIMEOUT_SECS` (5 s)) or SIGINT/SIGTERM during the
  preflight.

A dirty / mismatched build refusal with nothing to commit can come from the build itself: when a build-time `git` probe fails or gives up after 5 s, `cargo build` prints a `cargo:warning` and embeds `unknown`, and cargo keeps that value until one of the build script's `rerun-if-changed` inputs changes and `bench` is rebuilt — a new commit, a changed index or source tree, or `bench/build.rs` itself touched. A probe given up on while it was still starting — a stalled filesystem under `git` or under an earlier `PATH` entry — is left to init if its startup finishes after the build script or the CLI exits, so a stray read-only `git` can outlive the command that started it.

The recorded profile runs only the full fixed matrix; `check_request`
refuses `--servers`/`--workers`/`--qos` subsets.

`--resume` continues an interrupted run in the same directory: it
refuses records from another build or host (`check_resume` runs
`analysis::recompute` on every existing file), skips configurations
whose highest attempt passed, and continues attempt numbering for
the rest. Only a completed attempt — repetitions `1..=repetitions`
each present once, none marked `"cancelled": true` — counts against
`max_config_attempts`. An interrupted attempt (SIGINT/SIGTERM, or a
record that could not be written) costs nothing: the records it wrote
stay on disk, a run that returned after the signal is kept with
`"cancelled": true`, and `--resume` continues the configuration at the
next attempt number, so attempt numbers can exceed
`max_config_attempts`. `--resume` needs at least one published record
in `--out`; an interruption before the first record was published
leaves none, and `--resume` then answers `nothing to resume`. The same
answer comes when `--out` names the wrong directory (the recorded
command's `$(date +%F)` part changes at midnight), so first check
that `--out` names the interrupted run's directory, then start the
run again there without `--resume`. Interruptions during the preflight
exit 1 with `cancelled` on stderr.

A record write never replaces a file. Before a run starts, an
attempt number whose record, `<name>.json.tmp` or scratch directory
already exists — left by a run that was killed, say — is skipped at
no cost to the attempt budget, and those files stay untouched. A
record or temporary record that appears while a run is in progress
stops the run with exit 1 naming that path and leaves it untouched.
The scratch directory of a run whose record could not be written is
dropped only when that run created it itself, never one another run
owns. The occupancy check reads the directory before a run starts, so
it cannot order two runs started at the same time against the same
`--out`: give every recorded session its own output directory. An
output directory that cannot be listed completely is refused before
anything runs.

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
- `ProtocolViolation(text)` — recorded constants diverge, a counted
  run lacks a counter, a latency run carries one, or a latency rate
  does not match the throughput medians.
- `FailedConfigurations(names)` — the fixed matrix is missing or
  failed configurations (a highest attempt that is invalid, too
  spread, missing a metric, or not exactly repetitions `1..=5`),
  listed by kebab-case label (`uring-w1-q1-throughput`).

The list is not `render`'s check order — some `ProtocolViolation`
checks run before `FailedConfigurations` and the counter-evidence ones
after it. What the order decides is which error a counted run without
a counter prints: a record `recompute` rejects comes out as
`InconsistentRecord` first, a counted configuration whose highest
attempt failed comes out as `FailedConfigurations`, and
`ProtocolViolation` is left for a counted key whose published attempt
passed while an earlier attempt carries no counter.

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

When an attempt fails because one of its repetitions is invalid, `bench run`
prints `attempt <n> of … failed: repetition <n> is invalid: <reasons>; <host clause>`.
A cancelled repetition, or one that left a child running after cleanup,
fails with its own message and no host clause.

The host clause is `host CPU pressure <pct> % over the sampled interval`:
the share of the interval that run's CPU samples bracket — its window, or
less for a run cancelled before its window ended — during which some
runnable task waited for a CPU (`/proc/pressure/cpu`, `some` line). It
measures contention, not its cause: the benchmark's own pinned threads
count, and it does not name the process behind the contention. It counts
only waits inside this Linux guest. On WSL2 or another VM, load on the
host operating system or the hypervisor does not show in it, so a low
value does not prove the host was idle.

Values measured on the WSL2 development host (18 CPUs) with no other
workload: smoke at workers 1 up to 3.0 % (debug build up to 2.0 %), smoke
at workers 4 up to 8.9 %. Beside 18 busy loops, a workers-1 smoke run
measured 21-75 %. A value far above the band for its settings is a reason
to look for a competing workload: stop what you find and run the smoke
again. A harness defect can fail at any value, so a failure that repeats
on a quiet host is real.

`host CPU pressure was not measured` means the record holds no
contention evidence: `/proc/pressure/cpu` was unreadable or unparsable
at a sample, or the run returned before its window samples.

Every record keeps the raw value as `host_cpu_pressure`
(`sampled_ns`, `some_stall_us`). It never changes a record's validity.

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
- `uring-w1-q1-counted-p80-a1-r1.json`
- `mosquitto-w1-connections-a1-r1.json`

Worker is mandatory; QoS appears only on throughput, latency and
counted runs; `rate_fraction_pct` appears only on latency and counted
runs. The runner writes failed attempts under their own attempt
number, so a configuration that succeeds on attempt 2 leaves both
`…-a1-…` and `…-a2-…` on disk.

## Recorded raw JSON

`bench run` is the committed raw artefact: the `*.json` files under
`<out>/` are what `BENCHMARKS.md` is generated from. Do not delete
them between runs; `--resume` reads them, and the owner sign-off
cross-references them. `bench run --resume` and `bench doc` refuse a
`*.json` whose name is not the name its record computes, so never
rename or copy a record inside `<out>/`.

## Reading this file

If a later nightly `rustc` fails to build the bench crate, install the
toolchain named on the "Last verified" line and rebuild from the
manifest path above. If `bench doc` refuses with a constant
mismatch, re-read the constants table; the runner enforces it
verbatim.
