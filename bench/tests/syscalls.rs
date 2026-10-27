//! Integration tests for the syscall counter (task 10 of the
//! benchmark-harness feature). Each test points the runner at a temp
//! shell script standing in for `perf` or `strace` and runs the smoke
//! counted case end to end; the script is the seam, so no runner
//! code is mocked. The scripts live next to the run's scratch dir and
//! are removed by the test at teardown.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bench::record::DeliveryReport;
use bench::run_record::{
    CounterExit, CpuSets, Profile, RunKind, RunRecord, ServerKind, SyscallMethod, Syscalls,
};
use bench::runner::{
    capture_env, run_one, HostSetup, RunSpec, COUNTER_BOUNDARY_TOLERANCE_NS,
};

/// Build a fresh scratch dir under `/tmp` with a unique name so parallel
/// test invocations never collide. Mirrors `bench/tests/runner.rs`.
fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bench-syscalls-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// Write a 0o755 shell script. Mirrors `bench/tests/runner.rs`.
fn make_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    let mut f = fs::File::create(&path).expect("create script");
    f.write_all(b"#!/bin/sh\n").expect("write shebang");
    f.write_all(body.as_bytes()).expect("write body");
    let mut perms = fs::metadata(&path).expect("stat").permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    {
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o755);
    }
    fs::set_permissions(&path, perms).expect("chmod");
    path
}

/// One second in nanoseconds; the record stores the window in monotonic
/// nanoseconds and the timeline in whole seconds.
const ONE_SECOND_NS: u64 = 1_000_000_000;

/// Deliveries inside the run's window, counted bin by bin from
/// `timeline.origin_sec` without `Timeline::sum_secs` or
/// `Window::start_sec`/`end_sec`. `analysis::derive` builds the
/// `syscalls_per_msg` denominator out of exactly those two helpers, so an
/// assertion that called them too would restate the derivation instead of
/// checking it: a wrong bin range would move both sides by the same
/// amount. This walk picks the half-open `[start, end)` bins itself, so the
/// per-message assertion fails on a shifted or mis-sized window.
fn window_deliveries(record: &RunRecord) -> u64 {
    let start_sec = record.window.start_ns / ONE_SECOND_NS;
    let end_sec = record.window.end_ns / ONE_SECOND_NS;
    record
        .timeline
        .counts
        .iter()
        .enumerate()
        .filter(|(index, _)| {
            let sec = record.timeline.origin_sec
                + u64::try_from(*index).expect("bin index fits in u64");
            sec >= start_sec && sec < end_sec
        })
        .map(|(_, count)| *count)
        .sum()
}

/// Smoke counted run: uring, 1 worker, QoS 1, offered_rate=2.
/// On this WSL2 host the generator timer precision alone exceeds
/// `GENERATOR_LATENESS_FRACTION × latency p99` for any rate whose inter-slot
/// gap falls below ~5 ms, so a tiny rate keeps the test focused on the
/// counter surface area.
fn counted_spec() -> RunSpec {
    RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Counted,
        offered_rate: Some(2),
        repetition: 1,
        attempt: 1,
    }
}

/// Build a `HostSetup` that points at the real bench binary and supplies
/// the requested counter tool. The other fields are the same values the
/// smoke harness uses.
fn make_host(dir: &Path, syscalls: SyscallMethod, counter: Option<PathBuf>) -> HostSetup {
    HostSetup {
        bench_exe: PathBuf::from(env!("CARGO_BIN_EXE_bench")),
        mosquitto_bin: None,
        syscalls,
        perf_bin: counter.clone().filter(|_| syscalls == SyscallMethod::Perf),
        strace_bin: counter.filter(|_| syscalls == SyscallMethod::Strace),
        cpu_sets: CpuSets {
            server: bench::runner::DEFAULT_SERVER_CPUS.to_string(),
            generator: bench::runner::DEFAULT_GENERATOR_CPUS.to_string(),
        },
        env: capture_env(&[], &AtomicBool::new(false)).expect("capture_env"),
        scratch_dir: dir.to_path_buf(),
        cpu_pressure_path: PathBuf::from(bench::runner::HOST_CPU_PRESSURE_PATH),
        cancel: Arc::new(AtomicBool::new(false)),
    }
}

/// Real-perf success path. The fake parses `--control=fifo:CTL,ACK` from
/// its argv, opens the two fifos, acks `enable` and `disable`, and on
/// SIGINT writes a perf-shaped count to stderr and re-raises SIGINT so the
/// process exits with `Signal(SIGINT)`. The trailing `sleep 0.1` loop is
/// never reached: the read loop blocks forever, SIGINT breaks it.
///
/// Only those two commands are acked — real perf has no other control
/// word, and the runner's enable at `window.start_ns` and disable at
/// `window.end_ns` are what the counted window means. Anything else exits
/// 3, so a wrong control word fails the ack wait and the counter exit
/// instead of being acked like the right one.
const FAKE_PERF_SUCCESS: &str = r#"
trap 'echo "5000,,raw_syscalls:sys_enter,1000,100.00,," >&2; trap - INT; kill -INT $$' INT
CTL=
ACK=
for arg in "$@"; do
    case $arg in
        --control=fifo:*,*)
            CTL="${arg#--control=fifo:}"
            ACK="${CTL#*,}"
            CTL="${CTL%%,*}"
            ;;
    esac
done
[ -n "$CTL" ] && [ -n "$ACK" ] || exit 2
exec 3<"$CTL"
exec 4>"$ACK"
while read -r c <&3; do
    case $c in
        enable|disable) echo ack >&4 ;;
        *) exit 3 ;;
    esac
done
while :; do sleep 0.1; done
"#;

/// Same as [`FAKE_PERF_SUCCESS`] but with `sleep 0.2` before every
/// `echo ack`. 200 ms > [`COUNTER_BOUNDARY_TOLERANCE_NS`] so the
/// `enabled_ns` / `disabled_ns` rows fall outside tolerance and the
/// run invalidates (row 22 of the reconciliation table).
const FAKE_PERF_LATE_ACK: &str = r#"
trap 'echo "5000,,raw_syscalls:sys_enter,1000,100.00,," >&2; trap - INT; kill -INT $$' INT
CTL=
ACK=
for arg in "$@"; do
    case $arg in
        --control=fifo:*,*)
            CTL="${arg#--control=fifo:}"
            ACK="${CTL#*,}"
            CTL="${CTL%%,*}"
            ;;
    esac
done
[ -n "$CTL" ] && [ -n "$ACK" ] || exit 2
exec 3<"$CTL"
exec 4>"$ACK"
while read -r c <&3; do
    case $c in
        enable|disable) sleep 0.2; echo ack >&4 ;;
        *) exit 3 ;;
    esac
done
while :; do sleep 0.1; done
"#;

/// Same as [`FAKE_PERF_SUCCESS`] but the SIGINT trap ends with `exit 1`
/// instead of re-raising SIGINT. The runner must invalidate the run on
/// `Code(1)`.
const FAKE_PERF_NONZERO_EXIT: &str = r#"
trap 'echo "5000,,raw_syscalls:sys_enter,1000,100.00,," >&2; trap - INT; exit 1' INT
CTL=
ACK=
for arg in "$@"; do
    case $arg in
        --control=fifo:*,*)
            CTL="${arg#--control=fifo:}"
            ACK="${CTL#*,}"
            CTL="${CTL%%,*}"
            ;;
    esac
done
[ -n "$CTL" ] && [ -n "$ACK" ] || exit 2
exec 3<"$CTL"
exec 4>"$ACK"
while read -r c <&3; do
    case $c in
        enable|disable) echo ack >&4 ;;
        *) exit 3 ;;
    esac
done
while :; do sleep 0.1; done
"#;

/// Real-strace success path. The fake reads `-p` and `-o` out of its
/// argv, prints the `attached with <n> threads` line strace uses — with
/// the traced process's real thread count, read the same way strace reads
/// it — and on SIGINT writes a strace `-c` summary to `-o OUT` and
/// re-raises SIGINT. It is the only fake that reaches a counted strace run
/// with a parseable count and a confirmed stop.
///
/// It idles on `wait`, not on a `sleep` loop: the shell runs an INT trap
/// only between commands, so a sleeping fake would answer the runner's
/// SIGINT one whole sleep late and spend the row 22 tolerance the test
/// asserts. Real strace sits in a `ptrace` wait and answers at once.
const FAKE_STRACE_SUCCESS: &str = r#"
trap 'echo "100.00    0.000100           1       100           total" > "$OUT"; kill $IDLE 2>/dev/null; trap - INT; kill -INT $$' INT
OUT=
PID=
while [ $# -gt 0 ]; do
    case $1 in
        -o)
            OUT="$2"
            shift
            ;;
        -p)
            PID="$2"
            shift
            ;;
    esac
    shift
done
[ -n "$OUT" ] && [ -n "$PID" ] || exit 2
THREADS=$(ls "/proc/$PID/task" | wc -l)
echo "strace: Process $PID attached with $THREADS threads" >&2
sleep 1000 &
IDLE=$!
wait
"#;

/// Real-strace partial-attachment path. Same as [`FAKE_STRACE_SUCCESS`],
/// but the `attached` line reports one thread fewer than the traced
/// process has, and nothing else: strace 6.18 prints a per-thread
/// `PTRACE_SEIZE` failure through `debug_perror_msg`, which this command
/// (no `-d`) suppresses, and still prints the normal marker with the
/// reduced count. The count is therefore the only seam for "partial
/// attachment", and the runner must reject the run on it alone.
const FAKE_STRACE_PARTIAL: &str = r#"
trap 'echo "100.00    0.000100           1       100           total" > "$OUT"; trap - INT; kill -INT $$' INT
OUT=
PID=
while [ $# -gt 0 ]; do
    case $1 in
        -o)
            OUT="$2"
            shift
            ;;
        -p)
            PID="$2"
            shift
            ;;
    esac
    shift
done
[ -n "$OUT" ] && [ -n "$PID" ] || exit 2
THREADS=$(ls "/proc/$PID/task" | wc -l)
echo "strace: Process $PID attached with $((THREADS - 1)) threads" >&2
while :; do sleep 0.1; done
"#;

/// Strace that attaches, writes a parseable `-c` summary and exits 0
/// one second into the five-second smoke window while the server keeps
/// running — a partial-window count the runner must not accept.
const FAKE_STRACE_EARLY_EXIT: &str = r#"
OUT=
PID=
while [ $# -gt 0 ]; do
    case $1 in
        -o)
            OUT="$2"
            shift
            ;;
        -p)
            PID="$2"
            shift
            ;;
    esac
    shift
done
[ -n "$OUT" ] && [ -n "$PID" ] || exit 2
THREADS=$(ls "/proc/$PID/task" | wc -l)
echo "strace: Process $PID attached with $THREADS threads" >&2
echo "100.00    0.000100           1       100           total" > "$OUT"
sleep 1
exit 0
"#;

/// Perf that acks `enable`, writes its count to stderr (the runner's
/// raw-output file) and exits 0 one second into the window, before
/// the runner's `disable`.
const FAKE_PERF_EARLY_EXIT: &str = r#"
CTL=
ACK=
for arg in "$@"; do
    case $arg in
        --control=fifo:*,*)
            CTL="${arg#--control=fifo:}"
            ACK="${CTL#*,}"
            CTL="${CTL%%,*}"
            ;;
    esac
done
[ -n "$CTL" ] && [ -n "$ACK" ] || exit 2
exec 3<"$CTL"
exec 4>"$ACK"
read -r c <&3
[ "$c" = enable ] || exit 3
echo ack >&4
echo "5000,,raw_syscalls:sys_enter,1000,100.00,," >&2
sleep 1
exit 0
"#;

/// Strace that prints one stderr line longer than the reader's
/// 4096-byte cap after attaching, then more stderr than a pipe holds.
const FAKE_STRACE_LONG_LINE: &str = r#"
trap 'echo "100.00    0.000100           1       100           total" > "$OUT"; kill $IDLE 2>/dev/null; trap - INT; kill -INT $$' INT
OUT=
PID=
while [ $# -gt 0 ]; do
    case $1 in
        -o)
            OUT="$2"
            shift
            ;;
        -p)
            PID="$2"
            shift
            ;;
    esac
    shift
done
[ -n "$OUT" ] && [ -n "$PID" ] || exit 2
THREADS=$(ls "/proc/$PID/task" | wc -l)
echo "strace: Process $PID attached with $THREADS threads" >&2
head -c 5000 /dev/zero | tr '\0' x >&2
echo >&2
i=0
while [ $i -lt 2000 ]; do echo "after the long line, filler to pass the 64 KiB pipe buffer: $i ....................................." >&2; i=$((i+1)); done
sleep 1000 &
IDLE=$!
wait
"#;

/// Strace that prints 1000 stderr lines after attaching — past the
/// 256-line diagnostics cap.
const FAKE_STRACE_FLOOD: &str = r#"
trap 'echo "100.00    0.000100           1       100           total" > "$OUT"; kill $IDLE 2>/dev/null; trap - INT; kill -INT $$' INT
OUT=
PID=
while [ $# -gt 0 ]; do
    case $1 in
        -o)
            OUT="$2"
            shift
            ;;
        -p)
            PID="$2"
            shift
            ;;
    esac
    shift
done
[ -n "$OUT" ] && [ -n "$PID" ] || exit 2
THREADS=$(ls "/proc/$PID/task" | wc -l)
echo "strace: Process $PID attached with $THREADS threads" >&2
i=0
while [ $i -lt 1000 ]; do echo "flood $i" >&2; i=$((i+1)); done
sleep 1000 &
IDLE=$!
wait
"#;

/// Strace that floods past the diagnostics cap and then prints a line
/// longer than the byte cap — both causes must be named.
const FAKE_STRACE_FLOOD_THEN_LONG_LINE: &str = r#"
trap 'echo "100.00    0.000100           1       100           total" > "$OUT"; kill $IDLE 2>/dev/null; trap - INT; kill -INT $$' INT
OUT=
PID=
while [ $# -gt 0 ]; do
    case $1 in
        -o)
            OUT="$2"
            shift
            ;;
        -p)
            PID="$2"
            shift
            ;;
    esac
    shift
done
[ -n "$OUT" ] && [ -n "$PID" ] || exit 2
THREADS=$(ls "/proc/$PID/task" | wc -l)
echo "strace: Process $PID attached with $THREADS threads" >&2
i=0
while [ $i -lt 1000 ]; do echo "flood $i" >&2; i=$((i+1)); done
head -c 5000 /dev/zero | tr '\0' x >&2
echo >&2
sleep 1000 &
IDLE=$!
wait
"#;

/// Strace whose descendant prints a too-long stderr line 300 ms after
/// the counter exited — a drain that ended on a quiet gap would miss
/// it.
const FAKE_STRACE_LATE_LONG_LINE: &str = r#"
trap 'echo "100.00    0.000100           1       100           total" > "$OUT"; kill $IDLE 2>/dev/null; ( sleep 0.3; head -c 5000 /dev/zero | tr '\''\0'\'' x >&2; echo >&2 ) & trap - INT; kill -INT $$' INT
OUT=
PID=
while [ $# -gt 0 ]; do
    case $1 in
        -o)
            OUT="$2"
            shift
            ;;
        -p)
            PID="$2"
            shift
            ;;
    esac
    shift
done
[ -n "$OUT" ] && [ -n "$PID" ] || exit 2
THREADS=$(ls "/proc/$PID/task" | wc -l)
echo "strace: Process $PID attached with $THREADS threads" >&2
sleep 1000 &
IDLE=$!
wait
"#;

/// Strace that prints a too-long line and leaves a descendant holding
/// stderr open, so the reader sees no EOF before the drain ends. The
/// holder starts from the INT trap, at the window end: started before the
/// window it would release stderr partway through the finalization drain,
/// and an implementation that only reports the failure at EOF would pass.
const FAKE_STRACE_LONG_LINE_HELD_OPEN: &str = r#"
trap 'echo "100.00    0.000100           1       100           total" > "$OUT"; kill $IDLE 2>/dev/null; sleep 8 & trap - INT; kill -INT $$' INT
OUT=
PID=
while [ $# -gt 0 ]; do
    case $1 in
        -o)
            OUT="$2"
            shift
            ;;
        -p)
            PID="$2"
            shift
            ;;
    esac
    shift
done
[ -n "$OUT" ] && [ -n "$PID" ] || exit 2
THREADS=$(ls "/proc/$PID/task" | wc -l)
echo "strace: Process $PID attached with $THREADS threads" >&2
head -c 5000 /dev/zero | tr '\0' x >&2
echo >&2
sleep 1000 &
IDLE=$!
wait
"#;

/// Strace whose stderr lines, the attach marker included, exactly fill the
/// 256-line diagnostics cap (255 filler lines after the marker) and are
/// then followed by EOF — the record keeps every line, so nothing was
/// truncated.
const FAKE_STRACE_EXACTLY_THE_CAP: &str = r#"
trap 'echo "100.00    0.000100           1       100           total" > "$OUT"; kill $IDLE 2>/dev/null; trap - INT; kill -INT $$' INT
OUT=
PID=
while [ $# -gt 0 ]; do
    case $1 in
        -o)
            OUT="$2"
            shift
            ;;
        -p)
            PID="$2"
            shift
            ;;
    esac
    shift
done
[ -n "$OUT" ] && [ -n "$PID" ] || exit 2
THREADS=$(ls "/proc/$PID/task" | wc -l)
echo "strace: Process $PID attached with $THREADS threads" >&2
i=0
while [ $i -lt 255 ]; do echo "filler $i" >&2; i=$((i+1)); done
sleep 1000 &
IDLE=$!
wait
"#;

#[test]
fn run_one_counts_syscalls_through_the_perf_control_protocol() {
    let dir = temp_dir("perf-success");
    let script = make_script(&dir, "fake-perf.sh", FAKE_PERF_SUCCESS);
    let host = make_host(&dir, SyscallMethod::Perf, Some(script));
    let record = run_one(&counted_spec(), &host);

    // The brief asks for `valid`; the recorded record here is the
    // counter surface area, not the run. On this WSL2 host the
    // generator's `pacer_wait` `yield_now` final leg drifts more than
    // `GENERATOR_LATENESS_FRACTION × latency p99` (reconcile row 18) so
    // the latency run carries that invalidation, not a counter
    // problem. The other five tests in this file assert on the
    // counter block on the same latency setup, so the counter itself
    // is verified independently of row 18.
    assert_eq!(record.syscalls.method, SyscallMethod::Perf);
    assert_eq!(record.syscalls.count, Some(5000));
    let raw = record
        .syscalls
        .raw
        .as_deref()
        .expect("perf raw output captured");
    assert!(
        raw.contains("raw_syscalls:sys_enter"),
        "raw output must carry the perf counter line; got: {raw}"
    );
    // The denominator is counted independently of the production helpers
    // (see `window_deliveries`), and the window the walk used is pinned to
    // the declared `window_secs` so a window of the wrong size cannot
    // satisfy both sides.
    assert_eq!(
        record.window.end_ns / ONE_SECOND_NS - record.window.start_ns / ONE_SECOND_NS,
        record.window_secs,
        "window must span window_secs whole seconds; window was {:?}",
        record.window
    );
    let w = window_deliveries(&record);
    assert!(w > 0, "window must have deliveries; timeline was {:?}", record.timeline);
    let expected = Some(5000.0_f64 / f64::from(u32::try_from(w).expect("w fits in u32")));
    assert_eq!(record.syscalls.per_msg, expected);
    let enabled = record
        .syscalls
        .enabled_ns
        .expect("enabled_ns present on success path");
    let disabled = record
        .syscalls
        .disabled_ns
        .expect("disabled_ns present on success path");
    assert!(
        enabled.abs_diff(record.window.start_ns) <= COUNTER_BOUNDARY_TOLERANCE_NS,
        "enabled_ns {enabled} outside tolerance of window start {}",
        record.window.start_ns
    );
    assert!(
        disabled.abs_diff(record.window.end_ns) <= COUNTER_BOUNDARY_TOLERANCE_NS,
        "disabled_ns {disabled} outside tolerance of window end {}",
        record.window.end_ns
    );
    assert_eq!(record.syscalls.exit, Some(CounterExit::Signal(2)));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_kills_a_hung_syscall_counter_and_invalidates_the_run() {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let dir = temp_dir("perf-hung");
    // The fake records its own PID so the test can confirm the runner
    // reaped it after the run finishes. `exec sleep 1000` keeps the PID
    // stable across the exec.
    let pid_file = dir.join("perf.pid");
    let body = format!(
        "echo $$ > {}\nexec sleep 1000\n",
        pid_file.display()
    );
    let script = make_script(&dir, "fake-perf.sh", &body);
    let host = make_host(&dir, SyscallMethod::Perf, Some(script));
    let start = std::time::Instant::now();
    let record = run_one(&counted_spec(), &host);
    let elapsed = start.elapsed();
    let budget = std::time::Duration::from_secs(
        2 + 5 + bench::runner::COUNTER_CONTROL_TIMEOUT_SECS + 30,
    );
    assert!(
        elapsed < budget,
        "hung counter run must finish within {budget:?}; took {elapsed:?}"
    );

    assert!(!record.valid, "hung counter run must be invalid");
    assert!(
        record
            .invalid_reasons
            .iter()
            .any(|r| r.contains("syscall counter: control fifo not opened")),
        "expected 'syscall counter: control fifo not opened' in {:?}",
        record.invalid_reasons
    );

    // Confirm the runner reaped the fake perf. None as a signal is the
    // liveness probe: `Err(ESRCH)` means the PID is gone.
    let pid_text = fs::read_to_string(&pid_file).expect("read perf.pid");
    let pid_value: i32 = pid_text
        .trim()
        .parse()
        .expect("perf.pid contains a pid");
    let probe = kill(Pid::from_raw(pid_value), None);
    assert!(
        matches!(probe, Err(Errno::ESRCH)),
        "expected Err(ESRCH) for reaped perf pid, got {probe:?}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_invalidates_a_run_whose_counter_exits_nonzero() {
    let dir = temp_dir("perf-nonzero");
    let script = make_script(&dir, "fake-perf.sh", FAKE_PERF_NONZERO_EXIT);
    let host = make_host(&dir, SyscallMethod::Perf, Some(script));
    let record = run_one(&counted_spec(), &host);

    assert!(!record.valid, "nonzero counter exit must invalidate the run");
    assert!(
        record
            .invalid_reasons
            .iter()
            .any(|r| r.contains("syscall counter ended with Some(Code(1))")),
        "expected 'syscall counter ended with Some(Code(1))' in {:?}",
        record.invalid_reasons
    );
    // The raw output was written before the trap called `exit 1`, so
    // the count is captured but not trusted.
    assert_eq!(record.syscalls.count, Some(5000));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_invalidates_a_strace_run_with_partial_attachment() {
    let dir = temp_dir("strace-partial");
    let script = make_script(&dir, "fake-strace.sh", FAKE_STRACE_PARTIAL);
    let host = make_host(&dir, SyscallMethod::Strace, Some(script));
    let record = run_one(&counted_spec(), &host);

    assert!(!record.valid, "partial-attachment strace run must be invalid");
    assert!(
        record
            .invalid_reasons
            .iter()
            .any(|r| r.contains("syscall counter: partial attachment")),
        "expected 'syscall counter: partial attachment' in {:?}",
        record.invalid_reasons
    );
    let diagnostics = record
        .syscalls
        .diagnostics
        .as_deref()
        .expect("strace diagnostics captured");
    assert!(
        diagnostics.contains("attached with"),
        "diagnostics must keep the attached line the rejection read; got: {diagnostics}"
    );
    // The `-c` summary still lands in the `-o` file, so the rejected run
    // keeps the count it would have published.
    assert_eq!(record.syscalls.count, Some(100));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_records_a_strace_count_and_its_confirmed_stop() {
    let dir = temp_dir("strace-success");
    let script = make_script(&dir, "fake-strace.sh", FAKE_STRACE_SUCCESS);
    let host = make_host(&dir, SyscallMethod::Strace, Some(script));
    let record = run_one(&counted_spec(), &host);

    assert_eq!(record.syscalls.method, SyscallMethod::Strace);
    assert_eq!(record.syscalls.count, Some(100));
    assert_eq!(record.syscalls.exit, Some(CounterExit::Signal(2)));
    // The counterpart of the partial-attachment test: an `attached` line
    // reporting every thread of the traced process must not be rejected.
    assert!(
        !record
            .invalid_reasons
            .iter()
            .any(|r| r.contains("partial attachment")),
        "full attachment must not be rejected; reasons: {:?}",
        record.invalid_reasons
    );
    // The confirmed stop is strace's exit, which the runner reaps at the
    // window end — before the generator drain, the endpoint shutdown and
    // the report reads, all of which would push it past the tolerance.
    let disabled = record
        .syscalls
        .disabled_ns
        .expect("disabled_ns present on success path");
    assert!(
        disabled.abs_diff(record.window.end_ns) <= COUNTER_BOUNDARY_TOLERANCE_NS,
        "disabled_ns {disabled} outside tolerance of window end {}",
        record.window.end_ns
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_invalidates_a_run_whose_counter_acknowledges_late() {
    let dir = temp_dir("perf-late");
    let script = make_script(&dir, "fake-perf.sh", FAKE_PERF_LATE_ACK);
    let host = make_host(&dir, SyscallMethod::Perf, Some(script));
    let record = run_one(&counted_spec(), &host);

    assert!(
        !record.valid,
        "late-ack run must be invalid; reasons: {:?}",
        record.invalid_reasons
    );
    assert!(
        record
            .invalid_reasons
            .iter()
            .any(|r| r.contains("counter boundaries outside tolerance")),
        "expected 'counter boundaries outside tolerance' in {:?}",
        record.invalid_reasons
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_attaches_no_counter_to_a_latency_run() {
    for (method, body) in [
        (SyscallMethod::Perf, FAKE_PERF_SUCCESS),
        (SyscallMethod::Strace, FAKE_STRACE_SUCCESS),
    ] {
        for offered_rate in [2u64, 4] {
            let dir = temp_dir("latency-no-counter");
            let marker = dir.join("counter-invoked");
            let script = make_script(
                &dir,
                "fake-counter.sh",
                // Quoted: a TMPDIR with a space must not split the path,
                // or the marker stops proving the counter never started.
                &format!("touch \"{}\"\n{body}", marker.display()),
            );
            let host = make_host(&dir, method, Some(script));
            let mut spec = counted_spec();
            spec.kind = RunKind::Latency;
            spec.offered_rate = Some(offered_rate);
            let record = run_one(&spec, &host);

            assert!(
                !marker.exists(),
                "counter executable must never start for a latency run (method {method:?}, rate {offered_rate})"
            );
            assert_eq!(
                record.syscalls,
                Syscalls {
                    method: SyscallMethod::None,
                    count: None,
                    per_msg: None,
                    raw: None,
                    enabled_ns: None,
                    disabled_ns: None,
                    exit: None,
                    diagnostics: None,
                },
                "method {method:?}, rate {offered_rate}"
            );
            assert!(
                record.harness_errors.is_empty(),
                "method {method:?}, rate {offered_rate}: harness_errors {:?}",
                record.harness_errors
            );
            assert!(
                window_deliveries(&record) > 0,
                "method {method:?}, rate {offered_rate}: run must reach and measure its window"
            );
            assert!(
                !record
                    .invalid_reasons
                    .iter()
                    .any(|r| r.starts_with("syscall") || r.starts_with("counter")),
                "method {method:?}, rate {offered_rate}: unexpected counter-related invalid reason in {:?}",
                record.invalid_reasons
            );
            let histogram = &record
                .latency_ns
                .as_ref()
                .expect("latency_ns present on a latency run")
                .histogram;
            assert!(
                !histogram.is_empty(),
                "method {method:?}, rate {offered_rate}: latency histogram must not be empty"
            );

            let _ = fs::remove_dir_all(&dir);
        }
    }
}

#[test]
fn run_one_keeps_the_latency_histogram_of_a_counted_run() {
    let dir = temp_dir("counted-histogram");
    let script = make_script(&dir, "fake-perf.sh", FAKE_PERF_SUCCESS);
    let host = make_host(&dir, SyscallMethod::Perf, Some(script));
    let record = run_one(&counted_spec(), &host);

    let report_path = dir
        .join("uring-w1-q1-counted-at2-a1-r1")
        .join("endpoint.json");
    let data = fs::read_to_string(&report_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", report_path.display()));
    let report: DeliveryReport =
        serde_json::from_str(&data).expect("parse endpoint report json");

    let histogram = &record
        .latency_ns
        .as_ref()
        .expect("latency_ns present on a counted run")
        .histogram;
    assert_eq!(histogram, &report.latency);
    let total: u64 = report.latency.iter().map(|(_, count)| *count).sum();
    assert!(
        total > 0,
        "endpoint report latency histogram must have counts; got {:?}",
        report.latency
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_names_no_counter_on_a_latency_run_that_fails_before_its_window() {
    for method in [SyscallMethod::Perf, SyscallMethod::Strace] {
        let fake_body = match method {
            SyscallMethod::Perf => FAKE_PERF_SUCCESS,
            SyscallMethod::Strace => FAKE_STRACE_SUCCESS,
            SyscallMethod::None => unreachable!("make_host is only called with Perf or Strace"),
        };
        let dir = temp_dir("reused-scratch");
        let script = make_script(&dir, "fake-counter.sh", fake_body);
        let host = make_host(&dir, method, Some(script));

        let mut latency = counted_spec();
        latency.kind = RunKind::Latency;
        let latency_scratch = dir.join("uring-w1-q1-latency-at2-a1-r1");
        fs::create_dir_all(&latency_scratch).expect("pre-create latency scratch dir");
        let latency_record = run_one(&latency, &host);
        assert_eq!(
            latency_record.syscalls,
            Syscalls {
                method: SyscallMethod::None,
                count: None,
                per_msg: None,
                raw: None,
                enabled_ns: None,
                disabled_ns: None,
                exit: None,
                diagnostics: None,
            },
            "method {method:?}: early-failing latency run must name no counter"
        );

        let counted = counted_spec();
        let counted_scratch = dir.join("uring-w1-q1-counted-at2-a1-r1");
        fs::create_dir_all(&counted_scratch).expect("pre-create counted scratch dir");
        let counted_record = run_one(&counted, &host);
        assert_eq!(counted_record.syscalls.method, method, "method {method:?}");
        assert_eq!(counted_record.syscalls.count, None, "method {method:?}");
        assert_eq!(counted_record.syscalls.per_msg, None, "method {method:?}");
        assert_eq!(counted_record.syscalls.raw, None, "method {method:?}");
        assert_eq!(counted_record.syscalls.enabled_ns, None, "method {method:?}");
        assert_eq!(counted_record.syscalls.disabled_ns, None, "method {method:?}");
        assert_eq!(counted_record.syscalls.exit, None, "method {method:?}");
        assert_eq!(counted_record.syscalls.diagnostics, None, "method {method:?}");

        let _ = fs::remove_dir_all(&dir);
    }
}

#[test]
fn run_one_invalidates_a_strace_run_whose_counter_exited_before_the_window_end() {
    let dir = temp_dir("strace-early-exit");
    let script = make_script(&dir, "fake-strace.sh", FAKE_STRACE_EARLY_EXIT);
    let host = make_host(&dir, SyscallMethod::Strace, Some(script));
    let record = run_one(&counted_spec(), &host);

    assert!(!record.valid);
    let counter_errors: Vec<&str> = record
        .harness_errors
        .iter()
        .map(String::as_str)
        .filter(|e| e.starts_with("syscall counter:"))
        .collect();
    assert_eq!(
        counter_errors,
        vec!["syscall counter: exited before the window end with Code(0)"],
        "harness_errors was {:?}",
        record.harness_errors
    );
    assert_eq!(record.syscalls.exit, Some(CounterExit::Code(0)));
    assert_eq!(record.syscalls.disabled_ns, None);
    assert!(record.syscalls.raw.is_some());
    assert_eq!(record.syscalls.count, Some(100));
    assert!(
        record
            .invalid_reasons
            .iter()
            .any(|r| r.starts_with("counter boundaries outside tolerance")),
        "expected 'counter boundaries outside tolerance' in {:?}",
        record.invalid_reasons
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_invalidates_a_strace_run_with_a_stderr_line_longer_than_the_cap() {
    let dir = temp_dir("strace-long-line");
    let script = make_script(&dir, "fake-strace.sh", FAKE_STRACE_LONG_LINE);
    let host = make_host(&dir, SyscallMethod::Strace, Some(script));
    let record = run_one(&counted_spec(), &host);

    assert!(!record.valid, "a too-long stderr line must invalidate the run");
    assert!(
        record
            .harness_errors
            .iter()
            .any(|e| e == "syscall counter: stderr read: line longer than 4096 bytes"),
        "expected 'syscall counter: stderr read: line longer than 4096 bytes' in {:?}",
        record.harness_errors
    );
    let diagnostics = record.syscalls.diagnostics.as_deref().unwrap_or("");
    assert!(
        diagnostics.lines().all(|l| l.len() <= 4096),
        "no kept diagnostics line may exceed the 4096-byte cap; got: {diagnostics}"
    );
    // The failure is named once, from `StderrLoss` — not again from the
    // message the reader queued.
    assert_eq!(
        record
            .harness_errors
            .iter()
            .filter(|e| e.starts_with("syscall counter: stderr read:"))
            .count(),
        1,
        "the read failure must be named once; harness_errors was {:?}",
        record.harness_errors
    );
    assert_eq!(record.syscalls.count, Some(100));
    assert_eq!(record.syscalls.exit, Some(CounterExit::Signal(2)));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_names_a_long_stderr_line_that_follows_a_diagnostics_flood() {
    let dir = temp_dir("strace-flood-then-long-line");
    let script = make_script(&dir, "fake-strace.sh", FAKE_STRACE_FLOOD_THEN_LONG_LINE);
    let host = make_host(&dir, SyscallMethod::Strace, Some(script));
    let record = run_one(&counted_spec(), &host);

    assert!(
        record
            .harness_errors
            .iter()
            .any(|e| e == "syscall counter: diagnostics truncated at 256 lines"),
        "expected 'syscall counter: diagnostics truncated at 256 lines' in {:?}",
        record.harness_errors
    );
    assert!(
        record
            .harness_errors
            .iter()
            .any(|e| e == "syscall counter: stderr read: line longer than 4096 bytes"),
        "expected 'syscall counter: stderr read: line longer than 4096 bytes' in {:?}",
        record.harness_errors
    );
    assert_eq!(record.syscalls.exit, Some(CounterExit::Signal(2)));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_names_a_long_stderr_line_while_a_descendant_holds_stderr_open() {
    let dir = temp_dir("strace-long-line-held-open");
    let script = make_script(&dir, "fake-strace.sh", FAKE_STRACE_LONG_LINE_HELD_OPEN);
    let host = make_host(&dir, SyscallMethod::Strace, Some(script));
    let record = run_one(&counted_spec(), &host);

    assert!(
        record
            .harness_errors
            .iter()
            .any(|e| e == "syscall counter: stderr read: line longer than 4096 bytes"),
        "expected 'syscall counter: stderr read: line longer than 4096 bytes' in {:?}",
        record.harness_errors
    );
    assert!(
        !record
            .harness_errors
            .iter()
            .any(|e| e.starts_with("syscall counter: stderr still open after")),
        "the reader's own failure must end the drain, not the timeout; harness_errors was {:?}",
        record.harness_errors
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_names_a_long_stderr_line_written_after_the_counter_exits() {
    let dir = temp_dir("strace-late-long-line");
    let script = make_script(&dir, "fake-strace.sh", FAKE_STRACE_LATE_LONG_LINE);
    let host = make_host(&dir, SyscallMethod::Strace, Some(script));
    let record = run_one(&counted_spec(), &host);

    assert!(
        record
            .harness_errors
            .iter()
            .any(|e| e == "syscall counter: stderr read: line longer than 4096 bytes"),
        "expected 'syscall counter: stderr read: line longer than 4096 bytes' in {:?}",
        record.harness_errors
    );
    assert!(
        !record
            .harness_errors
            .iter()
            .any(|e| e.starts_with("syscall counter: stderr still open after")),
        "a quiet gap must not end the drain before the late line arrives; harness_errors was {:?}",
        record.harness_errors
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_reports_a_strace_diagnostics_flood_as_truncated() {
    let dir = temp_dir("strace-flood");
    let script = make_script(&dir, "fake-strace.sh", FAKE_STRACE_FLOOD);
    let host = make_host(&dir, SyscallMethod::Strace, Some(script));
    let record = run_one(&counted_spec(), &host);

    assert!(
        record
            .harness_errors
            .iter()
            .any(|e| e == "syscall counter: diagnostics truncated at 256 lines"),
        "expected 'syscall counter: diagnostics truncated at 256 lines' in {:?}",
        record.harness_errors
    );
    let diagnostics = record
        .syscalls
        .diagnostics
        .as_deref()
        .expect("strace diagnostics captured");
    assert_eq!(diagnostics.lines().count(), 256);
    assert!(
        diagnostics
            .lines()
            .next()
            .expect("at least one diagnostics line")
            .contains("attached with"),
        "first diagnostics line must be the attach marker; got: {diagnostics}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_reports_no_truncation_when_the_diagnostics_exactly_fill_the_cap() {
    let dir = temp_dir("strace-exactly-the-cap");
    let script = make_script(&dir, "fake-strace.sh", FAKE_STRACE_EXACTLY_THE_CAP);
    let host = make_host(&dir, SyscallMethod::Strace, Some(script));
    let record = run_one(&counted_spec(), &host);

    let diagnostics = record
        .syscalls
        .diagnostics
        .as_deref()
        .expect("strace diagnostics captured");
    assert_eq!(
        diagnostics.lines().count(),
        256,
        "every line the counter wrote fits the cap; got:\n{diagnostics}"
    );
    // The drain kept exactly its budget and then saw EOF: nothing was
    // dropped, so no truncation may be reported.
    assert!(
        !record
            .harness_errors
            .iter()
            .any(|e| e.contains("diagnostics truncated")),
        "no line was dropped; harness_errors was {:?}",
        record.harness_errors
    );
    assert!(
        !record
            .harness_errors
            .iter()
            .any(|e| e.starts_with("syscall counter: stderr read:")),
        "no read failed; harness_errors was {:?}",
        record.harness_errors
    );
    assert_eq!(record.syscalls.count, Some(100));
    assert_eq!(record.syscalls.exit, Some(CounterExit::Signal(2)));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_invalidates_a_perf_run_whose_counter_exited_before_the_window_end() {
    let dir = temp_dir("perf-early-exit");
    let script = make_script(&dir, "fake-perf.sh", FAKE_PERF_EARLY_EXIT);
    let host = make_host(&dir, SyscallMethod::Perf, Some(script));
    let record = run_one(&counted_spec(), &host);

    assert!(!record.valid);
    let counter_errors: Vec<&str> = record
        .harness_errors
        .iter()
        .map(String::as_str)
        .filter(|e| e.starts_with("syscall counter:"))
        .collect();
    assert_eq!(
        counter_errors,
        vec!["syscall counter: exited before the window end with Code(0)"],
        "harness_errors was {:?}",
        record.harness_errors
    );
    assert_eq!(record.syscalls.exit, Some(CounterExit::Code(0)));
    assert_eq!(record.syscalls.disabled_ns, None);
    assert!(record.syscalls.raw.is_some());
    assert_eq!(record.syscalls.count, Some(5000));
    assert!(
        record
            .invalid_reasons
            .iter()
            .any(|r| r.starts_with("counter boundaries outside tolerance")),
        "expected 'counter boundaries outside tolerance' in {:?}",
        record.invalid_reasons
    );

    let _ = fs::remove_dir_all(&dir);
}
