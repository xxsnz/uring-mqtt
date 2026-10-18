//! Single-run orchestration (task 9 of the benchmark-harness feature).
//! [`run_one`] executes one [`RunSpec`] as guarded child processes under
//! `taskset` in a fresh scratch directory and returns a [`RunRecord`]
//! carrying raw observations, derived metrics and reconciliation results.
//! Signatures and the constants here match `features/benchmark-harness/PLAN.md`
//! `## Data Model` and `## Interface Contracts`.
//!
//! Syscall counting (task 10) is wired here for the counted run
//! (`RunKind::Counted`, §9.1), open-loop at the matched 80 % rate.
//! `run_one` preps perf right after `READY`, spawns strace at
//! `window.start_ns`, and stops, reaps and finalizes either kind at
//! `window.end_ns`. The selected [`SyscallMethod`] and the tool paths
//! come from [`HostSetup`].

use std::fmt::Write as _;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nix::fcntl::{open, OFlag};
use nix::sys::signal::{kill, Signal};
use nix::sys::stat::Mode;
use nix::sys::wait::{waitid, Id, WaitPidFlag, WaitStatus};
use nix::unistd::Pid;

use crate::analysis::{
    count_driver_fds, derive, parse_cpu_pressure_some_total, parse_perf_count, parse_strace_total,
    reconcile,
};
use crate::payload::{monotonic_now_ns, PAYLOAD_BYTES};
use crate::record::{DeliveryReport, Window};
use crate::run_record::{
    Build, Counts, CounterExit, CpuSets, CpuUsage, DriverFds, Env, GeneratorErrors, HostCpuPressure,
    LatencySummary, Profile, RunKind, RunRecord, SCHEMA_VERSION, ServerKind, SyscallMethod,
    Syscalls,
};

/// Connection count every benchmark run sizes its endpoint and generator for
/// (§9.1: 100 connections).
pub const CONNECTIONS: u32 = 100;

/// QoS 1 in-flight publishes the generator admits per connection.
/// `PLAN.md ## Data Model` declares `QOS1_WINDOW = 32`.
pub const QOS1_WINDOW: u32 = 32;

/// Window size in seconds used to compute the throughput tail rate (§9.1:
/// 10-second tail).
pub const SUSTAINED_TAIL_SECS: u64 = 10;

/// Origin lead: the runner picks `origin = next whole second ≥ now + 1` so
/// an endpoint that takes longer to start than one second still leaves
/// room for the ARM handshake and the gen spawn before the schedule
/// begins.
pub const GEN_LEAD_SECS: u64 = 1;

/// How far perf/strace `enabled_ns`/`disabled_ns` may sit from the
/// corresponding window boundary before the run is invalidated.
pub const COUNTER_BOUNDARY_TOLERANCE_NS: u64 = 10_000_000;

/// Poll interval of every runner wait. Cancellation and child-exit polling
/// step in this interval so SIGINT/SIGTERM from the operator are seen
/// within the bound instead of after the next long sleep.
pub const WAIT_POLL_MS: u64 = 50;

/// Upper bound on the time between spawning the endpoint and reading its
/// `READY` line (`bench server`/`bench sub` print it once the start
/// handshake is ready).
pub const STARTUP_TIMEOUT_SECS: u64 = 10;

/// Slack the runner grants a child to exit after SIGTERM before it is
/// killed (`CHILD_EXIT_SLACK_SECS = 30`; an out-of-process bench child
/// that joins its runtime and writes a report inside this window is
/// well-behaved, and an instrumented counter that lingers past this
/// window is treated as a leak).
pub const CHILD_EXIT_SLACK_SECS: u64 = 30;

/// Upper bound on the perf/strace control-handshake round-trip. Task 10
/// owns the actual handshake; task 9 only carries the constant so the
/// runner keeps a single timeout table.
pub const COUNTER_CONTROL_TIMEOUT_SECS: u64 = 5;

/// Upper bound on one `--version` probe in [`capture_env`]. A supplied
/// perf/strace/mosquitto path that never answers must not hold the runner
/// before any record exists, so the probe is killed, reaped, and recorded
/// as the failure it was once this deadline passes.
pub const TOOL_VERSION_TIMEOUT_SECS: u64 = 5;

/// Cap on the stdout bytes one `--version` probe in [`capture_env`] is read
/// for. A version is one short line, so everything past this cap is noise;
/// reading it would let a tool that prints without a newline grow the
/// reader's line buffer for as long as [`TOOL_VERSION_TIMEOUT_SECS`] lasts.
/// The capped reader reports EOF at the cap, so a line the cap cuts arrives
/// without its newline and [`recv_version_line`] refuses it rather than
/// recording a cut version.
const TOOL_VERSION_STDOUT_BYTES_MAX: u64 = 4096;

/// Upper bound `ChildGuard::drop` waits for its survivor lines to be written
/// ([`report_within`]); past it the lines are best effort.
const GUARD_REPORT_TIMEOUT_SECS: u64 = 1;

/// Upper bound on the recorded-run provenance probe [`head_sha`]: a `git`
/// that never answers must not hold the runner before any record exists.
pub const GIT_HEAD_TIMEOUT_SECS: u64 = 5;

/// Upper bound on the reap after a cleanup SIGKILL ([`cleanup_probe`]). The
/// signal can be refused, so the reap waits this long for the child to exit
/// and then gives up with a named error instead of blocking.
const SIGKILL_REAP_TIMEOUT_SECS: u64 = 1;

/// Upper bound on the reap after a guard child's cleanup SIGKILL
/// ([`kill_within_bound`]). Longer than a probe's [`SIGKILL_REAP_TIMEOUT_SECS`]:
/// a run child is a multi-threaded server or generator whose io_uring
/// teardown, a tracer still holding its zombie, or a stalled vCPU can delay a
/// healthy exit past a second, and a slow exit reported as a survivor would
/// stop the matrix.
const GUARD_SIGKILL_REAP_TIMEOUT_SECS: u64 = 10;

/// Index of the start-time field of `/proc/<pid>/stat` among the fields after
/// `comm`: the kernel documents it as field 22, and the first field after the
/// parenthesised `comm` is field 3.
const PROC_STAT_START_TIME_FIELD: usize = 19;

/// Start time a survivor entry carries when the child's own start time could
/// not be read ([`kill_within_bound`]). [`survivor_resume_refusal`] then
/// counts any holder of the pid as that child.
const SURVIVOR_START_UNKNOWN: &str = "?";

/// Cap on the stdout bytes [`head_sha`] reads from its `git`. The probe
/// wants one short SHA, so everything past this cap is noise; reading it
/// would let a `git` on `PATH` that prints without stopping, or one
/// enormous line without a newline, grow the reader's line buffer and its
/// channel for as long as [`GIT_HEAD_TIMEOUT_SECS`] lasts. The capped
/// reader reports EOF at the cap, so a line the cap cuts arrives without its
/// newline and [`wait_for_head_sha`] refuses it rather than answering with a
/// cut SHA; the deadline still bounds the child.
const GIT_HEAD_STDOUT_BYTES_MAX: u64 = 4096;

/// `process_group` argument that puts a probe in a group of its own, led
/// by the probe: the group id is then the probe's pid, and one signal
/// reaches the probe and the descendants that stayed in its group (a
/// descendant that starts a group of its own leaves it).
const PROBE_OWN_PROCESS_GROUP: i32 = 0;

/// Prefix of the harness error [`run_one`] records when the scratch
/// directory of its spec already exists. It is the proof that this run
/// did not create that directory: `matrix::run_configs` reads it to
/// keep its hands off another run's raw evidence.
pub const SCRATCH_REUSED_PREFIX: &str = "scratch directory reused: ";

/// Prefix of the harness error [`run_one`] records for a child its guard
/// could not end. `matrix::run_configs` reads it to stop scheduling: a run
/// measured beside that child would not measure what it claims.
pub const CLEANUP_SURVIVOR_PREFIX: &str = "child survived cleanup: ";

/// Taskset CPU set used when the caller does not override `HostSetup.cpu_sets.server`.
pub const DEFAULT_SERVER_CPUS: &str = "0,2,4,6";

/// Taskset CPU set used when the caller does not override `HostSetup.cpu_sets.generator`.
pub const DEFAULT_GENERATOR_CPUS: &str = "8-17";

/// The kernel's host-wide CPU pressure file (PSI) `bench run` points
/// `HostSetup.cpu_pressure_path` at; a kernel without PSI has no such file.
pub const HOST_CPU_PRESSURE_PATH: &str = "/proc/pressure/cpu";

/// Latency rates the runner derives from the slowest throughput competitor:
/// 50 % and 80 % (§9.1).
pub const LATENCY_FRACTIONS_PCT: [u32; 2] = [50, 80];

/// Rate fraction the counted run is paced at (§9.1: the matched 80 %
/// rate).
pub const SYSCALL_FRACTION_PCT: u32 = 80;

/// One second in nanoseconds, used to round origin / window boundaries.
const ONE_SECOND_NS: u64 = 1_000_000_000;

/// 100 ms — the granularity of the runner's wait loop.
const STEP_NS: u64 = 100 * 1_000_000;

/// Guard label of the broker process (`bench server`, or mosquitto).
const ENDPOINT_LABEL: &str = "endpoint";

/// Guard label of the `bench sub` subscriber a mosquitto run adds.
const SUB_LABEL: &str = "sub";

/// Guard label of the `bench gen` load generator.
const GENERATOR_LABEL: &str = "generator";

/// Guard label of the perf/strace syscall counter.
const COUNTER_LABEL: &str = "counter";

/// Reader-thread label of one `--version` probe in [`capture_env`].
const TOOL_VERSION_LABEL: &str = "version-probe";

/// Reader-thread label of one `git rev-parse` probe in [`head_sha`].
const GIT_HEAD_LABEL: &str = "git-head";

/// What one bounded child-process probe is called: in the errors it returns
/// and in the names of the threads it starts.
#[derive(Clone, Copy)]
struct Probe {
    /// Leads every error the probe returns: `git rev-parse` or `--version`.
    command: &'static str,
    /// Names its threads `bench-runner-<label>-…`.
    label: &'static str,
    /// The probe's deadline, named in its `did not answer within` refusal.
    timeout_secs: u64,
}

/// The recorded-run provenance probe [`head_sha`] runs.
const GIT_HEAD_PROBE: Probe = Probe {
    command: "git rev-parse",
    label: GIT_HEAD_LABEL,
    timeout_secs: GIT_HEAD_TIMEOUT_SECS,
};

/// One `--version` probe [`capture_env`] runs per supplied tool.
const TOOL_VERSION_PROBE: Probe = Probe {
    command: "--version",
    label: TOOL_VERSION_LABEL,
    timeout_secs: TOOL_VERSION_TIMEOUT_SECS,
};

/// Reader-thread label of the strace counter's stderr.
const COUNTER_STDERR_LABEL: &str = "counter-stderr";

/// Poll interval for the non-blocking ack read on the perf control fifo.
/// The brief's bound is 1 ms; the deadline is the one above.
const ACK_POLL_MS: u64 = 1;

/// Poll interval for the ctl open retry on `ENXIO`. The brief's bound is
/// 10 ms; the deadline is the one above.
const CTL_OPEN_RETRY_MS: u64 = 10;

/// Poll interval for the counter's exit reap. Finer than [`WAIT_POLL_MS`]
/// for the same reason [`ACK_POLL_MS`] is: strace's confirmed stop is that
/// reap, and reconcile row 22 measures it against `window.end_ns` with a
/// `COUNTER_BOUNDARY_TOLERANCE_NS` budget, so a 50 ms step would spend five
/// times the whole budget before a prompt exit is even observed.
const COUNTER_EXIT_POLL_MS: u64 = 1;

/// Upper bound on the finalization drain of the strace counter's stderr,
/// measured from the start of the drain and not from its last line. The
/// counter is an operator-supplied executable: it can exit and leave a
/// descendant holding the inherited stderr pipe open, and such a descendant
/// would otherwise keep the drain off EOF for as long as it runs, so
/// `run_one` would never return its record and the per-call guard would
/// never fall.
const COUNTER_DRAIN_TIMEOUT_SECS: u64 = 5;

/// Cap on the lines `Syscalls::diagnostics` retains, counted across the
/// lines [`arm_counter`] consumed before the marker and the finalization
/// drain after it. A strace attach prints a handful; a source past this
/// cap is a stream, not a diagnostic, and the record names the truncation
/// rather than growing with it.
const COUNTER_DIAGNOSTIC_LINES_MAX: usize = 256;

/// Longest strace stderr line, in bytes without its newline, the reader
/// thread accepts. A longer line is a stream, not a diagnostic: the
/// reader reports it as a read failure and queues nothing more, so one
/// line cannot hold unbounded memory.
const COUNTER_STDERR_LINE_BYTES_MAX: u64 = 4096;

/// Bytes the runner writes to the perf ctl fifo at each control point.
const PERF_ENABLE: &[u8] = b"enable\n";
const PERF_DISABLE: &[u8] = b"disable\n";

/// Strace's "attached" stderr marker. The brief's harness error for
/// `COUNTER_CONTROL_TIMEOUT_SECS` is `syscall counter: not attached`.
const STRACE_ATTACHED_MARKER: &str = "attached";

/// The longer form of the marker above: strace reports the number of
/// threads its attach actually covered as `Process <pid> attached with
/// <n> threads`, and prints the short form when it covered one thread.
const STRACE_ATTACHED_WITH_MARKER: &str = "attached with ";

/// Wording of strace's own attach failure for the traced pid itself,
/// which it prints unconditionally (`perror_msg`).
const STRACE_PTRACE_ERROR_MARKER: &str = "ptrace(";

/// A complete fixed set of run parameters for one configuration.
/// `repetition` and `attempt` are the indices the matrix orchestration
/// keeps across attempts (task 11).
#[derive(Debug, Clone)]
pub struct RunSpec {
    pub profile: Profile,
    pub server: ServerKind,
    pub workers: u32,
    pub qos: Option<u8>,
    pub kind: RunKind,
    pub rate_fraction_pct: Option<u32>,
    pub offered_rate: Option<u64>,
    pub repetition: u32,
    pub attempt: u32,
}

/// Inputs the runner reads from the host; every member is either a path,
/// a task-list string, or the `cancel` flag SIGINT/SIGTERM set.
#[derive(Debug, Clone)]
pub struct HostSetup {
    pub bench_exe: PathBuf,
    pub mosquitto_bin: Option<PathBuf>,
    pub syscalls: SyscallMethod,
    pub perf_bin: Option<PathBuf>,
    pub strace_bin: Option<PathBuf>,
    pub cpu_sets: CpuSets,
    pub env: Env,
    pub scratch_dir: PathBuf,
    /// The CPU pressure file the runner reads at both window samples;
    /// [`HOST_CPU_PRESSURE_PATH`] for `bench run`.
    pub cpu_pressure_path: PathBuf,
    pub cancel: Arc<AtomicBool>,
}

/// Spec constants keyed by profile; recorded-run values match §9.1, smoke
/// is a smaller matrix whose DoD does not enforce spread and stops at
/// attempt 1.
#[derive(Debug, Clone, Copy)]
pub struct ProtocolConstants {
    pub warmup_secs: u64,
    pub window_secs: u64,
    pub repetitions: u32,
    pub drain_grace_secs: u64,
    pub max_config_attempts: u32,
    pub enforce_spread: bool,
}

impl ProtocolConstants {
    /// Recorded: warmup 10 s, window 60 s, 5 repetitions, 5 s drain, up to
    /// 3 attempts, spread enforced. Smoke: warmup 2 s, window 5 s, 2
    /// repetitions, 2 s drain, 1 attempt, spread not enforced (matches the
    /// matrix task 13 sets up).
    pub fn for_profile(profile: Profile) -> Self {
        match profile {
            Profile::Recorded => Self {
                warmup_secs: 10,
                window_secs: 60,
                repetitions: 5,
                drain_grace_secs: 5,
                max_config_attempts: 3,
                enforce_spread: true,
            },
            Profile::Smoke => Self {
                warmup_secs: 2,
                window_secs: 5,
                repetitions: 2,
                drain_grace_secs: 2,
                max_config_attempts: 1,
                enforce_spread: false,
            },
        }
    }
}

/// Render the seven-line mosquitto config the runner hands the broker via
/// `-c`. The lines are the approved set (R2-Q4: unlimited queues and
/// `set_tcp_nodelay true`), and mosquitto exits during startup on a line
/// it does not know, which `run_one` records as a harness error.
pub fn mosquitto_conf(port: u16) -> String {
    let n = u32::from(port);
    format!(
        "listener {n} 127.0.0.1\n\
         allow_anonymous true\n\
         persistence false\n\
         max_queued_messages 0\n\
         max_inflight_messages 0\n\
         set_tcp_nodelay true\n\
         log_dest none\n"
    )
}

/// Build the [`Env`] for one run. `tools` is the list of paths whose
/// first non-blank `--version` line goes into `env.tools`; an empty slice is fine
/// for a smoke run that uses no perf/strace/mosquitto. A tool that cannot
/// be started, prints no non-blank line, or misses the
/// [`TOOL_VERSION_TIMEOUT_SECS`] deadline fails the whole capture, so no
/// run starts against an environment whose tools were never identified.
/// `git_sha` and `git_dirty` are read from the build-time env vars the
/// build.rs sets so the record names the source the binary was built from.
/// `cancel` is the flag SIGINT/SIGTERM set: a probe running when it is set
/// is refused with `cancelled`. `signal` sends each probe's cleanup
/// SIGKILLs ([`cleanup_probe`]): nix `kill` in production ([`capture_env`]);
/// a test hands in a stand-in to drive a refused signal.
pub fn capture_env_with<K: FnMut(Pid, Signal) -> nix::Result<()> + Clone + Send + 'static>(
    tools: &[PathBuf],
    cancel: &AtomicBool,
    signal: K,
) -> Result<Env, String> {
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map_err(|e| format!("read osrelease: {e}"))?;
    let host = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map_err(|e| format!("read hostname: {e}"))?;
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").ok();
    let cpu_model = cpuinfo
        .as_deref()
        .and_then(|s| s.lines().find(|l| l.starts_with("model name")))
        .and_then(|l| l.split(':').nth(1))
        .unwrap_or("")
        .trim()
        .to_string();
    let mut tool_pairs: Vec<(String, String)> = Vec::new();
    for tool in tools {
        // A probe that cannot answer fails the capture. `Env.tools` is the
        // record's only statement of which perf, strace or mosquitto build
        // produced its numbers, so a run must not start with a placeholder
        // there: the caller gets the cause and rejects the environment
        // instead of recording a version nobody read.
        let version = tool_version(tool, cancel, signal.clone())
            .map_err(|e| format!("{} --version: {e}", tool.display()))?;
        tool_pairs.push((tool.to_string_lossy().into_owned(), version));
    }
    Ok(Env {
        kernel: kernel.trim().to_string(),
        cpu_model,
        host: host.trim().to_string(),
        git_sha: env!("BENCH_SOURCE_SHA").to_string(),
        // Only a probe that proved the tree clean reports clean: the build
        // writes `unknown` when `git status` could not run, and a recorded
        // run must be refused rather than claim a provenance nobody read.
        git_dirty: env!("BENCH_SOURCE_DIRTY") != "false",
        tools: tool_pairs,
    })
}

/// [`capture_env_with`] with nix `kill` as each probe's cleanup signal.
pub fn capture_env(tools: &[PathBuf], cancel: &AtomicBool) -> Result<Env, String> {
    capture_env_with(tools, cancel, kill::<Signal>)
}

/// The first non-blank `--version` line of one tool, or the cause when the
/// probe could not produce one. The probe runs the way [`head_sha_with`]'s
/// does: started inside [`TOOL_VERSION_TIMEOUT_SECS`] by [`start_probe`],
/// in a process group of its own, refused with `cancelled` as soon as
/// `cancel` is set, and on every path ended by [`cleanup_probe`], whose
/// reap is bounded by [`SIGKILL_REAP_TIMEOUT_SECS`] — a probe still running
/// after that is reported with its pid and left to a reaper thread. Its
/// stdout is read through a [`TOOL_VERSION_STDOUT_BYTES_MAX`] cap. The
/// group kill also ends a background process the tool left holding its
/// stdout, so the reader thread ([`spawn_stdout_reader`]) reaches EOF. The
/// exit status is not read: a version is recorded from what the tool prints.
fn tool_version<K: FnMut(Pid, Signal) -> nix::Result<()> + Clone + Send + 'static>(
    tool: &Path,
    cancel: &AtomicBool,
    mut signal: K,
) -> Result<String, String> {
    let deadline = Instant::now() + Duration::from_secs(TOOL_VERSION_PROBE.timeout_secs);
    let mut command = Command::new(tool);
    command
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(PROBE_OWN_PROCESS_GROUP);
    let mut child = start_probe(TOOL_VERSION_PROBE, command, deadline, cancel, signal.clone())?;
    let result = match child.stdout.take() {
        Some(stdout) => spawn_stdout_reader(TOOL_VERSION_LABEL, stdout.take(TOOL_VERSION_STDOUT_BYTES_MAX), BlankLines::Skip)
            .and_then(|lines| recv_version_line(&lines, deadline, cancel)),
        None => Err("--version stdout not piped".to_string()),
    };
    match (result, cleanup_probe(TOOL_VERSION_PROBE, child, &mut signal)) {
        (result, Ok(())) => result,
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(refusal), Err(cleanup)) => Err(format!("{refusal}; {cleanup}")),
    }
}

/// The first non-blank line a `--version` probe prints, trimmed, bounded
/// by `deadline`. Blank and whitespace-only lines never reach this
/// receiver ([`BlankLines::Skip`]), so a probe that reaches EOF without a
/// non-blank line answered with no version, and `capture_env` must fail
/// rather than record an empty one. The deadline is checked before every
/// receive and the receive waits only the time left, so nothing that
/// arrives after expiry is reported as an answer. `cancel` is read before
/// the deadline on every step of at most [`WAIT_POLL_MS`], so a set flag is
/// answered with `cancelled`.
fn recv_version_line(
    lines: &mpsc::Receiver<StdoutLine>,
    deadline: Instant,
    cancel: &AtomicBool,
) -> Result<String, String> {
    loop {
        if cancel.load(Ordering::SeqCst) {
            return Err("cancelled".to_string());
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "--version did not answer within {TOOL_VERSION_TIMEOUT_SECS}s"
            ));
        }
        match lines.recv_timeout(remaining.min(Duration::from_millis(WAIT_POLL_MS))) {
            Ok(StdoutLine::Line(line)) => {
                let version = line.trim();
                if !version.is_empty() {
                    // Only a line that ends in a newline is whole: the capped
                    // reader reports EOF at TOOL_VERSION_STDOUT_BYTES_MAX, so a
                    // line the cap cuts — or one the tool left unterminated at
                    // EOF — arrives without one and is never a version.
                    if !line.ends_with('\n') {
                        return Err(format!(
                            "--version printed a line with no newline within {TOOL_VERSION_STDOUT_BYTES_MAX} bytes"
                        ));
                    }
                    return Ok(version.to_string());
                }
            }
            Ok(StdoutLine::Eof) => return Err("--version printed no non-blank line".to_string()),
            Ok(StdoutLine::Failed(e)) => return Err(format!("read --version stdout: {e}")),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err("--version stdout reader stopped".to_string());
            }
        }
    }
}

/// `<git> rev-parse --short HEAD` of the working tree the runner is about
/// to invoke, for [`check_provenance`]. The answer is
/// bounded by [`GIT_HEAD_TIMEOUT_SECS`] on every path, startup included: a
/// probe that has not started by then is given up on by [`start_probe`], and
/// one that has not exited by then, or by the time `cancel` is set, is refused
/// and sent SIGKILL, and the reap after that is bounded by
/// [`SIGKILL_REAP_TIMEOUT_SECS`]; a probe that outlives both is reported with
/// its pid and left to a reaper thread. A probe that finished starting as
/// [`start_probe`] gave up is cleaned up there on the same terms
/// ([`abandon_startup`]); only one still inside its own startup has no pid to
/// report, so the returned error names that condition instead
/// ([`abandoned_startup_error`]).
/// `git` is the executable to run: `git` from `PATH` in production. `cancel`
/// is the shared flag SIGINT/SIGTERM set; polling it every [`WAIT_POLL_MS`]
/// gives the operator a same-tick cancellation instead of waiting out the
/// deadline. Its stdout is read through a [`GIT_HEAD_STDOUT_BYTES_MAX`] cap,
/// so neither the probe's memory nor its deadline depends on how much the
/// executable prints. The probe runs in its own process group, so the cleanup
/// reaches the descendants of a `git` that is a wrapper script: a background
/// process the wrapper leaves behind holds the inherited stdout pipe open and
/// would survive a signal addressed to the wrapper's pid alone.
/// `signal` sends the two cleanup SIGKILLs ([`cleanup_probe`]): nix `kill` in
/// production ([`head_sha`]); a test hands in a stand-in to drive a refused
/// signal. It is cloned for [`start_probe`], which cleans up a probe that
/// finished starting as it gave up ([`abandon_startup`]) and hands its thread
/// the cleanup of one that starts later still.
pub fn head_sha_with<K: FnMut(Pid, Signal) -> nix::Result<()> + Clone + Send + 'static>(
    git: &Path,
    cancel: &AtomicBool,
    mut signal: K,
) -> Result<String, String> {
    let deadline = Instant::now() + Duration::from_secs(GIT_HEAD_PROBE.timeout_secs);
    let mut command = Command::new(git);
    command
        .args(["rev-parse", "--short", "HEAD"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .process_group(PROBE_OWN_PROCESS_GROUP);
    let mut child = start_probe(GIT_HEAD_PROBE, command, deadline, cancel, signal.clone())?;
    let mut status: Option<WaitStatus> = None;
    let result = match child.stdout.take() {
        Some(stdout) => spawn_stdout_reader(
            GIT_HEAD_LABEL,
            stdout.take(GIT_HEAD_STDOUT_BYTES_MAX),
            BlankLines::Skip,
        )
        .and_then(|lines| wait_for_head_sha(&mut child, &lines, deadline, cancel, &mut status)),
        None => Err("git rev-parse stdout not piped".to_string()),
    };
    match (result, cleanup_probe(GIT_HEAD_PROBE, child, &mut signal)) {
        (result, Ok(())) => result,
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(refusal), Err(cleanup)) => Err(format!("{refusal}; {cleanup}")),
    }
}

/// Start `probe`'s command inside `deadline`, polling `cancel` every
/// [`WAIT_POLL_MS`] while it starts. `Command::spawn` is synchronous and
/// blocks until the child has execed or reported its exec error, so a probe's
/// executable — or a `PATH` entry ahead of it — on a stalled filesystem would
/// hold the preflight with neither deadline nor cancellation in reach.
/// Startup therefore runs on its own thread and this function waits only the
/// time left. A rendezvous channel carries the probe, so a send that
/// completes proves the probe was received here: a queued one this function
/// never took would be dropped — neither killed nor reaped — when it
/// returns. Every give-up therefore takes one last non-blocking receive
/// ([`abandon_startup`]), because a `spawn` that finished while this function
/// was descheduled leaves its probe in the channel's waiting send, and that
/// probe has a pid this function can clean up itself. When startup finishes
/// after the give-up instead, the starting thread still owns the probe and
/// hands it to [`cleanup_probe`], which signals and reaps it exactly as the
/// caller's own cleanup would, and reports on stderr what it can no longer
/// return. That authority ends with this process: a startup that finishes
/// after the CLI exits leaves an unsignalled probe to init, which
/// [`abandoned_startup_error`] names in the returned error because no pid
/// exists to name.
fn start_probe<K: FnMut(Pid, Signal) -> nix::Result<()> + Clone + Send + 'static>(
    probe: Probe,
    mut command: Command,
    deadline: Instant,
    cancel: &AtomicBool,
    mut signal: K,
) -> Result<Child, String> {
    let (started_tx, started_rx) = mpsc::sync_channel(0);
    // The give-up path cleans up a probe the starting thread has already
    // handed over, so it needs a signal of its own.
    let mut give_up_signal = signal.clone();
    let starter = std::thread::Builder::new()
        .name(format!("bench-runner-{}-starter", probe.label))
        .spawn(move || {
            let spawned = command.spawn().map_err(|e| format!("{}: {e}", probe.command));
            if let Err(mpsc::SendError(Ok(child))) = started_tx.send(spawned) {
                // The caller gave up and can no longer be returned to, so
                // stderr is the only sink left for the surviving probe's pid
                // and its refused signals (Q5: the exception is reported).
                if let Err(e) = cleanup_probe(probe, child, &mut signal) {
                    eprintln!("{e}");
                }
            }
        });
    if let Err(e) = starter {
        return Err(format!("{} starter thread: {e}", probe.command));
    }
    loop {
        if cancel.load(Ordering::SeqCst) {
            return Err(abandon_startup(
                probe,
                &started_rx,
                "cancelled".to_string(),
                &mut give_up_signal,
            ));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(abandon_startup(
                probe,
                &started_rx,
                format!(
                    "{} did not answer within {}s",
                    probe.command, probe.timeout_secs
                ),
                &mut give_up_signal,
            ));
        }
        match started_rx.recv_timeout(remaining.min(Duration::from_millis(WAIT_POLL_MS))) {
            Ok(started) => return started,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(format!("{} starter stopped", probe.command));
            }
        }
    }
}

/// Give up on [`start_probe`]'s startup with `refusal`, cleaning up a probe
/// that did finish starting first.
///
/// The rendezvous send blocks until this side receives, so a `Command::spawn`
/// that returned while the caller was descheduled — the deadline or `cancel`
/// landing in that window — leaves a started, signalable probe waiting in the
/// channel. Q5's exception is only for a probe the runner cannot signal, so
/// this one is taken and given the same [`cleanup_probe`] the caller's own
/// path gives it, and a cleanup failure follows the refusal in the same
/// `refusal; cleanup` order [`head_sha_with`] uses. Nothing to receive means
/// startup is still inside `spawn`: that is the reported exception, named in
/// the error after the refusal ([`abandoned_startup_error`]). A received
/// `spawn` failure and a starting
/// thread that ended without sending both leave no probe to clean up, so the
/// refusal stands alone.
fn abandon_startup<K: FnMut(Pid, Signal) -> nix::Result<()>>(
    probe: Probe,
    started_rx: &mpsc::Receiver<Result<Child, String>>,
    refusal: String,
    signal: &mut K,
) -> String {
    match started_rx.try_recv() {
        Ok(Ok(child)) => match cleanup_probe(probe, child, signal) {
            Ok(()) => refusal,
            Err(cleanup) => format!("{refusal}; {cleanup}"),
        },
        Ok(Err(_)) | Err(mpsc::TryRecvError::Disconnected) => refusal,
        Err(mpsc::TryRecvError::Empty) => abandoned_startup_error(probe, refusal),
    }
}

/// Name in the returned error the startup [`start_probe`] is giving up on,
/// after `refusal` and in the same `refusal; context` order a cleanup
/// failure uses ([`head_sha_with`]), so AC-15's `did not answer within` and
/// AC-16's `cancelled` keep the front of the error the caller matches on.
///
/// `Command::spawn` has not returned, so there is no pid to name and nothing
/// here can signal what it may already have forked. The starting thread
/// cleans up whatever it receives, but only while this process lives: `main`
/// exits right after this error, which ends that thread and leaves a probe
/// that started late to init, unsignalled. That is Q5's exception reached
/// through startup instead of through a refused SIGKILL — every probe is a
/// read-only `git rev-parse` or `--version`, and the operator is told.
///
/// The condition travels in the error rather than through an `eprintln!` of
/// its own: a write to stderr blocks for as long as a reader that stopped
/// consuming a full pipe leaves it full, and panics when the write fails, so
/// reporting it here would put an unbounded external write — and a panic —
/// on the one path [`head_sha_with`] must return from within its deadline.
/// Presentation is the caller's: `main` prints the error it is returned
/// (`bench/src/main.rs`).
fn abandoned_startup_error(probe: Probe, refusal: String) -> String {
    format!(
        "{refusal}; {} gave up on a probe that had not finished \
         starting; a probe that starts after this is signalled only while \
         this process lives, and is otherwise left to init",
        probe.command
    )
}

/// SIGKILL the probe's group and its own pid with `signal`, then reap it
/// within [`SIGKILL_REAP_TIMEOUT_SECS`] ([`reap_probe`]). The child leads
/// the group it was spawned into, so its pid is the group id and the negated
/// pid addresses the group.
fn cleanup_probe<K: FnMut(Pid, Signal) -> nix::Result<()>>(
    probe: Probe,
    child: Child,
    signal: &mut K,
) -> Result<(), String> {
    let pid = i32::try_from(child.id()).ok().map(Pid::from_raw);
    let group = pid.map(|pid| Pid::from_raw(-pid.as_raw()));
    // SIGKILL the whole group, so a `git` wrapper's background descendant
    // goes with the wrapper instead of outliving the probe with the
    // inherited stdout pipe. Nothing before this reaps the leader — the exit
    // check is [`probe_exit`], which leaves it a zombie — so the leader
    // still holds its pid here, and with it the id of the group it leads.
    // The signal therefore cannot reach anything but this probe's own
    // group, whether the leader is running or already exited.
    let mut signal_failures = Vec::new();
    if let Some(group) = group {
        if let Err(e) = signal(group, Signal::SIGKILL) {
            signal_failures.push(format!("group SIGKILL: {e}"));
        }
    }
    // And the probe's own pid: an executable is free to call `setpgid` and
    // join another group, and the group signal above then names a group
    // without the probe in it — an empty group answers `ESRCH`. An unreaped
    // child still owns its pid, so this signal can no more reach a stranger
    // than the group signal can, and a probe that already exited is a
    // zombie, which SIGKILL leaves for the reap. Neither signal is sure to
    // land: a probe that moved its real and saved UID away refuses both
    // with `EPERM`, so [`reap_probe`] bounds the reap on its own.
    if let Some(pid) = pid {
        if let Err(e) = signal(pid, Signal::SIGKILL) {
            signal_failures.push(format!("pid SIGKILL: {e}"));
        }
    }
    // After the signals, never before: the reap frees the leader's pid, so
    // no signal may follow it.
    reap_probe(probe, child, &signal_failures)
}

/// [`head_sha_with`] with nix `kill` as its cleanup signal: the provenance
/// probe `bench run --profile recorded` runs before any record exists.
pub fn head_sha(git: &Path, cancel: &AtomicBool) -> Result<String, String> {
    head_sha_with(git, cancel, kill::<Signal>)
}

/// Hand `child` to a detached thread named `bench-runner-<label>-reaper`
/// that owns its one `wait` and never signals it, so a child the runner
/// gave up on is reaped whenever it exits instead of staying a zombie.
/// `Err` is the thread start failure; the child is then dropped unreaped.
fn spawn_reaper(label: &str, mut child: Child) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name(format!("bench-runner-{label}-reaper"))
        .spawn(move || {
            let _ = child.wait();
        })
        .map(drop)
}

/// Reap a probe after its cleanup signals ([`cleanup_probe`]), bounded by
/// [`SIGKILL_REAP_TIMEOUT_SECS`]. The exit is read with [`probe_exit`], which
/// leaves the probe a zombie, and only an exited probe is reaped here, so
/// this `child.wait()` never blocks. A probe still running at the bound —
/// one its signals could not reach — is handed with its one reap to a
/// detached thread that collects it whenever it exits and never signals
/// it; the error names the probe's pid, the bound and every signal in
/// `signal_failures`. When the runner exits right after the error, the
/// thread ends with it and init reaps the probe instead; if the thread cannot
/// start, the probe stays unreaped until the runner exits and the error
/// says so.
fn reap_probe(probe: Probe, mut child: Child, signal_failures: &[String]) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(SIGKILL_REAP_TIMEOUT_SECS);
    let pid = child.id();
    let failure = loop {
        match probe_exit(&child) {
            Ok(Some(_)) => {
                let _ = child.wait();
                return Ok(());
            }
            Ok(None) => {}
            Err(e) => break format!("probe {pid} exit check failed: {e}"),
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break format!("probe {pid} still running {SIGKILL_REAP_TIMEOUT_SECS}s after SIGKILL");
        }
        sleep(remaining.min(Duration::from_millis(WAIT_POLL_MS)));
    };
    let mut error = format!("{} cleanup: {failure}", probe.command);
    for signal_failure in signal_failures {
        error.push_str("; ");
        error.push_str(signal_failure);
    }
    if let Err(e) = spawn_reaper(probe.label, child) {
        let _ = write!(error, "; reaper thread: {e}");
    }
    Err(error)
}

/// Wait for the `git rev-parse` child to print one line, bounded by
/// `deadline`, polling `cancel` every [`WAIT_POLL_MS`] steps and keeping
/// the first `WaitStatus` from [`probe_exit`] without calling it again. The
/// first non-blank line the receiver reads that ends in a newline, trimmed,
/// is the SHA — a line without its newline is one the stdout cap cut, or the
/// tail of a stream that ended mid-line, and is never an answer; EOF
/// with no line is a failed probe; a printed line on a non-zero exit is
/// overwritten by the exit error. The deadline is checked before the exit
/// status and the line, so a probe that answers only past the bound is
/// refused with `did not answer within` rather than accepted.
fn wait_for_head_sha(
    child: &mut Child,
    lines: &mpsc::Receiver<StdoutLine>,
    deadline: Instant,
    cancel: &AtomicBool,
    status: &mut Option<WaitStatus>,
) -> Result<String, String> {
    let mut first: Option<String> = None;
    let mut eof = false;
    loop {
        if cancel.load(Ordering::SeqCst) {
            return Err("cancelled".to_string());
        }
        // Before the exit status and the line, so a probe first observed
        // past the deadline is refused instead of accepted: the child can
        // print its SHA, keep running past the bound and exit while this
        // loop is off the CPU.
        if deadline.saturating_duration_since(Instant::now()).is_zero() {
            return Err(format!(
                "git rev-parse did not answer within {GIT_HEAD_TIMEOUT_SECS}s"
            ));
        }
        if status.is_none() {
            match probe_exit(child) {
                Ok(Some(s)) => *status = Some(s),
                Ok(None) => {}
                Err(e) => return Err(format!("git rev-parse: {e}")),
            }
        }
        if let Some(s) = status.as_ref() {
            if !matches!(s, WaitStatus::Exited(_, 0)) {
                return Err(format!("git rev-parse exited with {}", probe_exit_note(*s)));
            }
            if let Some(sha) = first.as_ref() {
                return Ok(sha.clone());
            }
            if eof {
                return Err("git rev-parse printed no line".to_string());
            }
            // Status arrived (success) but neither the SHA line nor EOF
            // has been seen yet — `probe_exit` saw the exit before the
            // reader got either the Line or the Eof. Fall through to the
            // recv path so the next iteration settles one way or the
            // other, instead of tight-looping on this status.
        }
        // A poll step that the deadline cuts to zero returns at once and the
        // check at the top of the loop reports the expiry.
        let poll = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(WAIT_POLL_MS));
        if eof {
            sleep(poll);
            continue;
        }
        match lines.recv_timeout(poll) {
            Ok(StdoutLine::Line(line)) => {
                // Only a line that ends in a newline is whole: the capped reader
                // reports EOF at GIT_HEAD_STDOUT_BYTES_MAX, so a line the cap cuts
                // arrives without one and must not be answered as a SHA.
                if first.is_none() && line.ends_with('\n') {
                    let trimmed = line.trim().to_string();
                    if !trimmed.is_empty() {
                        first = Some(trimmed);
                    }
                }
            }
            Ok(StdoutLine::Eof) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                eof = true;
            }
            Ok(StdoutLine::Failed(e)) => return Err(format!("read git rev-parse stdout: {e}")),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
}

/// Non-reaping exit check of a probe: `waitid` with `WNOWAIT`
/// reports the exit and leaves the probe a zombie, so the probe keeps its
/// pid — and the group it leads keeps its id — until [`head_sha`] has
/// signalled the group and reaped the leader itself. `Child::try_wait`
/// reaps instead, and a reaped leader whose group holds no surviving member
/// frees the group id for reuse: the cleanup signal could then reach an
/// unrelated group that took the id over. `Ok(None)` is a probe that is
/// still running.
fn probe_exit(child: &Child) -> Result<Option<WaitStatus>, String> {
    let pid = i32::try_from(child.id()).map_err(|e| e.to_string())?;
    match waitid(
        Id::Pid(Pid::from_raw(pid)),
        WaitPidFlag::WEXITED | WaitPidFlag::WNOWAIT | WaitPidFlag::WNOHANG,
    ) {
        Ok(WaitStatus::StillAlive) => Ok(None),
        Ok(status) => Ok(Some(status)),
        Err(e) => Err(e.to_string()),
    }
}

/// Describe the exit of [`head_sha`]'s probe the way [`exit_note`]
/// describes a guarded child's: its code when it exited on its own, its
/// signal when a signal ended it. Only those two states reach this note —
/// [`probe_exit`] asks for `WEXITED` alone.
fn probe_exit_note(status: WaitStatus) -> String {
    match status {
        WaitStatus::Exited(_, code) => format!("code {code}"),
        WaitStatus::Signaled(_, signal, _) => format!("signal {signal}"),
        other => format!("{other:?}"),
    }
}

/// Refuse a recorded-run binary whose source and checkout disagree (AC-57).
/// `build_sha`/`build_dirty` are read from the binary's own env vars; `head_sha`
/// is the `git rev-parse --short HEAD` of the working tree the runner is
/// about to invoke.
pub fn check_provenance(build_sha: &str, build_dirty: bool, head_sha: &str) -> Result<(), String> {
    if build_dirty {
        return Err(
            "bench was built from a dirty tree; commit and rebuild".to_string(),
        );
    }
    if build_sha != head_sha {
        return Err(format!(
            "bench was built from {build_sha}, checkout is at {head_sha}; rebuild"
        ));
    }
    Ok(())
}

/// Owns every child process `run_one` spawns. On every exit path — early
/// return, unwind, panic — the guard ends anything still running with
/// [`end_child`]: SIGKILL, a reap bounded by [`GUARD_SIGKILL_REAP_TIMEOUT_SECS`],
/// and a reaper thread for a child that outlives it. [`run_one`] records
/// such a child in its record; `Drop` reports it on stderr.
struct ChildGuard {
    children: Vec<(&'static str, Child)>,
    /// Sends the cleanup SIGKILL: nix `kill` in production ([`run_one`]);
    /// a test hands in a stand-in to drive a refused signal.
    signal: fn(Pid, Signal) -> nix::Result<()>,
}

impl ChildGuard {
    /// A guard whose cleanup SIGKILL goes through `signal`; a test hands in
    /// a stand-in to drive a refused signal.
    fn with_signal(signal: fn(Pid, Signal) -> nix::Result<()>) -> Self {
        Self {
            children: Vec::new(),
            signal,
        }
    }

    fn push(&mut self, name: &'static str, child: Child) {
        self.children.push((name, child));
    }

    fn is_cancelled(host: &HostSetup) -> bool {
        host.cancel.load(Ordering::Relaxed)
    }

    /// Mutable handle on the child pushed under `name`, if present. Every
    /// child is owned by the guard from the moment it exists, so the runner
    /// reads and signals it through this accessor instead of holding a
    /// second, unguarded [`Child`].
    fn child_mut(&mut self, name: &str) -> Option<&mut Child> {
        self.children
            .iter_mut()
            .find(|(n, _)| *n == name)
            .map(|(_, child)| child)
    }

    /// End every held child with [`end_child`] and empty the guard. Returns
    /// one error per child it could not end, in push order.
    fn end_all(&mut self) -> Vec<String> {
        let signal = self.signal;
        self.children
            .drain(..)
            .filter_map(|(name, child)| end_child(name, child, signal).err())
            .collect()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        // `run_one_with` empties the guard itself, so only an unwind leaves
        // children here. Every child is ended before anything is written, and
        // the report waits on stderr for a bounded time only: Drop cannot
        // return the failure, and must not hang behind a stalled reader.
        report_within(
            self.end_all(),
            std::io::stderr(),
            Duration::from_secs(GUARD_REPORT_TIMEOUT_SECS),
        );
    }
}

/// Write one line per report to `out` on a writer thread, and wait for
/// that thread at most `bound`: `ChildGuard::drop` must not hang behind
/// a stalled reader. A line not written when `bound` ends is best effort
/// and is lost if the process exits first; a thread that cannot start
/// writes nothing.
fn report_within<W: Write + Send + 'static>(reports: Vec<String>, mut out: W, bound: Duration) {
    if reports.is_empty() {
        return;
    }
    let (done_tx, done_rx) = mpsc::channel();
    let writer = std::thread::Builder::new()
        .name("bench-runner-guard-report".to_string())
        .spawn(move || {
            for report in &reports {
                let _ = writeln!(out, "{report}");
            }
            let _ = done_tx.send(());
        });
    if writer.is_ok() {
        let _ = done_rx.recv_timeout(bound);
    }
}

/// SIGKILL a guard child that std has not reaped, with `signal`, and wait
/// up to [`GUARD_SIGKILL_REAP_TIMEOUT_SECS`] for it to exit. The exit check is
/// `Child::try_wait`, not [`probe_exit`]: the runner reaps guard children
/// through std elsewhere (`run_guarded`'s generator wait, [`terminate_and_wait`],
/// [`wait_for_counter_exit`]), and `try_wait` answers those from std's own
/// record where `waitid` would ask about a pid that is no longer this
/// child's. A child std has reaped is never signalled. A guard child leads
/// no process group, so only its pid is signalled. `Err` names `label`, the
/// pid, the bound and a refused SIGKILL; the child is then still unreaped
/// and the caller decides what happens to it.
fn kill_within_bound<K: FnMut(Pid, Signal) -> nix::Result<()>>(
    label: &str,
    child: &mut Child,
    mut signal: K,
) -> Result<(), String> {
    let pid = child.id();
    match child.try_wait() {
        Ok(Some(_)) => return Ok(()),
        Ok(None) => {}
        Err(e) => {
            return Err(format!(
                "{label} cleanup: pid {pid} (start {SURVIVOR_START_UNKNOWN}) exit check failed: {e}"
            ))
        }
    }
    // Read while the child is running and unreaped, so the start time
    // is its own: with the pid it names this process to a later
    // `--resume` ([`survivor_resume_refusal`]).
    let started = proc_start_time(pid)
        .map_or_else(|| SURVIVOR_START_UNKNOWN.to_string(), |ticks| ticks.to_string());
    // Only after a `try_wait` that found the child running: an unreaped
    // child keeps its pid, so this cannot reach a stranger even if the
    // child exits in between.
    let refused = i32::try_from(pid)
        .ok()
        .map(Pid::from_raw)
        .and_then(|target| signal(target, Signal::SIGKILL).err());
    let deadline = Instant::now() + Duration::from_secs(GUARD_SIGKILL_REAP_TIMEOUT_SECS);
    let mut error = loop {
        match child.try_wait() {
            Ok(Some(_)) => return Ok(()),
            Ok(None) => {}
            Err(e) => break format!("{label} cleanup: pid {pid} (start {started}) exit check failed: {e}"),
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break format!(
                "{label} cleanup: pid {pid} (start {started}) still running {GUARD_SIGKILL_REAP_TIMEOUT_SECS}s after SIGKILL"
            );
        }
        sleep(remaining.min(Duration::from_millis(WAIT_POLL_MS)));
    };
    if let Some(e) = refused {
        let _ = write!(error, "; SIGKILL: {e}");
    }
    Err(error)
}

/// End a guard child for good: [`kill_within_bound`], and a child still
/// running after it is handed to a reaper thread ([`spawn_reaper`]) that
/// owns its one `wait` and never signals it, so the long-lived runner
/// collects it whenever it exits instead of keeping a zombie. `Err` is
/// [`kill_within_bound`]'s, followed by `; reaper thread: <e>` when that
/// thread cannot start.
fn end_child<K: FnMut(Pid, Signal) -> nix::Result<()>>(
    label: &str,
    mut child: Child,
    signal: K,
) -> Result<(), String> {
    let Err(mut error) = kill_within_bound(label, &mut child, signal) else {
        return Ok(());
    };
    if let Err(e) = spawn_reaper(label, child) {
        let _ = write!(error, "; reaper thread: {e}");
    }
    Err(error)
}

/// Read the kernel's per-process `utime`/`stime` ticks (`/proc/<pid>/stat`,
/// fields 14 and 15 of the second half of the line). The comm field
/// (process name) is wrapped in `(...)` and may itself contain spaces, so
/// we look for the matching parens before splitting on whitespace.
fn read_cpu_ticks(pid: u32) -> Result<(u64, u64), String> {
    let path = format!("/proc/{pid}/stat");
    let body = std::fs::read_to_string(&path).map_err(|e| format!("read {path}: {e}"))?;
    let close = body
        .rfind(')')
        .ok_or_else(|| format!("{path} missing comm field"))?;
    let after = &body[close + 1..];
    let fields: Vec<&str> = after.split_whitespace().collect();
    // After the `)` that ends `comm`, the split gives `state` first
    // (proc field 3). Counting forward, utime is proc field 14 — the
    // 12th token after `)`. stime is the next one.
    let utime = fields
        .get(11)
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| format!("{path} missing utime"))?;
    let stime = fields
        .get(12)
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| format!("{path} missing stime"))?;
    Ok((utime, stime))
}

/// The `some` line's `total=` counter of the pressure file at `path`, in
/// microseconds. `None` when the file cannot be read or holds no such
/// counter: host pressure is evidence, so its absence is never a harness
/// error.
fn read_cpu_pressure_total(path: &Path) -> Option<u64> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|text| parse_cpu_pressure_some_total(&text))
}

/// Start time of `pid` in clock ticks since boot (`/proc/<pid>/stat` field
/// 22). `Ok(None)` is the one answer that proves no process holds the pid:
/// its `/proc` entry is gone. Every other failure is `Err`, because a live
/// process whose stat cannot be read or parsed must not be mistaken for an
/// ended one ([`survivor_resume_refusal`]). The line is parsed as bytes
/// rather than decoded: a process can set its own name to invalid UTF-8
/// while keeping its start time, and a whole-file decode would then fail on
/// a process that is still running. Two processes that reuse one pid differ
/// in this field, so a pid with its start time names one process.
fn read_proc_start_time(pid: u32) -> Result<Option<u64>, String> {
    start_time_in(&format!("/proc/{pid}/stat"))
}

/// [`read_proc_start_time`] over one `/proc/<pid>/stat` path, so the
/// parser can be driven with a stat line a test writes itself.
fn start_time_in(path: &str) -> Result<Option<u64>, String> {
    let body = match std::fs::read(path) {
        Ok(body) => body,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("read {path}: {e}")),
    };
    // `comm` is parenthesised and may itself contain spaces and
    // brackets, so the fields are counted from the last `)`.
    let close = body
        .iter()
        .rposition(|b| *b == b')')
        .ok_or_else(|| format!("{path} missing comm field"))?;
    let field = body[close + 1..]
        .split(u8::is_ascii_whitespace)
        .filter(|field| !field.is_empty())
        .nth(PROC_STAT_START_TIME_FIELD)
        .ok_or_else(|| format!("{path} missing start time"))?;
    std::str::from_utf8(field)
        .ok()
        .and_then(|field| field.parse().ok())
        .map(Some)
        .ok_or_else(|| format!("{path} start time does not parse"))
}

/// Start time of `pid` in clock ticks since boot, or `None` when it cannot
/// be read. [`kill_within_bound`] records it beside the pid, where an
/// unreadable start time is the [`SURVIVOR_START_UNKNOWN`] marker.
pub fn proc_start_time(pid: u32) -> Option<u64> {
    read_proc_start_time(pid).ok().flatten()
}

/// Why the child a [`CLEANUP_SURVIVOR_PREFIX`] entry names must stop a
/// `--resume`, or `None` when no process holds its identity. The entry
/// carries `pid <pid> (start <ticks>)` from [`kill_within_bound`]: a
/// process that holds that pid with that start time is that child. With
/// the start time unknown ([`SURVIVOR_START_UNKNOWN`]) any holder of the
/// pid counts. An entry not in that shape, and a pid whose identity cannot
/// be read, both refuse too: a resume beside a child it cannot rule out
/// would be measured beside it.
pub fn survivor_resume_refusal(entry: &str) -> Option<String> {
    let Some((pid, start)) = survivor_identity(entry) else {
        return Some(
            "that entry does not name a pid and a start time, so that process cannot be ruled out"
                .to_string(),
        );
    };
    match read_proc_start_time(pid) {
        Ok(Some(found)) if start.is_some_and(|start| start != found) => None,
        Ok(Some(_)) => Some("that process is still running".to_string()),
        Ok(None) => None,
        Err(e) => Some(format!(
            "pid {pid} could not be checked ({e}), so that process cannot be ruled out"
        )),
    }
}

/// The pid and, when recorded, the start time a survivor entry names.
/// Only [`SURVIVOR_START_UNKNOWN`] is the unknown start time: any other
/// token that does not parse makes the whole entry malformed, which
/// [`survivor_resume_refusal`] counts as still running.
fn survivor_identity(entry: &str) -> Option<(u32, Option<u64>)> {
    let (_, rest) = entry.split_once(" pid ")?;
    let (pid, rest) = rest.split_once(" (start ")?;
    let (start, _) = rest.split_once(')')?;
    let start = match start {
        SURVIVOR_START_UNKNOWN => None,
        ticks => Some(ticks.parse().ok()?),
    };
    Some((pid.parse().ok()?, start))
}

/// Count the traced process's threads (`/proc/<pid>/task` entries). This
/// is the number strace's attach has to cover: strace enumerates the same
/// directory, reports a per-thread `PTRACE_SEIZE` failure only through
/// `debug_perror_msg` (silent without `-d`), and still prints its
/// "attached with <n> threads" line with the reduced `n`. The count is
/// therefore the only evidence a counting run has that every worker was
/// traced.
fn read_thread_count(pid: u32) -> Result<u64, String> {
    let path = format!("/proc/{pid}/task");
    let entries = std::fs::read_dir(&path).map_err(|e| format!("read {path}: {e}"))?;
    let mut count: u64 = 0;
    for entry in entries {
        entry.map_err(|e| format!("read {path}: {e}"))?;
        count += 1;
    }
    Ok(count)
}

fn started_at_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Compute the on-disk directory stem for one configuration. Mirrors
/// `RunRecord::file_name()` but stripped of the `.json` extension.
/// `matrix::run_configs` uses it to find the files an attempt number
/// already owns.
pub fn stem_for(spec: &RunSpec) -> String {
    build_minimal_record(spec).file_name().replace(".json", "")
}

/// Build a minimal [`RunRecord`] with the fields [`RunRecord::file_name`]
/// reads. The rest of the fields are placeholders the helpers will fill.
fn build_minimal_record(spec: &RunSpec) -> RunRecord {
    RunRecord {
        schema: SCHEMA_VERSION,
        profile: spec.profile,
        server: spec.server,
        workers: spec.workers,
        qos: spec.qos,
        kind: spec.kind,
        rate_fraction_pct: spec.rate_fraction_pct,
        offered_rate: spec.offered_rate,
        repetition: spec.repetition,
        attempt: spec.attempt,
        connections: 0,
        payload_bytes: 0,
        qos1_window: 0,
        warmup_secs: 0,
        window_secs: 0,
        drain_grace_secs: 0,
        build: Build::Release,
        origin_ns: 0,
        window: Window {
            start_ns: 0,
            end_ns: 0,
        },
        timeline: crate::record::Timeline {
            origin_sec: 0,
            counts: Vec::new(),
        },
        cycles_in_window: None,
        counts: Counts {
            scheduled: None,
            window_scheduled: None,
            offered: 0,
            acked: None,
            delivered: 0,
            dropped: None,
            unresolved: 0,
            duplicates: 0,
            foreign: 0,
            latency_over_range: 0,
            clock_anomalies: 0,
        },
        throughput_msgs_per_sec: None,
        last10s_msgs_per_sec: None,
        latency_ns: None,
        lateness: Vec::new(),
        lateness_p99_ns: None,
        last_write_start_ns: None,
        connections_per_sec: None,
        generator_errors: GeneratorErrors {
            connect: 0,
            protocol: 0,
            missed_slots: 0,
            stalled: 0,
            lateness_over_range: 0,
        },
        syscalls: Syscalls {
            method: SyscallMethod::None,
            count: None,
            per_msg: None,
            raw: None,
            enabled_ns: None,
            disabled_ns: None,
            exit: None,
            diagnostics: None,
        },
        driver: None,
        cpu_sets: CpuSets {
            server: String::new(),
            generator: String::new(),
        },
        env: Env {
            kernel: String::new(),
            cpu_model: String::new(),
            host: String::new(),
            git_sha: String::new(),
            git_dirty: false,
            tools: Vec::new(),
        },
        cpu: None,
        host_cpu_pressure: None,
        started_at_unix: 0,
        harness_errors: Vec::new(),
        valid: false,
        invalid_reasons: Vec::new(),
        cancelled: false,
    }
}

/// What a child's stdout reader thread forwards: one line, the end of the
/// stream, or the error that ended it.
#[derive(Debug, PartialEq, Eq)]
enum StdoutLine {
    Line(String),
    Eof,
    Failed(String),
}

/// Whether a stdout reader forwards whitespace-only lines. A receiver that
/// discards them anyway asks for [`BlankLines::Skip`], so a child printing
/// whitespace faster than the receiver reads cannot grow the reader's
/// unbounded channel; a receiver matching exact lines keeps them.
enum BlankLines {
    Keep,
    Skip,
}

/// Why [`drain_stderr_lines`] stopped, when it stopped for a reason the
/// record has to name. EOF, a read failure (named from [`StderrLoss`]), a
/// cancelled run and a disconnected reader carry no member here.
enum DrainEnd {
    /// [`COUNTER_DRAIN_TIMEOUT_SECS`] passed with the stream still open.
    TimedOut,
}

/// What the strace stderr reader thread could not hand over through its
/// bounded queue, recorded the moment it happened: whether it dropped a
/// line at a full queue, and the failure that ended its reading.
/// [`finalize_counter`] reads it after the drain, so neither a full queue
/// nor a drain that stopped early can erase either.
#[derive(Default)]
struct StderrLoss {
    dropped: bool,
    failure: Option<String>,
}

/// Read a child's stdout on its own thread. A blocking `read_line` on the
/// runner's thread would hold the runner past its own deadline when the
/// child keeps stdout open without printing a newline, so the deadline and
/// the cancellation flag are checked by the receiving loop instead. The
/// channel is unbounded: a caller whose child may print more than it reads
/// hands in a capped reader (`Read::take`) so peak memory stays bounded.
fn spawn_stdout_reader<R: Read + Send + 'static>(
    label: &'static str,
    stdout: R,
    blanks: BlankLines,
) -> Result<mpsc::Receiver<StdoutLine>, String> {
    let (tx, rx) = mpsc::channel();
    std::thread::Builder::new()
        .name(format!("bench-runner-{label}"))
        .spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            loop {
                line.clear();
                let message = match reader.read_line(&mut line) {
                    Ok(0) => StdoutLine::Eof,
                    // Dropped here rather than at the receiver: queueing a
                    // line the receiver discards makes peak memory depend
                    // on how fast the child prints.
                    Ok(_) if matches!(blanks, BlankLines::Skip) && line.trim().is_empty() => {
                        continue
                    }
                    Ok(_) => StdoutLine::Line(line.clone()),
                    Err(e) => StdoutLine::Failed(e.to_string()),
                };
                let last = !matches!(message, StdoutLine::Line(_));
                if tx.send(message).is_err() || last {
                    break;
                }
            }
        })
        // A host that cannot start another thread is a harness failure, not
        // a panic: `run_one` owes the matrix a record for every spec.
        .map_err(|e| format!("spawn {label} stdout reader: {e}"))?;
    Ok(rx)
}

/// Wait for an exact line on the handshake child's stdout, polling in
/// [`WAIT_POLL_MS`] steps until the deadline, the line arrives, or the
/// child closes stdout.
fn wait_for_line(
    host: &HostSetup,
    guards: &mut ChildGuard,
    handshake: &mut Handshake,
    deadline: Instant,
    expect: &str,
) -> Result<(), String> {
    let label = handshake.label;
    loop {
        match handshake
            .lines
            .recv_timeout(Duration::from_millis(WAIT_POLL_MS))
        {
            Ok(StdoutLine::Line(line)) => {
                if line.trim_end() == expect {
                    return Ok(());
                }
            }
            Ok(StdoutLine::Eof) => {
                let status = guards
                    .child_mut(label)
                    .and_then(|child| child.try_wait().ok().flatten());
                return Err(format!(
                    "{expect:?}: {label} closed stdout before printing it (exit {status:?})"
                ));
            }
            Ok(StdoutLine::Failed(e)) => return Err(format!("read {label} stdout: {e}")),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(format!("{label} stdout reader stopped"));
            }
        }
        if ChildGuard::is_cancelled(host) {
            return Err("cancelled".to_string());
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for {expect:?}"));
        }
    }
}

/// `ARM <origin> <window start> <window end>`: the recorder measures
/// latency over the window the record declares, so the start it is armed
/// with is the measurement-window start, not the origin. Arming it with
/// the origin would fold the warmup's samples into the histogram and make
/// its total disagree with `window_scheduled`.
fn send_arm(
    stdin: &mut impl Write,
    origin_ns: u64,
    window_start_ns: u64,
    window_end_ns: u64,
) -> std::io::Result<()> {
    let line = format!("ARM {origin_ns} {window_start_ns} {window_end_ns}\n");
    stdin.write_all(line.as_bytes())?;
    stdin.flush()
}

/// The stdin/stdout pair of the child whose `READY`/`ARMED` handshake the
/// runner drives. The [`Child`] itself stays in the [`ChildGuard`]; `label`
/// names it there so a handshake failure can report its exit status.
struct Handshake {
    label: &'static str,
    stdin: std::process::ChildStdin,
    lines: mpsc::Receiver<StdoutLine>,
}

/// What `spawn_endpoint` leaves its caller once every process it spawned is
/// owned by the [`ChildGuard`]: the broker PID the driver proof and the CPU
/// samples read, and the handshake of the `bench` child when there is one.
struct EndpointHandles {
    server_pid: u32,
    /// `None` for mosquitto/connections (no `bench server`/`bench sub`, so
    /// no handshake).
    handshake: Option<Handshake>,
}

/// Take the piped stdin/stdout of the guarded child `label` and start its
/// stdout reader thread.
fn take_handshake(guards: &mut ChildGuard, label: &'static str) -> Result<Handshake, String> {
    let child = guards
        .child_mut(label)
        .ok_or_else(|| format!("{label} is not guarded"))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| format!("{label} stdin not piped"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| format!("{label} stdout not piped"))?;
    Ok(Handshake {
        label,
        stdin,
        lines: spawn_stdout_reader(label, stdout, BlankLines::Keep)?,
    })
}

/// Where a child's stderr goes: `<scratch>/<label>.stderr`, so a startup
/// rejection — a `taskset` CPU set the host does not have, a mosquitto
/// option the packaged build does not know — keeps its cause next to the
/// run's reports instead of being written to `/dev/null`. A scratch
/// directory that cannot hold the file falls back to `/dev/null` rather
/// than failing the run for a diagnostic.
fn stderr_file(scratch: &Path, label: &str) -> Stdio {
    match std::fs::File::create(scratch.join(format!("{label}.stderr"))) {
        Ok(file) => Stdio::from(file),
        Err(_) => Stdio::null(),
    }
}

/// Spawn the endpoint process for a configuration, pushing it into `guards`
/// as soon as it exists so no later failure can leave it running. Returns
/// the broker PID and, for a `bench` endpoint, the handshake the caller
/// drives. For mosquitto+throughput/latency this also spawns the
/// bench-side subscriber; for mosquitto+connections there is none.
fn spawn_endpoint(
    spec: &RunSpec,
    host: &HostSetup,
    guards: &mut ChildGuard,
    scratch: &Path,
    port: u16,
    mosquitto_conf_path: Option<&Path>,
) -> Result<EndpointHandles, String> {
    let addr = format!("127.0.0.1:{port}");
    let conns = CONNECTIONS;
    match spec.server {
        ServerKind::Uring | ServerKind::Tokio => {
            let imp = match spec.server {
                ServerKind::Uring => "uring",
                ServerKind::Tokio => "tokio",
                ServerKind::Mosquitto => unreachable!("filtered by outer match"),
            };
            let endpoint_path = scratch.join("endpoint.json");
            let mut cmd = Command::new("taskset");
            cmd.arg("-c")
                .arg(&host.cpu_sets.server)
                .arg(&host.bench_exe)
                .arg("server")
                .arg("--impl")
                .arg(imp)
                .arg("--workers")
                .arg(spec.workers.to_string())
                .arg("--addr")
                .arg(&addr)
                .arg("--connections")
                .arg(conns.to_string())
                .arg("--report")
                .arg(&endpoint_path)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(stderr_file(scratch, ENDPOINT_LABEL));
            let child = cmd
                .spawn()
                .map_err(|e| format!("spawn endpoint: {e}"))?;
            let server_pid = child.id();
            guards.push(ENDPOINT_LABEL, child);
            let handshake = take_handshake(guards, ENDPOINT_LABEL)?;
            Ok(EndpointHandles {
                server_pid,
                handshake: Some(handshake),
            })
        }
        ServerKind::Mosquitto => {
            let conf = mosquitto_conf_path
                .ok_or_else(|| "mosquitto run needs a conf path".to_string())?;
            let mosquitto = host
                .mosquitto_bin
                .as_ref()
                .ok_or_else(|| "no mosquitto_bin".to_string())?;
            let mut cmd = Command::new("taskset");
            cmd.arg("-c")
                .arg(&host.cpu_sets.server)
                .arg(mosquitto)
                .arg("-c")
                .arg(conf)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(stderr_file(scratch, ENDPOINT_LABEL));
            let server = cmd
                .spawn()
                .map_err(|e| format!("spawn mosquitto: {e}"))?;
            let server_pid = server.id();
            guards.push(ENDPOINT_LABEL, server);
            // mosquitto opens the listener lazily; poll until it accepts.
            let deadline = Instant::now() + Duration::from_secs(STARTUP_TIMEOUT_SECS);
            while Instant::now() < deadline {
                if std::net::TcpStream::connect(&addr).is_ok() {
                    break;
                }
                if ChildGuard::is_cancelled(host) {
                    return Err("cancelled".to_string());
                }
                sleep(Duration::from_millis(WAIT_POLL_MS));
            }
            if Instant::now() >= deadline {
                return Err("mosquitto did not accept within the startup window".to_string());
            }
            if spec.kind == RunKind::Connections {
                return Ok(EndpointHandles {
                    server_pid,
                    handshake: None,
                });
            }
            // throughput / latency spawn a `bench sub`.
            let qos = spec.qos.unwrap_or(0);
            let sub_report = scratch.join("endpoint.json");
            let mut cmd = Command::new("taskset");
            cmd.arg("-c")
                .arg(&host.cpu_sets.server)
                .arg(&host.bench_exe)
                .arg("sub")
                .arg("--addr")
                .arg(&addr)
                .arg("--qos")
                .arg(qos.to_string())
                .arg("--connections")
                .arg(conns.to_string())
                .arg("--report")
                .arg(&sub_report)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(stderr_file(scratch, SUB_LABEL));
            let sub_child = cmd.spawn().map_err(|e| format!("spawn sub: {e}"))?;
            guards.push(SUB_LABEL, sub_child);
            let handshake = take_handshake(guards, SUB_LABEL)?;
            Ok(EndpointHandles {
                server_pid,
                handshake: Some(handshake),
            })
        }
    }
}

fn spawn_generator(
    spec: &RunSpec,
    host: &HostSetup,
    scratch: &Path,
    origin_ns: u64,
    window_start_ns: u64,
    window_end_ns: u64,
    drain_grace_secs: u64,
    port: u16,
) -> Result<Child, String> {
    let addr = format!("127.0.0.1:{port}");
    let conns = CONNECTIONS;
    let qos = spec.qos.unwrap_or(0);
    let (mode_str, rate) = match spec.kind {
        RunKind::Throughput => ("throughput".to_string(), 0u64),
        RunKind::Latency | RunKind::Counted => {
            ("latency".to_string(), spec.offered_rate.unwrap_or(0))
        }
        RunKind::Connections => ("connections".to_string(), 0u64),
    };
    let report_path = scratch.join("gen.json");
    let mut cmd = Command::new("taskset");
    cmd.arg("-c")
        .arg(&host.cpu_sets.generator)
        .arg(&host.bench_exe)
        .arg("gen")
        .arg("--addr")
        .arg(&addr)
        .arg("--mode")
        .arg(&mode_str)
        .arg("--rate")
        .arg(rate.to_string())
        .arg("--qos")
        .arg(qos.to_string())
        .arg("--connections")
        .arg(conns.to_string())
        .arg("--qos1-window")
        .arg(QOS1_WINDOW.to_string())
        .arg("--origin-ns")
        .arg(origin_ns.to_string())
        .arg("--window-start-ns")
        .arg(window_start_ns.to_string())
        .arg("--window-end-ns")
        .arg(window_end_ns.to_string())
        .arg("--drain-grace-secs")
        .arg(drain_grace_secs.to_string())
        .arg("--report")
        .arg(&report_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(stderr_file(scratch, GENERATOR_LABEL));
    cmd.spawn().map_err(|e| format!("spawn generator: {e}"))
}

fn sleep_until(host: &HostSetup, target_ns: u64) -> Result<(), String> {
    loop {
        let now = monotonic_now_ns();
        if now >= target_ns {
            return Ok(());
        }
        if ChildGuard::is_cancelled(host) {
            return Err("cancelled".to_string());
        }
        let remaining = target_ns - now;
        let step_ns = remaining.min(STEP_NS);
        sleep(Duration::from_nanos(step_ns));
    }
}

/// Sigterm a child with `signal`, then wait up to `slack_secs` for it to exit;
/// past that it is SIGKILLed with `signal` and reaped within
/// [`GUARD_SIGKILL_REAP_TIMEOUT_SECS`] ([`kill_within_bound`]), and the error names a
/// child still running after that by its pid; the child stays with the
/// caller, whose guard ends it for good. ESRCH on the SIGTERM (the child is
/// already gone) is ignored. Returns the exit status.
fn terminate_within<K: FnMut(Pid, Signal) -> nix::Result<()>>(
    cancel: &AtomicBool,
    label: &str,
    child: &mut Child,
    slack_secs: u64,
    mut signal: K,
) -> Result<std::process::ExitStatus, String> {
    let pid = i32::try_from(child.id()).expect("child pid fits in i32");
    match signal(Pid::from_raw(pid), Signal::SIGTERM) {
        Ok(()) | Err(nix::Error::ESRCH) => {}
        Err(e) => return Err(format!("SIGTERM {label}: {e}")),
    }
    let deadline = Instant::now() + Duration::from_secs(slack_secs);
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|e| format!("try_wait {label}: {e}"))?
        {
            return Ok(status);
        }
        if cancel.load(Ordering::Relaxed) {
            return Err("cancelled".to_string());
        }
        if Instant::now() >= deadline {
            let refusal = format!("{label} did not exit within {slack_secs}s of SIGTERM");
            return Err(match kill_within_bound(label, child, &mut signal) {
                Ok(()) => refusal,
                Err(cleanup) => format!("{refusal}; {cleanup}"),
            });
        }
        sleep(Duration::from_millis(WAIT_POLL_MS));
    }
}

/// [`terminate_within`] with the run's cancel flag, [`CHILD_EXIT_SLACK_SECS`]
/// and nix `kill`.
fn terminate_and_wait(
    host: &HostSetup,
    label: &'static str,
    child: &mut Child,
) -> Result<std::process::ExitStatus, String> {
    terminate_within(&host.cancel, label, child, CHILD_EXIT_SLACK_SECS, kill::<Signal>)
}

/// Describe a child that did not exit successfully: its code when it exited
/// on its own, its signal when it was killed. A `bench` child writes its
/// report before it fails, so this note is the only thing that keeps a
/// failed child from leaving a valid record.
fn exit_note(label: &str, status: ExitStatus) -> String {
    match status.code() {
        Some(c) => format!("{label} exited with code {c}"),
        None => format!("{label} exited with signal {}", status.signal().unwrap_or(0)),
    }
}

/// Which kind of counter a `Counter` value carries. The dispatcher on
/// `Counter::kind` keeps `arm_counter`/`disarm_counter`/`reap_counter`/
/// `finalize_counter` straight-line, with each branch handling only one
/// tool's quirks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CounterKind {
    Perf,
    Strace,
}

/// Live state of the perf/strace counter for one 80 % latency run. The
/// counter measures the server's syscalls over the measurement window —
/// perf is the primary tool and drives the `--control=fifo:` protocol,
/// strace is the fallback whose stderr line "attached" is the only
/// confirmation the runner has that it is tracing at all, and whose
/// thread count on that line is the only evidence the per-thread
/// `PTRACE_SEIZE` succeeded for every worker.
///
/// The child is owned by the [`ChildGuard`]; the `Counter` only carries the
/// control fds, the raw output path, the recorded boundary times and the
/// prep error, so a single value can move through the runner's phases
/// without re-opening files or re-spawning anything.
struct Counter {
    kind: CounterKind,
    /// Perf only: write side of the control fifo, opened with
    /// `O_WRONLY | O_NONBLOCK`. None when the open never succeeded.
    ctl_write: Option<OwnedFd>,
    /// Perf only: read side of the ack fifo, opened with
    /// `O_RDONLY | O_NONBLOCK`. Polled for the `ack\n` bytes the counter
    /// writes at each `enable` / `disable` boundary.
    ack_read: Option<OwnedFd>,
    /// Path to the file the counter writes its raw output to: perf's
    /// redirected stderr (`<scratch>/perf.txt`) or strace's `-o` file
    /// (`<scratch>/strace.txt`).
    raw_path: PathBuf,
    /// Strace only: receiver for the stderr reader thread. Carries the
    /// lines the bounded queue kept until the runner drains them for
    /// `diagnostics`.
    stderr_rx: Option<mpsc::Receiver<StdoutLine>>,
    /// Strace only: what the stderr reader thread dropped or failed on,
    /// recorded as it happened and read by [`finalize_counter`].
    stderr_loss: Option<Arc<Mutex<StderrLoss>>>,
    /// Strace only: the stderr lines [`arm_counter`] already consumed while
    /// it waited for the "attached" marker, the marker's own line included.
    /// They are kept here because the marker's thread count, and an attach
    /// failure strace prints before it, would otherwise never reach
    /// `diagnostics` or the partial-attachment check.
    armed_lines: Vec<String>,
    /// Strace only: the traced process's thread count, read just before
    /// the spawn. [`check_strace_attachment`] rejects an "attached" line
    /// reporting fewer threads than this. `None` when the read failed,
    /// which is itself a harness error.
    expected_threads: Option<u64>,
    /// `CLOCK_MONOTONIC` at the moment the counter confirmed each
    /// boundary: the perf `ack` byte, or the first strace "attached" line.
    /// `None` when the confirmation never arrived.
    enabled_ns: Option<u64>,
    disabled_ns: Option<u64>,
    /// Exit status of the counter child, already reaped. [`reap_counter`]
    /// collects it at the window end — or [`disarm_counter`], when the counter had already exited — so this is `Some` by the time
    /// [`finalize_counter`] builds the record; `None` means the stop never
    /// completed, or a failed prep skipped it.
    exit: Option<ExitStatus>,
    /// Prep-time error string. The counter is unusable past this point;
    /// arm/disarm/finalize become no-ops and the guard's child is the
    /// clean-up's responsibility. None on a clean prep.
    prep_error: Option<String>,
}

impl Counter {
    fn new_perf(ctl_write: OwnedFd, ack_read: OwnedFd, raw_path: PathBuf) -> Self {
        Self {
            kind: CounterKind::Perf,
            ctl_write: Some(ctl_write),
            ack_read: Some(ack_read),
            raw_path,
            stderr_rx: None,
            stderr_loss: None,
            armed_lines: Vec::new(),
            expected_threads: None,
            enabled_ns: None,
            disabled_ns: None,
            exit: None,
            prep_error: None,
        }
    }

    fn new_strace(
        raw_path: PathBuf,
        stderr_rx: mpsc::Receiver<StdoutLine>,
        stderr_loss: Arc<Mutex<StderrLoss>>,
        expected_threads: Option<u64>,
    ) -> Self {
        Self {
            kind: CounterKind::Strace,
            ctl_write: None,
            ack_read: None,
            raw_path,
            stderr_rx: Some(stderr_rx),
            stderr_loss: Some(stderr_loss),
            armed_lines: Vec::new(),
            expected_threads,
            enabled_ns: None,
            disabled_ns: None,
            exit: None,
            prep_error: None,
        }
    }

    /// Counter is dead on arrival: prep failed in a way that leaves the
    /// run invalid. The record's `invalid_reasons` carries `prep_error`;
    /// subsequent arm/disarm calls are no-ops. The selected tool and its
    /// raw-output path are kept, because the tool can have written its own
    /// cause (a permission or tracepoint error) to that file before the
    /// prep step failed.
    fn dead(kind: CounterKind, raw_path: PathBuf, prep_error: String) -> Self {
        Self {
            kind,
            ctl_write: None,
            ack_read: None,
            raw_path,
            stderr_rx: None,
            stderr_loss: None,
            armed_lines: Vec::new(),
            expected_threads: None,
            enabled_ns: None,
            disabled_ns: None,
            exit: None,
            prep_error: Some(prep_error),
        }
    }
}

/// True when `spec` is the counted run and `host.syscalls` names a
/// counter: the published latency runs never carry one (§9.1 Counted run).
fn should_count(spec: &RunSpec, host: &HostSetup) -> bool {
    spec.kind == RunKind::Counted
        && matches!(host.syscalls, SyscallMethod::Perf | SyscallMethod::Strace)
}

/// Open the perf ctl fifo for writing with `O_WRONLY | O_NONBLOCK`,
/// retrying every 10 ms on `ENXIO` (no reader on the other end) until
/// `COUNTER_CONTROL_TIMEOUT_SECS`. The retry budget is the one named in
/// the brief: 5 s, and 10 ms steps. The `Err` carries the cause, because a
/// `perf` that cannot be controlled fails for reasons an operator has to
/// tell apart: a missing reader for the whole budget, a permission denial,
/// and a cancelled run all end here.
fn open_ctl_with_retry(path: &Path, host: &HostSetup) -> Result<OwnedFd, String> {
    let deadline = Instant::now() + Duration::from_secs(COUNTER_CONTROL_TIMEOUT_SECS);
    loop {
        match open(path, OFlag::O_WRONLY | OFlag::O_NONBLOCK, Mode::empty()) {
            Ok(fd) => return Ok(fd),
            Err(nix::Error::ENXIO) => {
                if ChildGuard::is_cancelled(host) {
                    return Err("run cancelled".to_string());
                }
                if Instant::now() >= deadline {
                    return Err(format!(
                        "no reader within {COUNTER_CONTROL_TIMEOUT_SECS}s (ENXIO)"
                    ));
                }
                sleep(Duration::from_millis(CTL_OPEN_RETRY_MS));
            }
            Err(e) => return Err(e.to_string()),
        }
    }
}

/// Best-effort write of `buf` to `fd`, looping on short writes. Returns
/// the cause on hard failure. The write fd is opened non-blocking too, so
/// `EAGAIN` is not a transient condition the runner wants to spin on; the
/// control fifo should be drained by the counter within the ack deadline.
fn write_ctl(fd: &OwnedFd, buf: &[u8]) -> Result<(), String> {
    let mut written = 0;
    while written < buf.len() {
        match nix::unistd::write(fd.as_fd(), &buf[written..]) {
            Ok(0) | Err(nix::Error::EAGAIN) => {
                return Err(format!("ctl write stopped at {written} of {} bytes", buf.len()));
            }
            Ok(n) => written += n,
            Err(e) => return Err(format!("ctl write: {e}")),
        }
    }
    Ok(())
}

/// Drain the ack read fd until a line containing `ack` is observed, or the
/// deadline passes. The fd is non-blocking; the runner polls every
/// [`ACK_POLL_MS`] and yields between attempts. Returns the instant the
/// ack was seen, or `None` on timeout / cancellation / hard error.
fn wait_for_ack(fd: &OwnedFd, host: &HostSetup, harness_errors: &mut Vec<String>) -> Option<u64> {
    let deadline = Instant::now() + Duration::from_secs(COUNTER_CONTROL_TIMEOUT_SECS);
    let mut buf = [0u8; 16];
    loop {
        if ChildGuard::is_cancelled(host) {
            return None;
        }
        if Instant::now() >= deadline {
            harness_errors.push("syscall counter: no ack".to_string());
            return None;
        }
        match nix::unistd::read(fd.as_fd(), &mut buf) {
            Ok(0) => {
                // Counter closed the ack fd before writing; treat as
                // missing acknowledgement so the run invalidates.
                harness_errors.push("syscall counter: no ack".to_string());
                return None;
            }
            Ok(n) => {
                if buf[..n].windows(3).any(|w| w == b"ack") {
                    return Some(monotonic_now_ns());
                }
                // Partial or unrelated bytes: keep draining.
            }
            Err(nix::Error::EAGAIN) => sleep(Duration::from_millis(ACK_POLL_MS)),
            Err(nix::Error::EINTR) => {}
            Err(e) => {
                harness_errors.push(format!("syscall counter: ack read: {e}"));
                return None;
            }
        }
    }
}

/// Prepare the perf counter: mkfifo the two control fifos, spawn
/// `perf stat --control=fifo:...` under `taskset`, open the ack read-side
/// and the ctl write-side (with the ENXIO retry). Returns a [`Counter`]
/// even on failure so the run is still recorded; the failure reason lives
/// in `harness_errors` and on the `Counter` itself.
fn prepare_perf_counter(
    host: &HostSetup,
    dir: &Path,
    server_pid: u32,
    guards: &mut ChildGuard,
    harness_errors: &mut Vec<String>,
) -> Counter {
    // Named before the first failure branch so every dead counter below
    // carries the path perf writes its own error to.
    let perf_txt = dir.join("perf.txt");
    let dead = |msg: String, harness_errors: &mut Vec<String>| -> Counter {
        harness_errors.push(msg.clone());
        Counter::dead(CounterKind::Perf, perf_txt.clone(), msg)
    };
    let Some(perf) = host.perf_bin.as_ref() else {
        return dead("syscall counter: no perf_bin".to_string(), harness_errors);
    };
    let ctl_path = dir.join("perf.ctl");
    let ack_path = dir.join("perf.ack");
    // Best-effort clear of stale fifos from an earlier run; the run's
    // scratch dir is fresh, so unlink is defensive.
    let _ = nix::unistd::unlink(&ctl_path);
    let _ = nix::unistd::unlink(&ack_path);
    let mode = Mode::S_IRUSR | Mode::S_IWUSR;
    if let Err(e) = nix::unistd::mkfifo(&ctl_path, mode) {
        return dead(format!("syscall counter: mkfifo ctl: {e}"), harness_errors);
    }
    if let Err(e) = nix::unistd::mkfifo(&ack_path, mode) {
        return dead(format!("syscall counter: mkfifo ack: {e}"), harness_errors);
    }
    let perf_txt_file = match std::fs::File::create(&perf_txt) {
        Ok(f) => f,
        Err(e) => {
            return dead(
                format!("syscall counter: create perf.txt: {e}"),
                harness_errors,
            );
        }
    };
    let mut cmd = Command::new("taskset");
    cmd.arg("-c")
        .arg(&host.cpu_sets.server)
        .arg(perf)
        .arg("stat")
        .arg("--delay=-1")
        .arg(format!(
            "--control=fifo:{},{}",
            ctl_path.display(),
            ack_path.display()
        ))
        .arg("-x")
        .arg(",")
        .arg("-e")
        .arg("raw_syscalls:sys_enter")
        .arg("-p")
        .arg(server_pid.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(perf_txt_file));
    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return dead(format!("syscall counter: spawn perf: {e}"), harness_errors);
        }
    };
    guards.push(COUNTER_LABEL, child);
    let ack_read = match open(&ack_path, OFlag::O_RDONLY | OFlag::O_NONBLOCK, Mode::empty()) {
        Ok(fd) => fd,
        Err(e) => {
            return dead(format!("syscall counter: open ack: {e}"), harness_errors);
        }
    };
    let ctl_write = match open_ctl_with_retry(&ctl_path, host) {
        Ok(fd) => fd,
        Err(e) => {
            return dead(
                format!("syscall counter: control fifo not opened: {e}"),
                harness_errors,
            );
        }
    };
    Counter::new_perf(ctl_write, ack_read, perf_txt)
}

/// Spawn the strace counter at `window.start_ns` (the brief's spec, not
/// the perf one). The child is guarded; its stderr is read on its own
/// thread and kept until [`finalize_counter`] drains it for diagnostics.
/// Mirrors [`prepare_perf_counter`]: a failure returns a dead counter so
/// the run is still recorded with the reason on it.
fn spawn_strace_counter(
    host: &HostSetup,
    dir: &Path,
    server_pid: u32,
    guards: &mut ChildGuard,
    harness_errors: &mut Vec<String>,
) -> Counter {
    let strace_txt = dir.join("strace.txt");
    let dead = |msg: String, harness_errors: &mut Vec<String>| -> Counter {
        harness_errors.push(msg.clone());
        Counter::dead(CounterKind::Strace, strace_txt.clone(), msg)
    };
    let Some(strace) = host.strace_bin.as_ref() else {
        return dead("syscall counter: no strace_bin".to_string(), harness_errors);
    };
    // Read before the spawn, so the number cannot already include a
    // thread strace never saw. The server's threads are all created
    // before `READY`, and this runs at `window.start_ns`, so a thread
    // appearing between the two reads is not a case this harness has;
    // one that did would only raise strace's own count, which the
    // shortfall test below ignores.
    let expected_threads = match read_thread_count(server_pid) {
        Ok(n) => Some(n),
        Err(e) => {
            harness_errors.push(format!("syscall counter: server threads: {e}"));
            None
        }
    };
    let mut cmd = Command::new("taskset");
    cmd.arg("-c")
        .arg(&host.cpu_sets.server)
        .arg(strace)
        .arg("-c")
        .arg("-f")
        .arg("-p")
        .arg(server_pid.to_string())
        .arg("-o")
        .arg(&strace_txt)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return dead(format!("syscall counter: spawn strace: {e}"), harness_errors);
        }
    };
    // Guarded before the stderr handle is taken and the reader thread is
    // started: either step can fail, and a strace left unguarded would
    // keep tracing the server with nothing to kill and reap it.
    guards.push(COUNTER_LABEL, child);
    let stderr = guards.child_mut(COUNTER_LABEL).and_then(|c| c.stderr.take());
    let Some(stderr) = stderr else {
        return dead(
            "syscall counter: strace stderr not piped".to_string(),
            harness_errors,
        );
    };
    match spawn_counter_stderr_reader(stderr) {
        Ok((rx, loss)) => Counter::new_strace(strace_txt, rx, loss, expected_threads),
        Err(e) => dead(e, harness_errors),
    }
}

/// Reader thread for the strace counter's stderr. Every line is queued
/// up to the queue bound (strace prints `attached` and `ptrace(...)` on
/// a single thread's failure). Nothing receives between the attach
/// marker and the finalization drain, so the queue holds at most
/// [`COUNTER_DIAGNOSTIC_LINES_MAX`] lines — no more than the record can
/// keep. No send blocks: a blocked reader would stall strace's stderr
/// writes and, through its ptrace stops, the traced server. What the
/// queue cannot carry goes into the returned [`StderrLoss`] the moment
/// it happens — a dropped line, and the failure that ends the reading,
/// a line longer than [`COUNTER_STDERR_LINE_BYTES_MAX`] included — so a
/// full queue cannot erase it. After a failure the thread still reads
/// the pipe to EOF: closing it would hand strace SIGPIPE on its next
/// stderr write.
fn spawn_counter_stderr_reader(
    stderr: std::process::ChildStderr,
) -> Result<(mpsc::Receiver<StdoutLine>, Arc<Mutex<StderrLoss>>), String> {
    let (tx, rx) = mpsc::sync_channel(COUNTER_DIAGNOSTIC_LINES_MAX);
    let loss = Arc::new(Mutex::new(StderrLoss::default()));
    let thread_loss = Arc::clone(&loss);
    std::thread::Builder::new()
        .name(format!("bench-runner-{COUNTER_STDERR_LABEL}"))
        .spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut buf: Vec<u8> = Vec::new();
            loop {
                buf.clear();
                // One byte past the cap: a line of exactly the cap ends
                // in its newline, a longer one does not.
                let message = match (&mut reader)
                    .take(COUNTER_STDERR_LINE_BYTES_MAX + 1)
                    .read_until(b'\n', &mut buf)
                {
                    Ok(0) => StdoutLine::Eof,
                    Ok(_)
                        if buf.len() as u64 > COUNTER_STDERR_LINE_BYTES_MAX
                            && buf.last() != Some(&b'\n') =>
                    {
                        StdoutLine::Failed(format!(
                            "line longer than {COUNTER_STDERR_LINE_BYTES_MAX} bytes"
                        ))
                    }
                    Ok(_) => match String::from_utf8(buf.clone()) {
                        Ok(line) => StdoutLine::Line(line),
                        Err(_) => StdoutLine::Failed(
                            "stream did not contain valid UTF-8".to_string(),
                        ),
                    },
                    Err(e) => StdoutLine::Failed(e.to_string()),
                };
                match message {
                    StdoutLine::Line(_) => match tx.try_send(message) {
                        Ok(()) => {}
                        Err(mpsc::TrySendError::Full(_)) => {
                            thread_loss.lock().expect("stderr loss poisoned").dropped = true;
                        }
                        Err(mpsc::TrySendError::Disconnected(_)) => break,
                    },
                    StdoutLine::Failed(ref cause) => {
                        thread_loss.lock().expect("stderr loss poisoned").failure =
                            Some(cause.clone());
                        let _ = tx.try_send(message);
                        let _ = std::io::copy(&mut reader, &mut std::io::sink());
                        break;
                    }
                    StdoutLine::Eof => {
                        let _ = tx.try_send(message);
                        break;
                    }
                }
            }
        })
        .map_err(|e| format!("syscall counter: spawn stderr reader: {e}"))?;
    Ok((rx, loss))
}

/// Enable the counter at `window.start_ns`. Perf: write `enable\n`,
/// wait for `ack`. Strace: nothing — the brief's strace lifecycle spawns
/// the counter at this point and waits for the first "attached" line.
fn arm_counter(counter: &mut Counter, host: &HostSetup, harness_errors: &mut Vec<String>) {
    if counter.prep_error.is_some() {
        return;
    }
    match counter.kind {
        CounterKind::Perf => {
            let Some(ctl) = counter.ctl_write.as_ref() else {
                return;
            };
            if let Err(e) = write_ctl(ctl, PERF_ENABLE) {
                harness_errors.push(format!("syscall counter: {e}"));
                return;
            }
            let Some(ack) = counter.ack_read.as_ref() else {
                return;
            };
            counter.enabled_ns = wait_for_ack(ack, host, harness_errors);
        }
        CounterKind::Strace => {
            let Some(rx) = counter.stderr_rx.as_ref() else {
                return;
            };
            let (enabled_ns, lines) = wait_for_strace_attach(rx, host, harness_errors);
            counter.enabled_ns = enabled_ns;
            counter.armed_lines = lines;
        }
    }
}

/// Wait for strace's "attached" stderr line, up to
/// `COUNTER_CONTROL_TIMEOUT_SECS`. Returns the instant the marker arrived
/// and every line read on the way to it, including the marker's own: the
/// marker carries the attached thread count and an attach failure can
/// print before it, and both are the evidence AC-60's partial-attachment
/// rejection reads, so no consumed line within the record's cap is dropped;
/// at most [`COUNTER_DIAGNOSTIC_LINES_MAX`] + 1 lines are kept. A read
/// failure ends the wait without an error of its own: [`finalize_counter`]
/// names it from [`StderrLoss`].
fn wait_for_strace_attach(
    rx: &mpsc::Receiver<StdoutLine>,
    host: &HostSetup,
    harness_errors: &mut Vec<String>,
) -> (Option<u64>, Vec<String>) {
    let mut lines: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(COUNTER_CONTROL_TIMEOUT_SECS);
    loop {
        if ChildGuard::is_cancelled(host) {
            return (None, lines);
        }
        if Instant::now() >= deadline {
            harness_errors.push("syscall counter: not attached".to_string());
            return (None, lines);
        }
        match rx.recv_timeout(Duration::from_millis(WAIT_POLL_MS)) {
            Ok(StdoutLine::Line(line)) => {
                let attached = line.contains(STRACE_ATTACHED_MARKER);
                // One line past the record's cap: `finalize_counter` keeps
                // only the first `COUNTER_DIAGNOSTIC_LINES_MAX`, and the extra
                // line is how it knows this wait dropped some, so a flood
                // before the marker needs no more memory than that.
                if lines.len() <= COUNTER_DIAGNOSTIC_LINES_MAX {
                    lines.push(line.trim_end().to_string());
                }
                if attached {
                    return (Some(monotonic_now_ns()), lines);
                }
            }
            Ok(StdoutLine::Eof) => {
                harness_errors
                    .push("syscall counter: stderr closed before attached".to_string());
                return (None, lines);
            }
            Ok(StdoutLine::Failed(_)) => {
                // The reader recorded the cause in `StderrLoss`;
                // `finalize_counter` names it once.
                return (None, lines);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                harness_errors.push("syscall counter: stderr reader stopped".to_string());
                return (None, lines);
            }
        }
    }
}

/// Stop the counter at `window.end_ns`, without waiting for it to exit.
/// Perf: write `disable\n` and take `disabled_ns` from the `ack`, which is
/// perf's own confirmation that counting stopped. Both kinds then get the
/// SIGINT that makes them print their summary and leave; [`reap_counter`]
/// collects that exit. The two halves are separate because the CPU end
/// ticks are sampled between them: a counter may confirm the stop on time
/// and still take seconds to write its summary out, and those seconds
/// belong to neither `sampled_ns` nor the tick deltas it divides.
/// A counter that already exited gets no signal: its exit is recorded and
/// the run is invalid (see the liveness test below).
fn disarm_counter(
    counter: &mut Counter,
    guards: &mut ChildGuard,
    host: &HostSetup,
    harness_errors: &mut Vec<String>,
) {
    if counter.prep_error.is_some() {
        return;
    }
    // A counter that is no longer running stopped counting before
    // the window end, so its count covers less than the window.
    // `kill` cannot tell: the child stays unreaped in the guard and
    // a signal to the zombie succeeds. Std keeps the status
    // `try_wait` reaps, so the exit is recorded here, `reap_counter`
    // skips the counter, and `disabled_ns` stays `None` — row 22
    // rejects the record and this error names how it ended.
    if let Some(child) = guards.child_mut(COUNTER_LABEL) {
        match child.try_wait() {
            Ok(Some(status)) => {
                counter.exit = Some(status);
                harness_errors.push(format!(
                    "syscall counter: exited before the window end with {:?}",
                    counter_exit_from_status(status)
                ));
                return;
            }
            Ok(None) => {}
            Err(e) => harness_errors.push(format!("syscall counter: try_wait: {e}")),
        }
    }
    if counter.kind == CounterKind::Perf {
        // The SIGINT below brings the counter down whatever this write
        // does, so the failure does not change the sequence — but the
        // cause is kept, because the only other trace a lost write leaves
        // is the ack wait below reporting "no ack", which names neither
        // the write nor its OS error.
        if let Some(ctl) = counter.ctl_write.as_ref() {
            if let Err(e) = write_ctl(ctl, PERF_DISABLE) {
                harness_errors.push(format!("syscall counter: {e}"));
            }
        }
        if let Some(ack) = counter.ack_read.as_ref() {
            counter.disabled_ns = wait_for_ack(ack, host, harness_errors);
        }
    }
    // Strace prints its `-c` summary on SIGINT; perf's INT trap re-raises
    // SIGINT after printing its counts. The child is in the per-call
    // guard — prep (perf) or the window-start spawn (strace) put it there
    // before this function was reachable.
    if let Some(child) = guards.child_mut(COUNTER_LABEL) {
        let pid = i32::try_from(child.id()).expect("child pid fits in i32");
        let _ = kill(Pid::from_raw(pid), Signal::SIGINT);
    }
}

/// Reap the counter [`disarm_counter`] signalled, and for strace take
/// `disabled_ns` from that reap. Strace has no control channel, so its
/// confirmed stop is its exit; the reap belongs here rather than in
/// [`finalize_counter`] because reconcile row 22 measures `disabled_ns`
/// against `window.end_ns` with a `COUNTER_BOUNDARY_TOLERANCE_NS` budget,
/// and the generator drain, the endpoint shutdown and the report reads
/// between the two would spend it many times over on a counter that
/// stopped on time. The two CPU tick reads `run_one` does ahead of this
/// call cost microseconds against that 10 ms budget.
fn reap_counter(
    counter: &mut Counter,
    guards: &mut ChildGuard,
    host: &HostSetup,
    harness_errors: &mut Vec<String>,
) {
    if counter.prep_error.is_some() {
        return;
    }
    // Already reaped by `disarm_counter`'s liveness test: the counter
    // stopped before the window end and has no confirmed stop to
    // stamp.
    if counter.exit.is_some() {
        return;
    }
    counter.exit = wait_for_counter_exit(guards, host, harness_errors);
    if counter.kind == CounterKind::Strace && counter.exit.is_some() {
        counter.disabled_ns = Some(monotonic_now_ns());
    }
}

/// Map an [`ExitStatus`] to the [`CounterExit`] the record stores.
/// `Code(0)` and `Signal(SIGINT)` are the two endings reconcile row 23
/// accepts; everything else becomes a "syscall counter ended with …"
/// reason.
fn counter_exit_from_status(status: ExitStatus) -> CounterExit {
    status
        .code()
        .map(CounterExit::Code)
        .or_else(|| status.signal().map(CounterExit::Signal))
        .expect("ExitStatus has either a code or a signal")
}

/// Finalize the counter: read the raw output, parse the count, collect
/// strace's stderr, and return the [`Syscalls`] block. [`disarm_counter`]
/// and [`reap_counter`] already stopped and reaped a live counter at the
/// window end; a failed prep skipped that, so its child (when it has one)
/// is stopped and reaped here instead, leaving the guard empty for the rest
/// of the run.
fn finalize_counter(
    counter: &mut Counter,
    guards: &mut ChildGuard,
    host: &HostSetup,
    harness_errors: &mut Vec<String>,
) -> Syscalls {
    let method = match counter.kind {
        CounterKind::Perf => SyscallMethod::Perf,
        CounterKind::Strace => SyscallMethod::Strace,
    };
    if counter.prep_error.is_some() {
        if let Some(child) = guards.child_mut(COUNTER_LABEL) {
            let pid = i32::try_from(child.id()).expect("child pid fits in i32");
            let _ = kill(Pid::from_raw(pid), Signal::SIGINT);
        }
        counter.exit = wait_for_counter_exit(guards, host, harness_errors);
    }
    // Every strace stderr line the run saw: the ones `arm_counter`
    // consumed before the "attached" marker, then the rest. For perf this
    // is the None arm.
    let diagnostics = if matches!(counter.kind, CounterKind::Strace) {
        let mut lines = std::mem::take(&mut counter.armed_lines);
        // The cap spans both sources: the marker wait's lines are
        // already retained, so the drain only gets what is left of it.
        // The marker wait keeps one line past the cap, so more than the
        // cap here means it dropped lines itself.
        let mut truncated = lines.len() > COUNTER_DIAGNOSTIC_LINES_MAX;
        let budget = COUNTER_DIAGNOSTIC_LINES_MAX.saturating_sub(lines.len());
        if let Some(rx) = counter.stderr_rx.as_ref() {
            let (rest, dropped, drain_end) = drain_stderr_lines(rx, host, budget);
            lines.extend(rest);
            truncated |= dropped;
            if let Some(DrainEnd::TimedOut) = drain_end {
                harness_errors.push(format!(
                    "syscall counter: stderr still open after {COUNTER_DRAIN_TIMEOUT_SECS}s, diagnostics truncated at {} lines",
                    lines.len()
                ));
            }
        }
        // Read after the drain: the reader records a dropped line and its
        // ending failure before it queues anything after them.
        if let Some(loss) = counter.stderr_loss.as_ref() {
            let loss = loss.lock().expect("stderr loss poisoned");
            truncated |= loss.dropped;
            if let Some(cause) = loss.failure.as_ref() {
                harness_errors.push(format!("syscall counter: stderr read: {cause}"));
            }
        }
        if truncated {
            harness_errors.push(format!(
                "syscall counter: diagnostics truncated at {COUNTER_DIAGNOSTIC_LINES_MAX} lines"
            ));
        }
        lines.truncate(COUNTER_DIAGNOSTIC_LINES_MAX);
        check_strace_attachment(&lines, counter.expected_threads, harness_errors);
        (!lines.is_empty()).then(|| lines.join("\n"))
    } else {
        None
    };
    let raw = match std::fs::read_to_string(&counter.raw_path) {
        Ok(text) => Some(text),
        Err(e) => {
            if counter.prep_error.is_none() {
                harness_errors.push(format!(
                    "syscall counter: raw output missing: {}: {e}",
                    counter.raw_path.display()
                ));
            }
            None
        }
    };
    let count = match counter.kind {
        CounterKind::Perf => raw.as_deref().and_then(parse_perf_count),
        CounterKind::Strace => raw.as_deref().and_then(parse_strace_total),
    };
    if counter.prep_error.is_none() && count.is_none() && raw.is_some() {
        harness_errors.push("syscall counter: unparseable output".to_string());
    }
    let exit_field = counter.exit.map(counter_exit_from_status);
    Syscalls {
        method,
        count,
        per_msg: None,
        raw,
        enabled_ns: counter.enabled_ns,
        disabled_ns: counter.disabled_ns,
        exit: exit_field,
        diagnostics,
    }
}

/// Wait up to `CHILD_EXIT_SLACK_SECS` for the counter child to exit. The
/// guard's `Drop` is the unconditional last resort; a child that misses
/// the deadline still gets SIGKILL'd when the guard falls. Returns the
/// `ExitStatus` or `None` on cancellation / hard wait error.
fn wait_for_counter_exit(
    guards: &mut ChildGuard,
    host: &HostSetup,
    harness_errors: &mut Vec<String>,
) -> Option<ExitStatus> {
    let deadline = Instant::now() + Duration::from_secs(CHILD_EXIT_SLACK_SECS);
    loop {
        let Some(child) = guards.child_mut(COUNTER_LABEL) else {
            // Guard already dropped the child — nothing to wait on.
            return None;
        };
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) => {}
            Err(e) => {
                harness_errors.push(format!("syscall counter: try_wait: {e}"));
                return None;
            }
        }
        if ChildGuard::is_cancelled(host) {
            return None;
        }
        if Instant::now() >= deadline {
            harness_errors
                .push("syscall counter: did not exit within timeout".to_string());
            return None;
        }
        sleep(Duration::from_millis(COUNTER_EXIT_POLL_MS));
    }
}

/// Drain the remaining lines out of the strace stderr reader, for
/// `Syscalls::diagnostics`, under two bounds the drain holds whatever the
/// counter does: [`COUNTER_DRAIN_TIMEOUT_SECS`] from the first poll, and
/// `budget` further retained lines — lines past the budget are received
/// and dropped rather than ending the drain, and the returned flag says
/// whether any was. Both are needed because the counter is an
/// operator-supplied executable — an idle stream reaches EOF, but a
/// descendant that keeps the pipe open and writes more often than
/// [`WAIT_POLL_MS`] reaches neither EOF nor a quiet gap, and this runs
/// inside `run_one` before the record is returned and the per-call guard
/// falls.
///
/// EOF, a read failure, a cancelled run and a disconnected reader end the
/// drain without a [`DrainEnd`]; a read failure is named by
/// [`finalize_counter`] from [`StderrLoss`], which the reader filled
/// before it queued anything about it. A quiet gap no longer ends the
/// drain: the reader thread can lag behind bytes already in the pipe, and
/// only its own ending proves it has read them, so a stream that stays
/// open quietly now runs to [`COUNTER_DRAIN_TIMEOUT_SECS`] and is named
/// as still open.
fn drain_stderr_lines(
    rx: &mpsc::Receiver<StdoutLine>,
    host: &HostSetup,
    budget: usize,
) -> (Vec<String>, bool, Option<DrainEnd>) {
    let mut out: Vec<String> = Vec::new();
    let mut dropped = false;
    let deadline = Instant::now() + Duration::from_secs(COUNTER_DRAIN_TIMEOUT_SECS);
    loop {
        if ChildGuard::is_cancelled(host) {
            return (out, dropped, None);
        }
        if Instant::now() >= deadline {
            return (out, dropped, Some(DrainEnd::TimedOut));
        }
        match rx.recv_timeout(Duration::from_millis(WAIT_POLL_MS)) {
            Ok(StdoutLine::Line(line)) => {
                if out.len() < budget {
                    out.push(line.trim_end().to_string());
                } else {
                    dropped = true;
                }
            }
            // A quiet gap is not an end: the reader may not have reached
            // the bytes still in the pipe. Only the reader's own ending
            // — EOF, a failure it already recorded, or its exit — ends
            // the drain early; anything else waits for the deadline.
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Ok(StdoutLine::Failed(_) | StdoutLine::Eof)
            | Err(mpsc::RecvTimeoutError::Disconnected) => return (out, dropped, None),
        }
    }
}

/// Reject a strace attach that did not cover the whole traced process.
/// Two signals, because strace reports the two failures differently:
///
/// * the traced pid's own attach failure is a `ptrace(` line strace always
///   prints, so its presence anywhere in the stderr the run saw is enough;
/// * a per-thread failure is silent (`debug_perror_msg`, which needs `-d`,
///   and `-d` would also log every wait status for the whole window), and
///   strace still prints its normal "attached" line with the number of
///   threads it did cover. A number below the count read at spawn time is
///   the only evidence of that failure, and a short attach undercounts the
///   syscalls the published `syscalls_per_msg` divides.
fn check_strace_attachment(
    lines: &[String],
    expected_threads: Option<u64>,
    harness_errors: &mut Vec<String>,
) {
    if lines
        .iter()
        .any(|l| l.contains(STRACE_PTRACE_ERROR_MARKER))
    {
        harness_errors.push("syscall counter: partial attachment: ptrace failed".to_string());
    }
    let Some(expected) = expected_threads else {
        // The thread count could not be read; `spawn_strace_counter`
        // already recorded that as its own harness error.
        return;
    };
    let Some(attached) = lines
        .iter()
        .find(|l| l.contains(STRACE_ATTACHED_MARKER))
        .map(|l| parse_strace_attached_threads(l))
    else {
        // No marker line at all: `arm_counter` recorded the missing
        // attach, and there is no count to compare.
        return;
    };
    if attached < expected {
        harness_errors.push(format!(
            "syscall counter: partial attachment: attached {attached} of {expected} threads"
        ));
    }
}

/// Threads an "attached" line reports. `Process 7 attached with 4 threads`
/// is 4; the short `Process 7 attached` is the wording for a single
/// thread, which is also what strace prints when every sibling thread's
/// attach failed.
fn parse_strace_attached_threads(line: &str) -> u64 {
    line.split_once(STRACE_ATTACHED_WITH_MARKER)
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(1)
}


fn read_delivery_report(path: &Path) -> Option<DeliveryReport> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

fn read_gen_report(path: &Path) -> Option<crate::gen::GenReport> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

/// Run one configuration and return the `RunRecord` for it. Never panics:
/// every child-process failure becomes a `harness_error` entry and the
/// record is returned, so a single bad run does not halt the matrix.
pub fn run_one(spec: &RunSpec, host: &HostSetup) -> RunRecord {
    run_one_with(spec, host, kill::<Signal>)
}

/// [`run_one`] with the guard's cleanup SIGKILL sent through `signal`;
/// a test hands in a stand-in to make a child survive its cleanup.
pub fn run_one_with(
    spec: &RunSpec,
    host: &HostSetup,
    signal: fn(Pid, Signal) -> nix::Result<()>,
) -> RunRecord {
    let mut guards = ChildGuard::with_signal(signal);
    let record = run_guarded(spec, host, &mut guards);
    record_survivors(record, &mut guards)
}

/// End every child `guards` still holds ([`ChildGuard::end_all`]) and
/// record each one it could not end as a harness error prefixed with
/// [`CLEANUP_SURVIVOR_PREFIX`], then judge the record again, so it is
/// invalid and names the child's pid. A record with no survivor is
/// returned as it came.
fn record_survivors(mut record: RunRecord, guards: &mut ChildGuard) -> RunRecord {
    let survivors = guards.end_all();
    if survivors.is_empty() {
        return record;
    }
    record
        .harness_errors
        .extend(survivors.into_iter().map(|e| format!("{CLEANUP_SURVIVOR_PREFIX}{e}")));
    record.invalid_reasons = reconcile(&record);
    record.valid = record.invalid_reasons.is_empty();
    record
}

/// [`run_one`]'s body: every child it spawns goes into `guards`, which the
/// caller ends after it returns.
fn run_guarded(spec: &RunSpec, host: &HostSetup, guards: &mut ChildGuard) -> RunRecord {
    let constants = ProtocolConstants::for_profile(spec.profile);
    let mut harness_errors: Vec<String> = Vec::new();

    // Scratch directory.
    let stem = stem_for(spec);
    let dir = host.scratch_dir.join(&stem);
    if let Err(e) = std::fs::create_dir_all(&host.scratch_dir) {
        harness_errors.push(format!("create scratch dir: {e}"));
    }
    // A directory that already exists belongs to an earlier run of this
    // spec. Writing `endpoint.json`, `gen.json` and the children's stderr
    // into it would overwrite that run's raw evidence while this record is
    // invalid anyway, so nothing below this line runs.
    if let Err(_e) = std::fs::create_dir(&dir) {
        harness_errors.push(format!("{SCRATCH_REUSED_PREFIX}{}", dir.display()));
        return finalize_record(
            spec,
            host,
            constants,
            &harness_errors,
            build_kind(),
            RunTiming::default(),
            None,
            None,
            None,
            None,
            None,
        );
    }

    // An exhausted port band is a harness failure, not a panic: `run_one`
    // owes the matrix a record for every spec.
    let port = match crate::try_free_port() {
        Ok(port) => port,
        Err(e) => {
            harness_errors.push(e);
            return finalize_record(
                spec,
                host,
                constants,
                &harness_errors,
                build_kind(),
                RunTiming::default(),
                None,
                None,
                None,
                None,
                None,
            );
        }
    };

    // Mosquitto conf (only when the broker is mosquitto).
    let mut mosquitto_conf_path: Option<PathBuf> = None;
    if spec.server == ServerKind::Mosquitto {
        let conf = dir.join("mosquitto.conf");
        let body = mosquitto_conf(port);
        if let Err(e) = std::fs::write(&conf, body) {
            harness_errors.push(format!("write mosquitto conf: {e}"));
        } else {
            mosquitto_conf_path = Some(conf);
        }
    }

    // Endpoint. `spawn_endpoint` pushes every process it starts into
    // `guards` as soon as it exists, so no failure below this line can
    // leave one running.
    let mut endpoint = match spawn_endpoint(
        spec,
        host,
        guards,
        &dir,
        port,
        mosquitto_conf_path.as_deref(),
    ) {
        Ok(ep) => ep,
        Err(e) => {
            harness_errors.push(format!("endpoint startup failed: {e}"));
            return finalize_record(
                spec,
                host,
                constants,
                &harness_errors,
                build_kind(),
                RunTiming::default(),
                None,
                None,
                None,
                None,
                None,
            );
        }
    };

    // READY handshake. The handshake error is retained verbatim, and the
    // child's stderr is in the run's scratch directory, so a rejection
    // names its cause instead of a bare timeout.
    if let Some(handshake) = endpoint.handshake.as_mut() {
        let ready_deadline = Instant::now() + Duration::from_secs(STARTUP_TIMEOUT_SECS);
        if let Err(e) = wait_for_line(host, guards, handshake, ready_deadline, "READY") {
            harness_errors.push(format!(
                "endpoint startup failed: READY: {e} (stderr: {})",
                dir.join(format!("{}.stderr", handshake.label)).display()
            ));
            return finalize_record(
                spec,
                host,
                constants,
                &harness_errors,
                build_kind(),
                RunTiming::default(),
                None,
                None,
                None,
                None,
                None,
            );
        }
    }

    // Syscall counter. The prep point is right after `READY` and before
    // the origin: the perf counter must be open on the control fifos and
    // sitting at `--delay=-1` (disabled) before the schedule is chosen, so
    // a slow startup of the counter cannot eat the generator lead.
    // Strace defers its own spawn to `window.start_ns` (per the brief's
    // strace lifecycle) and is therefore `None` here.
    let counting = should_count(spec, host);
    let mut counter: Option<Counter> = if counting && host.syscalls == SyscallMethod::Perf {
        Some(prepare_perf_counter(
            host,
            &dir,
            endpoint.server_pid,
            guards,
            &mut harness_errors,
        ))
    } else {
        None
    };

    // Compute origin and window now — the lead begins only after READY,
    // so a slow endpoint cannot shorten it.
    let now = monotonic_now_ns();
    let lead_ns = GEN_LEAD_SECS * ONE_SECOND_NS;
    let earliest_ns = now.saturating_add(lead_ns);
    // Round UP to the next whole second.
    let origin_ns = if earliest_ns.is_multiple_of(ONE_SECOND_NS) {
        earliest_ns
    } else {
        (earliest_ns / ONE_SECOND_NS + 1) * ONE_SECOND_NS
    };
    let window_start_ns = origin_ns + constants.warmup_secs * ONE_SECOND_NS;
    let window_end_ns = window_start_ns + constants.window_secs * ONE_SECOND_NS;
    let timing = RunTiming {
        origin_ns,
        window: Window {
            start_ns: window_start_ns,
            end_ns: window_end_ns,
        },
    };

    // ARM handshake.
    if let Some(handshake) = endpoint.handshake.as_mut() {
        let arm_deadline = Instant::now() + Duration::from_secs(STARTUP_TIMEOUT_SECS);
        if let Err(e) = send_arm(
            &mut handshake.stdin,
            origin_ns,
            window_start_ns,
            window_end_ns,
        ) {
            harness_errors.push(format!("ARM write failed: {e}"));
        }
        if let Err(e) = wait_for_line(host, guards, handshake, arm_deadline, "ARMED") {
            harness_errors.push(format!(
                "endpoint startup failed: ARM handshake: {e} (stderr: {})",
                dir.join(format!("{}.stderr", handshake.label)).display()
            ));
            return finalize_record(
                spec,
                host,
                constants,
                &harness_errors,
                build_kind(),
                timing,
                None,
                None,
                None,
                None,
                None,
            );
        }
        if monotonic_now_ns() >= origin_ns {
            harness_errors.push("arm handshake exceeded the generator lead".to_string());
            return finalize_record(
                spec,
                host,
                constants,
                &harness_errors,
                build_kind(),
                timing,
                None,
                None,
                None,
                None,
                None,
            );
        }
    }

    // Spawn generator.
    let generator = match spawn_generator(
        spec,
        host,
        &dir,
        origin_ns,
        window_start_ns,
        window_end_ns,
        constants.drain_grace_secs,
        port,
    ) {
        Ok(g) => g,
        Err(e) => {
            harness_errors.push(format!("generator spawn failed: {e}"));
            return finalize_record(
                spec,
                host,
                constants,
                &harness_errors,
                build_kind(),
                timing,
                None,
                None,
                None,
                None,
                None,
            );
        }
    };
    let gen_pid = generator.id();
    guards.push(GENERATOR_LABEL, generator);

    // Wait until window.start.
    if let Err(e) = sleep_until(host, window_start_ns) {
        harness_errors.push(e);
    }

    // Enable the syscall counter at the window start. Strace spawns
    // here (per the brief) and waits for its "attached" stderr line;
    // perf writes `enable\n` and waits for the ack byte. A failed
    // prep recorded earlier on `counter.prep_error` is a no-op.
    if counting {
        if host.syscalls == SyscallMethod::Strace {
            counter = Some(spawn_strace_counter(
                host,
                &dir,
                endpoint.server_pid,
                guards,
                &mut harness_errors,
            ));
        }
        if let Some(c) = counter.as_mut() {
            arm_counter(c, host, &mut harness_errors);
        }
    }

    // CPU start sample, before the driver proof: `/proc/<pid>/fd` is a
    // directory walk, and the CPU percentage must describe the interval the
    // samples bracket, not a window the first sample already missed.
    // Mosquitto's PID is the server PID.
    let driver_pid = endpoint.server_pid;
    let server_start = read_cpu_ticks(driver_pid).ok();
    let gen_start = read_cpu_ticks(gen_pid).ok();
    let pressure_start = read_cpu_pressure_total(&host.cpu_pressure_path);
    let sample_start_ns = monotonic_now_ns();

    // Driver proof.
    let fd_dir = format!("/proc/{driver_pid}/fd");
    let driver = match count_driver_fds(Path::new(&fd_dir)) {
        Ok(d) => Some(d),
        Err(e) => {
            harness_errors.push(format!("driver fds: {e}"));
            None
        }
    };

    // Wait until window.end.
    if let Err(e) = sleep_until(host, window_end_ns) {
        harness_errors.push(e);
    }
    // Disable the syscall counter at the window end. Perf writes
    // `disable\n` and waits for the ack byte; both kinds then get SIGINT,
    // and `reap_counter` below collects the exit.
    if counting {
        if let Some(c) = counter.as_mut() {
            disarm_counter(c, guards, host, &mut harness_errors);
        }
    }
    // CPU end sample, before the counter is reaped: a counter can confirm
    // its stop on time and still spend seconds writing its summary out, and
    // a reap waited through here would add that time to `sampled_ns` and
    // the post-window ticks to both deltas, understating the saturation the
    // window actually reached.
    let server_end = read_cpu_ticks(driver_pid).ok();
    let gen_end = read_cpu_ticks(gen_pid).ok();
    let pressure_end = read_cpu_pressure_total(&host.cpu_pressure_path);
    // The observed interval, not `window_secs`: under the CPU contention
    // these runs measure, `sleep_until` can resume late and the start
    // sample can be delayed, and a percentage computed against the declared
    // window would then misstate the use the ticks prove. P-Q4 keeps CPU
    // evidence out of the validity rules, so the skew is carried, not
    // judged.
    let sampled_ns = monotonic_now_ns().saturating_sub(sample_start_ns);

    // Strace's `disabled_ns` is this reap, so it stays at the window end,
    // three `/proc` reads later.
    if counting {
        if let Some(c) = counter.as_mut() {
            reap_counter(c, guards, host, &mut harness_errors);
        }
    }

    let cpu = build_cpu_usage(
        server_start,
        server_end,
        gen_start,
        gen_end,
        sampled_ns,
        &mut harness_errors,
    );
    let host_cpu_pressure = match (pressure_start, pressure_end) {
        (Some(start), Some(end)) => Some(HostCpuPressure {
            sampled_ns,
            some_stall_us: end.saturating_sub(start),
        }),
        _ => None,
    };

    // Wait for generator to exit, with drain slack.
    let gen_deadline = Instant::now()
        + Duration::from_secs(constants.drain_grace_secs + CHILD_EXIT_SLACK_SECS);
    loop {
        let exited = guards
            .child_mut(GENERATOR_LABEL)
            .and_then(|child| child.try_wait().ok().flatten());
        if let Some(status) = exited {
            if !status.success() {
                harness_errors.push(exit_note(GENERATOR_LABEL, status));
            }
            break;
        }
        if ChildGuard::is_cancelled(host) {
            break;
        }
        if Instant::now() >= gen_deadline {
            harness_errors.push("generator missed its exit deadline".to_string());
            break;
        }
        sleep(Duration::from_millis(WAIT_POLL_MS));
    }

    // Mosquitto publishing requires the sub to drain before SIGTERM.
    if spec.server == ServerKind::Mosquitto && spec.kind != RunKind::Connections {
        sleep(Duration::from_secs(constants.drain_grace_secs));
    }

    // SIGTERM the subscriber, then the endpoint. The order is load-bearing
    // for a mosquitto publishing run, where the endpoint is mosquitto and
    // the subscriber is a `bench sub` connected to it: stopping mosquitto
    // first closes that socket, and `sub`'s read loop carries a peer close
    // out as `broker closed the connection` with exit 1
    // (`bench/src/sub.rs:345`), which would turn every reconciled mosquitto
    // run into a harness error. Signalled first, `sub` leaves through its
    // own shutdown path with exit 0 and its report written.
    //
    // A `bench` child writes its report before it fails, so a non-zero exit
    // has to become a harness error: without it a run whose endpoint failed
    // after reporting could still reconcile as valid. mosquitto's own exit
    // status is not part of that contract, so only `bench` children are
    // checked.
    let endpoint_is_bench = spec.server != ServerKind::Mosquitto;
    for (label, is_bench) in [(SUB_LABEL, true), (ENDPOINT_LABEL, endpoint_is_bench)] {
        let Some(child) = guards.child_mut(label) else {
            continue;
        };
        match terminate_and_wait(host, label, child) {
            Ok(status) if is_bench && !status.success() => {
                harness_errors.push(exit_note(label, status));
            }
            Ok(_) => {}
            Err(e) => harness_errors.push(e),
        }
    }

    // Read reports.
    let delivery_path = dir.join("endpoint.json");
    let gen_path = dir.join("gen.json");
    let needs_endpoint = spec.server != ServerKind::Mosquitto || spec.kind != RunKind::Connections;
    let delivery = read_delivery_report(&delivery_path);
    if delivery.is_none() && needs_endpoint {
        harness_errors.push(format!(
            "missing endpoint report: {}",
            delivery_path.display()
        ));
    }
    let gen_report = read_gen_report(&gen_path);
    if gen_report.is_none() {
        harness_errors.push(format!("missing gen report: {}", gen_path.display()));
    }

    // Finalize the counter: parse the raw output the stop at the window
    // end left behind, and build the `Syscalls` block. A run with no
    // counter — every kind but the counted run — records method
    // `None`, because `host.syscalls` names the tool the matrix would use
    // and this record carries no evidence from it.
    let syscalls = if let Some(c) = counter.as_mut() {
        finalize_counter(c, guards, host, &mut harness_errors)
    } else {
        Syscalls {
            method: SyscallMethod::None,
            count: None,
            per_msg: None,
            raw: None,
            enabled_ns: None,
            disabled_ns: None,
            exit: None,
            diagnostics: None,
        }
    };

    let mut record = finalize_record(
        spec,
        host,
        constants,
        &harness_errors,
        build_kind(),
        timing,
        driver,
        cpu,
        delivery,
        gen_report,
        Some(syscalls),
    );
    record.host_cpu_pressure = host_cpu_pressure;
    record
}

/// Origin and window captured at ARM time. The record stores these
/// verbatim so `derive` and `reconcile` see the same window the recorder
/// was armed with; a value computed later would point at a different
/// second and turn the timeline sum into zero.
#[derive(Debug, Clone, Copy)]
struct RunTiming {
    origin_ns: u64,
    window: Window,
}

impl Default for RunTiming {
    fn default() -> Self {
        Self {
            origin_ns: 0,
            window: Window {
                start_ns: 0,
                end_ns: 0,
            },
        }
    }
}

fn build_cpu_usage(
    server_start: Option<(u64, u64)>,
    server_end: Option<(u64, u64)>,
    gen_start: Option<(u64, u64)>,
    gen_end: Option<(u64, u64)>,
    sampled_ns: u64,
    harness_errors: &mut Vec<String>,
) -> Option<CpuUsage> {
    // No assumed tick rate: `cpu_percent` divides the recorded ticks by
    // `clk_tck`, so a guessed value would turn an unread probe into a wrong
    // CPU percentage on a host whose rate differs. The probe failure drops
    // the CPU evidence and is recorded as a harness error instead.
    let clk = nix::unistd::sysconf(nix::unistd::SysconfVar::CLK_TCK)
        .ok()
        .flatten()
        .and_then(|v| u64::try_from(v).ok())
        .filter(|v| *v > 0);
    let Some(clk) = clk else {
        harness_errors
            .push("cpu ticks unreadable: sysconf(_SC_CLK_TCK) gave no tick rate".to_string());
        return None;
    };
    match (server_start, server_end, gen_start, gen_end) {
        (Some((a, b)), Some((c, d)), Some((e, f)), Some((g, h))) => Some(CpuUsage {
            clk_tck: clk,
            sampled_ns,
            server_ticks: (c + d).saturating_sub(a + b),
            generator_ticks: (g + h).saturating_sub(e + f),
        }),
        (Some(_), Some(_), _, _) | (_, _, Some(_), Some(_)) => {
            harness_errors.push("cpu ticks unreadable: partial samples".to_string());
            None
        }
        _ => {
            harness_errors.push("cpu ticks unreadable: missing /proc".to_string());
            None
        }
    }
}

fn build_kind() -> Build {
    if cfg!(debug_assertions) {
        Build::Debug
    } else {
        Build::Release
    }
}

#[allow(clippy::too_many_arguments)]
fn finalize_record(
    spec: &RunSpec,
    host: &HostSetup,
    constants: ProtocolConstants,
    harness_errors: &[String],
    build: Build,
    timing: RunTiming,
    driver: Option<DriverFds>,
    cpu: Option<CpuUsage>,
    delivery: Option<DeliveryReport>,
    gen_report: Option<crate::gen::GenReport>,
    syscalls: Option<Syscalls>,
) -> RunRecord {
    let now = started_at_unix();
    // Use the origin/window captured at ARM time so the record's
    // `window` matches the second the recorder was armed with.
    // A fresh `harness_default_origin` would point at a different
    // second and turn `derive`'s timeline sum into zero.
    let (origin_ns, window) = if timing.origin_ns == 0 {
        let fallback = harness_default_origin(constants);
        let win = Window {
            start_ns: fallback + constants.warmup_secs * ONE_SECOND_NS,
            end_ns: fallback + (constants.warmup_secs + constants.window_secs) * ONE_SECOND_NS,
        };
        (fallback, win)
    } else {
        (timing.origin_ns, timing.window)
    };
    // The two reports default independently: a mosquitto connections run has
    // no endpoint report by design (decision Q7), so pairing them would
    // discard the generator's `cycles_in_window` and leave the only metric
    // that mode measures at zero.
    let delivery = delivery.unwrap_or_else(empty_delivery_report);
    let gen = gen_report.unwrap_or_else(empty_gen_report);

    let dropped_v = delivery.dropped.unwrap_or(0);
    let unresolved = gen
        .offered
        .saturating_sub(delivery.delivered)
        .saturating_sub(dropped_v);

    let counts = Counts {
        scheduled: gen.scheduled,
        window_scheduled: gen.window_scheduled,
        offered: gen.offered,
        acked: gen.acked,
        delivered: delivery.delivered,
        dropped: delivery.dropped,
        unresolved,
        duplicates: delivery.duplicates,
        foreign: delivery.foreign,
        latency_over_range: delivery.latency_over_range,
        clock_anomalies: delivery.clock_anomalies,
    };
    let gen_errors = GeneratorErrors {
        connect: gen.connect_errors,
        protocol: gen.protocol_errors,
        missed_slots: gen.missed_slots,
        stalled: gen.stalled,
        lateness_over_range: gen.lateness_over_range,
    };
    // The endpoint's raw histogram is the record's only latency
    // observation, and `derive` reads it back out of the record to compute
    // the percentiles, so it has to be installed before that call.
    let latency_ns = if spec.kind.is_open_loop() {
        Some(LatencySummary {
            p50: 0,
            p99: 0,
            max: 0,
            histogram: delivery.latency,
        })
    } else {
        None
    };
    let mut record = RunRecord {
        schema: SCHEMA_VERSION,
        profile: spec.profile,
        server: spec.server,
        workers: spec.workers,
        qos: spec.qos,
        kind: spec.kind,
        rate_fraction_pct: spec.rate_fraction_pct,
        offered_rate: spec.offered_rate,
        repetition: spec.repetition,
        attempt: spec.attempt,
        connections: CONNECTIONS,
        payload_bytes: u32::try_from(PAYLOAD_BYTES).expect("PAYLOAD_BYTES fits"),
        qos1_window: QOS1_WINDOW,
        warmup_secs: constants.warmup_secs,
        window_secs: constants.window_secs,
        drain_grace_secs: constants.drain_grace_secs,
        build,
        origin_ns,
        window,
        timeline: delivery.timeline,
        cycles_in_window: gen.cycles_in_window,
        counts,
        throughput_msgs_per_sec: None,
        last10s_msgs_per_sec: None,
        latency_ns,
        lateness: gen.lateness,
        lateness_p99_ns: None,
        last_write_start_ns: gen.last_write_start_ns,
        connections_per_sec: None,
        generator_errors: gen_errors,
        syscalls: syscalls.unwrap_or(Syscalls {
            method: if should_count(spec, host) {
                host.syscalls
            } else {
                SyscallMethod::None
            },
            count: None,
            per_msg: None,
            raw: None,
            enabled_ns: None,
            disabled_ns: None,
            exit: None,
            diagnostics: None,
        }),
        driver,
        cpu_sets: host.cpu_sets.clone(),
        env: host.env.clone(),
        cpu,
        host_cpu_pressure: None,
        started_at_unix: now,
        harness_errors: harness_errors.to_vec(),
        valid: false,
        invalid_reasons: Vec::new(),
        cancelled: false,
    };
    let derived = derive(&record);
    record.throughput_msgs_per_sec = derived.throughput_msgs_per_sec;
    record.last10s_msgs_per_sec = derived.last10s_msgs_per_sec;
    record.connections_per_sec = derived.connections_per_sec;
    record.lateness_p99_ns = derived.lateness_p99_ns;
    record.latency_ns = derived.latency_ns;
    record.syscalls.per_msg = derived.syscalls_per_msg;

    let reasons = reconcile(&record);
    record.invalid_reasons = reasons;
    record.valid = record.invalid_reasons.is_empty();

    record
}

/// What a child that wrote no report contributes to the record the runner
/// still has to produce: every count zero, every optional field absent. The
/// matching `missing … report` harness error is what invalidates the run.
fn empty_delivery_report() -> DeliveryReport {
    DeliveryReport {
        delivered: 0,
        duplicates: 0,
        foreign: 0,
        latency_over_range: 0,
        clock_anomalies: 0,
        dropped: None,
        timeline: crate::record::Timeline {
            origin_sec: 0,
            counts: Vec::new(),
        },
        latency: Vec::new(),
    }
}

/// [`empty_delivery_report`] for the generator side.
fn empty_gen_report() -> crate::gen::GenReport {
    crate::gen::GenReport {
        scheduled: None,
        window_scheduled: None,
        offered: 0,
        acked: None,
        missed_slots: 0,
        connect_errors: 0,
        protocol_errors: 0,
        stalled: 0,
        lateness_over_range: 0,
        last_write_start_ns: None,
        cycles_in_window: None,
        lateness: Vec::new(),
    }
}

// Helper: when harness errors prevent computing the origin from a real
// window, use the run's `now` rounded up to the next whole second.
fn harness_default_origin(constants: ProtocolConstants) -> u64 {
    let now = monotonic_now_ns();
    let lead_ns = (constants.warmup_secs + constants.window_secs + GEN_LEAD_SECS + 1)
        .saturating_mul(ONE_SECOND_NS);
    let target = now.saturating_add(lead_ns);
    if target.is_multiple_of(ONE_SECOND_NS) {
        target
    } else {
        (target / ONE_SECOND_NS + 1) * ONE_SECOND_NS
    }
}

// The handoff `start_probe` cannot make atomic is only reachable from
// inside the module: the probe is in a private channel and the window
// between `Command::spawn` returning and the caller's receive is the
// scheduler's to choose. These tests drive [`abandon_startup`] directly, so
// the window is taken rather than waited for. Every other probe behaviour
// is covered from `bench/tests/runner.rs`.
#[cfg(test)]
mod tests {
    use super::{
        abandon_startup, build_minimal_record, record_survivors, report_within, spawn_counter_stderr_reader,
        start_time_in, survivor_resume_refusal, terminate_within, wait_for_strace_attach, Arc, Child,
        ChildGuard, Command, CommandExt, CpuSets, Duration, Env, HostSetup, Instant, PathBuf, Pid,
        Profile, RunKind, RunSpec, Signal, Stdio, ServerKind, StdoutLine, SyscallMethod, SystemTime,
        UNIX_EPOCH, proc_start_time, COUNTER_DIAGNOSTIC_LINES_MAX, COUNTER_STDERR_LINE_BYTES_MAX,
        GUARD_SIGKILL_REAP_TIMEOUT_SECS, GUARD_REPORT_TIMEOUT_SECS, GIT_HEAD_PROBE,
        PROBE_OWN_PROCESS_GROUP, StderrLoss, SURVIVOR_START_UNKNOWN, WAIT_POLL_MS,
    };
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use std::io::{BufRead, BufReader, Write};
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
    use std::sync::mpsc;
    use std::sync::Mutex;
    use std::thread::sleep;

    /// Long enough that the fixture probe is certainly still running when
    /// the give-up cleans it up.
    const PROBE_SLEEP_SECS: u64 = 60;

    /// Slack a cleanup assertion grants the bound for the host's scheduling.
    const REAP_SLACK_SECS: u64 = 2;

    /// Lifetime of a fixture child whose SIGKILL the stand-in refuses: longer
    /// than the SIGTERM slack, the reap bound and the assertion slack together,
    /// so it is certainly still running when the bound ends.
    const UNKILLED_FIXTURE_SECS: u64 =
        TERM_SLACK_SECS + GUARD_SIGKILL_REAP_TIMEOUT_SECS + REAP_SLACK_SECS + 1;

    /// Poll interval of the reap assertions.
    const REAP_POLL: Duration = Duration::from_millis(20);

    /// SIGTERM slack the `terminate_within` test grants in place of the run's 30 s.
    const TERM_SLACK_SECS: u64 = 1;

    #[test]
    fn abandon_startup_cleans_up_a_probe_that_finished_starting() {
        // `Command::spawn` returned and its rendezvous send is waiting while
        // the caller is descheduled, and the deadline or `cancel` lands in
        // that window. This probe has a pid, so the give-up must signal and
        // reap it itself: Q5's exception covers only a probe the runner
        // cannot signal, and the starting thread's own cleanup does not
        // survive the CLI's exit.
        let child = Command::new("sleep")
            .arg(PROBE_SLEEP_SECS.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(PROBE_OWN_PROCESS_GROUP)
            .spawn()
            .expect("the fixture probe must start");
        let pid = i32::try_from(child.id()).expect("the fixture pid must fit an i32");

        // Capacity 1 stands in for the zero-capacity channel's waiting
        // send: either way `try_recv` has a started probe to take.
        let (started_tx, started_rx) = mpsc::sync_channel::<Result<Child, String>>(1);
        started_tx.send(Ok(child)).expect("the fixture handoff");

        let mut signalled = Vec::new();
        let refusal = abandon_startup(
            GIT_HEAD_PROBE,
            &started_rx,
            "cancelled".to_string(),
            &mut |target: Pid, signal: Signal| {
                signalled.push((target, signal));
                kill(target, signal)
            },
        );

        assert_eq!(
            refusal, "cancelled",
            "a completed cleanup must leave the refusal exactly as AC-16 has it"
        );
        assert_eq!(
            signalled,
            vec![
                (Pid::from_raw(-pid), Signal::SIGKILL),
                (Pid::from_raw(pid), Signal::SIGKILL),
            ],
            "the give-up must SIGKILL the probe's group and then its own pid"
        );
        assert_eq!(
            kill(Pid::from_raw(pid), None),
            Err(Errno::ESRCH),
            "the give-up must reap the probe it signalled"
        );
    }

    #[test]
    fn abandon_startup_names_a_startup_with_no_pid_in_the_refusal_it_returns() {
        // A startup still inside `Command::spawn` has no probe to take and
        // no pid to signal, so the condition travels in the returned error
        // — never through a write to stderr of its own, which a full pipe
        // would hold past the deadline this give-up exists to keep.
        let (started_tx, started_rx) = mpsc::sync_channel::<Result<Child, String>>(1);
        let mut signalled = 0_u32;
        let refusal = abandon_startup(
            GIT_HEAD_PROBE,
            &started_rx,
            "cancelled".to_string(),
            &mut |_: Pid, _: Signal| {
                signalled += 1;
                Ok(())
            },
        );
        drop(started_tx);

        assert!(
            refusal.starts_with("cancelled"),
            "AC-16's refusal must stay at the front of the error: {refusal}"
        );
        assert!(
            refusal.contains("had not finished starting"),
            "the error must name the startup it gave up on: {refusal}"
        );
        assert_eq!(
            signalled, 0,
            "a probe that has no pid yet must not be signalled"
        );
    }

    #[test]
    fn end_all_bounds_and_reports_a_child_its_sigkill_cannot_reach() {
        let child = Command::new("sleep")
            .arg(UNKILLED_FIXTURE_SECS.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the fixture child must start");
        let pid_u = child.id();
        let pid = i32::try_from(pid_u).expect("pid fits in i32");

        let mut guards = ChildGuard::with_signal(|_: Pid, _: Signal| Err(Errno::EPERM));
        guards.push("endpoint", child);
        let start = proc_start_time(pid_u).expect("fixture start time");

        let started = Instant::now();
        let errors = guards.end_all();
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_secs(GUARD_SIGKILL_REAP_TIMEOUT_SECS + REAP_SLACK_SECS),
            "end_all must give up on a SIGKILL it cannot send within {GUARD_SIGKILL_REAP_TIMEOUT_SECS}s",
        );
        assert_eq!(errors.len(), 1, "exactly one survivor error is expected");
        let expected = format!(
            "endpoint cleanup: pid {pid_u} (start {start}) still running {GUARD_SIGKILL_REAP_TIMEOUT_SECS}s after SIGKILL"
        );
        assert!(
            errors[0].starts_with(&expected),
            "the survivor must name label, pid, start time and bound; got: {}",
            errors[0]
        );
        assert!(
            errors[0].contains("SIGKILL: EPERM"),
            "the survivor must carry the refused SIGKILL: {}",
            errors[0]
        );

        // The reaper thread now owns the wait; a zombie answers `kill(pid, None)`
        // with `Ok`, so only its reap ends the loop.
        let reap_deadline = Instant::now()
            + Duration::from_secs(UNKILLED_FIXTURE_SECS + REAP_SLACK_SECS);
        loop {
            if let Err(Errno::ESRCH) = kill(Pid::from_raw(pid), None) {
                break;
            }
            assert!(
                Instant::now() < reap_deadline,
                "the reaper thread did not reap the survivor before the fixture exits"
            );
            sleep(REAP_POLL);
        }
    }

    #[test]
    fn end_all_kills_and_reaps_a_running_child() {
        let child = Command::new("sleep")
            .arg(60.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the fixture child must start");
        let pid_u = child.id();
        let pid = i32::try_from(pid_u).expect("pid fits in i32");

        let mut guards = ChildGuard::with_signal(kill::<Signal>);
        guards.push("sub", child);

        let errors = guards.end_all();
        assert!(errors.is_empty(), "killable child must end without an error");

        assert_eq!(
            kill(Pid::from_raw(pid), None),
            Err(Errno::ESRCH),
            "a child the guard killed must be reaped by the guard"
        );
    }

    #[test]
    fn end_all_ends_a_killable_child_behind_one_its_sigkill_cannot_reach() {
        // A run holds an endpoint, a subscriber, a generator and a counter.
        // A child the SIGKILL cannot reach must not keep the guard from
        // ending the children pushed behind it: the guard reports that one
        // child and still kills and reaps the rest, so a bounded give-up
        // never leaves an orphan beside the next run.
        static REFUSED_PID: AtomicI32 = AtomicI32::new(0);
        fn refuse_one_pid(pid: Pid, signal: Signal) -> nix::Result<()> {
            if pid.as_raw() == REFUSED_PID.load(Ordering::SeqCst) {
                Err(Errno::EPERM)
            } else {
                kill(pid, signal)
            }
        }

        let unreachable = Command::new("sleep")
            .arg(UNKILLED_FIXTURE_SECS.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the unreachable fixture child must start");
        let unreachable_pid_u = unreachable.id();
        let unreachable_pid = i32::try_from(unreachable_pid_u).expect("pid fits in i32");
        REFUSED_PID.store(unreachable_pid, Ordering::SeqCst);
        // Longer than the bound the guard spends on the child ahead of it,
        // so it is certainly still running when the guard reaches it.
        let killable = Command::new("sleep")
            .arg(UNKILLED_FIXTURE_SECS.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the killable fixture child must start");
        let killable_pid = i32::try_from(killable.id()).expect("pid fits in i32");

        let mut guards = ChildGuard::with_signal(refuse_one_pid);
        guards.push("endpoint", unreachable);
        guards.push("sub", killable);
        let start = proc_start_time(unreachable_pid_u).expect("fixture start time");

        let started = Instant::now();
        let errors = guards.end_all();
        let elapsed = started.elapsed();
        let killable_state = kill(Pid::from_raw(killable_pid), None);

        // Tear down before asserting: the fixture the stand-in refused is
        // finite, so the test waits for that exit and for the reap the
        // reaper thread owns. Neither child is signalled from here: the
        // reaper is free to free the survivor's pid number, the guard has
        // already reaped the killable child, and either pid may name another
        // process by now.
        let reap_deadline =
            Instant::now() + Duration::from_secs(UNKILLED_FIXTURE_SECS + REAP_SLACK_SECS);
        let reaped = loop {
            if proc_start_time(unreachable_pid_u) != Some(start) {
                break true;
            }
            if Instant::now() >= reap_deadline {
                break false;
            }
            sleep(REAP_POLL);
        };

        assert!(
            elapsed < Duration::from_secs(GUARD_SIGKILL_REAP_TIMEOUT_SECS + REAP_SLACK_SECS),
            "one survivor must cost one bound of {GUARD_SIGKILL_REAP_TIMEOUT_SECS}s, not one per child",
        );
        assert_eq!(
            errors.len(),
            1,
            "only the child the SIGKILL could not reach is reported: {errors:?}"
        );
        let expected = format!(
            "endpoint cleanup: pid {unreachable_pid_u} (start {start}) still running {GUARD_SIGKILL_REAP_TIMEOUT_SECS}s after SIGKILL"
        );
        assert!(
            errors[0].starts_with(&expected),
            "the survivor error must name the child the guard could not end; got: {}",
            errors[0]
        );
        assert_eq!(
            killable_state,
            Err(Errno::ESRCH),
            "the child pushed behind the survivor must still be killed and reaped"
        );
        assert!(
            reaped,
            "the reaper thread must reap the survivor once the fixture exits"
        );
    }

    #[test]
    fn end_all_sends_no_signal_to_a_child_std_already_reaped() {
        static SIGNALLED: AtomicBool = AtomicBool::new(false);
        #[allow(clippy::unnecessary_wraps)]
        fn record_signal(_: Pid, _: Signal) -> nix::Result<()> {
            SIGNALLED.store(true, Ordering::SeqCst);
            Ok(())
        }

        let mut child = Command::new("true")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the fixture child must start");
        let _ = child.wait().expect("reap the quick child first");

        let mut guards = ChildGuard::with_signal(record_signal);
        guards.push("generator", child);

        let errors = guards.end_all();
        assert!(errors.is_empty(), "an already-reaped child must not error");
        assert!(
            !SIGNALLED.load(Ordering::SeqCst),
            "the stand-in must not be called for a reaped child"
        );
    }

    #[test]
    fn record_survivors_returns_a_run_whose_children_all_ended_as_it_came() {
        let child = Command::new("sleep")
            .arg(60.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the fixture child must start");
        let pid_u = child.id();
        let pid = i32::try_from(pid_u).expect("pid fits in i32");

        let mut guards = ChildGuard::with_signal(kill::<Signal>);
        guards.push("sub", child);

        let spec = RunSpec {
            profile: Profile::Smoke,
            server: ServerKind::Uring,
            workers: 1,
            qos: Some(1),
            kind: RunKind::Throughput,
            rate_fraction_pct: None,
            offered_rate: None,
            repetition: 1,
            attempt: 1,
        };
        let mut record = build_minimal_record(&spec);
        record.harness_errors = Vec::new();
        record.invalid_reasons = Vec::new();
        record.valid = true;

        let out = record_survivors(record, &mut guards);

        assert!(out.harness_errors.is_empty(), "no survivor means no errors");
        assert!(out.invalid_reasons.is_empty(), "no survivor means no reasons");
        assert!(out.valid, "an unchanged valid record stays valid");
        assert_eq!(
            kill(Pid::from_raw(pid), None),
            Err(Errno::ESRCH),
            "the child's exit is reaped through `end_child`"
        );
    }

    #[test]
    fn terminate_within_bounds_and_reports_a_child_that_outlives_sigterm_and_sigkill() {
        fn refuse_sigkill(pid: Pid, signal: Signal) -> nix::Result<()> {
            if signal == Signal::SIGKILL {
                Err(Errno::EPERM)
            } else {
                kill(pid, signal)
            }
        }

        // The stand-in refuses the cleanup's SIGKILL, so the fixture child
        // outlives `terminate_within` and is still the caller's. `Drop`
        // takes it back with a real SIGKILL and reaps it, so an assertion
        // that fails cannot leave it running beside the tests that follow.
        struct Fixture(Child);

        impl Drop for Fixture {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg(format!(
                "trap '' TERM; echo ready; exec sleep {UNKILLED_FIXTURE_SECS}"
            ))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut fixture = Fixture(cmd.spawn().expect("the fixture child must start"));
        let pid_u = fixture.0.id();

        // The trap is installed once `ready` arrives; an ignored signal stays
        // ignored across `exec`.
        let stdout = fixture.0.stdout.take().expect("piped stdout");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        reader.read_line(&mut line).expect("read ready line");
        let start = proc_start_time(pid_u).expect("fixture start time");

        let started = Instant::now();
        let err = terminate_within(
            &AtomicBool::new(false),
            "endpoint",
            &mut fixture.0,
            TERM_SLACK_SECS,
            refuse_sigkill,
        )
        .expect_err("a child that ignores SIGTERM and refuses SIGKILL must error");
        let elapsed = started.elapsed();

        assert!(
            elapsed
                < Duration::from_secs(TERM_SLACK_SECS + GUARD_SIGKILL_REAP_TIMEOUT_SECS + REAP_SLACK_SECS),
            "terminate_within must bound its wait by slack + GUARD_SIGKILL_REAP_TIMEOUT_SECS"
        );
        let expected = format!(
            "endpoint did not exit within {TERM_SLACK_SECS}s of SIGTERM; endpoint cleanup: pid {pid_u} (start {start}) still running {GUARD_SIGKILL_REAP_TIMEOUT_SECS}s after SIGKILL"
        );
        assert!(
            err.starts_with(&expected),
            "the error must name the slack, the label, pid, start time and bound; got: {err}"
        );
        assert!(
            err.contains("SIGKILL: EPERM"),
            "the error must carry the refused SIGKILL: {err}"
        );
        assert_eq!(
            fixture.0.try_wait().expect("try_wait"),
            None,
            "the child is still the caller's, unreaped"
        );
    }

    #[test]
    fn terminate_within_returns_the_status_of_a_child_that_exits_on_sigterm() {
        // The bounded give-up must not cost a healthy child its SIGTERM
        // path: a child that exits on the signal is reaped and its status
        // returned. Only the deadline branch answers `Err`, so an `Ok` here
        // is proof the wait ended on the exit and not on the slack.
        let mut child = Command::new("sleep")
            .arg(PROBE_SLEEP_SECS.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the fixture child must start");
        let pid = i32::try_from(child.id()).expect("pid fits in i32");

        let status = terminate_within(
            &AtomicBool::new(false),
            "sub",
            &mut child,
            TERM_SLACK_SECS,
            kill::<Signal>,
        );
        let reaped = kill(Pid::from_raw(pid), None);

        let status = status.expect("a child that exits on SIGTERM must return its status");
        assert!(
            !status.success(),
            "a child killed by SIGTERM did not exit cleanly: {status:?}"
        );
        assert_eq!(
            reaped,
            Err(Errno::ESRCH),
            "the child must be reaped by the wait, not left a zombie"
        );
    }

    #[test]
    fn report_within_gives_up_on_a_writer_that_never_accepts() {
        struct Stalled;
        impl Write for Stalled {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                sleep(Duration::from_secs(UNKILLED_FIXTURE_SECS));
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let started = Instant::now();
        report_within(
            vec![
                "endpoint cleanup: pid 1 (start 1) still running 10s after SIGKILL".to_string(),
            ],
            Stalled,
            Duration::from_secs(GUARD_REPORT_TIMEOUT_SECS),
        );
        let elapsed = started.elapsed();

        assert!(
            elapsed
                < Duration::from_secs(GUARD_REPORT_TIMEOUT_SECS + REAP_SLACK_SECS),
            "report_within must bound its wait on the writer thread"
        );
    }

    #[test]
    fn report_within_writes_every_report_to_a_writer_that_accepts() {
        struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl Write for Captured {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                let mut g = self.0.lock().expect("lock");
                g.extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        report_within(
            vec!["a".to_string(), "b".to_string()],
            Captured(shared.clone()),
            Duration::from_secs(GUARD_REPORT_TIMEOUT_SECS),
        );

        assert_eq!(*shared.lock().expect("lock"), b"a\nb\n".to_vec());
    }

    /// A `/proc/<pid>/stat` line with `comm` set to `comm` and the start
    /// time set to `start`, in the kernel's field order. Fields 4 to 21 and
    /// the two behind the start time carry values that are distinct from
    /// one another, so the fixture pins field 22 on its own and a parser
    /// that read a neighbour would answer one of them instead of `start`.
    fn stat_line(comm: &[u8], start: u64) -> Vec<u8> {
        let mut line = b"4242 (".to_vec();
        line.extend_from_slice(comm);
        // `4242` is field 1, `comm` field 2, `S` field 3, then fields 4 to
        // 21, then the start time in field 22 and two fields behind it.
        line.extend_from_slice(b") S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18");
        line.extend_from_slice(format!(" {start} 19 20\n").as_bytes());
        line
    }

    /// A fresh path for one stat fixture, under the host's temp directory.
    fn stat_fixture(tag: &str, body: &[u8]) -> String {
        let path = std::env::temp_dir().join(format!(
            "bench-runner-stat-{tag}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
        std::fs::write(&path, body).expect("the stat fixture must be written");
        path.to_str().expect("the fixture path must be UTF-8").to_string()
    }

    // A process can set its own name to bytes that are not UTF-8 and keep
    // running with the start time a survivor entry recorded. Decoding the
    // whole line would fail there, and a failure read as "the process
    // ended" would let `--resume` measure beside that survivor.
    #[test]
    fn a_start_time_is_read_beside_a_process_name_that_is_not_utf8() {
        const START: u64 = 987_654;

        let path = stat_fixture("not-utf8", &stat_line(b"sl\xffeep (x)", START));
        let read = start_time_in(&path);
        let _ = std::fs::remove_file(&path);

        assert_eq!(read, Ok(Some(START)));
    }

    // Only a missing `/proc` entry proves the pid is free; a stat line the
    // parser cannot use is an inspection failure, not an ended process.
    #[test]
    fn an_unusable_stat_line_is_a_failure_and_not_an_ended_process() {
        let path = stat_fixture("unusable", b"4242 (sleep) S no fields here\n");
        let read = start_time_in(&path);
        let _ = std::fs::remove_file(&path);

        assert!(read.is_err(), "an unusable stat line must not answer Ok: {read:?}");
        assert_eq!(start_time_in(&path), Ok(None), "a missing stat frees the pid");
    }

    // The producer writes a number or `SURVIVOR_START_UNKNOWN`; any other
    // token means the entry cannot be trusted to name the child, so the
    // resume is refused rather than permitted as an unknown start time.
    #[test]
    fn a_survivor_start_token_that_does_not_parse_refuses_the_resume() {
        // `/proc` has no entry for pid 0, so the pid alone cannot refuse.
        const UNHELD_PID: u32 = 0;

        for token in ["invalid", "99999999999999999999999999", "-1", ""] {
            let entry = format!(
                "endpoint cleanup: pid {UNHELD_PID} (start {token}) still running 10s after SIGKILL"
            );
            assert!(
                survivor_resume_refusal(&entry).is_some(),
                "start token {token:?} must refuse the resume"
            );
        }

        for token in ["12345", SURVIVOR_START_UNKNOWN] {
            let entry = format!(
                "endpoint cleanup: pid {UNHELD_PID} (start {token}) still running 10s after SIGKILL"
            );
            assert_eq!(
                survivor_resume_refusal(&entry),
                None,
                "start token {token:?} names no live process"
            );
        }
    }

    /// Lines the flood fixture writes: `seq 0 19999` is 108 890 bytes, past
    /// the 64 KiB pipe buffer, so the fixture cannot exit before the
    /// reader has taken far more than the queue bound.
    const READER_FLOOD_LINES: usize = 20_000;

    /// Time the flood fixture and then the reader thread get to finish; a
    /// reader that blocks on its queue never lets either finish.
    const READER_FLOOD_BOUND: Duration = Duration::from_secs(10);

    /// Lines ahead of the attach marker — past `COUNTER_DIAGNOSTIC_LINES_MAX`.
    const ATTACH_FLOOD_LINES: usize = 300;

    /// Build a `HostSetup` for the in-crate reader / marker-wait tests.
    /// Only `syscalls` and `cancel` are read by these paths; every other
    /// field stays empty.
    fn counter_fixture_host() -> HostSetup {
        HostSetup {
            bench_exe: PathBuf::new(),
            mosquitto_bin: None,
            syscalls: SyscallMethod::Strace,
            perf_bin: None,
            strace_bin: None,
            cpu_sets: CpuSets {
                server: String::new(),
                generator: String::new(),
            },
            env: Env {
                kernel: String::new(),
                cpu_model: String::new(),
                host: String::new(),
                git_sha: String::new(),
                git_dirty: false,
                tools: Vec::new(),
            },
            scratch_dir: PathBuf::new(),
            cpu_pressure_path: PathBuf::new(),
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Wait for a child to exit and then for the reader thread to drop its
    /// `StderrLoss` clone, so the reader has finished and will queue
    /// nothing more. On timeout the child is killed and the test panics
    /// with `what` naming which wait timed out.
    fn wait_for_reader_done(child: &mut Child, loss: &Arc<Mutex<StderrLoss>>, what: &str) {
        let deadline = Instant::now() + READER_FLOOD_BOUND;
        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) => {
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("child did not exit before {READER_FLOOD_BOUND:?} while waiting for {what}");
                    }
                    sleep(Duration::from_millis(WAIT_POLL_MS));
                }
                Err(e) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("try_wait while waiting for {what}: {e}");
                }
            }
        }
        loop {
            if Arc::strong_count(loss) == 1 {
                return;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("reader thread did not finish before {READER_FLOOD_BOUND:?} while waiting for {what}");
            }
            sleep(Duration::from_millis(WAIT_POLL_MS));
        }
    }

    #[test]
    fn counter_stderr_reader_queues_only_the_first_lines_it_can_keep() {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(format!("seq 0 {} >&2", READER_FLOOD_LINES - 1))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn flood fixture");
        let stderr = child.stderr.take().expect("piped stderr");
        let (rx, loss) = spawn_counter_stderr_reader(stderr).expect("spawn probe");

        wait_for_reader_done(&mut child, &loss, "flood fixture exit");

        let queued: Vec<String> = rx
            .try_iter()
            .map(|m| match m {
                StdoutLine::Line(l) => l,
                StdoutLine::Eof => "<eof>".to_string(),
                StdoutLine::Failed(e) => format!("<failed {e}>"),
            })
            .collect();
        assert_eq!(
            queued.len(),
            COUNTER_DIAGNOSTIC_LINES_MAX,
            "queue must hold exactly COUNTER_DIAGNOSTIC_LINES_MAX lines; got {queued:?}"
        );
        assert_eq!(queued[0], "0\n", "queue head");
        assert_eq!(
            queued[COUNTER_DIAGNOSTIC_LINES_MAX - 1],
            format!("{}\n", COUNTER_DIAGNOSTIC_LINES_MAX - 1),
            "queue tail"
        );
        let locked = loss.lock().expect("stderr loss poisoned");
        assert!(locked.dropped, "a dropped line must be recorded");
        assert!(
            locked.failure.is_none(),
            "a flood the reader read to the end must not record a failure; got {:?}",
            locked.failure
        );
    }

    #[test]
    fn counter_stderr_reader_fails_a_line_one_byte_past_the_cap() {
        struct Fixture {
            script: &'static str,
            label: &'static str,
            no_eol_after: bool,
            exact_cap_then_eof: bool,
            non_utf8: bool,
        }
        let fixtures: [Fixture; 4] = [
            Fixture {
                script:
                    "head -c 4096 /dev/zero | tr '\\0' x >&2; echo >&2; \
                     head -c 4097 /dev/zero | tr '\\0' x >&2; echo >&2",
                label: "4097-byte line after a 4096-byte line",
                no_eol_after: false,
                exact_cap_then_eof: false,
                non_utf8: false,
            },
            Fixture {
                script: "head -c 4096 /dev/zero | tr '\\0' x >&2",
                label: "4096-byte line, EOF-terminated",
                no_eol_after: false,
                exact_cap_then_eof: true,
                non_utf8: false,
            },
            Fixture {
                script: "head -c 4097 /dev/zero | tr '\\0' x >&2",
                label: "4097-byte line, EOF-terminated",
                no_eol_after: true,
                exact_cap_then_eof: false,
                non_utf8: false,
            },
            Fixture {
                script: "printf '\\377\\n' >&2",
                label: "non-UTF-8 line",
                no_eol_after: false,
                exact_cap_then_eof: false,
                non_utf8: true,
            },
        ];

        for fixture in &fixtures {
            let mut child = Command::new("sh")
                .arg("-c")
                .arg(fixture.script)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn fixture");
            let stderr = child.stderr.take().expect("piped stderr");
            let (rx, loss) =
                spawn_counter_stderr_reader(stderr).expect("spawn probe");

            wait_for_reader_done(&mut child, &loss, fixture.label);

            let queued: Vec<StdoutLine> = rx.try_iter().collect();
            let failure_text = format!("line longer than {COUNTER_STDERR_LINE_BYTES_MAX} bytes");
            if fixture.non_utf8 {
                assert_eq!(
                    queued.len(),
                    1,
                    "[{}] exactly one Failed message expected; got {:?}",
                    fixture.label,
                    queued
                );
                assert!(
                    matches!(&queued[0], StdoutLine::Failed(e) if *e == "stream did not contain valid UTF-8"),
                    "[{}] expected Failed(UTF-8); got {:?}",
                    fixture.label,
                    queued[0]
                );
                assert_eq!(
                    loss.lock().expect("stderr loss poisoned").failure.as_deref(),
                    Some("stream did not contain valid UTF-8"),
                    "[{}] failure recorded",
                    fixture.label
                );
            } else if fixture.exact_cap_then_eof {
                assert_eq!(
                    queued.len(),
                    2,
                    "[{}] Line then Eof expected; got {:?}",
                    fixture.label,
                    queued
                );
                let line = match &queued[0] {
                    StdoutLine::Line(s) => s,
                    other => panic!("[{}] expected Line, got {:?}", fixture.label, other),
                };
                assert_eq!(
                    line.len() as u64,
                    COUNTER_STDERR_LINE_BYTES_MAX,
                    "[{}] exact-cap line length",
                    fixture.label
                );
                assert_eq!(queued[1], StdoutLine::Eof, "[{}] trailing Eof", fixture.label);
                assert!(
                    loss.lock().expect("stderr loss poisoned").failure.is_none(),
                    "[{}] no failure recorded",
                    fixture.label
                );
            } else if fixture.no_eol_after {
                assert_eq!(
                    queued.len(),
                    1,
                    "[{}] exactly one Failed message expected; got {:?}",
                    fixture.label,
                    queued
                );
                assert!(
                    matches!(&queued[0], StdoutLine::Failed(e) if *e == failure_text),
                    "[{}] expected Failed({:?}); got {:?}",
                    fixture.label,
                    failure_text,
                    queued[0]
                );
                assert_eq!(
                    loss.lock().expect("stderr loss poisoned").failure.as_deref(),
                    Some(failure_text.as_str()),
                    "[{}] failure recorded",
                    fixture.label
                );
            } else {
                assert_eq!(
                    queued.len(),
                    2,
                    "[{}] Line then Failed expected; got {:?}",
                    fixture.label,
                    queued
                );
                let first = match &queued[0] {
                    StdoutLine::Line(s) => s,
                    other => panic!("[{}] expected Line, got {:?}", fixture.label, other),
                };
                assert_eq!(
                    first.len(),
                    usize::try_from(COUNTER_STDERR_LINE_BYTES_MAX + 1)
                        .expect("line cap fits in usize"),
                    "[{}] exact-cap-plus-newline line length",
                    fixture.label
                );
                assert!(
                    matches!(&queued[1], StdoutLine::Failed(e) if *e == failure_text),
                    "[{}] expected Failed({:?}); got {:?}",
                    fixture.label,
                    failure_text,
                    queued[1]
                );
                assert_eq!(
                    loss.lock().expect("stderr loss poisoned").failure.as_deref(),
                    Some(failure_text.as_str()),
                    "[{}] failure recorded",
                    fixture.label
                );
            }
        }
    }

    #[test]
    fn strace_attach_wait_keeps_only_the_first_lines_it_can_keep() {
        let (tx, rx) = mpsc::channel();
        for i in 0..ATTACH_FLOOD_LINES {
            tx.send(StdoutLine::Line(format!("noise {i}\n")))
                .expect("send noise");
        }
        tx.send(StdoutLine::Line(
            "strace: Process 7 attached\n".to_string(),
        ))
        .expect("send marker");
        drop(tx);

        let mut errors = Vec::new();
        let host = counter_fixture_host();
        let (enabled, lines) = wait_for_strace_attach(&rx, &host, &mut errors);

        assert!(enabled.is_some(), "marker must be observed; errors={errors:?}");
        assert_eq!(
            lines.len(),
            COUNTER_DIAGNOSTIC_LINES_MAX + 1,
            "attach wait must keep one line past the record's cap and no more; the marker arrived after them and is not retained"
        );
        assert_eq!(lines[0], "noise 0", "first line retained");
        assert!(
            errors.is_empty(),
            "a marker-arrival wait must not push a read failure of its own; got {errors:?}"
        );
    }

    #[test]
    fn strace_attach_wait_ends_on_a_read_failure_without_naming_it() {
        let (tx, rx) = mpsc::channel();
        tx.send(StdoutLine::Line("strace: starting\n".to_string()))
            .expect("send line");
        tx.send(StdoutLine::Failed(
            "line longer than 4096 bytes".to_string(),
        ))
        .expect("send failure");
        drop(tx);

        let mut errors = Vec::new();
        let host = counter_fixture_host();
        let (enabled, lines) = wait_for_strace_attach(&rx, &host, &mut errors);

        assert_eq!(enabled, None, "no marker arrived, so nothing is enabled");
        assert_eq!(
            lines,
            vec!["strace: starting".to_string()],
            "the lines read before the failure are kept"
        );
        // `finalize_counter` names the failure once, from `StderrLoss`; a
        // second error here would report it twice for one record.
        assert!(
            errors.is_empty(),
            "the failure is named from StderrLoss, not here; got {errors:?}"
        );
    }
}
