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

use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nix::fcntl::{open, OFlag};
use nix::sys::signal::{kill, Signal};
use nix::sys::stat::Mode;
use nix::sys::wait::{waitid, Id, WaitPidFlag, WaitStatus};
use nix::unistd::Pid;

use crate::analysis::{count_driver_fds, derive, parse_perf_count, parse_strace_total, reconcile};
use crate::payload::{monotonic_now_ns, PAYLOAD_BYTES};
use crate::record::{DeliveryReport, Window};
use crate::run_record::{
    Build, Counts, CounterExit, CpuSets, CpuUsage, DriverFds, Env, GeneratorErrors, LatencySummary,
    Profile, RunKind, RunRecord, SCHEMA_VERSION, ServerKind, SyscallMethod, Syscalls,
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

/// Upper bound on the recorded-run provenance probe [`head_sha`]: a `git`
/// that never answers must not hold the runner before any record exists.
pub const GIT_HEAD_TIMEOUT_SECS: u64 = 5;

/// Cap on the stdout bytes [`head_sha`] reads from its `git`. The probe
/// wants one short SHA, so everything past this cap is noise; reading it
/// would let a `git` on `PATH` that prints without stopping, or one
/// enormous line without a newline, grow the reader's line buffer and its
/// channel for as long as [`GIT_HEAD_TIMEOUT_SECS`] lasts. The capped
/// reader reports EOF at the cap and the deadline still bounds the child.
const GIT_HEAD_STDOUT_BYTES_MAX: u64 = 4096;

/// `process_group` argument that puts [`head_sha`]'s probe in a group of
/// its own, led by the probe: the group id is then the probe's pid, and
/// one signal reaches the probe and the descendants that stayed in its
/// group (a descendant that starts a group of its own leaves it).
const GIT_HEAD_OWN_PROCESS_GROUP: i32 = 0;

/// Prefix of the harness error [`run_one`] records when the scratch
/// directory of its spec already exists. It is the proof that this run
/// did not create that directory: `matrix::run_configs` reads it to
/// keep its hands off another run's raw evidence.
pub const SCRATCH_REUSED_PREFIX: &str = "scratch directory reused: ";

/// Taskset CPU set used when the caller does not override `HostSetup.cpu_sets.server`.
pub const DEFAULT_SERVER_CPUS: &str = "0,2,4,6";

/// Taskset CPU set used when the caller does not override `HostSetup.cpu_sets.generator`.
pub const DEFAULT_GENERATOR_CPUS: &str = "8-17";

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
/// descendant holding the inherited stderr pipe open, and a descendant
/// that writes a line more often than [`WAIT_POLL_MS`] would otherwise
/// keep the drain off EOF for as long as it runs, so `run_one` would never
/// return its record and the per-call guard would never fall.
const COUNTER_DRAIN_TIMEOUT_SECS: u64 = 5;

/// Cap on the lines `Syscalls::diagnostics` retains, counted across the
/// lines [`arm_counter`] consumed before the marker and the finalization
/// drain after it. A strace attach prints a handful; a source past this
/// cap is a stream, not a diagnostic, and the record names the truncation
/// rather than growing with it.
const COUNTER_DIAGNOSTIC_LINES_MAX: usize = 256;

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
pub fn capture_env(tools: &[PathBuf]) -> Result<Env, String> {
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
        let version = tool_version(tool)
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

/// The first non-blank `--version` line of one tool, or the cause when the probe
/// could not produce one. Nothing here waits on the child without a
/// deadline: `Command::output` has none, and reading the pipe after the
/// child exits has none either, because a background process the probe
/// started inherits stdout and holds the write end open after its parent
/// is reaped. The line therefore arrives over a reader thread
/// ([`spawn_stdout_reader`]) bounded by [`TOOL_VERSION_TIMEOUT_SECS`], and
/// the child is killed and reaped on every path. A probe whose inherited
/// pipe never closes leaves its reader thread blocked — one per probed
/// tool, which is bounded by `tools`, and the run itself goes on.
fn tool_version(tool: &Path) -> Result<String, String> {
    let mut child = Command::new(tool)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| e.to_string())?;
    let deadline = Instant::now() + Duration::from_secs(TOOL_VERSION_TIMEOUT_SECS);
    let first = match child.stdout.take() {
        Some(stdout) => spawn_stdout_reader(TOOL_VERSION_LABEL, stdout, BlankLines::Skip)
            .and_then(|lines| recv_version_line(&lines, deadline)),
        None => Err("--version stdout not piped".to_string()),
    };
    // SIGKILL before the wait, never after: a probe reaped first frees its
    // pid for reuse, and the signal would then reach an unrelated process.
    // A probe that already exited is a zombie, which SIGKILL leaves alone
    // and the wait reaps.
    let pid = i32::try_from(child.id()).expect("child pid fits in i32");
    let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
    let _ = child.wait();
    first
}

/// The first non-blank line a `--version` probe prints, trimmed, bounded
/// by `deadline`. Blank and whitespace-only lines never reach this
/// receiver ([`BlankLines::Skip`]), so a probe that reaches EOF without a
/// non-blank line answered with no version, and `capture_env` must fail
/// rather than record an empty one. The deadline is checked before every
/// receive and the receive waits only the time left, so nothing that
/// arrives after expiry is reported as an answer.
fn recv_version_line(
    lines: &mpsc::Receiver<StdoutLine>,
    deadline: Instant,
) -> Result<String, String> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "--version did not answer within {TOOL_VERSION_TIMEOUT_SECS}s"
            ));
        }
        match lines.recv_timeout(remaining) {
            Ok(StdoutLine::Line(line)) => {
                let version = line.trim();
                if !version.is_empty() {
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
/// to invoke, for [`check_provenance`]. Bounded by
/// [`GIT_HEAD_TIMEOUT_SECS`]: the probe is killed and reaped if it has
/// not exited by then, or sooner if `cancel` is set. `git` is the
/// executable to run: `git` from `PATH` in production. `cancel` is the
/// shared flag SIGINT/SIGTERM set; polling it every [`WAIT_POLL_MS`]
/// gives the operator a same-tick cancellation instead of waiting out
/// the deadline. Its stdout is read through a
/// [`GIT_HEAD_STDOUT_BYTES_MAX`] cap, so neither the probe's memory nor
/// its deadline depends on how much the executable prints. The probe runs
/// in its own process group, so the cleanup reaches the descendants of a
/// `git` that is a wrapper script: a background process the wrapper leaves
/// behind holds the inherited stdout pipe open and would survive a signal
/// addressed to the wrapper's pid alone. The cleanup signals the probe's
/// own pid as well as that group, because an executable is free to call
/// `setpgid` and leave the group it was spawned into: the group signal
/// then reaches a group the probe is no longer in, and the pid signal is
/// what keeps the reap — and with it this function — bounded.
pub fn head_sha(git: &Path, cancel: &AtomicBool) -> Result<String, String> {
    let mut child = Command::new(git)
        .args(["rev-parse", "--short", "HEAD"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .process_group(GIT_HEAD_OWN_PROCESS_GROUP)
        .spawn()
        .map_err(|e| format!("git rev-parse: {e}"))?;
    // The child leads the group it was spawned into, so its pid is the
    // group id and the negated pid addresses the group.
    let pid = i32::try_from(child.id()).ok().map(Pid::from_raw);
    let group = pid.map(|pid| Pid::from_raw(-pid.as_raw()));
    let deadline = Instant::now() + Duration::from_secs(GIT_HEAD_TIMEOUT_SECS);
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
    // SIGKILL the whole group, so a `git` wrapper's background descendant
    // goes with the wrapper instead of outliving the probe with the
    // inherited stdout pipe. Nothing above reaps the leader — the exit
    // check is [`probe_exit`], which leaves it a zombie — so the leader
    // still holds its pid here, and with it the id of the group it leads.
    // The signal therefore cannot reach anything but this probe's own
    // group, whether the leader is running or already exited.
    if let Some(group) = group {
        let _ = kill(group, Signal::SIGKILL);
    }
    // And the probe's own pid, which is the signal the bound rests on. An
    // executable is free to call `setpgid` and join another group: the
    // group signal above then names a group without the probe in it — an
    // empty group answers `ESRCH` — and a probe that hangs on would block
    // the `child.wait()` below with no deadline. An unreaped child still
    // owns its pid, so this signal can no more reach a stranger than the
    // group signal can, and a probe that already exited is a zombie,
    // which SIGKILL leaves for the wait to reap.
    if let Some(pid) = pid {
        let _ = kill(pid, Signal::SIGKILL);
    }
    // After the signals, never before: this is the reap that frees the
    // leader's pid, and the group it names must already be gone.
    let _ = child.wait();
    result
}

/// Wait for the `git rev-parse` child to print one line, bounded by
/// `deadline`, polling `cancel` every [`WAIT_POLL_MS`] steps and keeping
/// the first `WaitStatus` from [`probe_exit`] without calling it again. The
/// first non-empty, trimmed line the receiver reads is the SHA; EOF
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
                if first.is_none() {
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

/// Non-reaping exit check of [`head_sha`]'s probe: `waitid` with `WNOWAIT`
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
/// return, unwind, panic — the guard sends SIGKILL to anything still
/// running and waits it, so the test thread cannot leave an orphan.
struct ChildGuard {
    children: Vec<(&'static str, Child)>,
}

impl ChildGuard {
    fn new() -> Self {
        Self {
            children: Vec::new(),
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
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        for (_name, child) in &mut self.children {
            // A child that already exited is reaped; only a still-running
            // child can leak the harness. SIGKILL is the unconditional
            // last resort; the runner has already signalled SIGTERM before
            // reaching this Drop on the happy path.
            if matches!(child.try_wait(), Ok(None)) {
                let pid = i32::try_from(child.id()).expect("child pid fits in i32");
                let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
                let _ = child.wait();
            }
        }
    }
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
        started_at_unix: 0,
        harness_errors: Vec::new(),
        valid: false,
        invalid_reasons: Vec::new(),
        cancelled: false,
    }
}

/// What a child's stdout reader thread forwards: one line, the end of the
/// stream, or the error that ended it.
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
/// record has to name. EOF, a cancelled run and a disconnected reader are
/// the clean endings and carry no member here.
enum DrainEnd {
    /// The reader thread's own read failed: the lines after this point were
    /// never read, so the retained diagnostics stop mid-stream.
    ReadFailed(String),
    /// [`COUNTER_DRAIN_TIMEOUT_SECS`] passed with the stream still open.
    TimedOut,
    /// [`COUNTER_DIAGNOSTIC_LINES_MAX`] lines were retained and the stream
    /// was still open.
    LineCap,
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

/// Sigterm a child, then wait up to `CHILD_EXIT_SLACK_SECS` for it to exit.
/// ESRCH (the child is already gone) is ignored. Returns the exit status.
fn terminate_and_wait(
    host: &HostSetup,
    label: &'static str,
    child: &mut Child,
) -> Result<std::process::ExitStatus, String> {
    let pid = i32::try_from(child.id()).expect("child pid fits in i32");
    match kill(Pid::from_raw(pid), Signal::SIGTERM) {
        Ok(()) | Err(nix::Error::ESRCH) => {}
        Err(e) => return Err(format!("SIGTERM {label}: {e}")),
    }
    let deadline = Instant::now() + Duration::from_secs(CHILD_EXIT_SLACK_SECS);
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|e| format!("try_wait {label}: {e}"))?
        {
            return Ok(status);
        }
        if ChildGuard::is_cancelled(host) {
            return Err("cancelled".to_string());
        }
        if Instant::now() >= deadline {
            let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
            let _ = child.wait();
            return Err(format!(
                "{label} did not exit within {CHILD_EXIT_SLACK_SECS}s of SIGTERM"
            ));
        }
        sleep(Duration::from_millis(WAIT_POLL_MS));
    }
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
    /// Strace only: receiver for the stderr reader thread. Carries every
    /// line until the runner drains it for `diagnostics`.
    stderr_rx: Option<mpsc::Receiver<StdoutLine>>,
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
    /// collects it at the window end, so this is `Some` by the time
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
        expected_threads: Option<u64>,
    ) -> Self {
        Self {
            kind: CounterKind::Strace,
            ctl_write: None,
            ack_read: None,
            raw_path,
            stderr_rx: Some(stderr_rx),
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
        Ok(rx) => Counter::new_strace(strace_txt, rx, expected_threads),
        Err(e) => dead(e, harness_errors),
    }
}

/// Reader thread for the strace counter's stderr. Every line is kept
/// (strace prints `attached` and `ptrace(...)` on a single thread's
/// failure) and the receiver stays open until the thread sees EOF.
fn spawn_counter_stderr_reader(
    stderr: std::process::ChildStderr,
) -> Result<mpsc::Receiver<StdoutLine>, String> {
    let (tx, rx) = mpsc::channel();
    std::thread::Builder::new()
        .name(format!("bench-runner-{COUNTER_STDERR_LABEL}"))
        .spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) => {
                        let _ = tx.send(StdoutLine::Eof);
                        break;
                    }
                    Ok(_) => {
                        if tx.send(StdoutLine::Line(line.clone())).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(StdoutLine::Failed(e.to_string()));
                        break;
                    }
                }
            }
        })
        .map_err(|e| format!("syscall counter: spawn stderr reader: {e}"))?;
    Ok(rx)
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
/// rejection reads, so no consumed line is dropped.
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
                lines.push(line.trim_end().to_string());
                if attached {
                    return (Some(monotonic_now_ns()), lines);
                }
            }
            Ok(StdoutLine::Eof) => {
                harness_errors
                    .push("syscall counter: stderr closed before attached".to_string());
                return (None, lines);
            }
            Ok(StdoutLine::Failed(e)) => {
                harness_errors.push(format!("syscall counter: stderr read: {e}"));
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
fn disarm_counter(
    counter: &mut Counter,
    guards: &mut ChildGuard,
    host: &HostSetup,
    harness_errors: &mut Vec<String>,
) {
    if counter.prep_error.is_some() {
        return;
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
        // The cap spans both sources: the marker wait's lines are already
        // retained, so the drain only gets what is left of it. A marker
        // wait that filled the cap on its own leaves a zero budget, which
        // the drain reports as the same truncation.
        let budget = COUNTER_DIAGNOSTIC_LINES_MAX.saturating_sub(lines.len());
        if let Some(rx) = counter.stderr_rx.as_ref() {
            let (rest, drain_end) = drain_stderr_lines(rx, host, budget);
            lines.extend(rest);
            match drain_end {
                Some(DrainEnd::ReadFailed(e)) => {
                    harness_errors.push(format!("syscall counter: stderr read: {e}"));
                }
                Some(DrainEnd::TimedOut) => {
                    harness_errors.push(format!(
                        "syscall counter: stderr still open after {COUNTER_DRAIN_TIMEOUT_SECS}s, diagnostics truncated at {} lines",
                        lines.len()
                    ));
                }
                Some(DrainEnd::LineCap) => {
                    harness_errors.push(format!(
                        "syscall counter: diagnostics truncated at {COUNTER_DIAGNOSTIC_LINES_MAX} lines"
                    ));
                }
                None => {}
            }
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
/// `budget` further retained lines. Both are needed because the counter is
/// an operator-supplied executable — an idle stream reaches EOF, but a
/// descendant that keeps the pipe open and writes more often than
/// [`WAIT_POLL_MS`] reaches neither EOF nor a quiet gap, and this runs
/// inside `run_one` before the record is returned and the per-call guard
/// falls.
///
/// EOF, a cancelled run and a disconnected reader end the drain cleanly —
/// the diagnostics is what the run recorded, not a stream the operator is
/// still following. The other three endings come back as a [`DrainEnd`]:
/// the lines after them were never read, so the caller has to reject the
/// run instead of publishing diagnostics that stop in the middle for an
/// unrecorded reason.
fn drain_stderr_lines(
    rx: &mpsc::Receiver<StdoutLine>,
    host: &HostSetup,
    budget: usize,
) -> (Vec<String>, Option<DrainEnd>) {
    let mut out: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(COUNTER_DRAIN_TIMEOUT_SECS);
    loop {
        if out.len() >= budget {
            return (out, Some(DrainEnd::LineCap));
        }
        if ChildGuard::is_cancelled(host) {
            return (out, None);
        }
        if Instant::now() >= deadline {
            return (out, Some(DrainEnd::TimedOut));
        }
        match rx.recv_timeout(Duration::from_millis(WAIT_POLL_MS)) {
            Ok(StdoutLine::Line(line)) => out.push(line.trim_end().to_string()),
            Ok(StdoutLine::Failed(e)) => return (out, Some(DrainEnd::ReadFailed(e))),
            Ok(StdoutLine::Eof) | Err(_) => return (out, None),
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
    let constants = ProtocolConstants::for_profile(spec.profile);
    let mut harness_errors: Vec<String> = Vec::new();
    let mut guards = ChildGuard::new();

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
        &mut guards,
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
        if let Err(e) = wait_for_line(host, &mut guards, handshake, ready_deadline, "READY") {
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
            &mut guards,
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
        if let Err(e) = wait_for_line(host, &mut guards, handshake, arm_deadline, "ARMED") {
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
                &mut guards,
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
            disarm_counter(c, &mut guards, host, &mut harness_errors);
        }
    }
    // CPU end sample, before the counter is reaped: a counter can confirm
    // its stop on time and still spend seconds writing its summary out, and
    // a reap waited through here would add that time to `sampled_ns` and
    // the post-window ticks to both deltas, understating the saturation the
    // window actually reached.
    let server_end = read_cpu_ticks(driver_pid).ok();
    let gen_end = read_cpu_ticks(gen_pid).ok();
    // The observed interval, not `window_secs`: under the CPU contention
    // these runs measure, `sleep_until` can resume late and the start
    // sample can be delayed, and a percentage computed against the declared
    // window would then misstate the use the ticks prove. P-Q4 keeps CPU
    // evidence out of the validity rules, so the skew is carried, not
    // judged.
    let sampled_ns = monotonic_now_ns().saturating_sub(sample_start_ns);

    // Strace's `disabled_ns` is this reap, so it stays at the window end,
    // two `/proc` reads later.
    if counting {
        if let Some(c) = counter.as_mut() {
            reap_counter(c, &mut guards, host, &mut harness_errors);
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
        finalize_counter(c, &mut guards, host, &mut harness_errors)
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

    finalize_record(
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
    )
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
