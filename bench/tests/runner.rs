//! Integration tests for `bench::runner` (task 9 of the benchmark-harness
//! feature). The smoke harness exercises `run_one` end to end against the
//! real `bench` binary; the script-based tests use a `tempfile`-style
//! shell script as `bench_exe` to test the no-reports and slow-startup
//! paths; the unit tests cover `check_provenance` and `mosquitto_conf`.

use std::fs;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::sys::signal::{kill, Signal};
use nix::sys::stat::Mode;
use nix::sys::wait::waitpid;
use nix::unistd::{getpgid, getpgrp, getpid, getppid, mkfifo, setpgid, Pid};

use bench::record::DeliveryReport;
use bench::run_record::{
    Build, Counts, CpuSets, CpuUsage, DriverFds, Env, GeneratorErrors, Profile, RunKind, RunRecord,
    ServerKind, SyscallMethod, Syscalls, SCHEMA_VERSION,
};
use bench::record::Timeline;
use bench::record::Window;

use bench::runner::{
    capture_env, capture_env_with, check_provenance, declared_rates, head_sha, head_sha_with,
    mosquitto_conf, run_one, run_one_with, stem_for, DeclaredRates, HostSetup, RunSpec,
    CLEANUP_SURVIVOR_PREFIX, CONNECTIONS, DECLARED_RATES_W1_Q0, DECLARED_RATES_W1_Q1,
    DECLARED_RATES_W4_Q0, DECLARED_RATES_W4_Q1, GIT_HEAD_TIMEOUT_SECS, SMOKE_DECLARED_RATES,
    TOOL_VERSION_TIMEOUT_SECS,
};

/// `bench/build.rs`, compiled into this test so its probe can be driven with
/// fixture commands; its `main` is never called.
#[path = "../build.rs"]
#[allow(dead_code)]
mod build_script;

/// Slack a probe assertion grants [`GIT_HEAD_TIMEOUT_SECS`] for the kill,
/// the reap and the test host's scheduling.
const PROBE_RETURN_SLACK_SECS: u64 = 2;

/// Poll interval of the probe assertions that wait for a pid file or for a
/// killed process to disappear.
const PROBE_POLL: Duration = Duration::from_millis(20);

/// Lifetime of the background process a fake `git` wrapper leaves behind.
/// Long enough to outlive the probe's whole deadline, so a descendant that
/// is still alive afterwards is a cleanup failure and not an exit of its
/// own.
const DESCENDANT_SLEEP_SECS: u64 = GIT_HEAD_TIMEOUT_SECS * 10;

/// Slack a reap assertion grants the host's scheduling.
const REAP_SLACK_SECS: u64 = 2;

/// How often a fixture of [`wait_while_kept`] looks at the keep-alive file
/// the test ends it with.
const FIXTURE_KEEP_POLL_SECS: u64 = 1;

/// Slack an exit request granted on top of [`FIXTURE_KEEP_POLL_SECS`]: the
/// fixture is inside one poll's `sleep` when the file goes, and the host's
/// scheduling follows that sleep.
const FIXTURE_EXIT_SLACK_SECS: u64 = PROBE_RETURN_SLACK_SECS;

/// The stdout read cap of `head_sha`, mirrored here because the runner
/// keeps it private. `head_sha_stops_reading_at_its_stdout_cap` hides a
/// SHA behind more bytes than this.
const GIT_HEAD_STDOUT_CAP_BYTES: usize = 4096;

/// The stdout read cap of `capture_env`'s `--version` probe, mirrored
/// because the runner keeps it private.
const TOOL_VERSION_STDOUT_CAP_BYTES: usize = 4096;

/// A version line as the fixtures print it, newline included.
const VERSION_LINE: &str = "faketool version 1.0\n";

/// The reap bound after a cleanup SIGKILL, mirrored here because the
/// runner keeps it private.
const SIGKILL_REAP_TIMEOUT_SECS: u64 = 1;

/// Mirror of the runner's private guard reap bound (`GUARD_SIGKILL_REAP_TIMEOUT_SECS`).
const GUARD_SIGKILL_REAP_TIMEOUT_SECS: u64 = 10;

/// Exit code the failing fake `bench` children use.
const FIXTURE_EXIT_CODE: i32 = 3;

/// Signal the signal-killed fake `bench` children send themselves (SIGKILL).
const FIXTURE_SIGNAL: i32 = 9;

/// The probe bound of `bench/build.rs`, mirrored because the build script
/// keeps it private.
const BUILD_PROBE_TIMEOUT_SECS: u64 = 5;

/// The reap bound after `bench/build.rs`'s kill, mirrored for the same reason.
const BUILD_PROBE_REAP_TIMEOUT_SECS: u64 = 1;

/// The stdout cap of `bench/build.rs`'s probe, mirrored for the same reason.
const BUILD_PROBE_STDOUT_CAP_BYTES: u64 = 1 << 20;

/// Whitespace-only padding that fixture prints ahead of its SHA: one byte
/// past the cap already puts the SHA out of the capped reader's reach, and
/// the line's own newline makes the margin two.
const CAP_PADDING_BYTES: usize = GIT_HEAD_STDOUT_CAP_BYTES + 1;

/// A SHA line as the fixtures print it, newline included.
const SHA_LINE: &str = "abc1234\n";

/// Padding that ends a SHA line's newline exactly on the read cap.
const EXACT_FIT_PADDING_BYTES: usize = GIT_HEAD_STDOUT_CAP_BYTES - SHA_LINE.len();

/// Padding that pushes the end of a SHA line two bytes past the read cap, so
/// the cap cuts the line inside the SHA.
const CUT_PADDING_BYTES: usize = EXACT_FIT_PADDING_BYTES + 2;

/// Padding that leaves room for only the first byte of a two-byte UTF-8
/// character before the read cap.
const UTF8_CUT_PADDING_BYTES: usize = GIT_HEAD_STDOUT_CAP_BYTES - 1;

/// Name of the fixture `head_sha_kills_a_git_that_left_its_process_group`
/// re-runs this test binary on, as a fake `git` that calls `setpgid`.
const GROUP_ESCAPE_FIXTURE: &str = "group_escape_fixture";

/// Environment variable through which that test hands the fixture the path
/// of the file the fixture records its move in. An inherited variable
/// keeps the path out of the command line, which libtest parses.
const GROUP_ESCAPE_FILE_ENV: &str = "BENCH_TEST_GROUP_ESCAPE_FILE";

/// Build a HostSetup pointing at the real bench binary with default CPU
/// sets, an empty tool list, and the supplied scratch_dir/cancel flag.
fn real_host_setup(scratch_dir: PathBuf, cancel: Arc<AtomicBool>) -> HostSetup {
    HostSetup {
        bench_exe: PathBuf::from(env!("CARGO_BIN_EXE_bench")),
        mosquitto_bin: None,
        syscalls: SyscallMethod::None,
        perf_bin: None,
        strace_bin: None,
        cpu_sets: CpuSets {
            server: bench::runner::DEFAULT_SERVER_CPUS.to_string(),
            generator: bench::runner::DEFAULT_GENERATOR_CPUS.to_string(),
        },
        env: capture_env(&[], &AtomicBool::new(false)).expect("capture_env"),
        scratch_dir,
        cpu_pressure_path: PathBuf::from(bench::runner::HOST_CPU_PRESSURE_PATH),
        cancel,
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bench-runner-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// The kernel's host-wide CPU pressure file, named here rather than through
/// `bench::runner` so the test's bound does not depend on the code under test.
const KERNEL_PRESSURE_PATH: &str = "/proc/pressure/cpu";

/// How long the pressure FIFO fixture waits for the runner to open each
/// read: the smoke run's startup, warm-up and window, with headroom.
const PRESSURE_FIFO_SERVE_TIMEOUT: Duration = Duration::from_secs(30);
/// How often the pressure FIFO fixture retries opening its write end.
const PRESSURE_FIFO_POLL: Duration = Duration::from_millis(1);
/// How far the time between the fixture's two deliveries may differ from
/// the record's `sampled_ns`: far below the 5 s smoke window, so two reads
/// taken together fail, and far above the fixture's scheduling delay.
const PRESSURE_PLACEMENT_SLACK: Duration = Duration::from_secs(1);

/// `total=` of the `some` line of the pressure file at `path`, parsed here
/// rather than through `bench::analysis` so the bound is independent of
/// the code under test.
fn some_total(path: &Path) -> Option<u64> {
    let text = fs::read_to_string(path).ok()?;
    let line = text
        .lines()
        .find(|line| line.split_whitespace().next() == Some("some"))?;
    line.split_whitespace()
        .find_map(|field| field.strip_prefix("total="))
        .and_then(|value| value.parse().ok())
}

/// Serve each of `totals`, in order, as the `some` counter of one read of
/// the FIFO at `path`: create the FIFO, wait for a reader to open it, write
/// one kernel-format pressure text, remove the path and only then close the
/// write end, so no later open can reach a FIFO without a writer; the next
/// read meets a fresh FIFO or, after the last total, no file at all. Sends
/// on `ready` once the first FIFO exists. Returns the instant each total
/// was delivered. Stops early when no reader opens the FIFO within
/// [`PRESSURE_FIFO_SERVE_TIMEOUT`], first opening the FIFO itself so a
/// reader that arrived at that instant reads end-of-file instead of
/// blocking.
fn serve_pressure_fifo(path: &Path, totals: &[u64], ready: &mpsc::Sender<()>) -> Vec<Instant> {
    let mut delivered = Vec::with_capacity(totals.len());
    for total in totals {
        mkfifo(path, Mode::S_IRUSR | Mode::S_IWUSR).expect("create the pressure FIFO");
        if delivered.is_empty() {
            ready.send(()).expect("signal that the pressure FIFO exists");
        }
        let deadline = Instant::now() + PRESSURE_FIFO_SERVE_TIMEOUT;
        let opened = loop {
            match fs::OpenOptions::new()
                .write(true)
                .custom_flags(OFlag::O_NONBLOCK.bits())
                .open(path)
            {
                Ok(fifo) => break Some(fifo),
                Err(err)
                    if err.raw_os_error() == Some(Errno::ENXIO as i32)
                        && Instant::now() < deadline =>
                {
                    sleep(PRESSURE_FIFO_POLL);
                }
                Err(_) => break None,
            }
        };
        let Some(mut fifo) = opened else {
            let release = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .expect("open the unread pressure FIFO");
            fs::remove_file(path).expect("remove the unread pressure FIFO");
            drop(release);
            return delivered;
        };
        write!(
            fifo,
            "some avg10=0.00 avg60=0.00 avg300=0.00 total={total}\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
        )
        .expect("write the pressure text");
        delivered.push(Instant::now());
        fs::remove_file(path).expect("remove the served pressure FIFO");
        drop(fifo);
    }
    delivered
}

fn make_script(dir: &std::path::Path, name: &str, body: &str) -> PathBuf {
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

/// `path` as one shell word, for a fixture body that names it in shell text.
/// A `TMPDIR` the host is free to set can hold a space, and an unquoted
/// redirection target would then split into a target and an argument: the
/// fixture's pid file would never appear and the test would fail on a host
/// setting rather than on the probe. Single quotes keep every byte literal,
/// and an embedded quote is closed, escaped and reopened.
fn shell_word(path: &std::path::Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', r"'\''"))
}

/// Shell text for a fixture that stays alive while `keep` exists and then
/// exits on its own, for a fixture whose test must end it without
/// signalling it.
///
/// A pid is not a lifetime-safe handle here. The identity check of
/// [`reap_fixture`] and the `kill` after it are two separate steps, and a
/// reaper the test does not control — the reaper thread `head_sha_with`
/// leaves behind, or init for an orphan — is free to free the pid number in
/// between, after which the signal would reach whoever the kernel hands it
/// to next. A file of the test's own cannot be reused like that, so a test
/// asks for the exit by removing it ([`end_fixture`]) and never signals.
/// The step count bounds a fixture whose test died before its cleanup, so
/// `DESCENDANT_SLEEP_SECS` stays the longest any fixture can outlive it.
fn wait_while_kept(keep: &std::path::Path) -> String {
    let steps = DESCENDANT_SLEEP_SECS / FIXTURE_KEEP_POLL_SECS;
    format!(
        "i=0\nwhile [ -e {keep} ] && [ \"$i\" -lt {steps} ]; do\n\
         sleep {FIXTURE_KEEP_POLL_SECS}\ni=$((i+1))\ndone\n",
        keep = shell_word(keep)
    )
}

/// The pid a fake `git` wrapper wrote to `path`, or `None` while the file
/// is missing or holds a partially written line. The terminating newline
/// is written last, so a line without it is not complete yet.
fn read_pid_file(path: &std::path::Path) -> Option<i32> {
    let body = fs::read_to_string(path).ok()?;
    body.strip_suffix('\n')?.trim().parse().ok()
}

/// A fixture process named by its pid and by the start time read while it
/// was still running. A pid alone names whoever holds it now: the probe
/// reaps the command it killed, and the kernel may hand that number to an
/// unrelated process of the same user before cleanup reads the pid file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Fixture {
    pid: Pid,
    started: u64,
}

/// Wait for the fake `git` to record its pid, then pin its identity while
/// it is still running. The probe kills what it gave up on no earlier than
/// its own deadline, so a capture inside `within` names the fixture itself.
/// `None` means no running process recorded a pid in time.
fn await_fixture(path: &std::path::Path, within: Duration) -> Option<Fixture> {
    let deadline = Instant::now() + within;
    loop {
        if let Some(pid) = read_pid_file(path) {
            let pid = Pid::from_raw(pid);
            return Some(Fixture {
                pid,
                started: proc_start_time(pid.as_raw())?,
            });
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(PROBE_POLL);
    }
}

/// Kill and reap `fixture`, and answer the liveness check observed before
/// that kill. A pid whose start time no longer matches the capture is
/// another process now, so it is reported gone and never signalled.
///
/// Call this only on a command of the probe's own that the probe has
/// released — see [`released_fixture`]. The identity check below and the
/// signal after it are two separate steps, so a reap that lands between them
/// would free the pid and point the signal at whoever the kernel hands that
/// number to next. Nothing can pin the pid across both steps here: the bench
/// crate forbids `unsafe_code`, so the pidfd that would hold the pid number
/// reserved is out of reach (nix 0.31.3 wraps no `pidfd_open`). Release is
/// what closes the window instead, and it closes it only for a command of the
/// probe's: that command is an unreaped child of this process, so once no
/// probe thread can reap it, its pid stays its own. Every other fixture —
/// one a reaper the test does not own may free the pid of — waits on a
/// keep-alive file and is ended through [`end_fixture`], which signals
/// nothing: a descendant the fake `git` backgrounds and orphans
/// (`build_probe_gives_up_on_a_command_whose_descendant_holds_stdout`), and
/// a probe the reaper thread of `head_sha_with` owns the reap of.
fn reap_fixture(fixture: Fixture) -> Result<(), Errno> {
    if proc_start_time(fixture.pid.as_raw()) != Some(fixture.started) {
        return Err(Errno::ESRCH);
    }
    let alive = kill(fixture.pid, None);
    if alive.is_ok() {
        let _ = kill(fixture.pid, Signal::SIGKILL);
        let _ = waitpid(fixture.pid, None);
    }
    alive
}

/// Ask `fixture` to exit by removing the keep-alive file it waits on
/// ([`wait_while_kept`]), and wait for it to go. `true` once it is gone,
/// `false` when `within` passes first.
///
/// Nothing is signalled and no pid is named to the kernel: only `/proc` is
/// read, so a pid the kernel has already handed to another process of this
/// user is reported as gone instead of being touched. That makes this
/// cleanup safe for a fixture whose reap belongs to someone else — the
/// reaper thread of `head_sha_with`, or init for a descendant the fake
/// `git` orphans — which is what a `kill` of the pid cannot be.
///
/// A fixture this test process still owns is a zombie between its exit and
/// its reaper's `wait`, and a zombie keeps its start time, so it is read as
/// gone here as well: it holds nothing open any more.
fn end_fixture(keep: &std::path::Path, fixture: Fixture, within: Duration) -> bool {
    let _ = fs::remove_file(keep);
    let deadline = Instant::now() + within;
    loop {
        if proc_start_time(fixture.pid.as_raw()) != Some(fixture.started)
            || is_zombie(fixture.pid.as_raw())
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(PROBE_POLL);
    }
}

/// `fixture` once the probe has released it, and `None` while the probe
/// still owns it.
///
/// `released` is [`await_release`]'s answer, not the probe's returned value.
/// A returned value proves only that `bounded_stdout` itself finished: the
/// command is started on another thread, which keeps the command and reaps it
/// through [`kill_with_release`]'s callback when startup finishes after
/// `bounded_stdout` gave up on the rendezvous. That thread is detached, so it
/// can still be inside its own kill and reap while the test reads the result.
/// Release covers both: every path that can reap the command holds a clone of
/// the callback and drops it only after it is done with the command.
///
/// Once released, a command of the probe's own is in one of two states that no
/// longer change under [`reap_fixture`]: already reaped — which the start time
/// there detects without signalling — or alive or a zombie and unreaped, where
/// the pid stays allocated to the fixture itself. Nothing else waits on it: it
/// is a child of this process, and every `waitpid` and `try_wait` in this
/// suite names a pid of its own. A descendant the fake `git` orphans has init
/// or a subreaper waiting on it instead, so release says nothing about its pid
/// and [`reap_fixture`] is not called on it.
///
/// A fixture the probe has not released is not signalled at all. The fixture
/// sleeps for `DESCENDANT_SLEEP_SECS` and then ends on its own, and the test
/// that waited for the release in vain fails on its own assertions.
fn released_fixture(fixture: Option<Fixture>, released: bool) -> Option<Fixture> {
    fixture.filter(|_| released)
}

/// `kill` paired with a receiver that reports when the probe has let go of
/// every clone of it.
///
/// `bounded_stdout` holds one clone of its kill callback per path that can
/// still reap the command — its own and the starting thread's — and each path
/// drops its clone only after it has finished with the command. A receiver
/// that reports its senders gone therefore proves that no thread of the probe
/// can kill, wait on, or reap that pid again. The callback also reports every
/// call, so a kill the probe makes cannot be mistaken for the release.
fn kill_with_release<K>(
    mut kill: K,
) -> (
    impl FnMut(&mut Child) -> std::io::Result<()> + Clone + Send + 'static,
    std::sync::mpsc::Receiver<()>,
)
where
    K: FnMut(&mut Child) -> std::io::Result<()> + Clone + Send + 'static,
{
    let (calls, released) = std::sync::mpsc::channel();
    let callback = move |child: &mut Child| {
        let _ = calls.send(());
        kill(child)
    };
    (callback, released)
}

/// `true` once every clone of [`kill_with_release`]'s callback is dropped, and
/// `false` when `within` passes first — a probe thread still stuck in its
/// command's startup, which keeps the command and may reap it at any moment.
fn await_release(released: &std::sync::mpsc::Receiver<()>, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        match released.recv_timeout(remaining) {
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return true,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return false,
            // A kill the probe made. The release is the disconnect alone.
            Ok(()) => {}
        }
    }
}

/// What is left of one build probe's return bound, counted from `launched`.
/// The bound covers the probe's own deadline, its reap and the test host's
/// scheduling, so waiting for a fixture to record its pid must spend the same
/// budget instead of adding to it.
fn probe_return_bound(launched: Instant) -> Duration {
    let bound = Duration::from_secs(
        BUILD_PROBE_TIMEOUT_SECS + BUILD_PROBE_REAP_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS,
    );
    (launched + bound).saturating_duration_since(Instant::now())
}

/// The `pid pgid` pair a fake `git` wrote to `path` after moving itself
/// out of the group it was spawned into, or `None` while the file is
/// missing or holds a partially written line. The terminating newline is
/// written last, so a line without it is not complete yet.
fn read_pid_pgid_file(path: &std::path::Path) -> Option<(i32, i32)> {
    let body = fs::read_to_string(path).ok()?;
    let (pid, pgid) = body.strip_suffix('\n')?.trim().split_once(' ')?;
    Some((pid.trim().parse().ok()?, pgid.trim().parse().ok()?))
}

/// The error `exec` reports for a script that some process still holds
/// open for writing.
const SCRIPT_BUSY: &str = "Text file busy";

/// How long [`probe_script`] keeps retrying a fixture the kernel reports
/// as busy before it hands the error to the test.
const SCRIPT_BUSY_RETRY_SECS: u64 = 5;

/// [`retry_busy`] over `head_sha` on a fixture script.
fn probe_script(
    script: &std::path::Path,
    cancel: &AtomicBool,
) -> (Result<String, String>, Duration) {
    retry_busy(|| head_sha(script, cancel))
}

/// Retry a probe while the kernel reports its script busy.
///
/// libtest runs these tests in parallel, and a child one test forks
/// inherits every descriptor another test's [`make_script`] has open on
/// its script until that child execs. An `exec` of the script inside that
/// window fails with `ETXTBSY` — a statement about the fixture's start,
/// not about the probe under test — so it is retried instead of asserted
/// on. The duration is of the last attempt alone, so a retry cannot push a
/// probe past the bound it is asserted to keep.
fn retry_busy(
    mut probe: impl FnMut() -> Result<String, String>,
) -> (Result<String, String>, Duration) {
    let deadline = Instant::now() + Duration::from_secs(SCRIPT_BUSY_RETRY_SECS);
    loop {
        let started = Instant::now();
        let result = probe();
        let elapsed = started.elapsed();
        let busy = result
            .as_ref()
            .err()
            .is_some_and(|e| e.contains(SCRIPT_BUSY));
        if !busy || Instant::now() >= deadline {
            return (result, elapsed);
        }
        std::thread::sleep(PROBE_POLL);
    }
}

/// `true` when `pid` names a process that has terminated and that nothing
/// has reaped yet. The group kill orphans the fake wrapper's descendant,
/// and a zombie still answers `kill(pid, None)` with `Ok` until its new
/// parent collects it — so this is what separates a killed descendant from
/// one that is still running.
fn is_zombie(pid: i32) -> bool {
    let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    // `comm` is parenthesised and can itself contain spaces and brackets,
    // so the state field is the first token after the last ')'.
    stat.rsplit_once(')')
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .is_some_and(|state| state == "Z")
}

/// Index of the start-time field of `/proc/<pid>/stat` among the fields
/// after `comm`: the kernel documents it as field 22, and the first field
/// after the parenthesised `comm` is field 3.
const PROC_STAT_START_TIME_AFTER_COMM: usize = 19;

/// The start time of `pid` in clock ticks since boot, or `None` when no
/// process holds that pid. Two processes that reuse one pid differ here, so
/// this is what separates a fixture from a later owner of its pid.
fn proc_start_time(pid: i32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // `comm` is parenthesised and can itself contain spaces and brackets,
    // so the fields are counted from the last ')' (see [`is_zombie`]).
    stat.rsplit_once(')')?
        .1
        .split_whitespace()
        .nth(PROC_STAT_START_TIME_AFTER_COMM)?
        .parse()
        .ok()
}

/// A command that runs `script` through `sh`. The script is never `exec`ed
/// itself, so a descriptor another parallel test holds open on its own script
/// cannot fail the start with `ETXTBSY` (see [`probe_script`]).
fn sh(script: &std::path::Path) -> Command {
    let mut command = Command::new("sh");
    command.arg(script);
    command
}

#[test]
fn smoke_uring_throughput_run_is_valid_with_driver_proof() {
    let dir = temp_dir("smoke-uring-thr");
    let host = real_host_setup(dir.clone(), Arc::new(AtomicBool::new(false)));
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let record = run_one(&spec, &host);

    assert!(record.valid, "smoke uring throughput must validate; reasons: {:?}", record.invalid_reasons);
    assert_eq!(
        record.driver,
        Some(DriverFds {
            io_uring_fds: 1,
            eventpoll_fds: 0
        }),
        "1-worker uring endpoint must show one io_uring fd and zero eventpoll fds"
    );
    assert!(record.counts.offered > 0, "smoke must publish some messages");
    assert_eq!(
        record.counts.delivered, record.counts.offered,
        "delivered must equal offered"
    );
    let cpu = record.cpu.expect("cpu ticks present");
    assert!(cpu.server_ticks > 0, "server ticks must be > 0");
    assert!(cpu.generator_ticks > 0, "generator ticks must be > 0");

    let _ = fs::remove_dir_all(&dir);
}

/// Number of workers the per-worker load assertions below drive a run with.
const WORKER_LOADS_WORKERS: u32 = 4;

/// Run a four-worker smoke latency run against `server` in a scratch
/// directory named `dir_name`, and assert the per-worker loads it records:
/// one entry per worker, every connection and every delivery attributed,
/// the busiest worker first, a p99 on every worker that carried a
/// connection, and the same loads in the run's `endpoint.json`.
///
/// Both server kinds are asserted the same way, so each one keeps its own
/// named test and both share this body.
fn assert_worker_loads_recorded(server: ServerKind, dir_name: &str) {
    let dir = temp_dir(dir_name);
    let host = real_host_setup(dir.clone(), Arc::new(AtomicBool::new(false)));
    let spec = RunSpec {
        profile: Profile::Smoke,
        server,
        workers: WORKER_LOADS_WORKERS,
        qos: Some(1),
        kind: RunKind::Latency,
        offered_rate: Some(SMOKE_DECLARED_RATES.lower),
        repetition: 1,
        attempt: 1,
    };

    let record = run_one(&spec, &host);

    let loads = record
        .worker_loads
        .clone()
        .expect("bench server reports worker loads");
    assert_eq!(loads.len(), WORKER_LOADS_WORKERS as usize);
    let total_connections: u32 = loads.iter().map(|l| l.connections).sum();
    assert_eq!(total_connections, CONNECTIONS);
    assert!(record.counts.delivered > 0);
    let total_delivered: u64 = loads.iter().map(|l| l.delivered).sum();
    assert_eq!(total_delivered, record.counts.delivered);
    assert!(loads[0].connections >= loads[WORKER_LOADS_WORKERS as usize - 1].connections);
    for load in &loads {
        if load.connections > 0 {
            assert!(load.latency_p99_ns.is_some());
        }
    }

    let endpoint: DeliveryReport = serde_json::from_str(
        &fs::read_to_string(dir.join(stem_for(&spec)).join("endpoint.json"))
            .expect("read endpoint.json"),
    )
    .expect("parse endpoint.json");
    assert_eq!(record.worker_loads, endpoint.worker_loads);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_four_worker_uring_run_records_every_connection_on_one_worker() {
    assert_worker_loads_recorded(ServerKind::Uring, "worker-loads-uring");
}

#[test]
fn a_four_worker_tokio_run_records_every_connection_on_one_worker() {
    assert_worker_loads_recorded(ServerKind::Tokio, "worker-loads-tokio");
}

#[test]
fn smoke_tokio_throughput_run_is_valid() {
    let dir = temp_dir("smoke-tokio-thr");
    let host = real_host_setup(dir.clone(), Arc::new(AtomicBool::new(false)));
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Tokio,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };
    let record = run_one(&spec, &host);

    assert!(record.valid, "smoke tokio throughput must validate; reasons: {:?}", record.invalid_reasons);
    assert_eq!(record.counts.delivered, record.counts.offered);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn smoke_uring_connections_run_is_valid() {
    let dir = temp_dir("smoke-uring-conn");
    let cancel = Arc::new(AtomicBool::new(false));
    let host = real_host_setup(dir.clone(), cancel);
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: None,
        kind: RunKind::Connections,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };
    let record = run_one(&spec, &host);

    assert!(
        record.valid,
        "smoke uring connections must be valid; invalid_reasons: {:?}, harness_errors: {:?}",
        record.invalid_reasons, record.harness_errors
    );
    assert!(
        record.cycles_in_window.unwrap_or(0) > 0,
        "connections mode must complete cycles"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_run_records_the_host_cpu_pressure_over_its_window() {
    let dir = temp_dir("pressure-real");
    let cancel = Arc::new(AtomicBool::new(false));
    let host = real_host_setup(dir.clone(), cancel);
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: None,
        kind: RunKind::Connections,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let before = some_total(Path::new(KERNEL_PRESSURE_PATH));
    let record = run_one(&spec, &host);
    let after = some_total(Path::new(KERNEL_PRESSURE_PATH));

    let _ = fs::remove_dir_all(&dir);

    let (Some(before), Some(after)) = (before, after) else {
        assert!(
            record.host_cpu_pressure.is_none(),
            "no pressure file readable on this host, so host_cpu_pressure must be None; got {:?}",
            record.host_cpu_pressure
        );
        return;
    };

    let pressure = record
        .host_cpu_pressure
        .expect("a readable pressure file must produce host_cpu_pressure");
    let cpu = record
        .cpu
        .expect("a run that samples pressure must also sample cpu");
    assert!(
        pressure.sampled_ns > 0,
        "host_cpu_pressure must cover a nonzero window; got {pressure:?}, before {before}, after {after}"
    );
    assert_eq!(
        pressure.sampled_ns, cpu.sampled_ns,
        "host_cpu_pressure must cover the same window as cpu; got {pressure:?}, before {before}, after {after}"
    );
    assert!(
        pressure.some_stall_us <= after - before,
        "host_cpu_pressure must not exceed the enclosing window's growth; got {pressure:?}, before {before}, after {after}"
    );
}

#[test]
fn a_run_records_the_growth_of_the_pressure_counter_between_its_window_samples() {
    let scratch = temp_dir("pressure-fifo-run");
    let fifo_dir = temp_dir("pressure-fifo");
    let fifo = fifo_dir.join("pressure");
    let cancel = Arc::new(AtomicBool::new(false));
    let mut host = real_host_setup(scratch.clone(), cancel);
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: None,
        kind: RunKind::Connections,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let (ready_tx, ready_rx) = mpsc::channel();
    let server_fifo = fifo.clone();
    let server = std::thread::spawn(move || {
        serve_pressure_fifo(&server_fifo, &[1_000_000, 1_000_250], &ready_tx)
    });
    ready_rx.recv().expect("pressure FIFO fixture ready");
    host.cpu_pressure_path = fifo.clone();
    let record = run_one(&spec, &host);
    let delivered = server.join().expect("pressure FIFO fixture");

    let _ = fs::remove_dir_all(&scratch);
    let _ = fs::remove_dir_all(&fifo_dir);

    assert_eq!(
        delivered.len(),
        2,
        "the fixture must have served both samples; got delivered {delivered:?}, record.host_cpu_pressure {:?}",
        record.host_cpu_pressure
    );
    let Some(pressure) = record.host_cpu_pressure else {
        panic!(
            "a FIFO served twice must produce host_cpu_pressure; got record.host_cpu_pressure {:?}, delivered {delivered:?}",
            record.host_cpu_pressure
        )
    };
    let cpu = record
        .cpu
        .expect("a run that samples pressure must also sample cpu");
    assert_eq!(
        pressure.some_stall_us, 250,
        "the counter's growth between the two reads must be the exact delta; got {pressure:?}, delivered {delivered:?}"
    );
    assert_eq!(
        pressure.sampled_ns, cpu.sampled_ns,
        "host_cpu_pressure must cover the same window as cpu; got {pressure:?}, delivered {delivered:?}"
    );
    let gap_ns = delivered[1]
        .duration_since(delivered[0])
        .as_nanos();
    let slack = u128::from(pressure.sampled_ns).abs_diff(gap_ns);
    assert!(
        slack <= PRESSURE_PLACEMENT_SLACK.as_nanos(),
        "the two reads must be the window's start and end samples, not two reads taken together; \
         got pressure.sampled_ns {}, delivery gap {} ns, slack {} ns, delivered {delivered:?}",
        pressure.sampled_ns,
        gap_ns,
        slack,
    );
}

#[test]
fn a_run_whose_pressure_file_is_gone_at_the_end_sample_records_none_and_stays_valid() {
    let scratch = temp_dir("pressure-fifo-end-missing");
    let fifo_dir = temp_dir("pressure-fifo");
    let fifo = fifo_dir.join("pressure");
    let cancel = Arc::new(AtomicBool::new(false));
    let mut host = real_host_setup(scratch.clone(), cancel);
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: None,
        kind: RunKind::Connections,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let (ready_tx, ready_rx) = mpsc::channel();
    let server_fifo = fifo.clone();
    let server = std::thread::spawn(move || {
        serve_pressure_fifo(&server_fifo, &[1_000_000], &ready_tx)
    });
    ready_rx.recv().expect("pressure FIFO fixture ready");
    host.cpu_pressure_path = fifo.clone();
    let record = run_one(&spec, &host);
    let delivered = server.join().expect("pressure FIFO fixture");

    let _ = fs::remove_dir_all(&scratch);
    let _ = fs::remove_dir_all(&fifo_dir);

    assert_eq!(
        delivered.len(),
        1,
        "the fixture must have served only the start sample; got delivered {delivered:?}, record.host_cpu_pressure {:?}",
        record.host_cpu_pressure
    );
    assert!(
        record.host_cpu_pressure.is_none(),
        "no end sample must mean no host_cpu_pressure; got {:?}, invalid_reasons {:?}, harness_errors {:?}",
        record.host_cpu_pressure,
        record.invalid_reasons,
        record.harness_errors
    );
    assert!(
        record.valid,
        "missing end-sample pressure must not invalidate the run; invalid_reasons {:?}, harness_errors {:?}",
        record.invalid_reasons,
        record.harness_errors
    );
    assert!(
        record.harness_errors.iter().all(|e| !e.contains("pressure")),
        "a missing pressure file must not produce a harness error; got {:?}",
        record.harness_errors
    );
}

#[test]
fn a_run_without_a_pressure_file_records_none_and_stays_valid() {
    // A kernel built without PSI has no pressure file at either sample.
    // Host pressure is evidence, so its absence must leave the verdict alone.
    let dir = temp_dir("pressure-absent");
    let cancel = Arc::new(AtomicBool::new(false));
    let mut host = real_host_setup(dir.clone(), cancel);
    host.cpu_pressure_path = dir.join("no-such-pressure-file");
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: None,
        kind: RunKind::Connections,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let record = run_one(&spec, &host);

    let _ = fs::remove_dir_all(&dir);

    assert!(
        record.host_cpu_pressure.is_none(),
        "no pressure file at either sample must mean no host_cpu_pressure; got {:?}",
        record.host_cpu_pressure
    );
    assert!(
        record.cpu.is_some(),
        "the run must still take its CPU samples; invalid_reasons {:?}, harness_errors {:?}",
        record.invalid_reasons,
        record.harness_errors
    );
    assert!(
        record.valid,
        "an absent pressure file must not invalidate the run; invalid_reasons {:?}, harness_errors {:?}",
        record.invalid_reasons,
        record.harness_errors
    );
    assert!(
        record.harness_errors.iter().all(|e| !e.contains("pressure")),
        "an absent pressure file must not produce a harness error; got {:?}",
        record.harness_errors
    );
}

#[test]
fn run_one_invalidates_a_run_whose_children_write_no_reports() {
    let dir = temp_dir("no-reports");
    let cancel = Arc::new(AtomicBool::new(false));

    // A fake `bench`: every invocation prints READY, reads ARM, prints
    // ARMED, and exits 0. It never writes the JSON reports the runner
    // expects, so the run must end with `missing gen report` and any
    // other no-report harness errors in `invalid_reasons`.
    let script = make_script(&dir, "fake-bench.sh", "echo READY\nread line\necho ARMED\nexit 0\n");

    let host = HostSetup {
        bench_exe: script.clone(),
        mosquitto_bin: None,
        syscalls: SyscallMethod::None,
        perf_bin: None,
        strace_bin: None,
        cpu_sets: CpuSets {
            server: bench::runner::DEFAULT_SERVER_CPUS.to_string(),
            generator: bench::runner::DEFAULT_GENERATOR_CPUS.to_string(),
        },
        env: capture_env(&[], &AtomicBool::new(false)).expect("capture_env"),
        scratch_dir: dir.clone(),
        cpu_pressure_path: PathBuf::from(bench::runner::HOST_CPU_PRESSURE_PATH),
        cancel,
    };
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let record = run_one(&spec, &host);

    assert!(!record.valid, "no-reports run must be invalid");
    assert!(
        record
            .invalid_reasons
            .iter()
            .any(|r| r.contains("missing gen report")),
        "expected 'missing gen report' in {:?}",
        record.invalid_reasons
    );

    // No `fake-bench.sh` process remains after run_one returns. Use
    // `pgrep -f` against the resolved script path so the search matches
    // only our child processes.
    let lookup = std::process::Command::new("pgrep")
        .arg("-f")
        .arg(script.to_string_lossy().as_ref())
        .output();
    if let Ok(out) = lookup {
        assert!(
            out.stdout.is_empty(),
            "expected no fake-bench processes, found: {:?}",
            String::from_utf8_lossy(&out.stdout)
        );
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_failed_child_names_the_stderr_file_it_left() {
    let dir = temp_dir("stderr-named");
    let cancel = Arc::new(AtomicBool::new(false));

    // Every role prints READY/ARMED like a real `bench`, then writes its
    // own cause to stderr before exiting non-zero, so the kept stderr file
    // can be checked against the exit note that cites it.
    let script = make_script(
        &dir,
        "fake-bench.sh",
        &format!(
            "echo READY\nread line\necho ARMED\necho \"fixture cause from $1\" >&2\nexit {FIXTURE_EXIT_CODE}\n"
        ),
    );

    let host = HostSetup {
        bench_exe: script.clone(),
        mosquitto_bin: None,
        syscalls: SyscallMethod::None,
        perf_bin: None,
        strace_bin: None,
        cpu_sets: CpuSets {
            server: bench::runner::DEFAULT_SERVER_CPUS.to_string(),
            generator: bench::runner::DEFAULT_GENERATOR_CPUS.to_string(),
        },
        env: capture_env(&[], &AtomicBool::new(false)).expect("capture_env"),
        scratch_dir: dir.clone(),
        cpu_pressure_path: PathBuf::from(bench::runner::HOST_CPU_PRESSURE_PATH),
        cancel,
    };
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let record = run_one(&spec, &host);

    let run_dir = dir.join(stem_for(&spec));
    let gen_err = run_dir.join("generator.stderr");
    let endpoint_err = run_dir.join("endpoint.stderr");

    assert!(
        record
            .harness_errors
            .contains(&format!("generator exited with code {FIXTURE_EXIT_CODE} (stderr: {})", gen_err.display())),
        "expected a generator exit note citing {}; got {:?}",
        gen_err.display(),
        record.harness_errors
    );
    assert!(
        record
            .harness_errors
            .contains(&format!("endpoint exited with code {FIXTURE_EXIT_CODE} (stderr: {})", endpoint_err.display())),
        "expected an endpoint exit note citing {}; got {:?}",
        endpoint_err.display(),
        record.harness_errors
    );
    assert!(
        fs::read_to_string(&gen_err)
            .expect("read generator stderr")
            .contains("fixture cause from gen"),
        "the cited generator stderr file must hold the generator's own cause"
    );
    assert!(
        fs::read_to_string(&endpoint_err)
            .expect("read endpoint stderr")
            .contains("fixture cause from server"),
        "the cited endpoint stderr file must hold the endpoint's own cause"
    );
    assert!(!record.valid, "a run with a failed child must be invalid");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_failed_child_whose_stderr_file_is_gone_says_none_was_kept() {
    let dir = temp_dir("stderr-gone");
    let cancel = Arc::new(AtomicBool::new(false));

    // Every role prints READY/ARMED, removes its own stderr file by the fd
    // it was opened on, then exits non-zero: the runner must say plainly
    // that no stderr file was kept rather than cite a path that is gone.
    let script = make_script(
        &dir,
        "fake-bench.sh",
        &format!(
            "echo READY\nread line\necho ARMED\nrm -f \"$(readlink /proc/$$/fd/2)\"\nexit {FIXTURE_EXIT_CODE}\n"
        ),
    );

    let host = HostSetup {
        bench_exe: script.clone(),
        mosquitto_bin: None,
        syscalls: SyscallMethod::None,
        perf_bin: None,
        strace_bin: None,
        cpu_sets: CpuSets {
            server: bench::runner::DEFAULT_SERVER_CPUS.to_string(),
            generator: bench::runner::DEFAULT_GENERATOR_CPUS.to_string(),
        },
        env: capture_env(&[], &AtomicBool::new(false)).expect("capture_env"),
        scratch_dir: dir.clone(),
        cpu_pressure_path: PathBuf::from(bench::runner::HOST_CPU_PRESSURE_PATH),
        cancel,
    };
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let record = run_one(&spec, &host);

    let run_dir = dir.join(stem_for(&spec));
    let gen_err = run_dir.join("generator.stderr");
    let endpoint_err = run_dir.join("endpoint.stderr");

    assert!(
        record.harness_errors.contains(&format!(
            "generator exited with code {FIXTURE_EXIT_CODE} (stderr not retained: {} does not exist)",
            gen_err.display()
        )),
        "expected a generator exit note saying none was kept; got {:?}",
        record.harness_errors
    );
    assert!(
        record.harness_errors.contains(&format!(
            "endpoint exited with code {FIXTURE_EXIT_CODE} (stderr not retained: {} does not exist)",
            endpoint_err.display()
        )),
        "expected an endpoint exit note saying none was kept; got {:?}",
        record.harness_errors
    );
    assert!(!gen_err.exists(), "the generator stderr file must be gone");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_child_killed_by_a_signal_names_the_stderr_file_it_left() {
    let dir = temp_dir("stderr-signal");
    let cancel = Arc::new(AtomicBool::new(false));

    // Every role prints READY/ARMED, writes its cause to stderr, then kills
    // itself with FIXTURE_SIGNAL instead of exiting, exercising the signal
    // branch of the exit note.
    let script = make_script(
        &dir,
        "fake-bench.sh",
        &format!(
            "echo READY\nread line\necho ARMED\necho \"fixture cause from $1\" >&2\nkill -{FIXTURE_SIGNAL} $$\n"
        ),
    );

    let host = HostSetup {
        bench_exe: script.clone(),
        mosquitto_bin: None,
        syscalls: SyscallMethod::None,
        perf_bin: None,
        strace_bin: None,
        cpu_sets: CpuSets {
            server: bench::runner::DEFAULT_SERVER_CPUS.to_string(),
            generator: bench::runner::DEFAULT_GENERATOR_CPUS.to_string(),
        },
        env: capture_env(&[], &AtomicBool::new(false)).expect("capture_env"),
        scratch_dir: dir.clone(),
        cpu_pressure_path: PathBuf::from(bench::runner::HOST_CPU_PRESSURE_PATH),
        cancel,
    };
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let record = run_one(&spec, &host);

    let run_dir = dir.join(stem_for(&spec));
    let gen_err = run_dir.join("generator.stderr");

    assert!(
        record.harness_errors.contains(&format!(
            "generator exited with signal {FIXTURE_SIGNAL} (stderr: {})",
            gen_err.display()
        )),
        "expected a generator exit note citing {}; got {:?}",
        gen_err.display(),
        record.harness_errors
    );
    assert!(
        fs::read_to_string(&gen_err)
            .expect("read generator stderr")
            .contains("fixture cause from gen"),
        "the cited generator stderr file must hold the generator's own cause"
    );
    assert!(!record.valid, "a run with a signal-killed child must be invalid");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_ready_failure_whose_stderr_file_is_gone_says_none_was_kept() {
    let dir = temp_dir("ready-stderr-gone");
    let cancel = Arc::new(AtomicBool::new(false));

    // The endpoint removes its own stderr file and exits before printing
    // READY, so the handshake fails and the READY error must say plainly
    // that no stderr file was kept.
    let script = make_script(
        &dir,
        "fake-bench.sh",
        &format!("rm -f \"$(readlink /proc/$$/fd/2)\"\nexit {FIXTURE_EXIT_CODE}\n"),
    );

    let host = HostSetup {
        bench_exe: script.clone(),
        mosquitto_bin: None,
        syscalls: SyscallMethod::None,
        perf_bin: None,
        strace_bin: None,
        cpu_sets: CpuSets {
            server: bench::runner::DEFAULT_SERVER_CPUS.to_string(),
            generator: bench::runner::DEFAULT_GENERATOR_CPUS.to_string(),
        },
        env: capture_env(&[], &AtomicBool::new(false)).expect("capture_env"),
        scratch_dir: dir.clone(),
        cpu_pressure_path: PathBuf::from(bench::runner::HOST_CPU_PRESSURE_PATH),
        cancel,
    };
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let record = run_one(&spec, &host);

    let run_dir = dir.join(stem_for(&spec));
    let endpoint_err = run_dir.join("endpoint.stderr");
    let suffix = format!("(stderr not retained: {} does not exist)", endpoint_err.display());

    let ready_errors: Vec<&String> = record
        .harness_errors
        .iter()
        .filter(|e| e.starts_with("endpoint startup failed: READY: "))
        .collect();
    assert_eq!(
        ready_errors.len(),
        1,
        "expected exactly one READY failure; got {:?}",
        record.harness_errors
    );
    assert!(
        ready_errors[0].ends_with(&suffix),
        "expected READY failure to end with {suffix:?}; got {:?}",
        ready_errors[0]
    );
    assert!(!endpoint_err.exists(), "the endpoint stderr file must be gone");
    assert!(!record.valid, "a run with a failed READY handshake must be invalid");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn an_arm_failure_whose_stderr_file_is_gone_says_none_was_kept() {
    let dir = temp_dir("arm-stderr-gone");
    let cancel = Arc::new(AtomicBool::new(false));

    // The endpoint prints READY, then removes its own stderr file and
    // exits before printing ARMED, so the ARM handshake fails and the ARM
    // error must say plainly that no stderr file was kept.
    let script = make_script(
        &dir,
        "fake-bench.sh",
        &format!("echo READY\nread line\nrm -f \"$(readlink /proc/$$/fd/2)\"\nexit {FIXTURE_EXIT_CODE}\n"),
    );

    let host = HostSetup {
        bench_exe: script.clone(),
        mosquitto_bin: None,
        syscalls: SyscallMethod::None,
        perf_bin: None,
        strace_bin: None,
        cpu_sets: CpuSets {
            server: bench::runner::DEFAULT_SERVER_CPUS.to_string(),
            generator: bench::runner::DEFAULT_GENERATOR_CPUS.to_string(),
        },
        env: capture_env(&[], &AtomicBool::new(false)).expect("capture_env"),
        scratch_dir: dir.clone(),
        cpu_pressure_path: PathBuf::from(bench::runner::HOST_CPU_PRESSURE_PATH),
        cancel,
    };
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let record = run_one(&spec, &host);

    let run_dir = dir.join(stem_for(&spec));
    let endpoint_err = run_dir.join("endpoint.stderr");
    let suffix = format!("(stderr not retained: {} does not exist)", endpoint_err.display());

    let arm_errors: Vec<&String> = record
        .harness_errors
        .iter()
        .filter(|e| e.starts_with("endpoint startup failed: ARM handshake: "))
        .collect();
    assert_eq!(
        arm_errors.len(),
        1,
        "expected exactly one ARM failure; got {:?}",
        record.harness_errors
    );
    assert!(
        arm_errors[0].ends_with(&suffix),
        "expected ARM failure to end with {suffix:?}; got {:?}",
        arm_errors[0]
    );
    assert!(!endpoint_err.exists(), "the endpoint stderr file must be gone");
    assert!(!record.valid, "a run with a failed ARM handshake must be invalid");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn run_one_with_records_a_child_its_cleanup_could_not_end() {
    let dir = temp_dir("survivor");
    let cancel = Arc::new(AtomicBool::new(false));

    // A fake endpoint: it prints its pid so the test can pin its identity,
    // closes stdout without writing `READY`, and then waits on the
    // keep-alive file this test ends it with. `run_guarded` therefore takes
    // its READY early return while the endpoint is still in the guard, and a
    // stand-in SIGKILL refuses its cleanup.
    let pid_file = dir.join("endpoint.pid");
    let keep = dir.join("keep");
    fs::write(&keep, b"").expect("create the fixture's keep-alive file");
    let script_body = format!(
        "echo $$ > {pid}\nexec >/dev/null\n{wait}",
        pid = shell_word(&pid_file),
        wait = wait_while_kept(&keep),
    );
    let script = make_script(&dir, "stalled-bench.sh", &script_body);

    let host = HostSetup {
        bench_exe: script.clone(),
        mosquitto_bin: None,
        syscalls: SyscallMethod::None,
        perf_bin: None,
        strace_bin: None,
        cpu_sets: CpuSets {
            server: bench::runner::DEFAULT_SERVER_CPUS.to_string(),
            generator: bench::runner::DEFAULT_GENERATOR_CPUS.to_string(),
        },
        env: capture_env(&[], &AtomicBool::new(false)).expect("capture_env"),
        scratch_dir: dir.clone(),
        cpu_pressure_path: PathBuf::from(bench::runner::HOST_CPU_PRESSURE_PATH),
        cancel,
    };
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    // Retry when another test's open descriptor turns `exec` into `ETXTBSY`.
    let mut record;
    let deadline = Instant::now() + Duration::from_secs(SCRIPT_BUSY_RETRY_SECS);
    let mut attempt = 0_u32;
    loop {
        attempt += 1;
        let mut host_attempt = host.clone();
        host_attempt.scratch_dir = dir.join(format!("scratch-{attempt}"));
        record = run_one_with(&spec, &host_attempt, |_, _| Err(Errno::EPERM));
        let busy = record.harness_errors.iter().any(|e| e.contains(SCRIPT_BUSY));
        if !busy {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "SCRIPT_BUSY retry budget exhausted"
        );
        sleep(PROBE_POLL);
    }

    // Captured while the survivor is certainly still running, so the pid
    // names the fixture and not a later owner of that number. The guard's
    // whole bound has already passed, so the pid file is there.
    let fixture = await_fixture(&pid_file, Duration::from_secs(GUARD_SIGKILL_REAP_TIMEOUT_SECS))
        .expect("the fake endpoint must record its pid");
    let pid = fixture.pid.as_raw();
    let start = fixture.started;

    // Release the fixture and wait for its reap before any assertion, so a
    // failed assertion below cannot leave it running. The exit is asked for
    // through the keep-alive file and the pid is never signalled: the reaper
    // thread the guard left behind owns the survivor's one `wait` and is free
    // to free that pid number for another process.
    let ended = end_fixture(
        &keep,
        fixture,
        Duration::from_secs(FIXTURE_KEEP_POLL_SECS + FIXTURE_EXIT_SLACK_SECS),
    );
    let reap_deadline = Instant::now() + Duration::from_secs(REAP_SLACK_SECS);
    let reaped = loop {
        if proc_start_time(fixture.pid.as_raw()) != Some(fixture.started) {
            break true;
        }
        if Instant::now() >= reap_deadline {
            break false;
        }
        sleep(PROBE_POLL);
    };
    let _ = fs::remove_dir_all(&dir);

    let any_ready = record
        .harness_errors
        .iter()
        .any(|e| e.starts_with("endpoint startup failed: READY"));
    assert!(
        any_ready,
        "expected a READY startup error in {:?}",
        record.harness_errors
    );
    let expected = format!(
        "{CLEANUP_SURVIVOR_PREFIX}endpoint cleanup: pid {pid} (start {start}) still running {GUARD_SIGKILL_REAP_TIMEOUT_SECS}s after SIGKILL"
    );
    let survivor_entry = record
        .harness_errors
        .iter()
        .find(|e| e.starts_with(&expected))
        .unwrap_or_else(|| panic!("expected survivor error starting with `{expected}` in {:?}", record.harness_errors));
    assert!(
        survivor_entry.contains("SIGKILL: EPERM"),
        "the survivor must carry the refused SIGKILL: {survivor_entry}"
    );
    assert!(
        record.invalid_reasons.contains(survivor_entry),
        "the survivor must reach `invalid_reasons`"
    );
    assert!(!record.valid, "a record with a survivor must be invalid");
    assert!(
        ended,
        "the survivor must exit once the test stops keeping it alive, \
         leaving nothing of this test running"
    );
    assert!(
        reaped,
        "the reaper thread the guard left behind must reap the survivor"
    );
}

#[test]
fn run_one_succeeds_when_the_endpoint_starts_slower_than_the_lead() {
    let dir = temp_dir("slow-startup");
    let real_bench = env!("CARGO_BIN_EXE_bench");
    // Sleeping 3 s for `server` is the slow-startup case; the `gen`
    // invocation must short-circuit straight to the real bench binary
    // so the generator actually runs and writes gen.json.
    let body = format!(
        "if [ \"$1\" = server ]; then sleep 3; fi\nexec {real_bench} \"$@\"\n"
    );
    let script = make_script(&dir, "slow-bench.sh", &body);

    let host = HostSetup {
        bench_exe: script,
        mosquitto_bin: None,
        syscalls: SyscallMethod::None,
        perf_bin: None,
        strace_bin: None,
        cpu_sets: CpuSets {
            server: bench::runner::DEFAULT_SERVER_CPUS.to_string(),
            generator: bench::runner::DEFAULT_GENERATOR_CPUS.to_string(),
        },
        env: capture_env(&[], &AtomicBool::new(false)).expect("capture_env"),
        scratch_dir: dir.clone(),
        cpu_pressure_path: PathBuf::from(bench::runner::HOST_CPU_PRESSURE_PATH),
        cancel: Arc::new(AtomicBool::new(false)),
    };
    let spec = RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
    };

    let record = run_one(&spec, &host);

    assert!(
        record.valid,
        "slow-startup run must validate; reasons: {:?}",
        record.invalid_reasons
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn provenance_check_refuses_a_stale_or_dirty_build() {
    // Stale: build SHA differs from the checkout.
    let err = check_provenance("abc1234", false, "def5678")
        .expect_err("stale build must be rejected");
    assert!(err.contains("rebuild"), "unexpected error: {err}");

    // Dirty: build_dirty=true.
    let err = check_provenance("abc1234", true, "abc1234")
        .expect_err("dirty build must be rejected");
    assert!(err.contains("dirty"), "unexpected error: {err}");

    // Matching and clean: Ok.
    check_provenance("abc1234", false, "abc1234").expect("matching build must pass");
}

#[test]
fn head_sha_returns_the_trimmed_line_git_prints() {
    let dir = temp_dir("head-sha-trim");
    let a = make_script(
        &dir,
        "fake-git-a.sh",
        "[ \"$*\" = \"rev-parse --short HEAD\" ] || exit 3\nprintf '  abc1234  \n'\n",
    );
    let b = make_script(
        &dir,
        "fake-git-b.sh",
        "[ \"$*\" = \"rev-parse --short HEAD\" ] || exit 3\nprintf '\n\tdef5678\n'\n",
    );

    assert_eq!(
        probe_script(&a, &AtomicBool::new(false)).0,
        Ok("abc1234".to_string())
    );
    assert_eq!(
        probe_script(&b, &AtomicBool::new(false)).0,
        Ok("def5678".to_string())
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn head_sha_with_returns_the_trimmed_line_git_prints() {
    // Pin that the injected signal seam itself answers: the production
    // wrapper above exercises nix `kill` through this code path.
    let dir = temp_dir("head-sha-with");
    let script = make_script(
        &dir,
        "fake-git-a.sh",
        "[ \"$*\" = \"rev-parse --short HEAD\" ] || exit 3\nprintf '  abc1234  \n'\n",
    );
    let cancel = AtomicBool::new(false);

    // `head_sha_with` does not retry on a busy script the way `head_sha`
    // does through `probe_script`; the ETXTBSY race is on `make_script`'s
    // write side, so repeat the call inline while the kernel reports it.
    let deadline = Instant::now() + Duration::from_secs(SCRIPT_BUSY_RETRY_SECS);
    let result = loop {
        let attempt = head_sha_with(&script, &cancel, kill::<Signal>);
        let busy = attempt
            .as_ref()
            .err()
            .is_some_and(|e| e.contains(SCRIPT_BUSY));
        if !busy || Instant::now() >= deadline {
            break attempt;
        }
        std::thread::sleep(PROBE_POLL);
    };

    assert_eq!(result, Ok("abc1234".to_string()));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn head_sha_reports_a_git_that_exits_non_zero() {
    let dir = temp_dir("head-sha-exit");
    let bad = make_script(&dir, "fake-git-bad.sh", "exit 128\n");

    let err = probe_script(&bad, &AtomicBool::new(false))
        .0
        .expect_err("a non-zero exit must fail");
    assert!(
        err.contains("git rev-parse exited with"),
        "err must name the cause: {err}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn head_sha_refuses_a_git_that_prints_no_sha() {
    // A `git` that exits 0 without naming a commit — and one that prints
    // only blank lines, which the reader skips — leaves the probe no SHA.
    // Returning an empty string there would put an empty `git_sha` into
    // every record of the recorded set, so the probe must refuse instead.
    let dir = temp_dir("head-sha-silent");
    let silent = make_script(&dir, "fake-git-silent.sh", "exit 0\n");
    let blank = make_script(&dir, "fake-git-blank.sh", "printf '\\n \\n\\t\\n'\n");

    for script in [&silent, &blank] {
        let err = probe_script(script, &AtomicBool::new(false))
            .0
            .expect_err("a git that prints no SHA must fail the probe");
        assert!(
            err.contains("git rev-parse printed no line"),
            "err must name the missing line for {}: {err}",
            script.display()
        );
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn head_sha_refuses_a_git_that_prints_a_sha_and_holds_past_its_deadline() {
    // The SHA arrives, then the executable keeps running well past the 5 s
    // deadline. Its answer is outside the bound AC-15 promises, so the
    // probe must report the deadline and not the printed SHA.
    let dir = temp_dir("head-sha-late");
    let late = make_script(&dir, "fake-git-late.sh", "printf 'abc1234\\n'\nsleep 8\n");

    let err = probe_script(&late, &AtomicBool::new(false))
        .0
        .expect_err("an answer past the deadline must fail the probe");
    assert!(
        err.contains("git rev-parse did not answer within"),
        "err must name the deadline: {err}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn head_sha_bounds_the_stdout_of_a_flooding_git() {
    // `yes` prints non-blank lines as fast as the pipe takes them and never
    // exits on its own, so an unbounded reader would grow its line queue for
    // the whole 5 s deadline. The capped reader stops at
    // GIT_HEAD_STDOUT_BYTES_MAX and closes the pipe, which this flooder
    // reports as a SIGPIPE exit; a flooder that survived the closed pipe
    // would still be refused at the deadline. Either way the probe refuses
    // the executable within its bound and keeps the first line it read out
    // of the answer. The deadline cannot tell the capped reader from an
    // uncapped one — `head_sha_stops_reading_at_its_stdout_cap` is the
    // assertion that can.
    let dir = temp_dir("head-sha-flood");
    let flood = make_script(
        &dir,
        "fake-git-flood.sh",
        "printf 'abc1234\\n'\nexec yes noise\n",
    );

    let (result, elapsed) = probe_script(&flood, &AtomicBool::new(false));
    let err = result.expect_err("a flooding git must fail the probe");
    assert!(
        err.contains("git rev-parse exited with")
            || err.contains("git rev-parse did not answer within"),
        "err must name the exit or the deadline: {err}"
    );
    assert!(
        elapsed < Duration::from_secs(GIT_HEAD_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS),
        "the probe must return within its bound, took {elapsed:?}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn head_sha_stops_reading_at_its_stdout_cap() {
    // The byte bound itself, observable: this fake `git` pads its stdout
    // with one whitespace-only line longer than the read cap, prints the
    // SHA behind that padding and exits 0. The reader skips whitespace-only
    // lines, so the padding is bytes and nothing else — a reader without a
    // cap would walk past it and answer `abc1234`, while the capped reader
    // reaches its limit inside the padding and reports a `git` that printed
    // no line. The refusal is therefore a statement about how much the
    // probe read, which no deadline assertion can make.
    let dir = temp_dir("head-sha-cap");
    let padded = make_script(
        &dir,
        "fake-git-padded.sh",
        &format!("printf '%{CAP_PADDING_BYTES}s\\n' ''\nprintf 'abc1234\\n'\n"),
    );

    let err = probe_script(&padded, &AtomicBool::new(false))
        .0
        .expect_err("a SHA behind more padding than the cap must not be read");
    assert!(
        err.contains("git rev-parse printed no line"),
        "err must name the missing line: {err}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn head_sha_returns_a_line_that_ends_exactly_at_its_stdout_cap() {
    // The SHA line's own newline lands on the last byte the capped reader
    // reads, so the line is whole and the probe answers it in full. AC-5's
    // boundary regression guard: the stream is exactly GIT_HEAD_STDOUT_CAP_BYTES
    // bytes long.
    let dir = temp_dir("head-sha-exact-fit");
    let exact = make_script(
        &dir,
        "fake-git-exact-fit.sh",
        &format!(
            "[ \"$*\" = \"rev-parse --short HEAD\" ] || exit 3\nprintf '%{EXACT_FIT_PADDING_BYTES}s' ''\nprintf 'abc1234\\n'\n"
        ),
    );

    let result = probe_script(&exact, &AtomicBool::new(false)).0;
    assert_eq!(result, Ok("abc1234".to_string()));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn head_sha_refuses_a_line_its_stdout_cap_cuts() {
    // A SHA line whose newline falls past the cap is a line the capped
    // reader never sees whole, so AC-6 requires a refusal instead of the
    // truncated bytes the reader did read.
    let dir = temp_dir("head-sha-cut");
    let cut = make_script(
        &dir,
        "fake-git-cut.sh",
        &format!(
            "[ \"$*\" = \"rev-parse --short HEAD\" ] || exit 3\nprintf '%{CUT_PADDING_BYTES}s' ''\nprintf 'abc1234\\n'\n"
        ),
    );

    let err = probe_script(&cut, &AtomicBool::new(false))
        .0
        .expect_err("a line the cap cuts must not be read");
    assert!(
        err.contains("git rev-parse printed no line"),
        "err must name the missing line: {err}"
    );

    // Padding that leaves the cap room for only the first byte of a
    // two-byte UTF-8 character cuts the stream mid-character rather than
    // mid-line; the reader must refuse that too, not just a cut ASCII line.
    let utf8_cut = make_script(
        &dir,
        "fake-git-utf8-cut.sh",
        &format!(
            "[ \"$*\" = \"rev-parse --short HEAD\" ] || exit 3\nprintf '%{UTF8_CUT_PADDING_BYTES}s' ''\nprintf '\\303\\251\\n'\n"
        ),
    );

    let err = probe_script(&utf8_cut, &AtomicBool::new(false))
        .0
        .expect_err("a character the cap cuts must not be read");
    assert!(
        err.contains("read git rev-parse stdout:"),
        "err must name the read failure: {err}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn head_sha_refuses_a_line_git_leaves_unterminated() {
    // `git` exits 0 without ever writing the line's newline. The reader
    // reaches EOF mid-line, and AC-6 requires that unterminated line be
    // refused rather than answered as the SHA.
    let dir = temp_dir("head-sha-unterminated");
    let unterminated = make_script(
        &dir,
        "fake-git-unterminated.sh",
        "[ \"$*\" = \"rev-parse --short HEAD\" ] || exit 3\nprintf 'abc1234'\n",
    );

    let err = probe_script(&unterminated, &AtomicBool::new(false))
        .0
        .expect_err("a line without its newline must not be read");
    assert!(
        err.contains("git rev-parse printed no line"),
        "err must name the missing line: {err}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn head_sha_kills_the_descendant_of_a_refused_git_wrapper() {
    // A `git` on PATH can be a wrapper script that leaves a background
    // process behind. The descendant inherits the probe's stdout pipe, so a
    // probe that signalled the wrapper's pid alone would report its refusal
    // while the descendant kept running with the pipe open. The wrapper
    // records its descendant's pid, the probe is cancelled as soon as that
    // pid is readable, and the descendant must be gone once the probe
    // returns.
    let dir = temp_dir("head-sha-descendant");
    let pid_file = dir.join("descendant.pid");
    let wrapper = make_script(
        &dir,
        "fake-git-wrapper.sh",
        &format!(
            "sleep {DESCENDANT_SLEEP_SECS} &\nprintf '%s\\n' \"$!\" > {}\nwait\n",
            shell_word(&pid_file)
        ),
    );

    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_for_watcher = Arc::clone(&cancel);
    let watched_pid_file = pid_file.clone();
    let watcher = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(GIT_HEAD_TIMEOUT_SECS);
        loop {
            if let Some(pid) = read_pid_file(&watched_pid_file) {
                cancel_for_watcher.store(true, Ordering::SeqCst);
                return Some(pid);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(PROBE_POLL);
        }
    });

    let err = probe_script(&wrapper, &cancel)
        .0
        .expect_err("a cancelled probe must fail");
    let descendant = watcher
        .join()
        .expect("watcher thread")
        .expect("the wrapper must record its descendant pid");
    assert!(err.contains("cancelled"), "err must name the cause: {err}");

    // The descendant is orphaned by the group kill and reaped by init, so
    // the ESRCH can arrive a scheduling moment after the probe returned —
    // and on a host whose init is slow to collect it, never: a zombie
    // still answers `kill`, so the state of the pid decides.
    let deadline = Instant::now() + Duration::from_secs(PROBE_RETURN_SLACK_SECS);
    loop {
        match kill(Pid::from_raw(descendant), None) {
            Err(nix::errno::Errno::ESRCH) => break,
            Ok(()) => {
                if is_zombie(descendant) {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "descendant pid {descendant} outlived the refused probe"
                );
            }
            Err(e) => panic!("kill({descendant}, None) -> unexpected errno {e}"),
        }
        std::thread::sleep(PROBE_POLL);
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn head_sha_kills_a_git_that_left_its_process_group() {
    // A `git` is free to call `setpgid` and join another group. The group
    // signal of the cleanup then names a group the probe is no longer in,
    // so only the signal addressed to the probe's own pid can end it — and
    // without that signal the reap after it blocks forever. The fixture is
    // this test binary re-run on [`GROUP_ESCAPE_FIXTURE`] (a shell cannot
    // make the `setpgid` call, and an interpreter on `PATH` is not a
    // prerequisite this suite declares): it moves itself into the test
    // process's group, records the move, and then outlives the whole
    // deadline. The probe must still return within its bound.
    let dir = temp_dir("head-sha-group-escape");
    let move_file = dir.join("moved.pgid");
    let fixture_exe = std::env::current_exe().expect("the path of this test binary");
    let escaper = make_script(
        &dir,
        "fake-git-escape.sh",
        &format!(
            "{GROUP_ESCAPE_FILE_ENV}={}\nexport {GROUP_ESCAPE_FILE_ENV}\n\
             exec {} {GROUP_ESCAPE_FIXTURE} --exact --ignored\n",
            shell_word(&move_file),
            shell_word(&fixture_exe)
        ),
    );

    // The probe runs off the test thread, so a cleanup that blocks fails
    // this assertion instead of hanging the whole suite.
    let (tx, rx) = std::sync::mpsc::channel();
    let probe = escaper.clone();
    let prober = std::thread::spawn(move || {
        let _ = tx.send(probe_script(&probe, &AtomicBool::new(false)).0);
    });
    let err = rx
        .recv_timeout(Duration::from_secs(
            GIT_HEAD_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS,
        ))
        .expect("the probe must return within its bound")
        .expect_err("a git that never answers must fail the probe");
    prober.join().expect("probe thread");
    assert!(
        err.contains("git rev-parse did not answer within"),
        "err must name the deadline: {err}"
    );

    // Without the move the group signal alone would have ended the probe,
    // and the bound above would prove nothing about the pid signal.
    let (pid, pgid) = read_pid_pgid_file(&move_file)
        .expect("the fixture must record the group it moved into");
    assert_ne!(
        pgid, pid,
        "the fixture must leave the group it leads for this test to mean anything"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn head_sha_bounds_reports_and_later_reaps_a_probe_its_sigkill_cannot_reach() {
    // A probe whose real and saved UID differ from the runner's own can
    // refuse both cleanup SIGKILLs with EPERM. The reap after them must
    // still return within its own bound instead of blocking on the
    // probe's exit, and the probe — unreachable by this runner — must
    // still be reaped once it exits on its own.
    let dir = temp_dir("head-sha-unkillable");
    let pid_file = dir.join("probe.pid");
    let script = make_script(
        &dir,
        "fake-git-unkillable.sh",
        &format!(
            "printf '%s\\n' \"$$\" > {}\nexec sleep {DESCENDANT_SLEEP_SECS}\n",
            shell_word(&pid_file)
        ),
    );

    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_for_watcher = Arc::clone(&cancel);
    let watched_pid_file = pid_file.clone();
    let watcher = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(GIT_HEAD_TIMEOUT_SECS);
        loop {
            if let Some(pid) = read_pid_file(&watched_pid_file) {
                cancel_for_watcher.store(true, Ordering::SeqCst);
                return Some(pid);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(PROBE_POLL);
        }
    });

    let (tx, rx) = std::sync::mpsc::channel();
    let prober = std::thread::spawn(move || {
        let _ = tx.send(retry_busy(|| {
            head_sha_with(&script, &cancel, |_, _| Err(Errno::EPERM))
        }));
    });

    let outcome = rx.recv_timeout(Duration::from_secs(
        GIT_HEAD_TIMEOUT_SECS + SIGKILL_REAP_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS,
    ));
    let probe_pid = watcher
        .join()
        .expect("watcher thread")
        .expect("the fixture must record its pid");
    // The injected signal refused both SIGKILLs, so nothing but this
    // test's own cleanup ends the fixture.
    let _ = kill(Pid::from_raw(probe_pid), Signal::SIGKILL);

    let (result, elapsed) = outcome.expect("the probe must return within its bound");
    let err = result.expect_err("a probe whose signals are refused must still fail");
    assert!(
        err.starts_with(&format!(
            "cancelled; git rev-parse cleanup: probe {probe_pid} still running"
        )),
        "err must name the cause and the bounded reap: {err}"
    );
    assert!(
        err.contains("group SIGKILL: EPERM"),
        "err must name the refused group signal: {err}"
    );
    assert!(
        err.contains("pid SIGKILL: EPERM"),
        "err must name the refused pid signal: {err}"
    );
    assert!(
        elapsed < Duration::from_secs(SIGKILL_REAP_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS),
        "the reap must return within its own bound: {elapsed:?}"
    );

    // AC-3: this test plays the long-lived runner, which must eventually
    // reap the probe once it exits on its own — a zombie nobody reaped is
    // as much a failure here as a probe still running.
    let deadline = Instant::now() + Duration::from_secs(PROBE_RETURN_SLACK_SECS);
    loop {
        match kill(Pid::from_raw(probe_pid), None) {
            Err(Errno::ESRCH) => break,
            Ok(()) => {
                assert!(
                    Instant::now() < deadline,
                    "probe pid {probe_pid} was not reaped after it exited"
                );
            }
            Err(e) => panic!("kill({probe_pid}, None) -> unexpected errno {e}"),
        }
        std::thread::sleep(PROBE_POLL);
    }

    prober.join().expect("probe thread");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn head_sha_reports_its_deadline_before_a_cleanup_it_could_not_complete() {
    // AC-4's other cause: the probe is refused by the deadline rather than
    // by cancellation, and its cleanup SIGKILLs are then refused too. AC-15
    // is matched on the `did not answer within` substring, so that refusal
    // must stay at the front of the combined error and the cleanup must
    // follow it.
    let dir = temp_dir("head-sha-unkillable-deadline");
    let pid_file = dir.join("probe.pid");
    let keep = dir.join("keep");
    fs::write(&keep, b"").expect("create the fixture's keep-alive file");
    let script = make_script(
        &dir,
        "fake-git-unkillable-silent.sh",
        &format!(
            "printf '%s\\n' \"$$\" > {}\n{}",
            shell_word(&pid_file),
            wait_while_kept(&keep)
        ),
    );

    let cancel = AtomicBool::new(false);
    let (tx, rx) = std::sync::mpsc::channel();
    let probe = script.clone();
    let prober = std::thread::spawn(move || {
        let _ = tx.send(retry_busy(|| {
            head_sha_with(&probe, &cancel, |_, _| Err(Errno::EPERM))
        }));
    });

    // Captured while the fixture is certainly still sleeping, so the pid
    // names the fixture and not a later owner of that number.
    let fixture = await_fixture(&pid_file, Duration::from_secs(GIT_HEAD_TIMEOUT_SECS));
    let outcome = rx.recv_timeout(Duration::from_secs(
        GIT_HEAD_TIMEOUT_SECS + SIGKILL_REAP_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS,
    ));
    // The injected signal refused both SIGKILLs, so only this cleanup ends
    // the fixture. It asks for that exit through the keep-alive file and
    // never signals the pid: the reaper thread `reap_probe` left behind owns
    // the probe's one reap and is free to free that pid number between an
    // identity check and a `kill` after it (`end_fixture`).
    let fixture = fixture.expect("the fixture must record its pid");
    let ended = end_fixture(
        &keep,
        fixture,
        Duration::from_secs(FIXTURE_KEEP_POLL_SECS + FIXTURE_EXIT_SLACK_SECS),
    );
    let _ = fs::remove_dir_all(&dir);

    let (result, elapsed) = outcome.expect("the probe must return within its bound");
    prober.join().expect("probe thread");
    let err = result.expect_err("a probe that never answers must fail");
    let pid = fixture.pid;
    assert!(
        err.starts_with(&format!(
            "git rev-parse did not answer within {GIT_HEAD_TIMEOUT_SECS}s; \
             git rev-parse cleanup: probe {pid} still running"
        )),
        "the deadline must lead the combined error: {err}"
    );
    assert!(
        elapsed
            < Duration::from_secs(
                GIT_HEAD_TIMEOUT_SECS + SIGKILL_REAP_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS
            ),
        "the deadline and the reap bound must both hold: {elapsed:?}"
    );
    assert!(
        ended,
        "the fixture must exit once the test stops keeping it alive, \
         leaving nothing of this test running"
    );
}

#[test]
fn head_sha_names_only_the_cleanup_signal_that_failed() {
    // AC-2 names each failed signal, which means it names no other: a
    // cleanup error that listed a signal the kernel accepted would send the
    // operator after a privilege problem that does not exist. The group
    // signal is refused with EPERM and the pid signal is accepted by a
    // stand-in that sends nothing, so the probe outlives the cleanup with
    // exactly one failure to report.
    let dir = temp_dir("head-sha-one-refused-signal");
    let pid_file = dir.join("probe.pid");
    let keep = dir.join("keep");
    fs::write(&keep, b"").expect("create the fixture's keep-alive file");
    let script = make_script(
        &dir,
        "fake-git-group-refused.sh",
        &format!(
            "printf '%s\\n' \"$$\" > {}\n{}",
            shell_word(&pid_file),
            wait_while_kept(&keep)
        ),
    );

    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_for_watcher = Arc::clone(&cancel);
    let watched_pid_file = pid_file.clone();
    let watcher = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(GIT_HEAD_TIMEOUT_SECS);
        loop {
            if read_pid_file(&watched_pid_file).is_some() {
                cancel_for_watcher.store(true, Ordering::SeqCst);
                return;
            }
            if Instant::now() >= deadline {
                return;
            }
            std::thread::sleep(PROBE_POLL);
        }
    });

    let (tx, rx) = std::sync::mpsc::channel();
    let probe = script.clone();
    let prober = std::thread::spawn(move || {
        // The group is addressed by the negated leader pid, so the sign of
        // the argument is what tells the two cleanup signals apart.
        let _ = tx.send(retry_busy(|| {
            head_sha_with(&probe, &cancel, |pid: Pid, _| {
                if pid.as_raw() < 0 {
                    Err(Errno::EPERM)
                } else {
                    Ok(())
                }
            })
        }));
    });

    let fixture = await_fixture(&pid_file, Duration::from_secs(GIT_HEAD_TIMEOUT_SECS));
    let outcome = rx.recv_timeout(Duration::from_secs(
        GIT_HEAD_TIMEOUT_SECS + SIGKILL_REAP_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS,
    ));
    watcher.join().expect("watcher thread");
    // Neither injected signal reached the fixture, so only this cleanup
    // ends it, through the keep-alive file: the reaper thread owns the reap
    // and may free the pid number (see the deadline test).
    let fixture = fixture.expect("the fixture must record its pid");
    let ended = end_fixture(
        &keep,
        fixture,
        Duration::from_secs(FIXTURE_KEEP_POLL_SECS + FIXTURE_EXIT_SLACK_SECS),
    );
    let _ = fs::remove_dir_all(&dir);

    let (result, _) = outcome.expect("the probe must return within its bound");
    prober.join().expect("probe thread");
    let err = result.expect_err("a probe that outlives its cleanup must fail");
    let pid = fixture.pid;
    assert!(
        err.starts_with(&format!(
            "cancelled; git rev-parse cleanup: probe {pid} still running"
        )),
        "err must name the cause and the bounded reap: {err}"
    );
    assert!(
        err.contains("group SIGKILL: EPERM"),
        "err must name the refused group signal: {err}"
    );
    assert!(
        !err.contains("pid SIGKILL"),
        "err must not name a signal that was accepted: {err}"
    );
    assert!(
        ended,
        "the fixture must exit once the test stops keeping it alive, \
         leaving nothing of this test running"
    );
}

/// The fake `git` of `head_sha_kills_a_git_that_left_its_process_group`,
/// not a test of its own: `#[ignore]` keeps it out of the suite, and that
/// test runs this binary with `--exact --ignored` to get a `git` that
/// leaves the process group the probe spawned it into. Without
/// [`GROUP_ESCAPE_FILE_ENV`] — a `--ignored` run of the whole suite — it
/// returns at once and moves nothing.
#[test]
#[ignore = "fixture process of head_sha_kills_a_git_that_left_its_process_group"]
fn group_escape_fixture() {
    let Ok(move_file) = std::env::var(GROUP_ESCAPE_FILE_ENV) else {
        return;
    };
    // The probe's parent is the test process that spawned the probe, so
    // its group is a group in this session that the probe does not lead.
    let parent_group = getpgid(Some(getppid())).expect("the parent's process group");
    setpgid(Pid::from_raw(0), parent_group).expect("join the parent's process group");
    // One write, with the terminating newline last, so a reader never
    // sees half a line.
    fs::write(&move_file, format!("{} {}\n", getpid(), getpgrp()))
        .expect("record the move");
    std::thread::sleep(Duration::from_secs(DESCENDANT_SLEEP_SECS));
}

#[test]
fn build_probe_returns_the_stdout_of_a_command_that_exits_zero() {
    let dir = temp_dir("build-probe-ok");

    let spaced = make_script(&dir, "spaced.sh", "printf ' abc1234 \\n'\n");
    assert_eq!(
        build_script::bounded_stdout(sh(&spaced), Child::kill),
        Some(b" abc1234 \n".to_vec()),
        "bounded_stdout must hand back the untrimmed bytes the command printed"
    );

    let binary = make_script(&dir, "binary.sh", "printf '\\377\\376'\n");
    assert_eq!(
        build_script::bounded_stdout(sh(&binary), Child::kill),
        Some(vec![0xff, 0xfe]),
        "bounded_stdout must hand back the bytes the command printed, not a text interpretation"
    );

    let empty = make_script(&dir, "empty.sh", "exit 0\n");
    assert_eq!(
        build_script::bounded_stdout(sh(&empty), Child::kill),
        Some(Vec::new()),
        "a command that exits 0 with no output must answer Some(empty)"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn bench_build_embeds_the_checkout_sha() {
    // The expected SHA comes from the runtime probe, which is bounded on its
    // own, so this test cannot hang the suite. Run from `bench/`, inside the
    // same repository whose state `bench/build.rs` embeds.
    let expected = head_sha(std::path::Path::new("git"), &AtomicBool::new(false))
        .expect("the checkout's git must answer");
    let observed = capture_env(&[], &AtomicBool::new(false)).expect("capture_env").git_sha;
    assert_eq!(
        observed, expected,
        "the build-time probe must embed the same SHA the runtime probe reads"
    );
}

#[test]
fn build_probe_gives_up_on_a_command_that_exits_non_zero() {
    let dir = temp_dir("build-probe-non-zero");
    let script = make_script(&dir, "non-zero.sh", "printf 'abc1234\\n'\nexit 128\n");
    assert_eq!(
        build_script::bounded_stdout(sh(&script), Child::kill),
        None,
        "a command that exits non-zero must give up the probe"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn build_probe_gives_up_on_a_command_that_never_exits() {
    // AC-9, AC-10: a `git` that never answers must not stall the probe, and
    // the probe must kill and reap what it gave up on.
    let dir = temp_dir("build-probe-never-exits");
    let pid_file = dir.join("probe.pid");
    let script = make_script(
        &dir,
        "fake-git-never-exits.sh",
        &format!(
            "printf '%s\\n' \"$$\" > {}\nexec sleep {DESCENDANT_SLEEP_SECS}\n",
            shell_word(&pid_file)
        ),
    );

    let (tx, rx) = std::sync::mpsc::channel();
    let probe = script.clone();
    let (kill_probe, released) = kill_with_release(Child::kill);
    // One deadline for the whole probe, started before it: the wait for the
    // fixture's pid spends this budget instead of extending it.
    let launched = Instant::now();
    let prober = std::thread::spawn(move || {
        let _ = tx.send(build_script::bounded_stdout(sh(&probe), kill_probe));
    });
    // The identity capture runs while the fixture is still sleeping, so
    // cleanup can tell the fixture from a later owner of its pid.
    let fixture = await_fixture(&pid_file, probe_return_bound(launched));
    let outcome = rx.recv_timeout(probe_return_bound(launched));
    // Cleanup runs before every assertion, so a probe that gave up without
    // reaping cannot leave the fixture sleeping for the rest of the suite.
    // It acts only on a fixture every probe thread has released.
    let released = await_release(
        &released,
        Duration::from_secs(BUILD_PROBE_REAP_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS),
    );
    let alive = released_fixture(fixture, released).map(reap_fixture);
    let _ = fs::remove_dir_all(&dir);

    let outcome = outcome.expect("the build probe must return within its bound");
    prober.join().expect("probe thread");
    assert_eq!(outcome, None, "the build probe must give up within its bound");
    assert_eq!(
        alive,
        Some(Err(Errno::ESRCH)),
        "the build probe must kill and reap what it gave up on"
    );
}

#[test]
fn build_probe_gives_up_within_its_bound_when_its_kill_is_refused() {
    // AC-9: a kill stand-in that signals nothing must still let the probe
    // give up within its bound.
    let dir = temp_dir("build-probe-kill-refused");
    let pid_file = dir.join("probe.pid");
    let script = make_script(
        &dir,
        "fake-git-unkillable.sh",
        &format!(
            "printf '%s\\n' \"$$\" > {}\nexec sleep {DESCENDANT_SLEEP_SECS}\n",
            shell_word(&pid_file)
        ),
    );

    let (tx, rx) = std::sync::mpsc::channel();
    let probe = script.clone();
    let (kill_probe, released) = kill_with_release(|_: &mut Child| Ok(()));
    let launched = Instant::now();
    let prober = std::thread::spawn(move || {
        let _ = tx.send(build_script::bounded_stdout(sh(&probe), kill_probe));
    });
    let fixture = await_fixture(&pid_file, probe_return_bound(launched));
    let outcome = rx.recv_timeout(probe_return_bound(launched));
    // The injected kill signalled nothing, so only this cleanup ends the
    // fixture. It runs before every assertion for that reason, and only on
    // a fixture every probe thread has released.
    let released = await_release(
        &released,
        Duration::from_secs(BUILD_PROBE_REAP_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS),
    );
    let alive = released_fixture(fixture, released).map(reap_fixture);
    let _ = fs::remove_dir_all(&dir);

    let outcome = outcome.expect("the build probe must return within its bound");
    prober.join().expect("probe thread");
    assert!(alive.is_some(), "the fixture must record its pid");
    assert_eq!(
        outcome, None,
        "the build probe must give up within its bound even when its kill is refused"
    );
}

#[test]
fn build_probe_gives_up_on_a_command_whose_descendant_holds_stdout() {
    // AC-11: a command whose descendant inherits and holds stdout open must
    // not stall the probe past its bound.
    let dir = temp_dir("build-probe-descendant-stdout");
    let pid_file = dir.join("descendant.pid");
    let keep = dir.join("keep");
    fs::write(&keep, b"").expect("create the descendant's keep-alive file");
    let script = make_script(
        &dir,
        "fake-git-descendant.sh",
        &format!(
            "(\n{})&\nprintf '%s\\n' \"$!\" > {}\nprintf 'abc1234\\n'\nexit 0\n",
            wait_while_kept(&keep),
            shell_word(&pid_file)
        ),
    );

    let (tx, rx) = std::sync::mpsc::channel();
    let probe = script.clone();
    let (kill_probe, released) = kill_with_release(Child::kill);
    let launched = Instant::now();
    let prober = std::thread::spawn(move || {
        let _ = tx.send(build_script::bounded_stdout(sh(&probe), kill_probe));
    });
    let fixture = await_fixture(&pid_file, probe_return_bound(launched));
    let outcome = rx.recv_timeout(probe_return_bound(launched));
    // This descendant is never signalled, unlike the commands the other
    // build-probe tests clean up. The fake `git` backgrounds it and exits,
    // so it is adopted by init or by a subreaper, and that parent can reap
    // it whenever it ends — release proves no probe thread will reap it, not
    // that no one else will. A pid freed that way can be handed to another
    // process of this user, and nothing here can pin the number across the
    // identity check and the signal after it ([`reap_fixture`]). Its exit is
    // asked for through the keep-alive file instead, which no reaper can
    // take from the test: nothing of this test is left running, and the
    // descendant stops holding the probe's stdout open, which ends the
    // reader thread blocked on it.
    let ended = fixture.map(|fixture| {
        end_fixture(
            &keep,
            fixture,
            Duration::from_secs(FIXTURE_KEEP_POLL_SECS + FIXTURE_EXIT_SLACK_SECS),
        )
    });
    let released = await_release(
        &released,
        Duration::from_secs(BUILD_PROBE_REAP_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS),
    );
    let recorded = released_fixture(fixture, released);
    let _ = fs::remove_dir_all(&dir);

    let outcome = outcome.expect("the build probe must return within its bound");
    prober.join().expect("probe thread");
    assert!(
        recorded.is_some(),
        "the fixture must record its descendant pid"
    );
    assert_eq!(
        outcome, None,
        "a command whose descendant holds stdout open must give up within the probe's bound"
    );
    assert_eq!(
        ended,
        Some(true),
        "the descendant must exit once the test stops keeping it alive, \
         leaving neither an orphan nor a blocked reader behind"
    );
}

#[test]
#[allow(clippy::cast_possible_truncation)]
fn build_probe_keeps_exactly_its_cap_and_refuses_one_byte_more() {
    // AC-7, AC-12: stdout exactly at the cap must be kept in full; the
    // fixture one byte past it must be refused.
    let dir = temp_dir("build-probe-cap");

    let exact = make_script(
        &dir,
        "exact-cap.sh",
        &format!("head -c {BUILD_PROBE_STDOUT_CAP_BYTES} /dev/zero\n"),
    );
    let (tx, rx) = std::sync::mpsc::channel();
    let probe = exact.clone();
    let prober = std::thread::spawn(move || {
        let _ = tx.send(build_script::bounded_stdout(sh(&probe), Child::kill));
    });
    let outcome = rx
        .recv_timeout(Duration::from_secs(
            BUILD_PROBE_TIMEOUT_SECS + BUILD_PROBE_REAP_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS,
        ))
        .expect("the build probe must return within its bound");
    prober.join().expect("probe thread");
    let kept = outcome.expect("stdout exactly at the cap must be kept");
    assert_eq!(
        kept.len(),
        BUILD_PROBE_STDOUT_CAP_BYTES as usize,
        "the probe must keep exactly its cap"
    );
    assert!(
        kept.iter().all(|&b| b == 0),
        "the kept bytes must be the /dev/zero bytes the fixture printed"
    );

    let over_cap = BUILD_PROBE_STDOUT_CAP_BYTES + 1;
    let over = make_script(&dir, "over-cap.sh", &format!("head -c {over_cap} /dev/zero\n"));
    let (tx, rx) = std::sync::mpsc::channel();
    let probe = over.clone();
    let prober = std::thread::spawn(move || {
        let _ = tx.send(build_script::bounded_stdout(sh(&probe), Child::kill));
    });
    let outcome = rx
        .recv_timeout(Duration::from_secs(
            BUILD_PROBE_TIMEOUT_SECS + BUILD_PROBE_REAP_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS,
        ))
        .expect("the build probe must return within its bound");
    prober.join().expect("probe thread");
    assert_eq!(outcome, None, "stdout one byte past the cap must be refused");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn build_probe_gives_up_on_a_command_that_cannot_start() {
    // A `git` that is not on PATH makes the spawn fail. The spawn runs on
    // the probe's own starting thread, so the failure has to travel back
    // over the rendezvous instead of being returned where it happened, and
    // `main` must still get the `unknown` third state out of it.
    let dir = temp_dir("build-probe-no-such-command");
    let missing = dir.join("no-such-command");
    assert_eq!(
        build_script::bounded_stdout(Command::new(&missing), Child::kill),
        None,
        "a command that cannot start must give up the probe"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn build_probe_answers_the_git_status_the_build_script_runs() {
    // Q7 put `--no-optional-locks` on both build-time probes so a probe
    // killed at its deadline cannot leave a stale `.git/index.lock`. It is
    // a global option: git rejects it after the subcommand, and a rejected
    // option is a non-zero exit, which the probe reads as a failure and
    // `main` embeds as `unknown`. `bench_build_embeds_the_checkout_sha`
    // proves the `rev-parse` probe answers; this is the same proof for the
    // `status` probe, whose value no record carries on its own.
    //
    // The command comes from `build.rs`'s own `git` helper, so a moved
    // `--no-optional-locks` fails here too. The helper probes the parent
    // directory — the test's cwd is `bench/`, as the build script's is.
    assert!(
        build_script::bounded_stdout(build_script::git(&["status", "--porcelain"]), Child::kill)
            .is_some(),
        "the checkout's git must accept the build probe's own argument order"
    );
}

#[test]
fn mosquitto_conf_renders_the_approved_options() {
    let conf = mosquitto_conf(1883);
    let expected = "listener 1883 127.0.0.1\n\
                    allow_anonymous true\n\
                    persistence false\n\
                    max_queued_messages 0\n\
                    max_inflight_messages 0\n\
                    set_tcp_nodelay true\n\
                    log_dest none\n";
    assert_eq!(conf, expected);
}

#[test]
fn declared_rates_maps_every_row_group_to_its_calibrated_pair() {
    // The committed calibration (`bench/CALIBRATION.md`) is what the §9.1
    // declared rates mean, so an edited rate is a changed experiment and
    // needs a fresh calibration and a fresh dataset. These literals are the
    // calibrated values, asserted at the public seam `matrix::run_matrix_with`
    // and `doc::row_rates` dispatch through.
    assert_eq!(DECLARED_RATES_W1_Q1, DeclaredRates { lower: 75_000, higher: 300_000 });
    assert_eq!(DECLARED_RATES_W1_Q0, DeclaredRates { lower: 150_000, higher: 400_000 });
    assert_eq!(DECLARED_RATES_W4_Q1, DeclaredRates { lower: 900_000, higher: 1_200_000 });
    assert_eq!(DECLARED_RATES_W4_Q0, DeclaredRates { lower: 1_300_000, higher: 1_500_000 });
    assert_eq!(SMOKE_DECLARED_RATES, DeclaredRates { lower: 40_000, higher: 60_000 });

    for (workers, qos, expected) in [
        (1, 1, DECLARED_RATES_W1_Q1),
        (1, 0, DECLARED_RATES_W1_Q0),
        (4, 1, DECLARED_RATES_W4_Q1),
        (4, 0, DECLARED_RATES_W4_Q0),
    ] {
        assert_eq!(
            declared_rates(Profile::Recorded, workers, qos),
            Some(expected),
            "recorded {workers} workers QoS {qos} must offer its own calibrated pair"
        );
        assert_eq!(
            declared_rates(Profile::Smoke, workers, qos),
            Some(SMOKE_DECLARED_RATES),
            "smoke offers one pair for every row group"
        );
        assert_eq!(
            expected.both(),
            [expected.lower, expected.higher],
            "the open-loop stage dispatches the lower rate first"
        );
    }

    // Outside the §9.1 matrix no calibration exists, so the lookup refuses
    // instead of lending a neighbouring row group's rates.
    assert_eq!(declared_rates(Profile::Recorded, 2, 1), None);
    assert_eq!(declared_rates(Profile::Recorded, 1, 2), None);
}

#[test]
fn capture_env_refuses_a_tool_that_prints_no_version() {
    let dir = temp_dir("blank-version");
    let tool = make_script(&dir, "blank-version.sh", "printf '\\n   \\n'\n");
    let err = capture_env(std::slice::from_ref(&tool), &AtomicBool::new(false))
        .expect_err("a probe that prints no version must fail the capture");
    assert!(err.contains(&tool.display().to_string()), "err must name the tool: {err}");
    assert!(
        err.contains("--version printed no non-blank line"),
        "err must name the cause: {err}"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn capture_env_records_the_first_non_blank_version_line() {
    let dir = temp_dir("leading-blank-version");
    let tool = make_script(
        &dir,
        "leading-blank-version.sh",
        "printf '\\n  \\nfaketool version 1.2.3  \\nsecond line\\n'\n",
    );
    let env =
        capture_env(std::slice::from_ref(&tool), &AtomicBool::new(false)).expect("a probe that prints a version must pass");
    assert_eq!(
        env.tools,
        vec![(
            tool.to_string_lossy().into_owned(),
            "faketool version 1.2.3".to_string()
        )]
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn capture_env_times_out_a_tool_that_prints_only_blank_lines() {
    // Three blank lines and then silence with stdout still open: the reader
    // drops every blank line, so only the deadline can end the probe. A probe
    // that waited for EOF instead would hang, and one that recorded a blank
    // line would pass with an empty version.
    let dir = temp_dir("blank-flood-version");
    let tool = make_script(
        &dir,
        "blank-flood-version.sh",
        &format!("printf '\\n \\n\\t\\n'\nexec sleep {DESCENDANT_SLEEP_SECS}\n"),
    );
    let err = retry_busy(|| {
        capture_env(std::slice::from_ref(&tool), &AtomicBool::new(false))
            .map(|env| env.tools[0].1.clone())
    })
    .0
    .expect_err("a probe that never prints a version must fail the capture");
    assert!(
        err.contains("--version did not answer within"),
        "err must name the deadline: {err}"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn capture_env_refuses_unterminated_version_flood_before_its_deadline() {
    // A tool that floods stdout with more bytes than the cap and never
    // stops must be refused as soon as the cap's EOF is reached, well
    // before the probe's deadline — not held until the deadline fires.
    let dir = temp_dir("version-flood-unterminated");
    let tool = make_script(
        &dir,
        "version-flood-unterminated.sh",
        &format!(
            "head -c {} /dev/zero | tr '\\000' x\nexec sleep {DESCENDANT_SLEEP_SECS}\n",
            2 * TOOL_VERSION_STDOUT_CAP_BYTES
        ),
    );
    let (result, elapsed) = retry_busy(|| {
        capture_env(std::slice::from_ref(&tool), &AtomicBool::new(false))
            .map(|env| env.tools[0].1.clone())
    });
    let err = result.expect_err("a flood that never ends its line must be refused");
    assert!(
        err.contains(&format!(
            "--version printed a line with no newline within {TOOL_VERSION_STDOUT_CAP_BYTES} bytes"
        )),
        "err must name the cause: {err}"
    );
    assert!(
        elapsed < Duration::from_secs(TOOL_VERSION_TIMEOUT_SECS),
        "the refusal must arrive before the deadline: {elapsed:?}"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn capture_env_refuses_unterminated_version_line_at_eof() {
    // A tool that exits right after printing a version with no trailing
    // newline reaches EOF mid-line; that line must be refused, not
    // recorded as the version.
    let dir = temp_dir("version-unterminated-eof");
    let tool = make_script(
        &dir,
        "version-unterminated-eof.sh",
        "printf 'faketool version 1.0'\n",
    );
    let err = retry_busy(|| {
        capture_env(std::slice::from_ref(&tool), &AtomicBool::new(false))
            .map(|env| env.tools[0].1.clone())
    })
    .0
    .expect_err("a line left unterminated at EOF must be refused");
    assert!(
        err.contains(&format!(
            "--version printed a line with no newline within {TOOL_VERSION_STDOUT_CAP_BYTES} bytes"
        )),
        "err must name the cause: {err}"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn capture_env_records_a_version_line_that_ends_exactly_at_its_stdout_cap() {
    // The version line's own newline lands on the last byte the capped
    // reader reads, so the line is whole and the probe records it in
    // full. Regression guard on the cap's boundary: passes before and
    // after the cap is added.
    let dir = temp_dir("version-exact-fit");
    let pad = TOOL_VERSION_STDOUT_CAP_BYTES - VERSION_LINE.len();
    let tool = make_script(
        &dir,
        "version-exact-fit.sh",
        &format!("printf '%{pad}s' ''\nprintf '{VERSION_LINE}'"),
    );
    let version = retry_busy(|| {
        capture_env(std::slice::from_ref(&tool), &AtomicBool::new(false))
            .map(|env| env.tools[0].1.clone())
    })
    .0
    .expect("a version line that ends exactly at the cap must be recorded");
    assert_eq!(version, "faketool version 1.0");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn capture_env_refuses_unterminated_version_character_its_cap_cuts() {
    // Padding that leaves the cap room for only the first byte of a
    // two-byte UTF-8 character cuts the stream mid-character; the reader
    // must refuse that as a read failure, not answer with a cut version.
    let dir = temp_dir("version-utf8-cut");
    let pad = TOOL_VERSION_STDOUT_CAP_BYTES - 1;
    let tool = make_script(
        &dir,
        "version-utf8-cut.sh",
        &format!("printf '%{pad}s' ''\nprintf '\\303\\251\\n'\n"),
    );
    let err = retry_busy(|| {
        capture_env(std::slice::from_ref(&tool), &AtomicBool::new(false))
            .map(|env| env.tools[0].1.clone())
    })
    .0
    .expect_err("a character the cap cuts must not be read");
    assert!(
        err.contains("read --version stdout:"),
        "err must name the read failure: {err}"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn capture_env_refuses_a_tool_whose_version_stdout_is_empty() {
    // A probe that prints no byte at all reaches EOF on its first
    // receive, with no blank line to skip. It must report the same
    // no-version cause as a probe that printed only blank lines.
    let dir = temp_dir("silent-version");
    let tool = make_script(&dir, "silent-version.sh", "exit 0\n");
    let err = capture_env(std::slice::from_ref(&tool), &AtomicBool::new(false))
        .expect_err("a probe that prints nothing must fail the capture");
    assert!(err.contains(&tool.display().to_string()), "err must name the tool: {err}");
    assert!(
        err.contains("--version printed no non-blank line"),
        "err must name the cause: {err}"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn capture_env_skips_tab_only_lines_and_trims_the_version() {
    // Whitespace is not only spaces: a tab-only line is blank and a
    // tab-padded version line still records the bare version.
    let dir = temp_dir("tab-version");
    let tool = make_script(
        &dir,
        "tab-version.sh",
        "printf '\\t\\n \\t \\n\\tfaketool version 4.5\\t\\n'\n",
    );
    let env =
        capture_env(std::slice::from_ref(&tool), &AtomicBool::new(false)).expect("a probe that prints a version must pass");
    assert_eq!(
        env.tools,
        vec![(
            tool.to_string_lossy().into_owned(),
            "faketool version 4.5".to_string()
        )]
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn capture_env_probes_every_supplied_tool_in_order() {
    // One blank-line prefix must not leak into the next probe: each
    // tool gets its own reader, and `env.tools` keeps the supplied
    // order so the `## Environment` rows name the right binaries.
    let dir = temp_dir("two-tool-version");
    let first = make_script(
        &dir,
        "first-version.sh",
        "printf '\\n\\nfirsttool version 1.0\\n'\n",
    );
    let second = make_script(&dir, "second-version.sh", "printf 'secondtool version 2.0\\n'\n");
    let env = capture_env(&[first.clone(), second.clone()], &AtomicBool::new(false)).expect("both probes must pass");
    assert_eq!(
        env.tools,
        vec![
            (
                first.to_string_lossy().into_owned(),
                "firsttool version 1.0".to_string()
            ),
            (
                second.to_string_lossy().into_owned(),
                "secondtool version 2.0".to_string()
            )
        ]
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn capture_env_with_bounds_reports_and_later_reaps_a_probe_its_sigkill_cannot_reach() {
    // AC-1/AC-2's refused-cleanup path for a `--version` probe, mirroring
    // `head_sha_reports_its_deadline_before_a_cleanup_it_could_not_complete`
    // (`head_sha_with`), except the probe is ended by cancellation rather
    // than by its deadline.
    let dir = temp_dir("capture-env-with-unkillable");
    let pid_file = dir.join("probe.pid");
    let keep = dir.join("keep");
    fs::write(&keep, b"").expect("create the fixture's keep-alive file");
    let tool = make_script(
        &dir,
        "unkillable-version.sh",
        &format!(
            "printf '%s\\n' \"$$\" > {}\n{}",
            shell_word(&pid_file),
            wait_while_kept(&keep)
        ),
    );

    let cancel = Arc::new(AtomicBool::new(false));
    let probe_cancel = Arc::clone(&cancel);
    let probe_tool = tool.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let prober = std::thread::spawn(move || {
        let _ = tx.send(retry_busy(|| {
            capture_env_with(std::slice::from_ref(&probe_tool), &probe_cancel, |_, _| {
                Err(Errno::EPERM)
            })
            .map(|_| String::new())
        }));
    });

    // Captured while the fixture is certainly still sleeping, so the pid
    // names the fixture and not a later owner of that number.
    let fixture = await_fixture(&pid_file, Duration::from_secs(TOOL_VERSION_TIMEOUT_SECS))
        .expect("the fixture must record its pid");
    cancel.store(true, Ordering::SeqCst);
    let cancelled_at = Instant::now();
    let outcome = rx.recv_timeout(Duration::from_secs(
        SIGKILL_REAP_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS,
    ));
    let returned_after = cancelled_at.elapsed();

    // Release the fixture and wait for its reap before any assertion, so a
    // failed assertion below cannot leave it running.
    let ended = end_fixture(
        &keep,
        fixture,
        Duration::from_secs(FIXTURE_KEEP_POLL_SECS + FIXTURE_EXIT_SLACK_SECS),
    );
    let reap_deadline = Instant::now() + Duration::from_secs(PROBE_RETURN_SLACK_SECS);
    let reaped: bool = loop {
        if proc_start_time(fixture.pid.as_raw()) != Some(fixture.started) {
            break true;
        }
        if Instant::now() >= reap_deadline {
            break false;
        }
        std::thread::sleep(PROBE_POLL);
    };
    let _ = fs::remove_dir_all(&dir);
    prober.join().expect("probe thread");

    let (result, _) = outcome.expect("the probe must return within its bound");
    assert!(
        returned_after < Duration::from_secs(SIGKILL_REAP_TIMEOUT_SECS + PROBE_RETURN_SLACK_SECS),
        "the probe must answer within its reap bound once cancelled: {returned_after:?}"
    );
    let err = result.expect_err("a cancelled and unkillable probe must fail");
    let pid = fixture.pid;
    assert!(
        err.starts_with(&format!(
            "{} --version: cancelled; --version cleanup: probe {pid} still running \
             {SIGKILL_REAP_TIMEOUT_SECS}s after SIGKILL",
            tool.display()
        )),
        "cancellation must lead the combined error: {err}"
    );
    assert!(
        err.contains("group SIGKILL: EPERM"),
        "err must name the refused group signal: {err}"
    );
    assert!(
        err.contains("pid SIGKILL: EPERM"),
        "err must name the refused pid signal: {err}"
    );
    assert!(
        ended,
        "the fixture must exit once the test stops keeping it alive"
    );
    assert!(reaped, "the fixture's pid must be reaped once it exits");
}

#[test]
fn capture_env_kills_the_descendant_a_version_wrapper_leaves_running() {
    // A `--version` wrapper can leave a background process behind, just as
    // a `git` wrapper can
    // (`head_sha_kills_the_descendant_of_a_refused_git_wrapper`). The
    // descendant inherits the probe's stdout pipe, so a probe that killed
    // only the wrapper's pid would report success while the descendant
    // kept that pipe open.
    let dir = temp_dir("capture-env-descendant");
    let pid_file = dir.join("descendant.pid");
    let tool = make_script(
        &dir,
        "fake-version-wrapper.sh",
        &format!(
            "sleep {DESCENDANT_SLEEP_SECS} &\nprintf '%s\\n' \"$!\" > {}\n\
             printf 'faketool version 9.9\\n'\nwait\n",
            shell_word(&pid_file)
        ),
    );

    let (result, _) = retry_busy(|| {
        capture_env(std::slice::from_ref(&tool), &AtomicBool::new(false))
            .map(|env| env.tools[0].1.clone())
    });
    assert_eq!(
        result,
        Ok("faketool version 9.9".to_string()),
        "the version line must survive the descendant cleanup"
    );

    let descendant = read_pid_file(&pid_file).expect("the wrapper must record its descendant pid");
    let deadline = Instant::now() + Duration::from_secs(PROBE_RETURN_SLACK_SECS);
    loop {
        match kill(Pid::from_raw(descendant), None) {
            Err(Errno::ESRCH) => break,
            Ok(()) => {
                if is_zombie(descendant) {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "descendant pid {descendant} outlived the probe"
                );
            }
            Err(e) => panic!("kill({descendant}, None) -> unexpected errno {e}"),
        }
        std::thread::sleep(PROBE_POLL);
    }

    let _ = fs::remove_dir_all(&dir);
}

// `DeliveryReport` and `RunRecord` are imported so this file's
// `use` block surfaces the public types the runner consumes; clippy
// trips if the imports go unused.
#[allow(dead_code)]
fn _shape_references() {
    let _ = SCHEMA_VERSION;
    let _ = Build::Release;
    let _ = Counts {
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
    };
    let _ = GeneratorErrors {
        connect: 0,
        protocol: 0,
        missed_slots: 0,
        stalled: 0,
        lateness_over_range: 0,
    };
    let _ = Syscalls {
        method: SyscallMethod::None,
        count: None,
        per_msg: None,
        raw: None,
        enabled_ns: None,
        disabled_ns: None,
        exit: None,
        diagnostics: None,
    };
    let _ = Timeline {
        origin_sec: 0,
        counts: Vec::new(),
    };
    let _ = Window {
        start_ns: 0,
        end_ns: 0,
    };
    let _ = Env {
        kernel: String::new(),
        cpu_model: String::new(),
        host: String::new(),
        git_sha: String::new(),
        git_dirty: false,
        tools: Vec::new(),
    };
    let _ = CpuUsage {
        clk_tck: 0,
        sampled_ns: 0,
        server_ticks: 0,
        generator_ticks: 0,
    };
    let _ = RunRecord {
        schema: SCHEMA_VERSION,
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
        repetition: 1,
        attempt: 1,
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
        timeline: Timeline {
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
        worker_loads: None,
        started_at_unix: 0,
        harness_errors: Vec::new(),
        valid: false,
        invalid_reasons: Vec::new(),
        cancelled: false,
    };
    let _ = DeliveryReport {
        delivered: 0,
        duplicates: 0,
        foreign: 0,
        latency_over_range: 0,
        clock_anomalies: 0,
        dropped: None,
        timeline: Timeline {
            origin_sec: 0,
            counts: Vec::new(),
        },
        latency: Vec::new(),
        worker_loads: None,
    };
}
