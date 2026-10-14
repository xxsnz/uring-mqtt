//! Integration tests for `bench::runner` (task 9 of the benchmark-harness
//! feature). The smoke harness exercises `run_one` end to end against the
//! real `bench` binary; the script-based tests use a `tempfile`-style
//! shell script as `bench_exe` to test the no-reports and slow-startup
//! paths; the unit tests cover `check_provenance` and `mosquitto_conf`.

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use nix::sys::signal::kill;
use nix::unistd::{getpgid, getpgrp, getpid, getppid, setpgid, Pid};

use bench::record::DeliveryReport;
use bench::run_record::{
    Build, Counts, CpuSets, CpuUsage, DriverFds, Env, GeneratorErrors, Profile, RunKind, RunRecord,
    ServerKind, SyscallMethod, Syscalls, SCHEMA_VERSION,
};
use bench::record::Timeline;
use bench::record::Window;

use bench::runner::{
    capture_env, check_provenance, head_sha, mosquitto_conf, run_one, HostSetup, RunSpec,
    GIT_HEAD_TIMEOUT_SECS,
};

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

/// The stdout read cap of `head_sha`, mirrored here because the runner
/// keeps it private. `head_sha_stops_reading_at_its_stdout_cap` hides a
/// SHA behind more bytes than this.
const GIT_HEAD_STDOUT_CAP_BYTES: usize = 4096;

/// Whitespace-only padding that fixture prints ahead of its SHA: one byte
/// past the cap already puts the SHA out of the capped reader's reach, and
/// the line's own newline makes the margin two.
const CAP_PADDING_BYTES: usize = GIT_HEAD_STDOUT_CAP_BYTES + 1;

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
        env: capture_env(&[]).expect("capture_env"),
        scratch_dir,
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

/// The pid a fake `git` wrapper wrote to `path`, or `None` while the file
/// is missing or holds a partially written line. The terminating newline
/// is written last, so a line without it is not complete yet.
fn read_pid_file(path: &std::path::Path) -> Option<i32> {
    let body = fs::read_to_string(path).ok()?;
    body.strip_suffix('\n')?.trim().parse().ok()
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

/// `head_sha` on a fixture script, with the time its last attempt took.
///
/// libtest runs these tests in parallel, and a child one test forks
/// inherits every descriptor another test's [`make_script`] has open on
/// its script until that child execs. An `exec` of the script inside that
/// window fails with `ETXTBSY` — a statement about the fixture's start,
/// not about the probe under test — so it is retried instead of asserted
/// on. `head_sha` itself never opens the script for writing, so a busy
/// script is always this race. The duration is of the last attempt alone,
/// so a retry cannot push a probe past the bound it is asserted to keep.
fn probe_script(
    script: &std::path::Path,
    cancel: &AtomicBool,
) -> (Result<String, String>, Duration) {
    let deadline = Instant::now() + Duration::from_secs(SCRIPT_BUSY_RETRY_SECS);
    loop {
        let started = Instant::now();
        let result = head_sha(script, cancel);
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
        rate_fraction_pct: None,
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
        rate_fraction_pct: None,
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
        rate_fraction_pct: None,
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
        env: capture_env(&[]).expect("capture_env"),
        scratch_dir: dir.clone(),
        cancel,
    };
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
        env: capture_env(&[]).expect("capture_env"),
        scratch_dir: dir.clone(),
        cancel: Arc::new(AtomicBool::new(false)),
    };
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
            pid_file.display()
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
            "{GROUP_ESCAPE_FILE_ENV}='{}'\nexport {GROUP_ESCAPE_FILE_ENV}\n\
             exec '{}' {GROUP_ESCAPE_FIXTURE} --exact --ignored\n",
            move_file.display(),
            fixture_exe.display()
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
fn capture_env_refuses_a_tool_that_prints_no_version() {
    let dir = temp_dir("blank-version");
    let tool = make_script(&dir, "blank-version.sh", "printf '\\n   \\n'\n");
    let err = capture_env(std::slice::from_ref(&tool))
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
        capture_env(std::slice::from_ref(&tool)).expect("a probe that prints a version must pass");
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
    // Blank lines with no pause for ~10 s, well past the 5 s probe
    // deadline, then EOF. The reader drops every one of them, so the
    // probe must stop at its deadline: a probe that waited for EOF
    // instead would report the wrong cause, and one whose reader
    // queued the lines would grow that queue for 10 s.
    let dir = temp_dir("blank-flood-version");
    let tool = make_script(
        &dir,
        "blank-flood-version.sh",
        "end=$(($(date +%s) + 10))\nwhile [ \"$(date +%s)\" -lt \"$end\" ]; do echo; done\n",
    );
    let err = capture_env(std::slice::from_ref(&tool))
        .expect_err("a probe that never prints a version must fail the capture");
    assert!(
        err.contains("--version did not answer within"),
        "err must name the deadline: {err}"
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
    let err = capture_env(std::slice::from_ref(&tool))
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
        capture_env(std::slice::from_ref(&tool)).expect("a probe that prints a version must pass");
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
    let env = capture_env(&[first.clone(), second.clone()]).expect("both probes must pass");
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
        rate_fraction_pct: None,
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
    };
}
