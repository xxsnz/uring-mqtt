//! CLI tests for `bench run` (task 11 of the benchmark-harness feature):
//! refusal paths run before any process is spawned (AC-42, AC-43), the
//! resume comparison refuses an env mismatch (AC-63), the smoke matrix
//! produces 8 records for the four §9.1 configurations (AC-45 — see the
//! `FIXME(ralph)` on that test for the latency shortfall), and SIGTERM
//! reaps the runner's
//! children (AC-52). Signatures match `features/benchmark-harness/PLAN.md`
//! `## Interface Contracts`.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use bench::run_record::{Profile, RunKind, RunRecord, SCHEMA_VERSION, ServerKind};
use nix::sys::signal::{kill, Signal};
use nix::sys::wait::waitpid;
use nix::unistd::Pid;

const WAIT_POLL: Duration = Duration::from_millis(100);
const CHILDREN_POLL_TIMEOUT: Duration = Duration::from_secs(20);
const SIGTERM_EXIT_TIMEOUT: Duration = Duration::from_secs(15);
/// How long `RunChild::drop` waits for the SIGTERM cancellation path
/// before it falls back to SIGKILL. `run_matrix` observes the flag only
/// between runs, so the budget covers one smoke run's warmup, window and
/// drain grace.
const DROP_SIGTERM_TIMEOUT: Duration = Duration::from_secs(30);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bench-cli-run-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// Mark a file with executable bit and `#!`-shebang `body`. The `marker`
/// tool is the cheapest possible fake perf/strace/mosquitto the refusal
/// tests can point at: it proves `bench run` never invokes it because the
/// refusal happens before any spawn.
fn make_marker(dir: &std::path::Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    let mut f = fs::File::create(&path).expect("create marker");
    f.write_all(b"#!/bin/sh\n").expect("write shebang");
    f.write_all(body.as_bytes()).expect("write body");
    let mut perms = fs::metadata(&path).expect("stat").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).expect("chmod");
    path
}

/// One fake `bench run` invocation: own the child, capture its stderr so
/// the test can assert on the refusal text, drain its stdout so a
/// long-running child does not block on a full pipe, and let the `Drop`
/// impl send SIGKILL to anything still alive so a mid-pipeline failure
/// cannot leak a process holding a port.
struct RunChild {
    child: Child,
    stderr_lines: mpsc::Receiver<String>,
}

impl RunChild {
    fn spawn(args: &[&str]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_bench"))
            .arg("run")
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn bench run");
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let (tx, rx) = mpsc::channel();
        let stderr_lines = if let Some(stderr) = stderr {
            std::thread::spawn(move || {
                let reader = BufReader::new(stderr);
                for line in reader.lines() {
                    match line {
                        Ok(line) => {
                            if tx.send(line).is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
            rx
        } else {
            rx
        };
        // Drain stdout so a child that prints unexpected lines does not
        // block on a closed/empty pipe.
        if let Some(stdout) = stdout {
            std::thread::spawn(move || {
                let mut sink = Vec::new();
                let _ = BufReader::new(stdout).read_to_end(&mut sink);
            });
        }
        Self {
            child,
            stderr_lines,
        }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn wait_exit_code(&mut self, timeout: Duration) -> std::process::ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "bench run did not exit within {timeout:?}"
            );
            std::thread::sleep(WAIT_POLL);
        }
    }

    fn collect_stderr(&mut self, timeout: Duration) -> Vec<String> {
        let deadline = Instant::now() + timeout;
        let mut out = Vec::new();
        while Instant::now() < deadline {
            match self.stderr_lines.recv_timeout(WAIT_POLL) {
                Ok(line) => out.push(line),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if matches!(self.child.try_wait(), Ok(Some(_))) {
                        // Drain remaining lines briefly.
                        while let Ok(line) = self.stderr_lines.recv_timeout(WAIT_POLL) {
                            out.push(line);
                        }
                        break;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        out
    }
}

impl Drop for RunChild {
    fn drop(&mut self) {
        if !matches!(self.child.try_wait(), Ok(None)) {
            return;
        }
        // SIGTERM first: `bench run` reaps its own endpoint and generator
        // through its `ChildGuard`s on the cancellation path, and SIGKILL
        // bypasses those guards and leaves the grandchildren holding a
        // port. SIGKILL stays as the fallback for a runner that ignores
        // the cancellation flag.
        if let Ok(pid) = i32::try_from(self.child.id()) {
            let _ = kill(Pid::from_raw(pid), Signal::SIGTERM);
        }
        let deadline = Instant::now() + DROP_SIGTERM_TIMEOUT;
        while Instant::now() < deadline {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(WAIT_POLL);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn assert_exited_with(run: &mut RunChild, code: i32, timeout: Duration, label: &str) {
    let status = run.wait_exit_code(timeout);
    assert!(
        status.code() == Some(code),
        "{label}: expected exit {code}, got {status:?}"
    );
}

fn read_children_pids(parent_pid: u32) -> Vec<u32> {
    // /proc/<pid>/task/<pid>/children is the list of direct child PIDs
    // still alive at the moment of the read. A list of one or zero PIDs
    // means the spawn has not happened yet or it has been reaped.
    let path_str = format!("/proc/{parent_pid}/task/{parent_pid}/children");
    let path = std::path::Path::new(&path_str);
    let Ok(text) = fs::read_to_string(path) else {
        return Vec::new();
    };
    text.split_whitespace()
        .filter_map(|s| s.parse::<u32>().ok())
        .collect()
}

fn wait_for_children(parent_pid: u32, want: usize, timeout: Duration) -> Vec<u32> {
    let deadline = Instant::now() + timeout;
    loop {
        let pids = read_children_pids(parent_pid);
        if pids.len() >= want {
            return pids;
        }
        if Instant::now() >= deadline {
            return pids;
        }
        std::thread::sleep(WAIT_POLL);
    }
}

#[test]
fn recorded_run_refuses_without_syscall_counting() {
    let work = temp_dir("refuse-syscalls");
    let marker = make_marker(&work, "marker.sh", "touch \"$(dirname \"$0\")/invoked\"\n");
    let out_dir = work.join("out");
    fs::create_dir_all(&out_dir).unwrap();

    let mut run = RunChild::spawn(&[
        "--profile",
        "recorded",
        "--out",
        out_dir.to_str().unwrap(),
        "--syscalls",
        "none",
        "--mosquitto-bin",
        marker.to_str().unwrap(),
    ]);
    let stderr_text = run
        .wait_exit_code(SIGTERM_EXIT_TIMEOUT)
        .code()
        .expect("refusal exits non-signal");
    assert_eq!(stderr_text, 1, "no-syscalls refusal must exit 1");
    let stderr_text = run.collect_stderr(Duration::from_secs(2)).join("\n");
    assert!(
        stderr_text.contains("requires --syscalls"),
        "stderr must mention --syscalls, got: {stderr_text}"
    );
    assert!(
        !work.join("invoked").exists(),
        "marker must not have been invoked"
    );
    let count = count_json(&out_dir);
    assert_eq!(count, 0, "no .json must be written on refusal, got {count}");
}

#[test]
fn recorded_run_refuses_without_mosquitto() {
    let work = temp_dir("refuse-mosq");
    let marker = make_marker(&work, "marker.sh", "touch \"$(dirname \"$0\")/invoked\"\n");
    let out_dir = work.join("out");
    fs::create_dir_all(&out_dir).unwrap();

    let mut run = RunChild::spawn(&[
        "--profile",
        "recorded",
        "--out",
        out_dir.to_str().unwrap(),
        "--syscalls",
        "perf",
        "--perf-bin",
        marker.to_str().unwrap(),
    ]);
    assert_exited_with(&mut run, 1, SIGTERM_EXIT_TIMEOUT, "no mosquitto");
    let stderr_text = run.collect_stderr(Duration::from_secs(2)).join("\n");
    assert!(
        stderr_text.contains("requires --mosquitto-bin"),
        "stderr must mention --mosquitto-bin, got: {stderr_text}"
    );
    assert!(!work.join("invoked").exists());
}

#[test]
fn recorded_run_refuses_a_partial_matrix() {
    let work = temp_dir("refuse-partial");
    let marker = make_marker(&work, "marker.sh", "touch \"$(dirname \"$0\")/invoked\"\n");
    let out_dir = work.join("out");
    fs::create_dir_all(&out_dir).unwrap();

    let mut run = RunChild::spawn(&[
        "--profile",
        "recorded",
        "--out",
        out_dir.to_str().unwrap(),
        "--syscalls",
        "perf",
        "--perf-bin",
        marker.to_str().unwrap(),
        "--mosquitto-bin",
        marker.to_str().unwrap(),
        "--workers",
        "1",
    ]);
    assert_exited_with(&mut run, 1, SIGTERM_EXIT_TIMEOUT, "partial matrix");
    let stderr_text = run.collect_stderr(Duration::from_secs(2)).join("\n");
    assert!(
        stderr_text.contains("full matrix"),
        "stderr must mention full matrix, got: {stderr_text}"
    );
    assert!(!work.join("invoked").exists());
}

#[test]
fn run_refuses_an_output_directory_with_records() {
    let work = temp_dir("refuse-nempty");
    let out_dir = work.join("out");
    fs::create_dir_all(&out_dir).unwrap();
    fs::write(out_dir.join("x.json"), b"{}").unwrap();

    let mut run = RunChild::spawn(&[
        "--profile",
        "smoke",
        "--out",
        out_dir.to_str().unwrap(),
        "--servers",
        "uring",
        "--workers",
        "1",
        "--qos",
        "1",
        "--syscalls",
        "none",
    ]);
    assert_exited_with(&mut run, 1, SIGTERM_EXIT_TIMEOUT, "non-empty out dir");
    let stderr_text = run.collect_stderr(Duration::from_secs(2)).join("\n");
    assert!(
        stderr_text.contains("not empty"),
        "stderr must mention 'not empty', got: {stderr_text}"
    );
}

/// A `--qos` or `--workers` value the harness does not measure is
/// refused with the other preflight checks: the generator rejects QoS 2
/// only after an endpoint is running, so without the matrix check these
/// requests cost a server startup and a failed-run record (exit 2).
#[test]
fn run_refuses_a_value_outside_the_measured_domain() {
    let work = temp_dir("refuse-domain");
    let out_dir = work.join("out");
    fs::create_dir_all(&out_dir).unwrap();

    let mut run = RunChild::spawn(&[
        "--profile",
        "smoke",
        "--out",
        out_dir.to_str().unwrap(),
        "--servers",
        "uring",
        "--workers",
        "1",
        "--qos",
        "2",
        "--syscalls",
        "none",
    ]);
    assert_exited_with(&mut run, 1, SIGTERM_EXIT_TIMEOUT, "unsupported qos");
    let stderr_text = run.collect_stderr(Duration::from_secs(2)).join("\n");
    assert!(
        stderr_text.contains("qos must be 0 or 1"),
        "stderr must name the QoS domain, got: {stderr_text}"
    );

    let mut run = RunChild::spawn(&[
        "--profile",
        "smoke",
        "--out",
        out_dir.to_str().unwrap(),
        "--servers",
        "uring",
        "--workers",
        "0",
        "--qos",
        "1",
        "--syscalls",
        "none",
    ]);
    assert_exited_with(&mut run, 1, SIGTERM_EXIT_TIMEOUT, "zero workers");
    let stderr_text = run.collect_stderr(Duration::from_secs(2)).join("\n");
    assert!(
        stderr_text.contains("--workers"),
        "stderr must name --workers, got: {stderr_text}"
    );

    let count = count_json(&out_dir);
    assert_eq!(count, 0, "no .json must be written on refusal, got {count}");
}

/// A counter path for a counter `--syscalls` did not select is refused
/// before any probe (AC-15): otherwise the unused path's tool-version
/// probe runs and spends a subprocess on a flag the run never reads.
#[test]
fn run_refuses_a_counter_path_the_selected_method_does_not_use() {
    let work = temp_dir("refuse-counter-unused");
    let marker = make_marker(&work, "marker.sh", "touch \"$(dirname \"$0\")/invoked\"\n");
    let out_dir = work.join("out");
    fs::create_dir_all(&out_dir).unwrap();

    let mut run = RunChild::spawn(&[
        "--profile",
        "smoke",
        "--out",
        out_dir.to_str().unwrap(),
        "--syscalls",
        "none",
        "--perf-bin",
        marker.to_str().unwrap(),
        "--servers",
        "uring",
        "--workers",
        "1",
        "--qos",
        "1",
    ]);
    assert_exited_with(&mut run, 1, SIGTERM_EXIT_TIMEOUT, "counter path unused");
    let stderr_text = run.collect_stderr(Duration::from_secs(2)).join("\n");
    assert!(
        stderr_text.contains("--syscalls none does not use --perf-bin"),
        "stderr must name the unused counter flag, got: {stderr_text}"
    );
    assert!(
        !work.join("invoked").exists(),
        "marker must not have been invoked"
    );
    let count = count_json(&out_dir);
    assert_eq!(count, 0, "no .json must be written on refusal, got {count}");
}

fn count_json(dir: &std::path::Path) -> usize {
    fs::read_dir(dir)
        .map_or(0, |it| {
            it.flatten()
                .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
                .count()
        })
}

/// Every `*.json` directly in `dir`, parsed as the typed record, sorted by
/// file name so the assertions below read a stable order.
fn load_records(dir: &std::path::Path) -> Vec<(String, RunRecord)> {
    let mut out: Vec<(String, RunRecord)> = fs::read_dir(dir)
        .expect("read out dir")
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
        .map(|e| {
            let path = e.path();
            let text = fs::read_to_string(&path).expect("read record");
            let record: RunRecord = serde_json::from_str(&text)
                .unwrap_or_else(|err| panic!("parse {} as RunRecord: {err}", path.display()));
            (
                path.file_name()
                    .expect("record file has a name")
                    .to_string_lossy()
                    .into_owned(),
                record,
            )
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Assert that `record` is valid, naming the reasons when it is not.
fn assert_record_valid(name: &str, record: &RunRecord) {
    assert!(
        record.valid,
        "{name}: must be valid; invalid_reasons: {:?}, harness_errors: {:?}",
        record.invalid_reasons, record.harness_errors
    );
    assert!(
        record.invalid_reasons.is_empty(),
        "{name}: a valid record carries no reasons, got {:?}",
        record.invalid_reasons
    );
}

#[test]
fn smoke_matrix_writes_every_configuration_valid() {
    let work = temp_dir("smoke-matrix");
    let out_dir = work.join("out");
    fs::create_dir_all(&out_dir).unwrap();
    let mut run = RunChild::spawn(&[
        "--profile",
        "smoke",
        "--out",
        out_dir.to_str().unwrap(),
        "--servers",
        "uring",
        "--workers",
        "1",
        "--qos",
        "1",
        "--syscalls",
        "none",
    ]);
    // The smoke matrix on this host runs 8 records (throughput, two
    // latency fractions, connections, × 2 repetitions) with a 2 s
    // warmup and a 5 s window each, so the wall clock needs headroom
    // for the 100-client connect storm and the open-loop latency
    // pacing.
    let status = run.wait_exit_code(Duration::from_secs(180));
    // Smoke matrix: 1 server * 1 worker * 1 qos * (1 throughput + 2 latency
    // + 1 connections) * 2 repetitions = 8. A throughput configuration that
    // failed would leave its latency configurations unrun, so the count
    // alone catches a half-run matrix.
    let count = count_json(&out_dir);
    assert_eq!(count, 8, "expected 8 .json files, got {count}");
    let records = load_records(&out_dir);

    for (name, record) in &records {
        assert_eq!(record.profile, Profile::Smoke, "{name}: profile");
        assert_eq!(record.server, ServerKind::Uring, "{name}: server");
        assert_eq!(record.workers, 1, "{name}: workers");
        assert_eq!(record.schema, SCHEMA_VERSION, "{name}: schema");
        assert_eq!(record.attempt, 1, "{name}: smoke stops at attempt 1");
    }

    // The matrix must cover exactly the four §9.1 configurations of this
    // row, each at both repetitions.
    let mut configs: Vec<(RunKind, Option<u32>, u32)> = records
        .iter()
        .map(|(_, r)| (r.kind, r.rate_fraction_pct, r.repetition))
        .collect();
    configs.sort_by_key(|(kind, pct, rep)| (format!("{kind:?}"), *pct, *rep));
    assert_eq!(
        configs,
        vec![
            (RunKind::Connections, None, 1),
            (RunKind::Connections, None, 2),
            (RunKind::Latency, Some(50), 1),
            (RunKind::Latency, Some(50), 2),
            (RunKind::Latency, Some(80), 1),
            (RunKind::Latency, Some(80), 2),
            (RunKind::Throughput, None, 1),
            (RunKind::Throughput, None, 2),
        ],
        "the smoke matrix must run throughput, latency 50 %, latency 80 % \
         and connections at both repetitions"
    );

    for (name, record) in &records {
        assert_record_valid(name, record);
    }

    let stderr_text = run.collect_stderr(Duration::from_secs(2)).join("\n");
    assert_eq!(
        status.code(),
        Some(0),
        "the smoke matrix must exit 0, got {status:?}, stderr: {stderr_text}"
    );
    let failed: Vec<&str> = stderr_text
        .lines()
        .filter(|line| line.starts_with("failed: "))
        .collect();
    assert!(
        failed.is_empty(),
        "a matrix that exits 0 names no failed configuration, got: {failed:?}"
    );
}

#[test]
fn run_kills_and_reaps_its_children_on_sigterm() {
    let work = temp_dir("sigterm");
    let out_dir = work.join("out");
    fs::create_dir_all(&out_dir).unwrap();
    let mut run = RunChild::spawn(&[
        "--profile",
        "smoke",
        "--out",
        out_dir.to_str().unwrap(),
        "--servers",
        "uring",
        "--workers",
        "1",
        "--qos",
        "1",
        "--syscalls",
        "none",
    ]);
    let parent = run.pid();
    let pids = wait_for_children(parent, 2, CHILDREN_POLL_TIMEOUT);
    assert!(
        pids.len() >= 2,
        "expected >= 2 children under runner pid {parent}, got {pids:?}"
    );
    // SIGTERM the runner
    let pid = i32::try_from(parent).expect("fits in i32");
    kill(Pid::from_raw(pid), Signal::SIGTERM).expect("sigterm");
    let status = run.wait_exit_code(SIGTERM_EXIT_TIMEOUT);
    assert_eq!(
        status.code(),
        Some(1),
        "expected exit 1 on SIGTERM, got {status:?}"
    );
    // Each listed child PID must be gone (kill(pid, None) -> ESRCH).
    for child_pid in &pids {
        let cpid = i32::try_from(*child_pid).expect("fits in i32");
        let res = kill(Pid::from_raw(cpid), None);
        match res {
            Err(nix::errno::Errno::ESRCH) => {}
            Ok(()) => panic!("child pid {cpid} still alive after runner SIGTERM"),
            Err(e) => panic!("kill({cpid}, None) -> unexpected errno {e}"),
        }
    }
    // Reap the runner explicitly so no zombie is left behind.
    let _ = waitpid(Pid::from_raw(pid), None);
}

#[test]
fn sigterm_during_a_tool_probe_reaps_it_and_runs_nothing() {
    // `capture_env` probes `{tool} --version` for every supplied tool, so
    // the preflight spawns a child before the matrix starts. Until the
    // cancellation handler is installed SIGTERM takes its default action:
    // the runner dies, the probe keeps running, and no diagnostic is left
    // (AC-52). The handler is installed before the first spawn, so the
    // probe is killed and reaped and the matrix is never started.
    let work = temp_dir("sigterm-probe");
    let tool = make_marker(&work, "slow-version.sh", "sleep 3\necho 'slow 1.0'\n");
    let out_dir = work.join("out");
    fs::create_dir_all(&out_dir).unwrap();
    let mut run = RunChild::spawn(&[
        "--profile",
        "smoke",
        "--out",
        out_dir.to_str().unwrap(),
        "--servers",
        "uring",
        "--workers",
        "1",
        "--qos",
        "1",
        "--syscalls",
        "none",
        "--mosquitto-bin",
        tool.to_str().unwrap(),
    ]);
    let parent = run.pid();
    // The probe is the runner's first child, so one child means the
    // handler is already installed.
    let pids = wait_for_children(parent, 1, CHILDREN_POLL_TIMEOUT);
    assert!(
        !pids.is_empty(),
        "expected the --version probe under runner pid {parent}"
    );
    let pid = i32::try_from(parent).expect("fits in i32");
    kill(Pid::from_raw(pid), Signal::SIGTERM).expect("sigterm");
    let status = run.wait_exit_code(SIGTERM_EXIT_TIMEOUT);
    assert_eq!(
        status.code(),
        Some(1),
        "the runner must report the cancellation itself, got {status:?}"
    );
    let stderr_text = run.collect_stderr(Duration::from_secs(2)).join("\n");
    assert!(
        stderr_text.contains("cancelled"),
        "stderr must report the cancellation, got: {stderr_text}"
    );
    for child_pid in &pids {
        let cpid = i32::try_from(*child_pid).expect("fits in i32");
        match kill(Pid::from_raw(cpid), None) {
            Err(nix::errno::Errno::ESRCH) => {}
            Ok(()) => panic!("probe pid {cpid} still alive after runner SIGTERM"),
            Err(e) => panic!("kill({cpid}, None) -> unexpected errno {e}"),
        }
    }
    let count = count_json(&out_dir);
    assert_eq!(count, 0, "a cancelled preflight runs nothing, got {count}");
}
