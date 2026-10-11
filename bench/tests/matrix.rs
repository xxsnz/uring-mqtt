//! Integration tests for `bench::matrix` (task 11 of the benchmark-harness
//! feature). The unit tests in this file cover the pure orchestration
//! logic — `servers_for` filtering, `run_configs` retry bookkeeping,
//! `resume_plan` pass/skip decisions, and `check_resume`'s environment
//! comparison. The CLI tests for `bench run` itself live in `cli_run.rs`.

#![allow(clippy::too_many_lines)] // test fixtures are dense on purpose

use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use bench::analysis::matched_rates;
use bench::matrix::{
    check_request, check_resume, read_records, resume_plan, run_configs, servers_for, MatrixOutcome,
    MatrixSpec, ResumeState, RECORDED_QOS, RECORDED_SERVERS, RECORDED_WORKERS,
};
use bench::record::Timeline;
use bench::record::Window;
use bench::run_record::{
    Build, ConfigKey, Counts, CpuSets, CpuUsage, DriverFds, Env, GeneratorErrors, LatencySummary,
    Profile, RunKind, RunRecord, ServerKind, SyscallMethod, Syscalls, SCHEMA_VERSION,
};
use bench::runner::{ProtocolConstants, RunSpec};

const ORIGIN_NS: u64 = 10_000_000_000;
const WINDOW_START_NS: u64 = 12_000_000_000;
const WINDOW_END_NS: u64 = 17_000_000_000;
const WARMUP_SECS: u64 = 2;
const WINDOW_SECS: u64 = 5;
const ONE_SECOND_NS: u64 = 1_000_000_000;

/// A `RunRecord` the matrix can use as a passing repetition. A throughput
/// or connections record is what `reconcile` and `derive` return nothing
/// for: its timeline carries `window_msgs_per_sec` in each window second
/// and nothing in the warmup, which is what `derive` reads the two rates
/// out of and what row 29 compares against `counts.delivered`. The latency
/// variant keeps the stored summary its callers assert on — it has no
/// `offered_rate`, so rows 1, 15, 25 and 27 cannot be satisfied — and the
/// tests that use it read the stored `valid` through `config_passes`.
/// The fields that vary per call (server, qos, attempt, repetition, kind)
/// are passed in.
#[allow(clippy::cast_precision_loss)] // whole msgs/s counts, exact as f64
fn passing_record(
    spec: &RunSpec,
    rep: u32,
    attempt: u32,
    window_msgs_per_sec: u64,
) -> RunRecord {
    // A connections run publishes nothing, so its timeline and its
    // delivery count are both empty; every other kind delivers the same
    // rate in each of the window's seconds.
    let delivered = if spec.kind == RunKind::Connections {
        0
    } else {
        window_msgs_per_sec * WINDOW_SECS
    };
    let counts_vec: Vec<u64> = if spec.kind == RunKind::Connections {
        Vec::new()
    } else {
        let mut bins = vec![0; usize::try_from(WARMUP_SECS).expect("WARMUP_SECS fits")];
        bins.extend(std::iter::repeat_n(
            window_msgs_per_sec,
            usize::try_from(WINDOW_SECS).expect("WINDOW_SECS fits"),
        ));
        bins
    };
    let timeline = Timeline {
        origin_sec: ORIGIN_NS / ONE_SECOND_NS,
        counts: counts_vec,
    };
    let counts = Counts {
        scheduled: None,
        window_scheduled: None,
        offered: delivered,
        acked: if spec.kind == RunKind::Connections {
            None
        } else {
            Some(delivered)
        },
        delivered,
        dropped: Some(0),
        unresolved: 0,
        duplicates: 0,
        foreign: 0,
        latency_over_range: 0,
        clock_anomalies: 0,
    };
    let latency_ns = if spec.kind == RunKind::Latency {
        Some(LatencySummary {
            p50: 40_000,
            p99: 80_000,
            max: 80_000,
            histogram: vec![(40_000, 989), (80_000, 11)],
        })
    } else {
        None
    };
    let lateness = if spec.kind == RunKind::Latency {
        vec![(1_000, 1_000)]
    } else {
        Vec::new()
    };
    let lateness_p99_ns = if spec.kind == RunKind::Latency {
        Some(1_000)
    } else {
        None
    };
    let last_write_start_ns = if spec.kind == RunKind::Latency {
        Some(WINDOW_END_NS - 1)
    } else {
        None
    };
    let throughput_msgs_per_sec = if spec.kind == RunKind::Throughput {
        Some(window_msgs_per_sec as f64)
    } else {
        None
    };
    let last10s_msgs_per_sec = throughput_msgs_per_sec;
    let cycles_in_window = if spec.kind == RunKind::Connections {
        Some(50)
    } else {
        None
    };
    let connections_per_sec = if spec.kind == RunKind::Connections {
        Some(10.0)
    } else {
        None
    };
    RunRecord {
        schema: SCHEMA_VERSION,
        profile: spec.profile,
        server: spec.server,
        workers: spec.workers,
        qos: spec.qos,
        kind: spec.kind,
        rate_fraction_pct: spec.rate_fraction_pct,
        offered_rate: spec.offered_rate,
        repetition: rep,
        attempt,
        connections: 100,
        payload_bytes: 64,
        qos1_window: 32,
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        drain_grace_secs: 2,
        build: Build::Release,
        origin_ns: ORIGIN_NS,
        window: Window {
            start_ns: WINDOW_START_NS,
            end_ns: WINDOW_END_NS,
        },
        timeline,
        cycles_in_window,
        counts,
        throughput_msgs_per_sec,
        last10s_msgs_per_sec,
        latency_ns,
        lateness,
        lateness_p99_ns,
        last_write_start_ns,
        connections_per_sec,
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
        driver: if spec.server == ServerKind::Uring {
            Some(DriverFds {
                io_uring_fds: spec.workers,
                eventpoll_fds: 0,
            })
        } else {
            Some(DriverFds {
                io_uring_fds: 0,
                eventpoll_fds: 1,
            })
        },
        cpu_sets: CpuSets {
            server: "0,2,4,6".to_string(),
            generator: "8-17".to_string(),
        },
        env: Env {
            kernel: "test-kernel".to_string(),
            cpu_model: "test-cpu".to_string(),
            host: "test-host".to_string(),
            git_sha: "abc1234".to_string(),
            git_dirty: false,
            tools: Vec::new(),
        },
        cpu: Some(CpuUsage {
            clk_tck: 100,
            sampled_ns: WINDOW_SECS * ONE_SECOND_NS,
            server_ticks: 22_800,
            generator_ticks: 24_000,
        }),
        started_at_unix: 1_791_273_600,
        harness_errors: Vec::new(),
        valid: true,
        invalid_reasons: Vec::new(),
    }
}

fn throughput_spec() -> RunSpec {
    RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        rate_fraction_pct: None,
        offered_rate: None,
        repetition: 0,
        attempt: 1,
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bench-matrix-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn set_mode(path: &Path, mode: u32) {
    let mut perms = fs::metadata(path).expect("stat").permissions();
    perms.set_mode(mode);
    fs::set_permissions(path, perms).expect("chmod");
}

#[test]
fn servers_for_keeps_mosquitto_only_for_one_worker() {
    let servers = RECORDED_SERVERS.to_vec();
    let one = servers_for(1, &servers);
    assert_eq!(one.len(), 3);
    assert!(one.contains(&ServerKind::Mosquitto));
    let four = servers_for(4, &servers);
    assert!(!four.contains(&ServerKind::Mosquitto));
    assert_eq!(four.len(), 2);
}

#[test]
fn failed_configuration_is_rerun_and_failed_attempts_are_kept() {
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let out_dir = temp_dir("rerun");
    let scratch = temp_dir("rerun-scratch");
    let cancel = AtomicBool::new(false);
    let spec = throughput_spec();
    let mut calls: Vec<(u32, u32)> = Vec::new();
    let outcome = run_configs(&[spec], &constants, &out_dir, &scratch, &cancel, |s| {
        // The record carries the attempt and repetition the seam passed,
        // so the assertions below read the executor's own inputs.
        calls.push((s.attempt, s.repetition));
        // First attempt (2 calls): both invalid. Second attempt (2 calls): both valid.
        let mut rec = passing_record(s, s.repetition, s.attempt, 100);
        if s.attempt == 1 {
            rec.valid = false;
            rec.invalid_reasons.push("test-marker".to_string());
        }
        rec
    })
    .expect("run_configs");

    assert_eq!(outcome.failed, Vec::<ConfigKey>::new());
    assert_eq!(outcome.passed.len(), 1);
    assert_eq!(
        calls,
        vec![(1, 1), (1, 2), (2, 1), (2, 2)],
        "both repetitions of attempt 1, then both of attempt 2"
    );
    // 2 attempt-1 files + 2 attempt-2 files = 4
    let mut by_attempt: HashMap<u32, Vec<PathBuf>> = HashMap::new();
    for entry in fs::read_dir(&out_dir).expect("read out dir").flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "json") {
            let stem = path.file_stem().expect("stem").to_string_lossy().to_string();
            // <server>-w<W>[-q<Q>]-<kind>[-p<pct>]-a<attempt>-r<rep>.json
            let attempt_str = stem
                .rsplit("-a")
                .next()
                .expect("a-")
                .split('-')
                .next()
                .expect("attempt");
            let a: u32 = attempt_str.parse().expect("parse attempt");
            by_attempt.entry(a).or_default().push(path);
        }
    }
    assert_eq!(by_attempt.get(&1).map(Vec::len), Some(2));
    assert_eq!(by_attempt.get(&2).map(Vec::len), Some(2));
    assert_eq!(calls.len(), 4);
}

#[test]
fn each_configuration_advances_from_its_own_attempt() {
    // A resumed matrix can hand one config attempt 2 and another attempt
    // 1. Each must run its own attempt number; a shared counter would put
    // the second config's record under the first config's filename.
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let out_dir = temp_dir("own-attempt");
    let scratch = temp_dir("own-attempt-scratch");
    let cancel = AtomicBool::new(false);
    let first = throughput_spec();
    let mut second = throughput_spec();
    second.server = ServerKind::Tokio;
    second.attempt = 2;
    let mut calls: Vec<(ServerKind, u32)> = Vec::new();
    let outcome = run_configs(
        &[first, second],
        &constants,
        &out_dir,
        &scratch,
        &cancel,
        |s| {
            calls.push((s.server, s.attempt));
            passing_record(s, s.repetition, s.attempt, 100)
        },
    )
    .expect("run_configs");

    assert_eq!(outcome.failed, Vec::<ConfigKey>::new());
    assert_eq!(outcome.passed.len(), 2);
    assert_eq!(calls.len(), 4);
    for (server, attempt) in &calls {
        let want = if *server == ServerKind::Uring { 1 } else { 2 };
        assert_eq!(
            *attempt, want,
            "{server:?} must run its own attempt, got {attempt}"
        );
    }
}

#[test]
fn a_resumed_configuration_stops_at_the_absolute_attempt_cap() {
    // Decision Q9 caps a configuration at three attempts in total. A
    // config resumed at attempt 3 of 3 runs that attempt and then fails;
    // counting three more rounds from the resumed number would give it
    // five.
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let out_dir = temp_dir("attempt-cap");
    let scratch = temp_dir("attempt-cap-scratch");
    let cancel = AtomicBool::new(false);
    let mut spec = throughput_spec();
    spec.attempt = 3;
    let mut attempts: Vec<u32> = Vec::new();
    let outcome = run_configs(&[spec], &constants, &out_dir, &scratch, &cancel, |s| {
        attempts.push(s.attempt);
        let mut rec = passing_record(s, s.repetition, s.attempt, 100);
        rec.valid = false;
        rec.invalid_reasons.push("test-marker".to_string());
        rec
    })
    .expect("run_configs");

    assert_eq!(attempts, vec![3, 3], "attempt 3 only, both repetitions");
    assert_eq!(outcome.passed, Vec::<ConfigKey>::new());
    assert_eq!(outcome.failed.len(), 1);
}

#[test]
fn cancellation_stops_scheduling_and_leaves_the_resume_budget() {
    // SIGTERM during a round must not charge the repetitions and attempts
    // it never started. Running them out would write a cancellation record
    // for attempts 1 to 3, and `resume_plan` would then ask for attempt 4,
    // which the absolute cap refuses — the interruption would cost the
    // configuration its whole budget.
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let out_dir = temp_dir("cancel");
    let scratch = temp_dir("cancel-scratch");
    let cancel = AtomicBool::new(false);
    let spec = throughput_spec();
    let mut attempts: Vec<(u32, u32)> = Vec::new();
    let err = run_configs(&[spec], &constants, &out_dir, &scratch, &cancel, |s| {
        attempts.push((s.attempt, s.repetition));
        // The signal arrives while the first repetition runs, so that run
        // produces an invalid record and nothing after it is dispatched.
        cancel.store(true, Ordering::SeqCst);
        let mut rec = passing_record(s, s.repetition, s.attempt, 100);
        rec.valid = false;
        rec.invalid_reasons.push("test-marker".to_string());
        rec
    })
    .expect_err("run_configs must report the cancellation");

    assert_eq!(err, "cancelled", "the matrix reports cancellation: {err}");
    assert_eq!(
        attempts,
        vec![(1, 1)],
        "only the run in progress is dispatched"
    );
    // The one interrupted repetition is on disk, so `--resume` continues at
    // attempt 2 with two attempts still available.
    let existing = read_records(&out_dir).expect("read_records");
    assert_eq!(existing.len(), 1, "only the interrupted run is persisted");
    let plan = resume_plan(&existing, &constants);
    assert_eq!(
        plan.get(&existing[0].key()),
        Some(&ResumeState::NextAttempt(2)),
        "resume must continue at attempt 2, not past the cap"
    );
}

#[test]
fn a_record_that_cannot_be_persisted_drops_its_scratch_directory() {
    // `run_one` refuses a spec whose scratch directory is already there.
    // When the record write fails, that orphan would make the attempt
    // impossible to repeat, so `run_configs` removes it.
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let work = temp_dir("write-fail");
    let out_dir = work.join("out");
    let scratch = work.join("scratch");
    fs::create_dir_all(&out_dir).expect("create out dir");
    let cancel = AtomicBool::new(false);
    let spec = throughput_spec();
    // The record's own name is the scratch directory's name, so build the
    // record the executor will return first and derive the directory from
    // it. The run's raw evidence stands in for what `run_one` leaves there.
    let mut planned = spec.clone();
    planned.repetition = 1;
    let stem = passing_record(&planned, 1, 1, 100)
        .file_name()
        .replace(".json", "");
    let run_scratch = scratch.join(&stem);
    fs::create_dir_all(&run_scratch).expect("create run scratch");
    fs::write(run_scratch.join("gen.json"), b"{}").expect("write evidence");
    // A read-only output directory makes every record write fail the way a
    // full filesystem would, while `create_dir_all` on it still succeeds.
    set_mode(&out_dir, 0o500);

    let err = run_configs(&[spec], &constants, &out_dir, &scratch, &cancel, |s| {
        passing_record(s, s.repetition, s.attempt, 100)
    })
    .expect_err("run_configs must report the write failure");
    set_mode(&out_dir, 0o700);

    assert!(
        err.contains(&stem),
        "the write failure names the record, got {err}"
    );
    assert!(
        !run_scratch.exists(),
        "the unpersisted run's scratch directory must be gone, {} still there",
        run_scratch.display()
    );
}

#[test]
fn check_request_refuses_a_repeated_matrix_value() {
    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1, 1],
        out_dir: temp_dir("req-dup"),
        resume: false,
    };
    let err = check_request(&spec, SyscallMethod::None, None, None)
        .expect_err("a repeated qos dispatches the same configuration twice");
    assert!(err.contains("--qos"), "{err}");

    let mut dup_workers = spec.clone();
    dup_workers.qos = vec![1];
    dup_workers.workers = vec![1, 1];
    let err = check_request(&dup_workers, SyscallMethod::None, None, None)
        .expect_err("a repeated worker count dispatches the same configuration twice");
    assert!(err.contains("--workers"), "{err}");

    let mut dup_servers = spec.clone();
    dup_servers.qos = vec![1];
    dup_servers.servers = vec![ServerKind::Uring, ServerKind::Uring];
    let err = check_request(&dup_servers, SyscallMethod::None, None, None)
        .expect_err("a repeated server dispatches the same configuration twice");
    assert!(err.contains("--servers"), "{err}");
}

/// A QoS the harness does not publish and a zero worker count are
/// refused before any process is spawned: `bench gen` rejects QoS 2 only
/// once a run reaches it, and `bench server` never bounds its worker
/// count, so both would otherwise cost a server startup and a
/// failed-run record.
#[test]
fn check_request_refuses_a_value_outside_the_measured_domain() {
    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![2],
        out_dir: temp_dir("req-domain"),
        resume: false,
    };
    let err = check_request(&spec, SyscallMethod::None, None, None)
        .expect_err("qos 2 is not a measured QoS");
    assert!(err.contains("--qos"), "{err}");
    assert!(err.contains("0 or 1"), "{err}");

    let mut zero_workers = spec.clone();
    zero_workers.qos = vec![1];
    zero_workers.workers = vec![0];
    let err = check_request(&zero_workers, SyscallMethod::None, None, None)
        .expect_err("a server runs at least one worker");
    assert!(err.contains("--workers"), "{err}");

    let mut valid = spec.clone();
    valid.qos = vec![1, 0];
    valid.workers = vec![1, 4];
    check_request(&valid, SyscallMethod::None, None, None)
        .expect("every measured value passes the domain check");
}

fn write_record(dir: &Path, rec: &RunRecord) {
    let path = dir.join(rec.file_name());
    let file = fs::File::create(&path).expect("create");
    serde_json::to_writer_pretty(&file, rec).expect("write");
}

fn make_record(
    profile: Profile,
    server: ServerKind,
    workers: u32,
    qos: Option<u8>,
    kind: RunKind,
    rate_fraction_pct: Option<u32>,
    attempt: u32,
    rep: u32,
    valid: bool,
    git_sha: &str,
) -> RunRecord {
    let spec = RunSpec {
        profile,
        server,
        workers,
        qos,
        kind,
        rate_fraction_pct,
        offered_rate: None,
        repetition: rep,
        attempt,
    };
    let mut rec = passing_record(&spec, rep, attempt, 100);
    rec.git_sha_inherit(git_sha);
    rec.valid = valid;
    if !valid {
        rec.invalid_reasons.push("test-marker".to_string());
    }
    rec
}

// Trait extension used only by the fixture builders below: stamps the
// same `git_sha` we use in this test's `Env` so the resume-comparison
// tests do not need to rebuild the whole env each time.
trait RecordShaExt {
    fn git_sha_inherit(&mut self, sha: &str);
}
impl RecordShaExt for RunRecord {
    fn git_sha_inherit(&mut self, sha: &str) {
        self.env.git_sha = sha.to_string();
    }
}

#[test]
fn resume_plan_skips_passed_configurations_and_continues_attempt_numbers() {
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let out_dir = temp_dir("resume-plan");
    // Config A: attempt 1, reps 1-2, both valid -> Passed
    let a = make_record(
        Profile::Smoke,
        ServerKind::Uring,
        1,
        Some(1),
        RunKind::Throughput,
        None,
        1,
        1,
        true,
        "abc1234",
    );
    let mut a2 = a.clone();
    a2.repetition = 2;
    write_record(&out_dir, &a);
    write_record(&out_dir, &a2);
    // Config B: attempt 1, rep 1 valid, rep 2 invalid -> NextAttempt(2)
    let b = make_record(
        Profile::Smoke,
        ServerKind::Tokio,
        1,
        Some(1),
        RunKind::Throughput,
        None,
        1,
        1,
        true,
        "abc1234",
    );
    let mut b2 = b.clone();
    b2.repetition = 2;
    b2.valid = false;
    b2.invalid_reasons.push("test-marker".to_string());
    write_record(&out_dir, &b);
    write_record(&out_dir, &b2);
    // Config C: attempt 2, rep 1 only (interrupted) -> NextAttempt(3)
    let c = make_record(
        Profile::Smoke,
        ServerKind::Uring,
        1,
        Some(0),
        RunKind::Throughput,
        None,
        2,
        1,
        true,
        "abc1234",
    );
    write_record(&out_dir, &c);

    let existing = read_records(&out_dir).expect("read_records");
    let plan = resume_plan(&existing, &constants);
    assert_eq!(plan.get(&a.key()), Some(&ResumeState::Passed));
    assert_eq!(plan.get(&b.key()), Some(&ResumeState::NextAttempt(2)));
    assert_eq!(plan.get(&c.key()), Some(&ResumeState::NextAttempt(3)));
}

#[test]
fn resume_refuses_records_from_another_build() {
    let out_dir = temp_dir("resume-env");
    let mut rec = make_record(
        Profile::Smoke,
        ServerKind::Uring,
        1,
        Some(1),
        RunKind::Throughput,
        None,
        1,
        1,
        true,
        "0000000",
    );
    write_record(&out_dir, &rec);
    // New env
    rec.env.git_sha = "abc1234".to_string();
    // check_resume compares to the *new* env
    let existing = read_records(&out_dir).expect("read");
    let err = check_resume(&existing, Profile::Smoke, SyscallMethod::None, &rec.env, &rec.cpu_sets)
        .expect_err("check_resume must refuse a different git_sha");
    assert!(err.contains("git_sha"), "err must mention git_sha: {err}");
    // Identical env -> Ok
    rec.env.git_sha = "0000000".to_string();
    let existing = read_records(&out_dir).expect("read");
    check_resume(&existing, Profile::Smoke, SyscallMethod::None, &rec.env, &rec.cpu_sets)
        .expect("identical env");
}

/// `doc::render` refuses a recorded directory holding more than one
/// `env.cpu_model` or `env.tools` value, so a resume that accepted either
/// change would schedule hours of new attempts whose combined directory
/// the renderer then refuses to publish. Both are refused up front.
#[test]
fn resume_refuses_a_changed_cpu_model_or_tool_version() {
    let out_dir = temp_dir("resume-env-doc-fields");
    let mut rec = make_record(
        Profile::Smoke,
        ServerKind::Uring,
        1,
        Some(1),
        RunKind::Throughput,
        None,
        1,
        1,
        true,
        "abc1234",
    );
    rec.env.tools = vec![("/usr/bin/perf".to_string(), "perf 6.11".to_string())];
    write_record(&out_dir, &rec);
    let existing = read_records(&out_dir).expect("read");

    let mut new_env = rec.env.clone();
    new_env.cpu_model = "another-cpu".to_string();
    let err = check_resume(&existing, Profile::Smoke, SyscallMethod::None, &new_env, &rec.cpu_sets)
        .expect_err("check_resume must refuse a different cpu_model");
    assert!(err.contains("cpu_model"), "err must name cpu_model: {err}");

    let mut new_env = rec.env.clone();
    new_env.tools = vec![("/usr/bin/perf".to_string(), "perf 6.12".to_string())];
    let err = check_resume(&existing, Profile::Smoke, SyscallMethod::None, &new_env, &rec.cpu_sets)
        .expect_err("check_resume must refuse a different tool version");
    assert!(err.contains("tools"), "err must name tools: {err}");
    assert!(err.contains("perf 6.12"), "err must name the version: {err}");

    check_resume(&existing, Profile::Smoke, SyscallMethod::None, &rec.env, &rec.cpu_sets)
        .expect("an unchanged cpu model and tool set resume");
}

/// `Env.tools` lists every counter path the operator supplied, so a
/// `--syscalls perf` session and a `--syscalls strace` resume of it share
/// one environment: without this comparison the resume skips the passed
/// perf configurations, runs the rest under strace, and `doc::render`
/// then refuses the mixed published methods. Records without counter
/// evidence — every kind but the 80 % latency run — do not constrain the
/// selection.
///
/// The comparison reads `syscalls.method` alone, so the fixture carries
/// the counter on a throughput record: this file's latency variant has no
/// `offered_rate` and `recompute` refuses it before any method check can
/// be asserted on the accepting path.
#[test]
fn resume_refuses_a_changed_syscall_counter_method() {
    let out_dir = temp_dir("resume-syscall-method");
    let mut rec = make_record(
        Profile::Smoke,
        ServerKind::Uring,
        1,
        Some(1),
        RunKind::Throughput,
        None,
        1,
        1,
        true,
        "abc1234",
    );
    rec.env.tools = vec![
        ("/usr/bin/perf".to_string(), "perf 6.11".to_string()),
        ("/usr/bin/strace".to_string(), "strace 6.18".to_string()),
    ];
    rec.syscalls = Syscalls {
        method: SyscallMethod::Perf,
        count: Some(1_000),
        per_msg: Some(2.0),
        raw: Some("1000,,raw_syscalls:sys_enter,,,,".to_string()),
        enabled_ns: Some(WINDOW_START_NS),
        disabled_ns: Some(WINDOW_END_NS),
        exit: None,
        diagnostics: None,
    };
    write_record(&out_dir, &rec);
    let existing = read_records(&out_dir).expect("read");

    let err = check_resume(
        &existing,
        Profile::Smoke,
        SyscallMethod::Strace,
        &rec.env,
        &rec.cpu_sets,
    )
    .expect_err("check_resume must refuse a counted record from another method");
    assert!(err.contains("perf"), "err must name the record's method: {err}");
    assert!(err.contains("strace"), "err must name the selection: {err}");
    assert!(
        err.contains(&rec.file_name()),
        "err must name the record file: {err}"
    );

    let err = check_resume(
        &existing,
        Profile::Smoke,
        SyscallMethod::None,
        &rec.env,
        &rec.cpu_sets,
    )
    .expect_err("check_resume must refuse dropping the counter of a counted record");
    assert!(err.contains("none"), "err must name the selection: {err}");

    check_resume(
        &existing,
        Profile::Smoke,
        SyscallMethod::Perf,
        &rec.env,
        &rec.cpu_sets,
    )
    .expect("the same method resumes");

    // An uncounted record leaves the selection free: a session interrupted
    // before its first 80 % latency run can resume under any method.
    let uncounted = temp_dir("resume-syscall-method-uncounted");
    let plain = make_record(
        Profile::Smoke,
        ServerKind::Uring,
        1,
        Some(1),
        RunKind::Throughput,
        None,
        1,
        1,
        true,
        "abc1234",
    );
    write_record(&uncounted, &plain);
    let existing = read_records(&uncounted).expect("read");
    check_resume(
        &existing,
        Profile::Smoke,
        SyscallMethod::Perf,
        &plain.env,
        &plain.cpu_sets,
    )
    .expect("an uncounted record resumes under perf");
}

#[test]
fn resume_refuses_a_record_whose_summary_left_its_raw_observations() {
    // Every env field can still match while the stored summary no longer
    // follows from the raw timeline. `resume_plan` would read that record's
    // `valid` as evidence the configuration passed, and
    // `passing_records_for_key` would pace the new latency runs at the
    // changed throughput. `finalize_record` derives both fields from the
    // raw data, so a mismatch is an edit made outside the harness.
    let out_dir = temp_dir("resume-recompute");
    let mut rec = make_record(
        Profile::Smoke,
        ServerKind::Uring,
        1,
        Some(1),
        RunKind::Throughput,
        None,
        1,
        1,
        true,
        "abc1234",
    );
    write_record(&out_dir, &rec);
    let existing = read_records(&out_dir).expect("read");
    check_resume(&existing, Profile::Smoke, SyscallMethod::None, &rec.env, &rec.cpu_sets)
        .expect("an untouched record resumes");

    rec.throughput_msgs_per_sec = rec.throughput_msgs_per_sec.map(|v| v * 2.0);
    write_record(&out_dir, &rec);
    let existing = read_records(&out_dir).expect("read");
    let err = check_resume(&existing, Profile::Smoke, SyscallMethod::None, &rec.env, &rec.cpu_sets)
        .expect_err("check_resume must refuse a changed metric");
    assert!(
        err.contains("throughput_msgs_per_sec"),
        "err must name the field: {err}"
    );
    assert!(
        err.contains(&rec.file_name()),
        "err must name the record file: {err}"
    );
}

#[test]
fn matched_rates_smoke_test() {
    // matched_rates is defined in analysis.rs but the matrix calls it.
    // A quick sanity check: from [200.0] -> (100, 160).
    let rates = matched_rates(&[200.0]).expect("non-empty");
    assert_eq!(rates, (100, 160));
}

#[test]
fn check_request_smoke_does_not_require_tool_paths() {
    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir: temp_dir("req-smoke"),
        resume: false,
    };
    check_request(&spec, SyscallMethod::None, None, None).expect("smoke check_request");
}

#[test]
fn check_request_recorded_requires_tools() {
    let spec = MatrixSpec {
        profile: Profile::Recorded,
        servers: RECORDED_SERVERS.to_vec(),
        workers: RECORDED_WORKERS.to_vec(),
        qos: RECORDED_QOS.to_vec(),
        out_dir: temp_dir("req-rec"),
        resume: false,
    };
    let tool = Some(Path::new("/bin/true"));
    // No syscalls -> err mentions --syscalls
    let err = check_request(&spec, SyscallMethod::None, None, tool)
        .expect_err("recorded refuses --syscalls none");
    assert!(err.contains("--syscalls"), "{err}");
    // No mosquitto -> err mentions --mosquitto-bin
    let err = check_request(&spec, SyscallMethod::Perf, tool, None)
        .expect_err("recorded refuses without --mosquitto-bin");
    assert!(err.contains("--mosquitto-bin"), "{err}");
    // Partial matrix -> err mentions full matrix
    let mut partial = spec.clone();
    partial.workers = vec![1];
    let err = check_request(&partial, SyscallMethod::Perf, tool, tool)
        .expect_err("recorded refuses a partial matrix");
    assert!(err.contains("full matrix"), "{err}");
}

#[test]
fn check_request_requires_the_selected_counter_executable() {
    // `run_one` reports a missing counter path only once a counted run
    // reaches the counter, so an accepted request would run every earlier
    // configuration and then invalidate every counted one.
    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir: temp_dir("req-counter"),
        resume: false,
    };
    let err = check_request(&spec, SyscallMethod::Perf, None, None)
        .expect_err("--syscalls perf needs --perf-bin");
    assert!(err.contains("--perf-bin"), "{err}");
    let err = check_request(&spec, SyscallMethod::Strace, None, None)
        .expect_err("--syscalls strace needs --strace-bin");
    assert!(err.contains("--strace-bin"), "{err}");
    check_request(&spec, SyscallMethod::Perf, Some(Path::new("/bin/true")), None)
        .expect("a counter path satisfies the check");
}

#[test]
fn check_request_refuses_a_matrix_no_server_can_run() {
    // `servers_for` filters mosquitto out of every multi-worker row, so
    // this request leaves every stage without a configuration: without the
    // refusal `run_matrix` writes no record and the command exits 0.
    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Mosquitto],
        workers: vec![4],
        qos: vec![1],
        out_dir: temp_dir("req-norun"),
        resume: false,
    };
    assert!(servers_for(4, &spec.servers).is_empty());
    let err = check_request(&spec, SyscallMethod::None, None, None)
        .expect_err("a matrix with no runnable server measures nothing");
    assert!(err.contains("no server runs"), "{err}");

    // The same server is runnable on the single-worker row, so a request
    // that includes it is accepted.
    let mut with_one = spec.clone();
    with_one.workers = vec![1, 4];
    check_request(&with_one, SyscallMethod::None, None, None)
        .expect("the 1-worker row runs mosquitto");
}

#[test]
fn resume_plan_saturates_an_attempt_number_it_cannot_advance() {
    // `attempt` deserializes any `u32`. A record carrying one this runner
    // never produces must not overflow the resume decision; the saturated
    // successor is past `max_config_attempts`, so `run_configs` leaves the
    // configuration unrun and failed.
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let rec = make_record(
        Profile::Smoke,
        ServerKind::Uring,
        1,
        Some(1),
        RunKind::Throughput,
        None,
        u32::MAX,
        1,
        false,
        "abc1234",
    );
    let plan = resume_plan(std::slice::from_ref(&rec), &constants);
    assert_eq!(
        plan.get(&rec.key()),
        Some(&ResumeState::NextAttempt(u32::MAX))
    );
}

#[test]
fn check_request_refuses_non_empty_out_dir_without_resume() {
    let out_dir = temp_dir("req-nempty");
    fs::write(out_dir.join("x.json"), b"{}").expect("write");
    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir,
        resume: false,
    };
    let err =
        check_request(&spec, SyscallMethod::None, None, None).expect_err("refuses non-empty");
    assert!(err.contains("not empty"), "{err}");
}

#[test]
fn outcome_default_has_empty_passed_and_failed() {
    let outcome = MatrixOutcome::default();
    assert!(outcome.passed.is_empty());
    assert!(outcome.failed.is_empty());
}
