//! Integration tests for `bench::matrix` (task 11 of the benchmark-harness
//! feature). The unit tests in this file cover the pure orchestration
//! logic — `servers_for` filtering, `run_configs` retry bookkeeping,
//! `resume_plan` pass/skip decisions, and `check_resume`'s environment
//! comparison. The CLI tests for `bench run` itself live in `cli_run.rs`.

#![allow(clippy::too_many_lines)] // test fixtures are dense on purpose
#![allow(clippy::type_complexity)] // dispatch tuples mix five kinds of value on purpose

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use bench::analysis::{reconcile, CapacityRow};
use bench::matrix::{
    check_request, check_resume, read_records, resume_plan, run_configs, run_configs_with_stop,
    run_matrix_with, MatrixOutcome, MatrixSpec, ResumeState, servers_for, RECORDED_QOS,
    RECORDED_SERVERS, RECORDED_WORKERS,
};
use bench::record::Timeline;
use bench::record::Window;
use bench::run_record::{
    Build, ConfigKey, Counts, CpuSets, CpuUsage, DriverFds, Env, GeneratorErrors, LatencySummary,
    Profile, RunKind, RunRecord, ServerKind, SyscallMethod, Syscalls, SCHEMA_VERSION,
};
use bench::runner::{
    CLEANUP_SURVIVOR_PREFIX, HostSetup, ProtocolConstants, RunSpec, SCRATCH_REUSED_PREFIX,
    proc_start_time,
};

const ORIGIN_NS: u64 = 10_000_000_000;
const WINDOW_START_NS: u64 = 12_000_000_000;
const WINDOW_END_NS: u64 = 17_000_000_000;
const WARMUP_SECS: u64 = 2;
const WINDOW_SECS: u64 = 5;
const ONE_SECOND_NS: u64 = 1_000_000_000;
/// The harness error a run records for a child its guard could not end.
const SURVIVOR: &str = "endpoint cleanup: pid 4242 (start 1) still running 10s after SIGKILL";

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
        host_cpu_pressure: None,
        worker_loads: None,
        started_at_unix: 1_791_273_600,
        harness_errors: Vec::new(),
        valid: true,
        invalid_reasons: Vec::new(),
        cancelled: false,
    }
}

/// Marks `rec` as a run whose child survived cleanup: pushes a
/// `CLEANUP_SURVIVOR_PREFIX`-tagged `report` onto both `harness_errors` and
/// `invalid_reasons`, and marks the record invalid.
fn with_survivor(mut rec: RunRecord, report: &str) -> RunRecord {
    rec.harness_errors
        .push(format!("{CLEANUP_SURVIVOR_PREFIX}{report}"));
    rec.invalid_reasons
        .push(format!("{CLEANUP_SURVIVOR_PREFIX}{report}"));
    rec.valid = false;
    rec
}

fn throughput_spec() -> RunSpec {
    RunSpec {
        profile: Profile::Smoke,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
        repetition: 0,
        attempt: 1,
    }
}

/// The key of the `throughput_spec` configuration at `workers`.
fn throughput_key(workers: u32) -> ConfigKey {
    ConfigKey {
        server: ServerKind::Uring,
        workers,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
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
    let outcome = run_configs(&[spec], &HashMap::new(), &constants, &out_dir, &scratch, &cancel, |s| {
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
        &HashMap::new(),
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
fn a_resumed_configuration_runs_only_its_remaining_attempts() {
    // Decision Q9 caps a configuration at three completed attempts. A
    // config resumed with one attempt left runs attempt 3 once and then
    // fails.
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
    let key = passing_record(&spec, 1, 3, 100).key();
    let mut attempts: Vec<u32> = Vec::new();
    let outcome = run_configs(
        &[spec],
        &HashMap::from([(key, 1)]),
        &constants,
        &out_dir,
        &scratch,
        &cancel,
        |s| {
            attempts.push(s.attempt);
            let mut rec = passing_record(s, s.repetition, s.attempt, 100);
            rec.valid = false;
            rec.invalid_reasons.push("test-marker".to_string());
            rec
        },
    )
    .expect("run_configs");

    assert_eq!(attempts, vec![3, 3], "attempt 3 only, both repetitions");
    assert_eq!(outcome.passed, Vec::<ConfigKey>::new());
    assert_eq!(outcome.failed.len(), 1);
}

#[test]
fn cancellation_stops_scheduling_and_leaves_the_resume_budget() {
    // SIGTERM during a round must not charge the repetitions and attempts
    // it never started. Running them out would write a cancellation record
    // for attempts 1 to 3, and every one of them would complete an attempt
    // — the interruption would cost the configuration its whole budget.
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
    let err = run_configs(&[spec], &HashMap::new(), &constants, &out_dir, &scratch, &cancel, |s| {
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
        Some(&ResumeState::NextAttempt { attempt: 2, remaining: 3 }),
        "resume must continue at attempt 2, not past the cap"
    );
}

#[test]
fn a_child_that_survived_cleanup_stops_scheduling_and_leaves_the_resume_budget() {
    // A child the run could not end is still running: scheduling another
    // run beside it would measure it against that leftover process, so the
    // matrix stops the moment a record carries a survivor error, ahead of
    // any cancellation, and the attempt it cut is incomplete.
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let out_dir = temp_dir("survivor");
    let scratch = temp_dir("survivor-scratch");
    let cancel = AtomicBool::new(false);
    let spec = throughput_spec();
    let mut attempts: Vec<(u32, u32)> = Vec::new();
    let expected = format!("{CLEANUP_SURVIVOR_PREFIX}{SURVIVOR}; no further run is scheduled beside it");

    let err = run_configs(&[spec], &HashMap::new(), &constants, &out_dir, &scratch, &cancel, |s| {
        attempts.push((s.attempt, s.repetition));
        with_survivor(passing_record(s, s.repetition, s.attempt, 100), SURVIVOR)
    })
    .expect_err("run_configs must report the surviving child");

    assert_eq!(err, expected);
    assert_eq!(attempts, vec![(1, 1)]);
    let existing = read_records(&out_dir).expect("read_records");
    assert_eq!(existing.len(), 1, "the survivor's own record is persisted");
    let plan = resume_plan(&existing, &constants);
    assert_eq!(
        plan.get(&existing[0].key()),
        Some(&ResumeState::NextAttempt { attempt: 2, remaining: 3 }),
        "the attempt the survivor cut short is incomplete, so resume repeats it"
    );
}

#[test]
fn a_survivor_in_the_last_repetition_of_the_last_attempt_leaves_that_attempt_unspent() {
    // Previously the attempt counted complete once every repetition ran,
    // and gave `remaining: 0`; a survivor in the last repetition must
    // still leave the attempt's budget unspent.
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 1,
        enforce_spread: false,
    };
    let out_dir = temp_dir("survivor-last");
    let scratch = temp_dir("survivor-last-scratch");
    let cancel = AtomicBool::new(false);
    let spec = throughput_spec();
    let mut attempts: Vec<(u32, u32)> = Vec::new();
    let expected = format!("{CLEANUP_SURVIVOR_PREFIX}{SURVIVOR}; no further run is scheduled beside it");

    let err = run_configs(&[spec], &HashMap::new(), &constants, &out_dir, &scratch, &cancel, |s| {
        attempts.push((s.attempt, s.repetition));
        if s.repetition == 2 {
            with_survivor(passing_record(s, s.repetition, s.attempt, 100), SURVIVOR)
        } else {
            passing_record(s, s.repetition, s.attempt, 100)
        }
    })
    .expect_err("run_configs must report the surviving child");

    assert_eq!(err, expected);
    assert_eq!(attempts, vec![(1, 1), (1, 2)]);
    let existing = read_records(&out_dir).expect("read_records");
    assert_eq!(existing.len(), 2, "both repetitions are persisted");
    let plan = resume_plan(&existing, &constants);
    assert_eq!(
        plan.get(&existing[0].key()),
        Some(&ResumeState::NextAttempt { attempt: 2, remaining: 1 }),
        "the survivor leaves the last attempt unspent, not complete"
    );
}

#[test]
fn a_survivor_whose_record_cannot_be_written_is_still_reported() {
    // A survivor must survive a failed write: it is the only report of a
    // leftover child, so losing it to a write failure would hide the
    // process an operator needs to find and end.
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let work = temp_dir("survivor-write-fail");
    let out_dir = work.join("out");
    let scratch = work.join("scratch");
    fs::create_dir_all(&out_dir).expect("create out dir");
    let cancel = AtomicBool::new(false);
    let spec = throughput_spec();
    let mut planned = spec.clone();
    planned.repetition = 1;
    let stem = passing_record(&planned, 1, 1, 100)
        .file_name()
        .replace(".json", "");
    let run_scratch = scratch.join(&stem);
    let mut attempts: Vec<(u32, u32)> = Vec::new();
    let second_report = "sub cleanup: pid 4343 (start 1) still running 10s after SIGKILL";

    let err = run_configs(&[spec], &HashMap::new(), &constants, &out_dir, &scratch, &cancel, |s| {
        attempts.push((s.attempt, s.repetition));
        fs::create_dir_all(&run_scratch).expect("create run scratch");
        fs::write(run_scratch.join("gen.json"), b"{}").expect("write evidence");
        let record = passing_record(s, s.repetition, s.attempt, 100);
        fs::write(out_dir.join(format!("{}.tmp", record.file_name())), b"planted")
            .expect("plant the temporary record");
        with_survivor(with_survivor(record, SURVIVOR), second_report)
    })
    .expect_err("run_configs must report the surviving children and the write failure");

    assert!(
        err.starts_with(&format!(
            "{CLEANUP_SURVIVOR_PREFIX}{SURVIVOR}; {CLEANUP_SURVIVOR_PREFIX}{second_report}; no further run is scheduled beside it; "
        )),
        "both survivors precede the write failure, in record order, got {err}"
    );
    assert!(
        err.contains(&stem),
        "the write failure names the record, got {err}"
    );
    assert_eq!(attempts, vec![(1, 1)]);
}

#[test]
fn a_survivor_is_reported_ahead_of_the_cancellation_it_ran_into() {
    // A surviving child outranks a cancellation that arrived in the same
    // run: the leftover process is the more urgent problem, so its error
    // is reported instead of "cancelled".
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let out_dir = temp_dir("survivor-cancel");
    let scratch = temp_dir("survivor-cancel-scratch");
    let cancel = AtomicBool::new(false);
    let spec = throughput_spec();
    let mut attempts: Vec<(u32, u32)> = Vec::new();
    let expected = format!("{CLEANUP_SURVIVOR_PREFIX}{SURVIVOR}; no further run is scheduled beside it");

    let err = run_configs(&[spec], &HashMap::new(), &constants, &out_dir, &scratch, &cancel, |s| {
        attempts.push((s.attempt, s.repetition));
        cancel.store(true, Ordering::SeqCst);
        with_survivor(passing_record(s, s.repetition, s.attempt, 100), SURVIVOR)
    })
    .expect_err("run_configs must report the surviving child, not the cancellation");

    assert_eq!(err, expected);
    assert_eq!(attempts, vec![(1, 1)]);
    let existing = read_records(&out_dir).expect("read_records");
    assert_eq!(existing.len(), 1);
    assert!(
        existing[0].cancelled,
        "the record still carries the cancellation it ran into"
    );
}

#[test]
fn a_run_cut_short_by_a_signal_is_recorded_as_cancelled() {
    // The run cut short by the signal must carry `"cancelled": true` on
    // disk; a run that finished before the signal arrived must carry
    // `"cancelled": false`, not the absence of the key.
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let out_dir = temp_dir("cancel-flag");
    let scratch = temp_dir("cancel-flag-scratch");
    let cancel = AtomicBool::new(false);
    let mut tokio = throughput_spec();
    tokio.server = ServerKind::Tokio;
    let specs = [throughput_spec(), tokio];
    run_configs(&specs, &HashMap::new(), &constants, &out_dir, &scratch, &cancel, |s| {
        if s.server == ServerKind::Tokio {
            cancel.store(true, Ordering::SeqCst);
        }
        passing_record(s, s.repetition, s.attempt, 100)
    })
    .expect_err("cancelled");

    let uring_text = fs::read_to_string(out_dir.join("uring-w1-q1-throughput-a1-r1.json"))
        .expect("read the uring record");
    let uring: serde_json::Value = serde_json::from_str(&uring_text).expect("parse uring record");
    assert_eq!(
        uring["cancelled"],
        serde_json::Value::Bool(false),
        "the run that finished before the signal is not cancelled"
    );

    let tokio_text = fs::read_to_string(out_dir.join("tokio-w1-q1-throughput-a1-r1.json"))
        .expect("read the tokio record");
    let tokio: serde_json::Value = serde_json::from_str(&tokio_text).expect("parse tokio record");
    assert_eq!(
        tokio["cancelled"],
        serde_json::Value::Bool(true),
        "the run in progress when the signal arrived is cancelled"
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
    // it. The stub creates the run's scratch directory and raw evidence
    // during the run, as `run_one` does.
    let mut planned = spec.clone();
    planned.repetition = 1;
    let stem = passing_record(&planned, 1, 1, 100)
        .file_name()
        .replace(".json", "");
    let run_scratch = scratch.join(&stem);

    let err = run_configs(&[spec], &HashMap::new(), &constants, &out_dir, &scratch, &cancel, |s| {
        fs::create_dir_all(&run_scratch).expect("create run scratch");
        fs::write(run_scratch.join("gen.json"), b"{}").expect("write evidence");
        let record = passing_record(s, s.repetition, s.attempt, 100);
        // A temporary record planted while the run is in progress — after
        // `attempt_in_use` inspected the attempt and before the write —
        // fails the write the way a full filesystem would. `create_new`
        // refuses it whatever the test runner's privileges, where a
        // read-only output directory would still let a `CAP_DAC_OVERRIDE`
        // runner through.
        fs::write(out_dir.join(format!("{}.tmp", record.file_name())), b"planted")
            .expect("plant the temporary record");
        record
    })
    .expect_err("run_configs must report the write failure");

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
fn a_run_that_did_not_create_its_scratch_directory_leaves_it_alone() {
    // Two runs can inspect the same empty output directory before either
    // creates anything, so `attempt_in_use` passes for both. Only one wins
    // `run_one`'s exclusive `create_dir`; the loser's record carries the
    // reuse marker, and its own failed write must not take the winner's
    // raw evidence with it.
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let work = temp_dir("scratch-not-ours");
    let out_dir = work.join("out");
    let scratch = work.join("scratch");
    fs::create_dir_all(&out_dir).expect("create out dir");
    let cancel = AtomicBool::new(false);
    let spec = throughput_spec();
    let mut planned = spec.clone();
    planned.repetition = 1;
    let stem = passing_record(&planned, 1, 1, 100)
        .file_name()
        .replace(".json", "");
    let run_scratch = scratch.join(&stem);

    let err = run_configs(&[spec], &HashMap::new(), &constants, &out_dir, &scratch, &cancel, |s| {
        // The winning run creates the directory and its evidence while this
        // one is in progress, which is why this one reports the reuse.
        fs::create_dir_all(&run_scratch).expect("create the other run's scratch");
        fs::write(run_scratch.join("gen.json"), b"{}").expect("write evidence");
        let mut record = passing_record(s, s.repetition, s.attempt, 100);
        record
            .harness_errors
            .push(format!("{SCRATCH_REUSED_PREFIX}{}", run_scratch.display()));
        // The winner publishes its own temporary record for this attempt
        // while this run is in progress, so this run's write collides.
        // `create_new` refuses it whatever the test runner's privileges,
        // where a read-only output directory would still let a
        // `CAP_DAC_OVERRIDE` runner through.
        fs::write(out_dir.join(format!("{}.tmp", record.file_name())), b"planted")
            .expect("plant the winner's temporary record");
        record
    })
    .expect_err("run_configs must report the write failure");

    assert!(
        err.contains(&stem),
        "the write failure names the record, got {err}"
    );
    assert!(
        run_scratch.join("gen.json").exists(),
        "the other run's raw evidence must survive, {} is gone",
        run_scratch.join("gen.json").display()
    );
}

#[test]
fn a_planted_temporary_record_is_refused_and_its_target_left_intact() {
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let work = temp_dir("tmp-planted");
    let out_dir = work.join("out");
    let scratch = work.join("scratch");
    fs::create_dir_all(&out_dir).expect("create out dir");
    let cancel = AtomicBool::new(false);
    let spec = throughput_spec();
    let mut planned = spec.clone();
    planned.repetition = 1;
    let name = passing_record(&planned, 1, 1, 100).file_name();
    let victim = work.join("victim.txt");
    fs::write(&victim, b"original").expect("write victim");
    let tmp = out_dir.join(format!("{name}.tmp"));

    let err = run_configs(&[spec], &HashMap::new(), &constants, &out_dir, &scratch, &cancel, |s| {
        std::os::unix::fs::symlink(&victim, &tmp).expect("plant tmp");
        passing_record(s, s.repetition, s.attempt, 100)
    })
    .expect_err("run_configs must refuse the planted temporary record");

    assert!(
        err.contains("already exists; refusing to write through it"),
        "error must name the refusal, got {err}"
    );
    assert_eq!(
        fs::read(&victim).expect("read victim"),
        b"original",
        "the symlink target must stay byte-for-byte unchanged"
    );
    assert!(
        fs::symlink_metadata(&tmp)
            .expect("stat tmp")
            .file_type()
            .is_symlink(),
        "the planted symlink must stay in place"
    );
    assert!(
        !out_dir.join(&name).exists(),
        "no final record must be published"
    );
}

#[test]
fn an_existing_record_is_refused_and_left_intact() {
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let work = temp_dir("record-existing");
    let out_dir = work.join("out");
    let scratch = work.join("scratch");
    fs::create_dir_all(&out_dir).expect("create out dir");
    let cancel = AtomicBool::new(false);
    let spec = throughput_spec();
    let mut planned = spec.clone();
    planned.repetition = 1;
    let name = passing_record(&planned, 1, 1, 100).file_name();

    let err = run_configs(&[spec], &HashMap::new(), &constants, &out_dir, &scratch, &cancel, |s| {
        fs::write(out_dir.join(&name), b"existing").expect("plant record");
        passing_record(s, s.repetition, s.attempt, 100)
    })
    .expect_err("run_configs must refuse to overwrite the existing record");

    assert!(
        err.contains("already exists; refusing to overwrite it"),
        "error must name the refusal, got {err}"
    );
    assert_eq!(
        fs::read(out_dir.join(&name)).expect("read record"),
        b"existing",
        "the existing record must stay byte-for-byte unchanged"
    );
    assert!(
        !out_dir.join(format!("{name}.tmp")).exists(),
        "no leftover temporary file must remain"
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
    let err = check_request(&spec, SyscallMethod::None, None, None, None)
        .expect_err("a repeated qos dispatches the same configuration twice");
    assert!(err.contains("--qos"), "{err}");

    let mut dup_workers = spec.clone();
    dup_workers.qos = vec![1];
    dup_workers.workers = vec![1, 1];
    let err = check_request(&dup_workers, SyscallMethod::None, None, None, None)
        .expect_err("a repeated worker count dispatches the same configuration twice");
    assert!(err.contains("--workers"), "{err}");

    let mut dup_servers = spec.clone();
    dup_servers.qos = vec![1];
    dup_servers.servers = vec![ServerKind::Uring, ServerKind::Uring];
    let err = check_request(&dup_servers, SyscallMethod::None, None, None, None)
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
    let err = check_request(&spec, SyscallMethod::None, None, None, None)
        .expect_err("qos 2 is not a measured QoS");
    assert!(err.contains("--qos"), "{err}");
    assert!(err.contains("0 or 1"), "{err}");

    let mut zero_workers = spec.clone();
    zero_workers.qos = vec![1];
    zero_workers.workers = vec![0];
    let err = check_request(&zero_workers, SyscallMethod::None, None, None, None)
        .expect_err("a server runs at least one worker");
    assert!(err.contains("--workers"), "{err}");

    let mut valid = spec.clone();
    valid.qos = vec![1, 0];
    valid.workers = vec![1, 4];
    check_request(&valid, SyscallMethod::None, None, None, None)
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
    offered_rate: Option<u64>,
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
        offered_rate,
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
    // Config B: attempt 1, rep 1 valid, rep 2 invalid -> NextAttempt { attempt: 2, remaining: 2 }
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
    // Config C: attempt 2, rep 1 only (interrupted) -> NextAttempt { attempt: 3, remaining: 3 }
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
    assert_eq!(plan.get(&b.key()), Some(&ResumeState::NextAttempt { attempt: 2, remaining: 2 }));
    assert_eq!(plan.get(&c.key()), Some(&ResumeState::NextAttempt { attempt: 3, remaining: 3 }));
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

/// Lifetime of the live child a survivor entry names; the test ends it itself.
const LIVE_CHILD_SECS: u64 = 60;

/// A live child a survivor entry can name. `Drop` ends and reaps it, so a
/// failed assertion cannot leave it running beside the tests that follow.
struct LiveChild(std::process::Child);

impl LiveChild {
    fn spawn() -> Self {
        LiveChild(
            std::process::Command::new("sleep")
                .arg(LIVE_CHILD_SECS.to_string())
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("spawn live child"),
        )
    }

    fn pid(&self) -> u32 {
        self.0.id()
    }

    /// Ends and reaps the child, so a test can observe the pid it held
    /// once no process holds it. Idempotent, so `Drop` may repeat it.
    fn end(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for LiveChild {
    fn drop(&mut self) {
        self.end();
    }
}

/// A record naming the given survivor entry, with `invalid_reasons` and
/// `valid` recomputed so `check_resume`'s own recompute agrees with it.
fn resumable_record_naming(entry: &str) -> RunRecord {
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
    rec.harness_errors.push(format!("{CLEANUP_SURVIVOR_PREFIX}{entry}"));
    rec.invalid_reasons = reconcile(&rec);
    rec.valid = rec.invalid_reasons.is_empty();
    rec
}

#[test]
fn a_resume_beside_a_running_survivor_is_refused_until_it_ends() {
    let out_dir = temp_dir("resume-survivor");
    let mut child = LiveChild::spawn();
    let pid = child.pid();
    let start = proc_start_time(pid).expect("live child start time");
    let entry = format!("endpoint cleanup: pid {pid} (start {start}) still running 10s after SIGKILL");
    let rec = resumable_record_naming(&entry);
    write_record(&out_dir, &rec);
    let existing = read_records(&out_dir).expect("read");

    let err = check_resume(&existing, Profile::Smoke, SyscallMethod::None, &rec.env, &rec.cpu_sets)
        .expect_err("a running survivor must refuse the resume");
    assert!(err.contains(&rec.file_name()), "err must name the record file: {err}");
    assert!(err.contains(&entry), "err must name the survivor entry: {err}");
    assert!(err.contains("end it before --resume"), "err must name the remedy: {err}");

    child.end();

    check_resume(&existing, Profile::Smoke, SyscallMethod::None, &rec.env, &rec.cpu_sets)
        .expect("an ended survivor resumes");
}

#[test]
fn a_resume_beside_a_survivor_pid_now_held_by_another_process_proceeds() {
    let out_dir = temp_dir("resume-survivor-reused");
    let child = LiveChild::spawn();
    let pid = child.pid();
    let start = proc_start_time(pid).expect("live child start time");
    let entry = format!(
        "endpoint cleanup: pid {pid} (start {}) still running 10s after SIGKILL",
        start + 1
    );
    let rec = resumable_record_naming(&entry);
    write_record(&out_dir, &rec);
    let existing = read_records(&out_dir).expect("read");

    check_resume(&existing, Profile::Smoke, SyscallMethod::None, &rec.env, &rec.cpu_sets)
        .expect("a pid now held by another process must not block the resume");
}

#[test]
fn a_resume_beside_a_live_pid_with_no_recorded_start_time_is_refused() {
    let out_dir = temp_dir("resume-survivor-unknown");
    let child = LiveChild::spawn();
    let pid = child.pid();
    let entry = format!("endpoint cleanup: pid {pid} (start ?) exit check failed: test");
    let rec = resumable_record_naming(&entry);
    write_record(&out_dir, &rec);
    let existing = read_records(&out_dir).expect("read");

    let err = check_resume(&existing, Profile::Smoke, SyscallMethod::None, &rec.env, &rec.cpu_sets)
        .expect_err("a live pid with no recorded start time must refuse the resume");
    assert!(err.contains(&entry), "err must name the survivor entry: {err}");
}

#[test]
fn a_resume_is_refused_by_a_live_survivor_an_ended_one_precedes() {
    // One run can leave several children, and the operator may have ended
    // only some of them. Every survivor entry of a record is checked, so an
    // entry whose process is gone does not stand in for the ones behind it.
    let out_dir = temp_dir("resume-survivor-second");
    let mut ended = LiveChild::spawn();
    let ended_pid = ended.pid();
    let ended_start = proc_start_time(ended_pid).expect("ended child start time");
    ended.end();

    let live = LiveChild::spawn();
    let live_pid = live.pid();
    let live_start = proc_start_time(live_pid).expect("live child start time");

    let ended_entry =
        format!("sub cleanup: pid {ended_pid} (start {ended_start}) still running 10s after SIGKILL");
    let live_entry = format!(
        "endpoint cleanup: pid {live_pid} (start {live_start}) still running 10s after SIGKILL"
    );
    let mut rec = resumable_record_naming(&ended_entry);
    rec.harness_errors
        .push(format!("{CLEANUP_SURVIVOR_PREFIX}{live_entry}"));
    rec.invalid_reasons = reconcile(&rec);
    rec.valid = rec.invalid_reasons.is_empty();
    write_record(&out_dir, &rec);
    let existing = read_records(&out_dir).expect("read");

    let err = check_resume(
        &existing,
        Profile::Smoke,
        SyscallMethod::None,
        &rec.env,
        &rec.cpu_sets,
    )
    .expect_err("a survivor behind an ended one must refuse the resume");
    assert!(
        err.contains(&live_entry),
        "err must name the survivor that is still running: {err}"
    );
    assert!(
        err.contains("end it before --resume"),
        "err must name the remedy: {err}"
    );
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
/// evidence — every kind but the counted run — do not constrain the
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
    // before its first counted run can resume under any method.
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
    // `valid` as evidence the configuration passed. `finalize_record`
    // derives that field from the raw data, so a mismatch is an edit made
    // outside the harness.
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

/// AC-7 (failure mode): a record from an earlier schema follows an
/// earlier naming convention, so its schema is named before its file
/// name is even compared against the new one.
#[test]
fn a_resume_names_the_schema_of_a_record_from_an_earlier_version() {
    let dir = temp_dir("matrix-with-old-schema");
    let out_dir = dir.join("out");
    fs::create_dir_all(&out_dir).expect("create out dir");
    let spec = RunSpec {
        profile: Profile::Recorded,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Latency,
        offered_rate: Some(300_000),
        repetition: 1,
        attempt: 1,
    };
    let rec = passing_record(&spec, 1, 1, 100);
    let mut value = serde_json::to_value(&rec).expect("serialize record");
    value["schema"] = 2.into();
    let path = out_dir.join("uring-w1-q1-latency-p80-a1-r1.json");
    fs::write(&path, serde_json::to_string(&value).expect("serialize value"))
        .expect("write old-schema record");

    let matrix_spec = MatrixSpec {
        profile: Profile::Recorded,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir: out_dir.clone(),
        resume: true,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let err = run_matrix_with(&matrix_spec, &host, |_| {
        panic!("the stub must not be called for a resume refused by its record schema")
    })
    .expect_err("a record from an earlier schema must refuse the resume");
    assert_eq!(err, format!("{}: record schema 2 != 3", path.display()));
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
    check_request(&spec, SyscallMethod::None, None, None, None).expect("smoke check_request");
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
    let err = check_request(&spec, SyscallMethod::None, None, None, tool)
        .expect_err("recorded refuses --syscalls none");
    assert!(err.contains("--syscalls"), "{err}");
    // No mosquitto -> err mentions --mosquitto-bin
    let err = check_request(&spec, SyscallMethod::Perf, tool, None, None)
        .expect_err("recorded refuses without --mosquitto-bin");
    assert!(err.contains("--mosquitto-bin"), "{err}");
    // Partial matrix -> err mentions full matrix
    let mut partial = spec.clone();
    partial.workers = vec![1];
    let err = check_request(&partial, SyscallMethod::Perf, tool, None, tool)
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
    let err = check_request(&spec, SyscallMethod::Perf, None, None, None)
        .expect_err("--syscalls perf needs --perf-bin");
    assert!(err.contains("--perf-bin"), "{err}");
    let err = check_request(&spec, SyscallMethod::Strace, None, None, None)
        .expect_err("--syscalls strace needs --strace-bin");
    assert!(err.contains("--strace-bin"), "{err}");
    check_request(&spec, SyscallMethod::Perf, Some(Path::new("/bin/true")), None, None)
        .expect("a counter path satisfies the check");
}

/// A counter path for a counter `--syscalls` did not select is refused
/// (AC-15): otherwise the unused path's tool-version probe runs for a
/// flag the run never reads.
#[test]
fn check_request_refuses_a_counter_path_the_selected_method_does_not_use() {
    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir: temp_dir("req-counter-unused"),
        resume: false,
    };
    let tool = Some(Path::new("/bin/true"));
    let err = check_request(&spec, SyscallMethod::Perf, tool, tool, None)
        .expect_err("perf does not use --strace-bin");
    assert!(err.contains("does not use --strace-bin"), "{err}");
    let err = check_request(&spec, SyscallMethod::None, tool, None, None)
        .expect_err("none does not use --perf-bin");
    assert!(
        err.contains("--syscalls none does not use --perf-bin"),
        "{err}"
    );
    let err = check_request(&spec, SyscallMethod::Strace, tool, tool, None)
        .expect_err("strace does not use --perf-bin");
    assert!(err.contains("does not use --perf-bin"), "{err}");
    // The unused path is named even when the selected counter's own path
    // is missing: both combinations satisfy AC-15's IF, and the missing
    // path's refusal would ask for a second path instead of for the one
    // the selection does not read.
    let err = check_request(&spec, SyscallMethod::Perf, None, tool, None)
        .expect_err("perf does not use --strace-bin");
    assert!(err.contains("does not use --strace-bin"), "{err}");
    let err = check_request(&spec, SyscallMethod::Strace, tool, None, None)
        .expect_err("strace does not use --perf-bin");
    assert!(err.contains("does not use --perf-bin"), "{err}");
    check_request(&spec, SyscallMethod::Strace, None, tool, None)
        .expect("strace with only --strace-bin passes");
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
    let err = check_request(&spec, SyscallMethod::None, None, None, None)
        .expect_err("a matrix with no runnable server measures nothing");
    assert!(err.contains("no server runs"), "{err}");

    // The same server is runnable on the single-worker row, so a request
    // that includes it is accepted.
    let mut with_one = spec.clone();
    with_one.workers = vec![1, 4];
    check_request(&with_one, SyscallMethod::None, None, None, None)
        .expect("the 1-worker row runs mosquitto");
}

#[test]
fn resume_plan_saturates_an_attempt_number_it_cannot_advance() {
    // `attempt` deserializes any `u32`. A record carrying one this runner
    // never produces must not overflow the resume decision; an attempt
    // that cannot advance gets `remaining: 0`, so `run_configs` leaves
    // the configuration unrun and failed.
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
        Some(&ResumeState::NextAttempt {
            attempt: u32::MAX,
            remaining: 0,
        })
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
        check_request(&spec, SyscallMethod::None, None, None, None).expect_err("refuses non-empty");
    assert!(err.contains("not empty"), "{err}");
}

#[test]
fn outcome_default_has_empty_passed_and_failed() {
    let outcome = MatrixOutcome::default();
    assert!(outcome.passed.is_empty());
    assert!(outcome.failed.is_empty());
}

/// `HostSetup` whose every path is a placeholder so the stub runner is
/// the only thing the matrix actually invokes. Mirrors the `cpu_sets` and
/// `env` of `passing_record` so the dispatched records' environment
/// matches what the test wrote; the runner is bypassed entirely.
fn stub_host(dir: &Path, syscalls: SyscallMethod) -> HostSetup {
    HostSetup {
        bench_exe: PathBuf::from("/nonexistent/bench"),
        mosquitto_bin: None,
        syscalls,
        perf_bin: None,
        strace_bin: None,
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
        scratch_dir: dir.join(".scratch"),
        cpu_pressure_path: PathBuf::from(bench::runner::HOST_CPU_PRESSURE_PATH),
        cancel: std::sync::Arc::new(AtomicBool::new(false)),
    }
}

#[test]
fn run_matrix_with_dispatches_throughput_then_latency_then_connections() {
    let dir = temp_dir("matrix-with-order");
    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir: dir.join("out"),
        resume: false,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let mut calls: Vec<(RunKind, Option<u64>, u32)> = Vec::new();
    let outcome = run_matrix_with(&spec, &host, |s| {
        calls.push((s.kind, s.offered_rate, s.repetition));
        passing_record(s, s.repetition, s.attempt, 100)
    })
    .expect("run_matrix_with");
    assert!(
        outcome.failed.is_empty(),
        "no failures expected, got failed={:?}",
        outcome.failed
    );
    assert_eq!(
        calls,
        vec![
            (RunKind::Throughput, None, 1),
            (RunKind::Throughput, None, 2),
            (RunKind::Latency, Some(40_000), 1),
            (RunKind::Latency, Some(60_000), 1),
            (RunKind::Latency, Some(40_000), 2),
            (RunKind::Latency, Some(60_000), 2),
            (RunKind::Connections, None, 1),
            (RunKind::Connections, None, 2),
        ]
    );
    assert!(spec
        .out_dir
        .join("uring-w1-q1-latency-at40000-a1-r1.json")
        .exists());
    assert!(spec
        .out_dir
        .join("uring-w1-q1-latency-at60000-a1-r2.json")
        .exists());
}

#[test]
fn run_matrix_with_runs_each_worker_count_row_by_row() {
    let dir = temp_dir("matrix-with-rows");
    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring],
        workers: vec![1, 4],
        qos: vec![1, 0],
        out_dir: dir.join("out"),
        resume: false,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let mut calls: Vec<(u32, Option<u8>, RunKind)> = Vec::new();
    run_matrix_with(&spec, &host, |s| {
        calls.push((s.workers, s.qos, s.kind));
        passing_record(s, s.repetition, s.attempt, 100)
    })
    .expect("run_matrix_with");
    let mut collapsed: Vec<(u32, Option<u8>, &str)> = Vec::new();
    for (w, qos, kind) in &calls {
        let stage = match *kind {
            RunKind::Throughput => "throughput",
            RunKind::Latency | RunKind::Counted => "open-loop",
            RunKind::Connections => "connections",
        };
        let triple = (*w, *qos, stage);
        if collapsed.last() != Some(&triple) {
            collapsed.push(triple);
        }
    }
    assert_eq!(
        collapsed,
        vec![
            (1, Some(1), "throughput"),
            (1, Some(1), "open-loop"),
            (1, Some(0), "throughput"),
            (1, Some(0), "open-loop"),
            (1, None, "connections"),
            (4, Some(1), "throughput"),
            (4, Some(1), "open-loop"),
            (4, Some(0), "throughput"),
            (4, Some(0), "open-loop"),
            (4, None, "connections"),
        ]
    );
}

/// AC-12: a counted run joins the latency runs in the same interleaved
/// round at the row group's higher declared rate (300000 msgs/s for
/// 1 worker QoS 1), whatever the throughput medians are.
#[test]
fn run_matrix_with_dispatches_a_counted_run_beside_the_latency_runs() {
    for method in [SyscallMethod::Perf, SyscallMethod::Strace] {
        let dir = temp_dir("matrix-with-counted");
        let spec = MatrixSpec {
            profile: Profile::Recorded,
            servers: vec![ServerKind::Uring, ServerKind::Tokio],
            workers: vec![1],
            qos: vec![1],
            out_dir: dir.join("out"),
            resume: false,
        };
        let host = stub_host(&dir, method);
        let mut calls: Vec<(u32, ServerKind, RunKind, Option<u64>)> = Vec::new();
        let outcome = run_matrix_with(&spec, &host, |s| {
            if s.kind == RunKind::Latency || s.kind == RunKind::Counted {
                calls.push((s.repetition, s.server, s.kind, s.offered_rate));
            }
            let mut rec = passing_record(s, s.repetition, s.attempt, 100);
            if s.kind == RunKind::Counted {
                rec.syscalls.per_msg = Some(5.0);
            }
            rec
        })
        .expect("run_matrix_with");

        let mut expected: Vec<(u32, ServerKind, RunKind, Option<u64>)> = Vec::new();
        for r in 1..=5u32 {
            for &server in &[ServerKind::Uring, ServerKind::Tokio] {
                expected.push((r, server, RunKind::Latency, Some(75_000)));
                expected.push((r, server, RunKind::Latency, Some(300_000)));
                expected.push((r, server, RunKind::Counted, Some(300_000)));
            }
        }
        assert_eq!(calls, expected, "method {method:?}: dispatch order/rates");
        assert!(
            outcome.failed.is_empty(),
            "method {method:?}: no failures expected, got {:?}",
            outcome.failed
        );
    }
}

/// AC-13: without a syscall counter selected, no counted run is ever
/// dispatched.
#[test]
fn run_matrix_with_dispatches_no_counted_run_without_a_counter() {
    let dir = temp_dir("matrix-with-no-counter");
    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir: dir.join("out"),
        resume: false,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let mut kinds: Vec<RunKind> = Vec::new();
    let outcome = run_matrix_with(&spec, &host, |s| {
        kinds.push(s.kind);
        passing_record(s, s.repetition, s.attempt, 100)
    })
    .expect("run_matrix_with");
    assert!(
        !kinds.contains(&RunKind::Counted),
        "no counted run without a counter, got {kinds:?}"
    );
    assert!(outcome.failed.is_empty());
}

/// AC-5: a failed throughput configuration does not hold back its row
/// group's open-loop stage — the latency and counted runs still
/// dispatch at the row group's declared rates.
#[test]
fn run_matrix_with_dispatches_the_open_loop_stage_of_a_row_whose_throughput_failed() {
    let dir = temp_dir("matrix-with-counted-failed-row");
    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir: dir.join("out"),
        resume: false,
    };
    let host = stub_host(&dir, SyscallMethod::Perf);
    let mut dispatched_kinds: Vec<RunKind> = Vec::new();
    let outcome = run_matrix_with(&spec, &host, |s| {
        dispatched_kinds.push(s.kind);
        let mut rec = passing_record(s, s.repetition, s.attempt, 100);
        if s.kind == RunKind::Throughput {
            rec.valid = false;
            rec.invalid_reasons.push("test-marker".to_string());
        }
        rec
    })
    .expect("run_matrix_with");

    assert!(
        dispatched_kinds.contains(&RunKind::Latency) && dispatched_kinds.contains(&RunKind::Counted),
        "the open-loop stage must still dispatch when its row's throughput failed, got {dispatched_kinds:?}"
    );
    assert!(
        outcome.failed.contains(&ConfigKey {
            server: ServerKind::Uring,
            workers: 1,
            qos: Some(1),
            kind: RunKind::Throughput,
            offered_rate: None,
        }),
        "the failed throughput configuration must be marked failed, got {:?}",
        outcome.failed
    );
    assert!(
        !outcome
            .failed
            .iter()
            .any(|k| k.kind == RunKind::Latency || k.kind == RunKind::Counted),
        "the open-loop stage must not be marked failed by its row's throughput, got {:?}",
        outcome.failed
    );
}

/// AC-12: one counted configuration per (server, workers, qos) — each
/// row of the matrix gets its own counted run at that row's higher
/// declared rate, and the row's counted configuration lands in
/// `MatrixOutcome.passed`. A counted run scheduled once per worker
/// count, or once per server, fails the expected list.
#[test]
fn run_matrix_with_counts_every_worker_and_qos_row() {
    let dir = temp_dir("matrix-with-counted-rows");
    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring],
        workers: vec![1, 4],
        qos: vec![1, 0],
        out_dir: dir.join("out"),
        resume: false,
    };
    let host = stub_host(&dir, SyscallMethod::Perf);
    let mut counted: Vec<(u32, Option<u8>, Option<u64>, u32)> = Vec::new();
    let outcome = run_matrix_with(&spec, &host, |s| {
        if s.kind == RunKind::Counted {
            counted.push((s.workers, s.qos, s.offered_rate, s.repetition));
        }
        let mut rec = passing_record(s, s.repetition, s.attempt, 100);
        if s.kind == RunKind::Counted {
            rec.syscalls.per_msg = Some(5.0);
        }
        rec
    })
    .expect("run_matrix_with");

    let mut expected: Vec<(u32, Option<u8>, Option<u64>, u32)> = Vec::new();
    for w in [1u32, 4] {
        for q in [Some(1u8), Some(0)] {
            for rep in 1..=2u32 {
                expected.push((w, q, Some(60_000), rep));
            }
        }
    }
    assert_eq!(counted, expected, "one counted run per (workers, qos) row");
    assert!(
        outcome.failed.is_empty(),
        "no failures expected, got {:?}",
        outcome.failed
    );
    for w in [1u32, 4] {
        for q in [Some(1u8), Some(0)] {
            assert!(
                outcome.passed.iter().any(|k| k.server == ServerKind::Uring
                    && k.workers == w
                    && k.qos == q
                    && k.kind == RunKind::Counted
                    && k.offered_rate == Some(60_000)),
                "counted configuration workers={w} qos={q:?} missing from passed={:?}",
                outcome.passed
            );
        }
    }
}

/// A `--resume` continues a counted configuration's own attempt number,
/// like every other kind: the records of its failed attempt 1 are on
/// disk, so the resumed session dispatches attempt 2 at the higher
/// declared rate and passes the configuration. `check_resume` accepts those
/// records, so the counted key reaching `resume_plan` is also covered.
#[test]
fn run_matrix_with_resumes_a_counted_configuration_at_its_next_attempt() {
    let dir = temp_dir("matrix-with-counted-resume");
    let out_dir = dir.join("out");
    fs::create_dir_all(&out_dir).expect("create out dir");
    for rep in 1..=5u32 {
        let mut rec = make_record(
            Profile::Recorded,
            ServerKind::Uring,
            1,
            Some(1),
            RunKind::Counted,
            Some(300_000),
            1,
            rep,
            false,
            "abc1234",
        );
        // `check_resume` recomputes every record it reads, so the stored
        // verdict has to be the one `reconcile` reports for it.
        rec.invalid_reasons = reconcile(&rec);
        rec.valid = rec.invalid_reasons.is_empty();
        assert!(
            !rec.valid,
            "fixture must be a failed attempt, reasons {:?}",
            rec.invalid_reasons
        );
        write_record(&out_dir, &rec);
    }

    let spec = MatrixSpec {
        profile: Profile::Recorded,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir,
        resume: true,
    };
    let host = stub_host(&dir, SyscallMethod::Perf);
    let mut counted: Vec<(u32, u32, Option<u64>)> = Vec::new();
    let outcome = run_matrix_with(&spec, &host, |s| {
        if s.kind == RunKind::Counted {
            counted.push((s.attempt, s.repetition, s.offered_rate));
        }
        let mut rec = passing_record(s, s.repetition, s.attempt, 100);
        if s.kind == RunKind::Counted {
            rec.syscalls.per_msg = Some(5.0);
        }
        rec
    })
    .expect("run_matrix_with");

    assert_eq!(
        counted,
        (1..=5u32).map(|rep| (2, rep, Some(300_000))).collect::<Vec<_>>(),
        "the resumed counted configuration runs attempt 2 at the higher declared rate"
    );
    let key = ConfigKey {
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Counted,
        offered_rate: Some(300_000),
    };
    assert!(
        outcome.passed.contains(&key),
        "the resumed counted configuration must pass, passed={:?} failed={:?}",
        outcome.passed,
        outcome.failed
    );
}

/// An invalid record whose stored verdict matches what `reconcile`
/// recomputes for it (one duplicate), the shape `check_resume`'s
/// recomputation requires of a seeded fixture. Repetition and attempt
/// come from `spec`, so a completed, failed attempt can be seeded by
/// calling this once per repetition.
fn failing_record(spec: &RunSpec) -> RunRecord {
    let mut rec = passing_record(spec, spec.repetition, spec.attempt, 100);
    rec.counts.duplicates = 1;
    rec.invalid_reasons = reconcile(&rec);
    rec.valid = rec.invalid_reasons.is_empty();
    assert!(
        !rec.valid,
        "fixture must be a failed attempt, reasons {:?}",
        rec.invalid_reasons
    );
    rec
}

/// AC-12: a configuration interrupted mid-session — one server stopped
/// between its repetitions, the other cut short during one — resumes
/// every configuration it cut short at the next attempt number, not only
/// the one that was running when the signal arrived.
#[test]
fn an_interrupted_session_resumes_every_configuration_it_cut_short() {
    let dir = temp_dir("interrupted-session");
    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring, ServerKind::Tokio],
        workers: vec![1],
        qos: vec![1],
        out_dir: dir.join("out"),
        resume: false,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let cancel = host.cancel.clone();

    let err = run_matrix_with(&spec, &host, |s| {
        if s.kind == RunKind::Throughput && s.server == ServerKind::Tokio && s.repetition == 1 {
            cancel.store(true, Ordering::SeqCst);
        }
        passing_record(s, s.repetition, s.attempt, 100)
    })
    .expect_err("an interrupted session must return cancelled");
    assert_eq!(err, "cancelled");

    // The records the interrupted session published, as a later process
    // reads them: the attempt it cut short stays on disk under its own
    // name, so the resumed attempt must not land on these bytes.
    let out_dir = spec.out_dir.clone();
    let before: Vec<(PathBuf, Vec<u8>)> = ["uring", "tokio"]
        .into_iter()
        .map(|server| out_dir.join(format!("{server}-w1-q1-throughput-a1-r1.json")))
        .map(|path| {
            let bytes = fs::read(&path)
                .unwrap_or_else(|e| panic!("{}: must exist after phase 1: {e}", path.display()));
            (path, bytes)
        })
        .collect();

    cancel.store(false, Ordering::SeqCst);
    let mut resumed_spec = spec.clone();
    resumed_spec.resume = true;
    let mut dispatched: Vec<(ServerKind, u32, u32)> = Vec::new();
    let outcome = run_matrix_with(&resumed_spec, &host, |s| {
        if s.kind == RunKind::Throughput {
            dispatched.push((s.server, s.attempt, s.repetition));
        }
        passing_record(s, s.repetition, s.attempt, 100)
    })
    .expect("resumed run_matrix_with");

    assert_eq!(
        dispatched,
        vec![
            (ServerKind::Uring, 2, 1),
            (ServerKind::Tokio, 2, 1),
            (ServerKind::Uring, 2, 2),
            (ServerKind::Tokio, 2, 2),
        ]
    );
    assert!(outcome.failed.is_empty(), "failed={:?}", outcome.failed);

    // What the resumed attempt left on disk, which is what a later
    // process actually reads: one record per server and repetition, each
    // naming attempt 2 and none of them cancelled.
    for server in ["uring", "tokio"] {
        for rep in 1..=2u32 {
            let path = out_dir.join(format!("{server}-w1-q1-throughput-a2-r{rep}.json"));
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: must exist after resume: {e}", path.display()));
            let value: serde_json::Value =
                serde_json::from_str(&text).expect("parse the resumed record");
            assert_eq!(value["attempt"], 2, "{}: attempt", path.display());
            assert_eq!(value["repetition"], rep, "{}: repetition", path.display());
            assert_eq!(
                value["cancelled"],
                serde_json::Value::Bool(false),
                "{}: cancelled",
                path.display()
            );
        }
    }
    // The interrupted attempt's records are untouched by the resume.
    for (path, bytes) in &before {
        let now = fs::read(path).expect("read the preserved record");
        assert_eq!(now, *bytes, "{}: must be unchanged", path.display());
    }
}

/// AC-12, AC-13: a configuration cancelled during the last repetition of
/// its last permitted attempt still has an unspent attempt once its
/// interrupted one is recognized as never having completed, and resumes
/// at the next attempt number rather than being left failed.
#[test]
fn a_run_cancelled_in_the_last_attempt_resumes_with_its_unspent_attempt() {
    let dir = temp_dir("cancelled-last-attempt");
    let out_dir = dir.join("out");
    fs::create_dir_all(&out_dir).expect("create out dir");
    let mut before: Vec<(PathBuf, Vec<u8>)> = Vec::new();
    for attempt in 1..=2u32 {
        for rep in 1..=5u32 {
            let seed_spec = RunSpec {
                profile: Profile::Recorded,
                server: ServerKind::Uring,
                workers: 1,
                qos: Some(1),
                kind: RunKind::Throughput,
                offered_rate: None,
                repetition: rep,
                attempt,
            };
            let rec = failing_record(&seed_spec);
            let path = out_dir.join(rec.file_name());
            write_record(&out_dir, &rec);
            before.push((path.clone(), fs::read(&path).expect("read seeded record")));
        }
    }

    let spec = MatrixSpec {
        profile: Profile::Recorded,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir: out_dir.clone(),
        resume: true,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let cancel = host.cancel.clone();

    let err = run_matrix_with(&spec, &host, |s| {
        if s.kind == RunKind::Throughput {
            let mut rec = passing_record(s, s.repetition, s.attempt, 100);
            rec.invalid_reasons = reconcile(&rec);
            rec.valid = rec.invalid_reasons.is_empty();
            if s.attempt == 3 && s.repetition == 5 {
                cancel.store(true, Ordering::SeqCst);
            }
            rec
        } else {
            passing_record(s, s.repetition, s.attempt, 100)
        }
    })
    .expect_err("cancellation in the last attempt's last repetition must return cancelled");
    assert_eq!(err, "cancelled");

    for rep in 1..=5u32 {
        let path = out_dir.join(format!("uring-w1-q1-throughput-a3-r{rep}.json"));
        assert!(path.exists(), "{}: must exist after phase 1", path.display());
        let text = fs::read_to_string(&path).expect("read attempt 3 record");
        let value: serde_json::Value = serde_json::from_str(&text).expect("parse attempt 3 record");
        let want = serde_json::Value::Bool(rep == 5);
        assert_eq!(value["cancelled"], want, "repetition {rep} cancelled flag");
    }

    cancel.store(false, Ordering::SeqCst);
    let mut dispatched: Vec<(u32, u32)> = Vec::new();
    let outcome = run_matrix_with(&spec, &host, |s| {
        if s.kind == RunKind::Throughput {
            dispatched.push((s.attempt, s.repetition));
            failing_record(s)
        } else {
            passing_record(s, s.repetition, s.attempt, 100)
        }
    })
    .expect("resumed run_matrix_with");

    assert_eq!(
        dispatched,
        (1..=5u32).map(|rep| (4, rep)).collect::<Vec<_>>()
    );
    let key = ConfigKey {
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
    };
    assert!(
        outcome.failed.contains(&key),
        "failed={:?}",
        outcome.failed
    );
    for (path, bytes) in &before {
        let now = fs::read(path).expect("read preserved record");
        assert_eq!(now, *bytes, "{}: must be unchanged", path.display());
    }
}

/// AC-13 (regression guard): a configuration whose completed attempts
/// already spent its whole `max_config_attempts` budget stays failed and
/// is never dispatched again.
#[test]
fn a_spent_configuration_is_not_resumed() {
    let dir = temp_dir("spent-configuration");
    let out_dir = dir.join("out");
    fs::create_dir_all(&out_dir).expect("create out dir");
    for rep in 1..=2u32 {
        let seed_spec = RunSpec {
            profile: Profile::Smoke,
            server: ServerKind::Uring,
            workers: 1,
            qos: Some(1),
            kind: RunKind::Throughput,
            offered_rate: None,
            repetition: rep,
            attempt: 1,
        };
        let rec = failing_record(&seed_spec);
        write_record(&out_dir, &rec);
    }

    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir,
        resume: true,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let mut dispatched: Vec<(u32, u32)> = Vec::new();
    let outcome = run_matrix_with(&spec, &host, |s| {
        if s.kind == RunKind::Throughput {
            dispatched.push((s.attempt, s.repetition));
        }
        passing_record(s, s.repetition, s.attempt, 100)
    })
    .expect("run_matrix_with");

    assert!(
        dispatched.is_empty(),
        "a spent configuration must not be dispatched, got {dispatched:?}"
    );
    let key = ConfigKey {
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
    };
    assert!(
        outcome.failed.contains(&key),
        "failed={:?}",
        outcome.failed
    );
}

/// AC-19: an attempt number that cannot advance past `u32::MAX` is still
/// resumed exactly once, at `u32::MAX`, rather than being refused as past
/// the absolute attempt cap.
#[test]
fn an_attempt_number_that_cannot_advance_is_resumed_only_once() {
    let dir = temp_dir("attempt-cannot-advance");
    let out_dir = dir.join("out");
    fs::create_dir_all(&out_dir).expect("create out dir");
    for rep in 1..=5u32 {
        let seed_spec = RunSpec {
            profile: Profile::Recorded,
            server: ServerKind::Uring,
            workers: 1,
            qos: Some(1),
            kind: RunKind::Throughput,
            offered_rate: None,
            repetition: rep,
            attempt: u32::MAX - 1,
        };
        let rec = failing_record(&seed_spec);
        write_record(&out_dir, &rec);
    }

    let spec = MatrixSpec {
        profile: Profile::Recorded,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir,
        resume: true,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let mut dispatched: Vec<(u32, u32)> = Vec::new();
    let outcome = run_matrix_with(&spec, &host, |s| {
        if s.kind == RunKind::Throughput {
            dispatched.push((s.attempt, s.repetition));
        }
        failing_record(s)
    })
    .expect("run_matrix_with");

    assert_eq!(
        dispatched,
        (1..=5u32).map(|rep| (u32::MAX, rep)).collect::<Vec<_>>()
    );
    let key = ConfigKey {
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
    };
    assert!(
        outcome.failed.contains(&key),
        "failed={:?}",
        outcome.failed
    );
}

/// AC-20: an attempt number whose files already exist — a record, a
/// temporary record (file or dangling symlink), or a scratch directory,
/// for any of its repetitions — is skipped at no cost to the attempt
/// budget, and every planted artifact is left untouched.
#[test]
fn an_attempt_whose_files_exist_is_skipped_at_no_cost() {
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let spec = throughput_spec();
    let key = passing_record(&spec, 1, 1, 100).key();
    let a1_r2_name = passing_record(&spec, 2, 1, 100).file_name();
    let a1_r1_stem = passing_record(&spec, 1, 1, 100).file_name().replace(".json", "");

    // (a) a final record for attempt 1, repetition 2.
    {
        let work = temp_dir("skip-final-record");
        let out_dir = work.join("out");
        let scratch = work.join("scratch");
        fs::create_dir_all(&out_dir).expect("create out dir");
        fs::create_dir_all(&scratch).expect("create scratch dir");
        let planted = out_dir.join(&a1_r2_name);
        fs::write(&planted, b"published").expect("plant final record");
        let remaining = HashMap::from([(key, 1u32)]);
        let cancel = AtomicBool::new(false);
        let mut attempts: Vec<u32> = Vec::new();
        let outcome = run_configs(std::slice::from_ref(&spec), &remaining, &constants, &out_dir, &scratch, &cancel, |s| {
            attempts.push(s.attempt);
            passing_record(s, s.repetition, s.attempt, 100)
        })
        .expect("case a: an occupied final record must not block dispatch");
        assert_eq!(attempts, vec![2, 2], "case a: attempt 1 must be skipped at no cost");
        assert_eq!(outcome.passed.len(), 1, "case a");
        assert_eq!(
            fs::read(&planted).expect("read planted final record"),
            b"published",
            "case a: the planted record must be untouched"
        );
    }

    // (b) a temporary record for attempt 1, repetition 2.
    {
        let work = temp_dir("skip-tmp-record");
        let out_dir = work.join("out");
        let scratch = work.join("scratch");
        fs::create_dir_all(&out_dir).expect("create out dir");
        fs::create_dir_all(&scratch).expect("create scratch dir");
        let planted = out_dir.join(format!("{a1_r2_name}.tmp"));
        fs::write(&planted, b"partial").expect("plant temporary record");
        let remaining = HashMap::from([(key, 1u32)]);
        let cancel = AtomicBool::new(false);
        let mut attempts: Vec<u32> = Vec::new();
        let outcome = run_configs(std::slice::from_ref(&spec), &remaining, &constants, &out_dir, &scratch, &cancel, |s| {
            attempts.push(s.attempt);
            passing_record(s, s.repetition, s.attempt, 100)
        })
        .expect("case b: an occupied temporary record must not block dispatch");
        assert_eq!(attempts, vec![2, 2], "case b: attempt 1 must be skipped at no cost");
        assert_eq!(outcome.passed.len(), 1, "case b");
        assert_eq!(
            fs::read(&planted).expect("read planted temporary record"),
            b"partial",
            "case b: the planted temporary record must be untouched"
        );
    }

    // (c) a dangling symlink standing in for a temporary record.
    {
        let work = temp_dir("skip-dangling-symlink");
        let out_dir = work.join("out");
        let scratch = work.join("scratch");
        fs::create_dir_all(&out_dir).expect("create out dir");
        fs::create_dir_all(&scratch).expect("create scratch dir");
        let planted = out_dir.join(format!("{a1_r2_name}.tmp"));
        let target = work.join("missing");
        std::os::unix::fs::symlink(&target, &planted).expect("plant dangling symlink");
        let remaining = HashMap::from([(key, 1u32)]);
        let cancel = AtomicBool::new(false);
        let mut attempts: Vec<u32> = Vec::new();
        let outcome = run_configs(std::slice::from_ref(&spec), &remaining, &constants, &out_dir, &scratch, &cancel, |s| {
            attempts.push(s.attempt);
            passing_record(s, s.repetition, s.attempt, 100)
        })
        .expect("case c: an occupied dangling symlink must not block dispatch");
        assert_eq!(attempts, vec![2, 2], "case c: attempt 1 must be skipped at no cost");
        assert_eq!(outcome.passed.len(), 1, "case c");
        let meta = fs::symlink_metadata(&planted).expect("stat planted symlink");
        assert!(meta.file_type().is_symlink(), "case c: the planted path must stay a symlink");
        assert_eq!(
            fs::read_link(&planted).expect("read planted symlink target"),
            target,
            "case c: the planted symlink's target must be untouched"
        );
    }

    // (d) a scratch directory for attempt 1, repetition 1.
    {
        let work = temp_dir("skip-scratch-dir");
        let out_dir = work.join("out");
        let scratch = work.join("scratch");
        fs::create_dir_all(&out_dir).expect("create out dir");
        let run_scratch = scratch.join(&a1_r1_stem);
        fs::create_dir_all(&run_scratch).expect("create scratch dir");
        let evidence = run_scratch.join("gen.json");
        fs::write(&evidence, b"evidence").expect("plant scratch evidence");
        let remaining = HashMap::from([(key, 1u32)]);
        let cancel = AtomicBool::new(false);
        let mut attempts: Vec<u32> = Vec::new();
        let outcome = run_configs(std::slice::from_ref(&spec), &remaining, &constants, &out_dir, &scratch, &cancel, |s| {
            attempts.push(s.attempt);
            passing_record(s, s.repetition, s.attempt, 100)
        })
        .expect("case d: an occupied scratch directory must not block dispatch");
        assert_eq!(attempts, vec![2, 2], "case d: attempt 1 must be skipped at no cost");
        assert_eq!(outcome.passed.len(), 1, "case d");
        assert_eq!(
            fs::read(&evidence).expect("read planted scratch evidence"),
            b"evidence",
            "case d: the planted scratch directory's evidence must be untouched"
        );
    }
}

/// AC-20: two occupied attempt numbers in a row are both skipped, and
/// neither costs the attempt budget — a run cut short twice leaves two
/// unpublished attempts behind. The record the dispatched attempt
/// publishes leaves no temporary file of its own.
#[test]
fn two_occupied_attempt_numbers_in_a_row_are_both_skipped_at_no_cost() {
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let work = temp_dir("skip-two-attempts");
    let out_dir = work.join("out");
    let scratch = work.join("scratch");
    fs::create_dir_all(&out_dir).expect("create out dir");
    fs::create_dir_all(&scratch).expect("create scratch dir");
    let spec = throughput_spec();
    let key = passing_record(&spec, 1, 1, 100).key();
    // Attempt 1 left a published record, attempt 2 a temporary one.
    let attempt1 = out_dir.join(passing_record(&spec, 1, 1, 100).file_name());
    fs::write(&attempt1, b"published").expect("plant attempt 1 record");
    let attempt2 = out_dir.join(format!("{}.tmp", passing_record(&spec, 1, 2, 100).file_name()));
    fs::write(&attempt2, b"partial").expect("plant attempt 2 temporary record");

    // A budget of one: a skip that charged it would dispatch nothing.
    let remaining = HashMap::from([(key, 1u32)]);
    let cancel = AtomicBool::new(false);
    let mut attempts: Vec<u32> = Vec::new();
    let outcome = run_configs(
        std::slice::from_ref(&spec),
        &remaining,
        &constants,
        &out_dir,
        &scratch,
        &cancel,
        |s| {
            attempts.push(s.attempt);
            passing_record(s, s.repetition, s.attempt, 100)
        },
    )
    .expect("two occupied attempt numbers must not block dispatch");

    assert_eq!(
        attempts,
        vec![3, 3],
        "attempts 1 and 2 must both be skipped at no cost"
    );
    assert_eq!(outcome.passed.len(), 1, "the third attempt must pass");
    assert_eq!(
        fs::read(&attempt1).expect("read planted attempt 1 record"),
        b"published",
        "the planted record must be untouched"
    );
    assert_eq!(
        fs::read(&attempt2).expect("read planted attempt 2 temporary record"),
        b"partial",
        "the planted temporary record must be untouched"
    );
    for rep in 1..=2u32 {
        let published = passing_record(&spec, rep, 3, 100).file_name();
        assert!(
            out_dir.join(&published).exists(),
            "attempt 3 repetition {rep} must be published as {published}"
        );
        assert!(
            !out_dir.join(format!("{published}.tmp")).exists(),
            "the published record must leave no temporary file of its own"
        );
    }
}

/// AC-21: when an attempt number's occupancy cannot be determined — its
/// scratch directory cannot be inspected — `run_configs` returns the
/// inspection error before dispatching anything.
#[test]
fn an_attempt_that_cannot_be_inspected_is_not_dispatched() {
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 2,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let out_dir = temp_dir("inspect-fail-out");
    let work = temp_dir("inspect-fail-scratch");
    // A regular file in the scratch directory's place makes every
    // occupancy inspection under it fail with `ENOTDIR`, which no
    // privilege bypasses — an unreadable directory would still be
    // inspectable by a `CAP_DAC_OVERRIDE` runner.
    let scratch = work.join("scratch");
    fs::write(&scratch, b"not a directory").expect("plant the scratch file");
    let spec = throughput_spec();
    let cancel = AtomicBool::new(false);
    let mut calls = 0u32;
    let err = run_configs(&[spec], &HashMap::new(), &constants, &out_dir, &scratch, &cancel, |s| {
        calls += 1;
        passing_record(s, s.repetition, s.attempt, 100)
    })
    .expect_err("an uninspectable scratch directory must stop the dispatch");
    assert!(
        err.starts_with("inspect "),
        "the error must name the inspection failure, got {err}"
    );
    assert_eq!(calls, 0, "nothing must be dispatched when occupancy cannot be determined");
}

/// AC-20: a fresh (non-`--resume`) run skips an attempt number left
/// unpublished by a run cut short before its first record was written,
/// at no cost to the attempt budget.
#[test]
fn a_fresh_run_skips_an_attempt_left_unpublished() {
    let dir = temp_dir("fresh-run-skips-unpublished");
    let out_dir = dir.join("out");
    fs::create_dir_all(&out_dir).expect("create out dir");
    let host = stub_host(&dir, SyscallMethod::None);

    let seed_spec = throughput_spec();
    let name = passing_record(&seed_spec, 1, 1, 100).file_name();
    let tmp_path = out_dir.join(format!("{name}.tmp"));
    fs::write(&tmp_path, b"partial").expect("plant temporary record");
    let stem = name.replace(".json", "");
    let run_scratch = host.scratch_dir.join(&stem);
    fs::create_dir_all(&run_scratch).expect("create scratch dir");
    let evidence = run_scratch.join("gen.json");
    fs::write(&evidence, b"evidence").expect("plant scratch evidence");

    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir,
        resume: false,
    };
    let mut dispatched: Vec<(u32, u32)> = Vec::new();
    run_matrix_with(&spec, &host, |s| {
        if s.kind == RunKind::Throughput {
            dispatched.push((s.attempt, s.repetition));
        }
        passing_record(s, s.repetition, s.attempt, 100)
    })
    .expect("run_matrix_with");

    assert_eq!(
        dispatched,
        vec![(2, 1), (2, 2)],
        "attempt 1 must be skipped at no cost to the smoke budget of one"
    );
    assert_eq!(
        fs::read(&tmp_path).expect("read seeded temporary record"),
        b"partial",
        "the seeded temporary record must be untouched"
    );
    assert_eq!(
        fs::read(&evidence).expect("read seeded scratch evidence"),
        b"evidence",
        "the seeded scratch evidence must be untouched"
    );
}

/// AC-20: a `--resume` run skips an attempt number left unpublished by a
/// run cut short before its first record was written, at no cost to the
/// attempt budget the completed attempt did not already spend.
#[test]
fn a_resume_skips_an_attempt_left_unpublished() {
    let dir = temp_dir("resume-skips-unpublished");
    let out_dir = dir.join("out");
    fs::create_dir_all(&out_dir).expect("create out dir");
    let host = stub_host(&dir, SyscallMethod::None);

    for rep in 1..=5u32 {
        let seed_spec = RunSpec {
            profile: Profile::Recorded,
            server: ServerKind::Uring,
            workers: 1,
            qos: Some(1),
            kind: RunKind::Throughput,
            offered_rate: None,
            repetition: rep,
            attempt: 1,
        };
        let rec = failing_record(&seed_spec);
        write_record(&out_dir, &rec);
    }
    let attempt2_rep1 = RunSpec {
        profile: Profile::Recorded,
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
        repetition: 1,
        attempt: 2,
    };
    let name = passing_record(&attempt2_rep1, 1, 2, 100).file_name();
    let tmp_path = out_dir.join(format!("{name}.tmp"));
    fs::write(&tmp_path, b"partial").expect("plant temporary record");
    let stem = name.replace(".json", "");
    let run_scratch = host.scratch_dir.join(&stem);
    fs::create_dir_all(&run_scratch).expect("create scratch dir");
    let evidence = run_scratch.join("gen.json");
    fs::write(&evidence, b"evidence").expect("plant scratch evidence");

    let spec = MatrixSpec {
        profile: Profile::Recorded,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir,
        resume: true,
    };
    let mut dispatched: Vec<(u32, u32)> = Vec::new();
    let outcome = run_matrix_with(&spec, &host, |s| {
        if s.kind == RunKind::Throughput {
            dispatched.push((s.attempt, s.repetition));
            failing_record(s)
        } else {
            passing_record(s, s.repetition, s.attempt, 100)
        }
    })
    .expect("run_matrix_with");

    assert_eq!(
        dispatched,
        (3..=4u32)
            .flat_map(|a| (1..=5u32).map(move |r| (a, r)))
            .collect::<Vec<_>>(),
        "attempt 2 must be skipped at no cost: one completed attempt leaves two"
    );
    let key = ConfigKey {
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Throughput,
        offered_rate: None,
    };
    assert!(
        outcome.failed.contains(&key),
        "failed={:?}",
        outcome.failed
    );
    assert_eq!(
        fs::read(&tmp_path).expect("read seeded temporary record"),
        b"partial",
        "the seeded temporary record must be untouched"
    );
    assert_eq!(
        fs::read(&evidence).expect("read seeded scratch evidence"),
        b"evidence",
        "the seeded scratch evidence must be untouched"
    );
}

/// The on-disk form of a counted record (PLAN.md ## Data Model):
/// `"kind": "counted"` at the current schema, read back as
/// `RunKind::Counted`. The records the matrix writes are what `--resume`
/// and `bench doc` read, so the label is a persisted contract.
#[test]
fn a_counted_record_round_trips_through_its_json_form() {
    let dir = temp_dir("counted-json");
    let rec = make_record(
        Profile::Recorded,
        ServerKind::Uring,
        1,
        Some(1),
        RunKind::Counted,
        Some(300_000),
        1,
        1,
        true,
        "abc1234",
    );
    write_record(&dir, &rec);

    let text = fs::read_to_string(dir.join("uring-w1-q1-counted-at300000-a1-r1.json"))
        .expect("read the counted record");
    assert!(
        text.contains("\"kind\": \"counted\""),
        "the counted kind serializes lowercase, got {text}"
    );
    assert!(
        text.contains(&format!("\"schema\": {SCHEMA_VERSION}")),
        "the record carries the current schema, got {text}"
    );

    let records = read_records(&dir).expect("read_records");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].kind, RunKind::Counted);
    assert_eq!(records[0].offered_rate, Some(300_000));
}

/// A record written before this field existed has no `"cancelled"` key
/// at all; `read_records` must still read it back as not cancelled.
#[test]
fn a_record_without_a_cancelled_field_reads_as_not_cancelled() {
    let dir = temp_dir("no-cancelled-field");
    let rec = make_record(
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
    write_record(&dir, &rec);

    let path = dir.join(rec.file_name());
    let text = fs::read_to_string(&path).expect("read the record");
    let mut value: serde_json::Value = serde_json::from_str(&text).expect("parse the record");
    value
        .as_object_mut()
        .expect("record is a JSON object")
        .remove("cancelled");
    let file = fs::File::create(&path).expect("recreate the record file");
    serde_json::to_writer_pretty(&file, &value).expect("write the record back without the key");

    let records = read_records(&dir).expect("read_records");
    assert_eq!(
        serde_json::to_value(&records[0]).expect("serialize the record")["cancelled"],
        serde_json::Value::Bool(false),
        "a record with no cancelled key must read back as not cancelled"
    );
}

/// A copied or renamed record must not count as a repetition that never
/// ran: `read_records` refuses a `*.json` whose file name is not the
/// name its own `file_name()` computes (AC-4).
#[test]
fn read_records_refuses_a_file_whose_name_is_not_its_records_name() {
    let dir = temp_dir("mismatched-file-name");
    let rec = make_record(
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
    write_record(&dir, &rec);
    fs::copy(dir.join(rec.file_name()), dir.join("copy.json")).expect("copy record");

    let err = read_records(&dir).expect_err("a renamed copy must not read back");
    assert!(
        err.contains("copy.json"),
        "error must name the offending file, got {err}"
    );
    assert!(
        err.contains(&rec.file_name()),
        "error must name the expected file name, got {err}"
    );
}

/// The key of the uring 1-worker QoS 1 latency configuration at the
/// higher declared rate, as `ConfigKey`'s `Debug` prints it.
const W1_Q1_HIGHER_LATENCY: &str =
    "ConfigKey { server: Uring, workers: 1, qos: Some(1), kind: Latency, offered_rate: Some(300000) }";

/// A recorded open-loop configuration whose failed repetitions hit only
/// capacity rows is published "not sustained at R" and the session keeps
/// going: the run itself is a result, not a harness failure.
#[test]
fn a_recorded_configuration_failing_only_capacity_rows_is_published_not_sustained() {
    let dir = temp_dir("matrix-not-sustained");
    let spec = MatrixSpec {
        profile: Profile::Recorded,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir: dir.join("out"),
        resume: false,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let mut calls: Vec<(RunKind, Option<u64>)> = Vec::new();
    let outcome = run_matrix_with(&spec, &host, |s| {
        calls.push((s.kind, s.offered_rate));
        let mut rec = passing_record(s, s.repetition, s.attempt, 100);
        if s.kind == RunKind::Latency && s.offered_rate == Some(300_000) && s.repetition == 1 {
            rec.generator_errors.missed_slots = 5;
            rec.valid = false;
            rec.invalid_reasons = vec!["missed slots 5".to_string()];
        }
        rec
    })
    .expect("run_matrix_with");

    let key = ConfigKey {
        server: ServerKind::Uring,
        workers: 1,
        qos: Some(1),
        kind: RunKind::Latency,
        offered_rate: Some(300_000),
    };
    assert_eq!(
        outcome.not_sustained,
        vec![(key, vec![CapacityRow::MissedSlots])]
    );
    assert_eq!(outcome.stopped, None);
    assert!(
        outcome.failed.is_empty(),
        "a not-sustained configuration is a published result, failed={:?}",
        outcome.failed
    );
    assert_eq!(
        calls
            .iter()
            .filter(|(kind, rate)| *kind == RunKind::Latency && *rate == Some(300_000))
            .count(),
        15,
        "the failed configuration ran three attempts of five repetitions"
    );
    assert!(
        calls.iter().any(|(kind, _)| *kind == RunKind::Connections),
        "the session continues past a not-sustained configuration"
    );
    assert_eq!(
        outcome.stderr_lines(Profile::Recorded),
        vec![format!(
            "not sustained at 300000 msgs/s: {W1_Q1_HIGHER_LATENCY}: missed slots"
        )]
    );
    assert_eq!(outcome.exit_code(), 0);
}

/// A recorded open-loop configuration that fails a repetition on no
/// capacity row is exhausted: the whole session stops with the cause
/// named and exit status 2.
#[test]
fn a_recorded_configuration_failing_a_non_capacity_row_stops_the_session() {
    let dir = temp_dir("matrix-session-stop");
    let spec = MatrixSpec {
        profile: Profile::Recorded,
        servers: vec![ServerKind::Uring],
        workers: vec![1, 4],
        qos: vec![1],
        out_dir: dir.join("out"),
        resume: false,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let mut calls: Vec<(u32, RunKind)> = Vec::new();
    let outcome = run_matrix_with(&spec, &host, |s| {
        calls.push((s.workers, s.kind));
        let mut rec = passing_record(s, s.repetition, s.attempt, 100);
        if s.kind == RunKind::Latency && s.offered_rate == Some(300_000) && s.repetition == 1 {
            rec.valid = false;
            rec.invalid_reasons = vec!["test-marker".to_string()];
        }
        rec
    })
    .expect("run_matrix_with");

    let cause = "attempt 1: repetition 1 is invalid on no capacity row: test-marker";
    assert_eq!(
        outcome.stopped,
        Some(format!("{W1_Q1_HIGHER_LATENCY}: {cause}"))
    );
    assert!(
        !calls.iter().any(|(workers, _)| *workers == 4),
        "nothing is dispatched after the session stopped, calls={calls:?}"
    );
    assert!(
        !calls.iter().any(|(_, kind)| *kind == RunKind::Connections),
        "nothing is dispatched after the session stopped, calls={calls:?}"
    );
    assert_eq!(
        outcome.stderr_lines(Profile::Recorded),
        vec![format!("session stopped: {W1_Q1_HIGHER_LATENCY}: {cause}")]
    );
    assert_eq!(outcome.exit_code(), 2);
}

/// A smoke failure is a harness-health failure: the smoke profile keeps
/// the pass gate, so a failed configuration is reported and the session
/// exits 2.
#[test]
fn a_smoke_open_loop_failure_is_a_harness_health_failure() {
    let dir = temp_dir("matrix-smoke-health");
    let spec = MatrixSpec {
        profile: Profile::Smoke,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir: dir.join("out"),
        resume: false,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let outcome = run_matrix_with(&spec, &host, |s| {
        let mut rec = passing_record(s, s.repetition, s.attempt, 100);
        if s.kind == RunKind::Latency && s.offered_rate == Some(60_000) && s.repetition == 1 {
            rec.generator_errors.missed_slots = 5;
            rec.valid = false;
            rec.invalid_reasons = vec!["missed slots 5".to_string()];
        }
        rec
    })
    .expect("run_matrix_with");

    assert_eq!(
        outcome.failed,
        vec![ConfigKey {
            server: ServerKind::Uring,
            workers: 1,
            qos: Some(1),
            kind: RunKind::Latency,
            offered_rate: Some(60_000),
        }]
    );
    assert!(outcome.not_sustained.is_empty());
    assert_eq!(outcome.stopped, None);
    assert_eq!(
        outcome.stderr_lines(Profile::Smoke),
        vec!["smoke harness-health failure: ConfigKey { server: Uring, workers: 1, qos: Some(1), kind: Latency, offered_rate: Some(60000) }".to_string()]
    );
    assert_eq!(outcome.exit_code(), 2);
}

/// One stage can hold both outcomes: a configuration that failed only on
/// capacity rows is published not sustained, and the exhausted one beside
/// it stops the session.
#[test]
fn a_stop_keeps_the_not_sustained_outcome_of_the_same_stage() {
    let dir = temp_dir("matrix-stop-keeps-not-sustained");
    let spec = MatrixSpec {
        profile: Profile::Recorded,
        servers: vec![ServerKind::Uring, ServerKind::Tokio],
        workers: vec![1],
        qos: vec![1],
        out_dir: dir.join("out"),
        resume: false,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let outcome = run_matrix_with(&spec, &host, |s| {
        let mut rec = passing_record(s, s.repetition, s.attempt, 100);
        if s.kind == RunKind::Latency && s.offered_rate == Some(300_000) && s.repetition == 1 {
            rec.valid = false;
            if s.server == ServerKind::Uring {
                rec.generator_errors.missed_slots = 5;
                rec.invalid_reasons = vec!["missed slots 5".to_string()];
            } else {
                rec.invalid_reasons = vec!["test-marker".to_string()];
            }
        }
        rec
    })
    .expect("run_matrix_with");

    assert_eq!(
        outcome.not_sustained,
        vec![(
            ConfigKey {
                server: ServerKind::Uring,
                workers: 1,
                qos: Some(1),
                kind: RunKind::Latency,
                offered_rate: Some(300_000),
            },
            vec![CapacityRow::MissedSlots]
        )]
    );
    assert_eq!(
        outcome.stopped,
        Some("ConfigKey { server: Tokio, workers: 1, qos: Some(1), kind: Latency, offered_rate: Some(300000) }: attempt 1: repetition 1 is invalid on no capacity row: test-marker".to_string())
    );
    assert!(
        outcome.failed.is_empty(),
        "the exhausted configuration is named by the stop, failed={:?}",
        outcome.failed
    );
    let lines = outcome.stderr_lines(Profile::Recorded);
    assert_eq!(lines.len(), 2, "lines={lines:?}");
    assert_eq!(
        lines[0],
        format!("not sustained at 300000 msgs/s: {W1_Q1_HIGHER_LATENCY}: missed slots")
    );
    assert!(lines[1].starts_with("session stopped: "), "lines={lines:?}");
    assert_eq!(outcome.exit_code(), 2);
}

/// A session can end with all three outcomes at once: a configuration
/// published not sustained on several capacity rows, a failed one from an
/// earlier stage, and the stop that ended it. `stderr_lines` prints them in
/// that order and names every capacity row of the not-sustained one.
#[test]
fn stderr_lines_name_the_not_sustained_rows_then_the_failures_then_the_stop() {
    let dir = temp_dir("matrix-stderr-line-order");
    let spec = MatrixSpec {
        profile: Profile::Recorded,
        servers: vec![ServerKind::Uring, ServerKind::Tokio],
        workers: vec![1],
        qos: vec![1],
        out_dir: dir.join("out"),
        resume: false,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let outcome = run_matrix_with(&spec, &host, |s| {
        let mut rec = passing_record(s, s.repetition, s.attempt, 100);
        if s.server == ServerKind::Tokio && s.kind == RunKind::Throughput {
            rec.valid = false;
            rec.invalid_reasons = vec!["test-marker".to_string()];
        }
        if s.kind == RunKind::Latency && s.offered_rate == Some(300_000) && s.repetition == 1 {
            rec.valid = false;
            if s.server == ServerKind::Uring {
                rec.counts.unresolved = 3;
                rec.generator_errors.missed_slots = 5;
                rec.invalid_reasons =
                    vec!["unresolved 3".to_string(), "missed slots 5".to_string()];
            } else {
                rec.invalid_reasons = vec!["test-marker".to_string()];
            }
        }
        rec
    })
    .expect("run_matrix_with");

    assert_eq!(
        outcome.not_sustained,
        vec![(
            ConfigKey {
                server: ServerKind::Uring,
                workers: 1,
                qos: Some(1),
                kind: RunKind::Latency,
                offered_rate: Some(300_000),
            },
            vec![CapacityRow::Unresolved, CapacityRow::MissedSlots]
        )]
    );
    assert_eq!(
        outcome.failed,
        vec![ConfigKey {
            server: ServerKind::Tokio,
            workers: 1,
            qos: Some(1),
            kind: RunKind::Throughput,
            offered_rate: None,
        }]
    );
    assert_eq!(
        outcome.stderr_lines(Profile::Recorded),
        vec![
            format!(
                "not sustained at 300000 msgs/s: {W1_Q1_HIGHER_LATENCY}: unresolved, missed slots"
            ),
            "failed: ConfigKey { server: Tokio, workers: 1, qos: Some(1), kind: Throughput, offered_rate: None }".to_string(),
            "session stopped: ConfigKey { server: Tokio, workers: 1, qos: Some(1), kind: Latency, offered_rate: Some(300000) }: attempt 1: repetition 1 is invalid on no capacity row: test-marker".to_string(),
        ]
    );
    assert_eq!(outcome.exit_code(), 2);
}

/// A row group the calibration never covered has no declared rates, so the
/// session names it instead of pacing the open-loop stage at a guess.
#[test]
fn a_row_group_without_declared_rates_is_refused_by_name() {
    let dir = temp_dir("matrix-undeclared-row-group");
    let spec = MatrixSpec {
        profile: Profile::Recorded,
        servers: vec![ServerKind::Uring],
        workers: vec![2],
        qos: vec![1],
        out_dir: dir.join("out"),
        resume: false,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let mut kinds: Vec<RunKind> = Vec::new();
    let err = run_matrix_with(&spec, &host, |s| {
        kinds.push(s.kind);
        passing_record(s, s.repetition, s.attempt, 100)
    })
    .expect_err("a row group without declared rates is refused");

    assert_eq!(err, "no declared rates for workers 2 QoS 1");
    assert!(
        !kinds.contains(&RunKind::Latency),
        "no open-loop run is dispatched without declared rates, kinds={kinds:?}"
    );
}

/// A resumed session whose open-loop stage holds a configuration whose
/// budget is already spent stops before it dispatches a single run of
/// that stage.
#[test]
fn a_resumed_session_stops_before_dispatching_beside_an_exhausted_configuration() {
    let dir = temp_dir("matrix-resume-stop");
    let out_dir = dir.join("out");
    fs::create_dir_all(&out_dir).expect("create out dir");
    for attempt in 1..=3u32 {
        for rep in 1..=5u32 {
            let mut rec = make_record(
                Profile::Recorded,
                ServerKind::Uring,
                1,
                Some(1),
                RunKind::Latency,
                Some(300_000),
                attempt,
                rep,
                false,
                "abc1234",
            );
            rec.generator_errors.stalled = 3;
            rec.invalid_reasons = reconcile(&rec);
            rec.valid = rec.invalid_reasons.is_empty();
            assert!(
                !rec.valid,
                "fixture must be a failed attempt, reasons {:?}",
                rec.invalid_reasons
            );
            write_record(&out_dir, &rec);
        }
    }

    let spec = MatrixSpec {
        profile: Profile::Recorded,
        servers: vec![ServerKind::Uring],
        workers: vec![1],
        qos: vec![1],
        out_dir,
        resume: true,
    };
    let host = stub_host(&dir, SyscallMethod::None);
    let mut kinds: Vec<RunKind> = Vec::new();
    let outcome = run_matrix_with(&spec, &host, |s| {
        kinds.push(s.kind);
        passing_record(s, s.repetition, s.attempt, 100)
    })
    .expect("run_matrix_with");

    assert!(
        !kinds.contains(&RunKind::Latency),
        "the exhausted stage dispatches nothing, kinds={kinds:?}"
    );
    assert!(
        !kinds.contains(&RunKind::Connections),
        "the exhausted stage dispatches nothing, kinds={kinds:?}"
    );
    let stopped = outcome.stopped.clone().expect("the session stopped");
    assert!(
        stopped.starts_with(&format!(
            "{W1_Q1_HIGHER_LATENCY}: attempt 1: repetition 1 is invalid on no capacity row: "
        )),
        "stopped={stopped}"
    );
    assert!(stopped.contains("stalled 3"), "stopped={stopped}");
    assert_eq!(outcome.exit_code(), 2);
}

/// The stop check is offered every configuration whose budget is spent,
/// and each one exactly once: the open-loop stage decides one outcome per
/// configuration on this seam, so a key offered twice would publish the
/// same verdict twice. `Ok(None)` keeps the session going, so both spent
/// configurations are reported in `failed`.
#[test]
fn the_stop_check_sees_every_spent_configuration_once() {
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 1,
        drain_grace_secs: 2,
        max_config_attempts: 2,
        enforce_spread: false,
    };
    let out_dir = temp_dir("stop-seen-once");
    let scratch = temp_dir("stop-seen-once-scratch");
    let cancel = AtomicBool::new(false);
    let mut specs = [throughput_spec(), throughput_spec()];
    specs[1].workers = 4;
    let mut calls: Vec<(u32, u32)> = Vec::new();
    let mut offered: Vec<ConfigKey> = Vec::new();
    let outcome = run_configs_with_stop(
        &specs,
        &HashMap::new(),
        &constants,
        &out_dir,
        &scratch,
        &cancel,
        |s| {
            calls.push((s.workers, s.attempt));
            let mut rec = passing_record(s, s.repetition, s.attempt, 100);
            rec.valid = false;
            rec.invalid_reasons.push("test-marker".to_string());
            rec
        },
        |key| {
            offered.push(key);
            Ok(None)
        },
    )
    .expect("run_configs_with_stop");

    assert_eq!(offered, vec![throughput_key(1), throughput_key(4)]);
    assert_eq!(outcome.failed, vec![throughput_key(1), throughput_key(4)]);
    assert!(outcome.passed.is_empty(), "passed={:?}", outcome.passed);
    assert_eq!(outcome.stopped, None);
    assert_eq!(
        calls,
        vec![(1, 1), (4, 1), (1, 2), (4, 2)],
        "both configurations spend their two attempts"
    );
}

/// A stop cause returns at once: the cause is reported in `stopped`, every
/// pending configuration stays in `failed`, and nothing more is
/// dispatched — not even the configuration beside it that still has
/// attempts left (AC-27).
#[test]
fn a_stop_cause_dispatches_nothing_further() {
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 1,
        drain_grace_secs: 2,
        max_config_attempts: 3,
        enforce_spread: false,
    };
    let out_dir = temp_dir("stop-cause");
    let scratch = temp_dir("stop-cause-scratch");
    let cancel = AtomicBool::new(false);
    let mut specs = [throughput_spec(), throughput_spec()];
    specs[1].workers = 4;
    // The 1-worker configuration has one attempt left, the 4-worker one
    // keeps its full budget: the stop must cut the round the first has
    // spent, with attempts still unspent beside it.
    let remaining: HashMap<ConfigKey, u32> =
        [(throughput_key(1), 1), (throughput_key(4), 3)].into_iter().collect();
    let mut calls: Vec<(u32, u32)> = Vec::new();
    let mut offered: Vec<ConfigKey> = Vec::new();
    let outcome = run_configs_with_stop(
        &specs,
        &remaining,
        &constants,
        &out_dir,
        &scratch,
        &cancel,
        |s| {
            calls.push((s.workers, s.attempt));
            let mut rec = passing_record(s, s.repetition, s.attempt, 100);
            rec.valid = false;
            rec.invalid_reasons.push("test-marker".to_string());
            rec
        },
        |key| {
            offered.push(key);
            Ok(Some(format!("{key:?}: spent")))
        },
    )
    .expect("run_configs_with_stop");

    assert_eq!(offered, vec![throughput_key(1)]);
    assert_eq!(
        outcome.stopped,
        Some(format!("{:?}: spent", throughput_key(1)))
    );
    assert_eq!(outcome.failed, vec![throughput_key(1), throughput_key(4)]);
    assert!(outcome.passed.is_empty(), "passed={:?}", outcome.passed);
    assert_eq!(
        calls,
        vec![(1, 1), (4, 1)],
        "the stop cut the session before the second round"
    );
}

/// A stop check that fails is a harness failure, not a verdict: its error
/// propagates instead of being read as "no stop".
#[test]
fn a_failing_stop_check_propagates_its_error() {
    let constants = ProtocolConstants {
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        repetitions: 1,
        drain_grace_secs: 2,
        max_config_attempts: 1,
        enforce_spread: false,
    };
    let out_dir = temp_dir("stop-error");
    let scratch = temp_dir("stop-error-scratch");
    let cancel = AtomicBool::new(false);
    let err = run_configs_with_stop(
        &[throughput_spec()],
        &HashMap::new(),
        &constants,
        &out_dir,
        &scratch,
        &cancel,
        |s| {
            let mut rec = passing_record(s, s.repetition, s.attempt, 100);
            rec.valid = false;
            rec.invalid_reasons.push("test-marker".to_string());
            rec
        },
        |_| Err("read records: no such file".to_string()),
    )
    .expect_err("a failing stop check must be reported");
    assert_eq!(err, "read records: no such file");
}

/// A record at a rate that is not a declared rate of its row group
/// measured another experiment: `--resume` refuses it.
#[test]
fn resume_refuses_a_record_at_a_rate_that_is_not_declared() {
    let rec = make_record(
        Profile::Recorded,
        ServerKind::Uring,
        1,
        Some(1),
        RunKind::Latency,
        Some(299_999),
        1,
        1,
        true,
        "abc1234",
    );
    let err = check_resume(
        std::slice::from_ref(&rec),
        Profile::Recorded,
        SyscallMethod::None,
        &rec.env,
        &rec.cpu_sets,
    )
    .expect_err("an undeclared rate must be refused");
    assert_eq!(
        err,
        "uring-w1-q1-latency-at299999-a1-r1.json: offered_rate 299999 is not a declared rate of workers 1 QoS 1; a changed declared rate needs a fresh dataset"
    );

    let mut without_rate = rec.clone();
    without_rate.offered_rate = None;
    let err = check_resume(
        std::slice::from_ref(&without_rate),
        Profile::Recorded,
        SyscallMethod::None,
        &without_rate.env,
        &without_rate.cpu_sets,
    )
    .expect_err("a record without an offered rate must be refused");
    assert_eq!(
        err,
        "uring-w1-q1-latency-a1-r1.json: offered_rate none is not a declared rate of workers 1 QoS 1; a changed declared rate needs a fresh dataset"
    );

    let mut declared = rec.clone();
    declared.offered_rate = Some(300_000);
    declared.invalid_reasons = reconcile(&declared);
    declared.valid = declared.invalid_reasons.is_empty();
    check_resume(
        &[declared],
        Profile::Recorded,
        SyscallMethod::None,
        &rec.env,
        &rec.cpu_sets,
    )
    .expect("a record at a declared rate is accepted");
}
