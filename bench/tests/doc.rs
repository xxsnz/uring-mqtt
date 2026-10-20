//! Public-seam tests for `bench::doc` (task 12 of the benchmark-harness
//! feature): every refusal from `DocError`, the median/p99/cpu rows, the
//! sign-off checklist, and the smoke header (AC-47, AC-48, AC-49).

// Test fixtures are constructed to match `analysis::derive` exactly;
// the matching `f64` conversions lose precision on the same scale the
// reconcile spec already accepts.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use bench::doc::{render, DocError};
use bench::gen::slot_counts;
use bench::matrix::{servers_for, RECORDED_QOS, RECORDED_SERVERS, RECORDED_WORKERS};
use bench::record::{Timeline, Window};
use bench::run_record::{
    Build, ConfigKey, CounterExit, Counts, CpuSets, CpuUsage, Env, GeneratorErrors,
    LatencySummary, Profile, RunKind, RunRecord, ServerKind, Syscalls, SyscallMethod, SCHEMA_VERSION,
};

const WARMUP_SECS: u64 = 10;
const WINDOW_SECS: u64 = 60;
const REPETITIONS: u32 = 5;
const CONNECTIONS: u32 = 100;
const QOS1_WINDOW: u32 = 32;
const PAYLOAD_BYTES: u32 = 64;
const DRAIN_GRACE_SECS: u64 = 5;
const ONE_SECOND_NS: u64 = 1_000_000_000;
const ORIGIN_NS: u64 = 1_700_000_000_000_000_000;
const WINDOW_START_NS: u64 = ORIGIN_NS + WARMUP_SECS * ONE_SECOND_NS;
const WINDOW_END_NS: u64 = WINDOW_START_NS + WINDOW_SECS * ONE_SECOND_NS;

const PERF_RAW: &str = "6000,,raw_syscalls:sys_enter,60000000000,100.00,,\n";
/// `strace -c` summary whose `total` row names the same call count as
/// [`PERF_RAW`], so swapping a record's method leaves `recompute` happy.
const STRACE_RAW: &str = "100.00    0.001000           0      6000           total\n";
const PERF_COUNT: u64 = 6000;
const THROUGHPUT_RATES: [u64; 5] = [100, 101, 99, 102, 100];
const LAT_50_HIST: &[(u64, u64)] = &[(40_000, 3000)];
const LAT_80_HIST: &[(u64, u64)] = &[(80_000, 4800)];
/// `reconcile` row 12 for `generator_errors.stalled = 3`. `render` calls
/// `recompute`, which re-derives `invalid_reasons` and compares string for
/// string, so a fixture that has a stalled count of three must store this text.
const STALLED_3_REASON: &str = "stalled 3 (publishes whose write did not complete or whose PUBACK never arrived, plus connection tasks still running at the hard deadline)";

fn window_obj() -> Window {
    Window {
        start_ns: WINDOW_START_NS,
        end_ns: WINDOW_END_NS,
    }
}

fn env() -> Env {
    Env {
        kernel: "6.18.33.2-microsoft-standard-WSL2".to_string(),
        cpu_model: "Intel(R) Core(TM) i7-14650HX".to_string(),
        host: "test-host".to_string(),
        git_sha: "b219eb1".to_string(),
        git_dirty: false,
        tools: vec![
            ("/usr/bin/perf".to_string(), "perf version 6.8.12".to_string()),
            (
                "/usr/sbin/mosquitto".to_string(),
                "mosquitto version 2.0.18".to_string(),
            ),
        ],
    }
}

fn cpu_sets() -> CpuSets {
    CpuSets {
        server: "0,2,4,6".to_string(),
        generator: "8-17".to_string(),
    }
}

fn cpu_usage() -> CpuUsage {
    CpuUsage {
        clk_tck: 100,
        sampled_ns: WINDOW_SECS * ONE_SECOND_NS,
        server_ticks: 22_800,
        generator_ticks: 24_000,
    }
}

fn base_counts() -> Counts {
    Counts {
        scheduled: None,
        window_scheduled: None,
        offered: 0,
        acked: None,
        delivered: 0,
        dropped: Some(0),
        unresolved: 0,
        duplicates: 0,
        foreign: 0,
        latency_over_range: 0,
        clock_anomalies: 0,
    }
}

fn base_syscalls() -> Syscalls {
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
}

fn base_generator_errors() -> GeneratorErrors {
    GeneratorErrors {
        connect: 0,
        protocol: 0,
        missed_slots: 0,
        stalled: 0,
        lateness_over_range: 0,
    }
}

fn empty_timeline() -> Timeline {
    Timeline {
        origin_sec: ORIGIN_NS / ONE_SECOND_NS,
        counts: Vec::new(),
    }
}

/// Throughput record: a uniform `rate` per second across the 60 s window.
#[allow(clippy::cast_precision_loss)]
fn throughput_record(server: ServerKind, workers: u32, qos: u8, rep: u32) -> RunRecord {
    let rate = THROUGHPUT_RATES[usize::try_from(rep - 1).expect("rep in 1..=5")];
    let delivered = rate * WINDOW_SECS;
    let timeline = Timeline {
        origin_sec: WINDOW_START_NS / ONE_SECOND_NS,
        counts: vec![rate; usize::try_from(WINDOW_SECS).expect("fits in usize")],
    };
    let driver = match server {
        ServerKind::Uring => Some(bench::run_record::DriverFds {
            io_uring_fds: workers,
            eventpoll_fds: 0,
        }),
        ServerKind::Tokio => Some(bench::run_record::DriverFds {
            io_uring_fds: 0,
            eventpoll_fds: 1,
        }),
        ServerKind::Mosquitto => None,
    };
    RunRecord {
        schema: SCHEMA_VERSION,
        profile: Profile::Recorded,
        server,
        workers,
        qos: Some(qos),
        kind: RunKind::Throughput,
        rate_fraction_pct: None,
        offered_rate: None,
        repetition: rep,
        attempt: 1,
        connections: CONNECTIONS,
        payload_bytes: PAYLOAD_BYTES,
        qos1_window: QOS1_WINDOW,
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        drain_grace_secs: DRAIN_GRACE_SECS,
        build: Build::Release,
        origin_ns: ORIGIN_NS,
        window: window_obj(),
        timeline,
        cycles_in_window: None,
        counts: Counts {
            offered: delivered,
            acked: Some(delivered),
            delivered,
            ..base_counts()
        },
        throughput_msgs_per_sec: Some(delivered as f64 / WINDOW_SECS as f64),
        last10s_msgs_per_sec: Some(delivered as f64 / WINDOW_SECS as f64),
        latency_ns: None,
        lateness: Vec::new(),
        lateness_p99_ns: None,
        last_write_start_ns: None,
        connections_per_sec: None,
        generator_errors: base_generator_errors(),
        syscalls: base_syscalls(),
        driver,
        cpu_sets: cpu_sets(),
        env: env(),
        cpu: Some(cpu_usage()),
        host_cpu_pressure: None,
        started_at_unix: 1_791_273_600,
        harness_errors: Vec::new(),
        valid: true,
        invalid_reasons: Vec::new(),
        cancelled: false,
    }
}

/// Latency record at `pct` (50 or 80) for QoS 1, `connections` evenly split.
#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
fn latency_record(
    server: ServerKind,
    workers: u32,
    qos: u8,
    pct: u32,
    rep: u32,
) -> RunRecord {
    let (hist, total_window) = if pct == 50 {
        (LAT_50_HIST.to_vec(), 3000u64)
    } else {
        (LAT_80_HIST.to_vec(), 4800u64)
    };
    let rate = if pct == 50 { 50u64 } else { 80u64 };
    let (scheduled, window_scheduled) = slot_counts(rate, ORIGIN_NS, window_obj());
    let total_scheduled = scheduled;
    // Timeline = uniform `rate` per second across warmup + window seconds.
    let timeline_counts = vec![rate; usize::try_from(WARMUP_SECS + WINDOW_SECS).expect("fits")];
    let delivered = total_scheduled;
    let syscalls = base_syscalls();
    let driver = match server {
        ServerKind::Uring => Some(bench::run_record::DriverFds {
            io_uring_fds: workers,
            eventpoll_fds: 0,
        }),
        ServerKind::Tokio => Some(bench::run_record::DriverFds {
            io_uring_fds: 0,
            eventpoll_fds: 1,
        }),
        ServerKind::Mosquitto => None,
    };
    RunRecord {
        schema: SCHEMA_VERSION,
        profile: Profile::Recorded,
        server,
        workers,
        qos: Some(qos),
        kind: RunKind::Latency,
        rate_fraction_pct: Some(pct),
        offered_rate: Some(rate),
        repetition: rep,
        attempt: 1,
        connections: CONNECTIONS,
        payload_bytes: PAYLOAD_BYTES,
        qos1_window: QOS1_WINDOW,
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        drain_grace_secs: DRAIN_GRACE_SECS,
        build: Build::Release,
        origin_ns: ORIGIN_NS,
        window: window_obj(),
        timeline: Timeline {
            origin_sec: ORIGIN_NS / ONE_SECOND_NS,
            counts: timeline_counts,
        },
        cycles_in_window: None,
        counts: Counts {
            scheduled: Some(scheduled),
            window_scheduled: Some(window_scheduled),
            offered: delivered,
            acked: Some(delivered),
            delivered,
            ..base_counts()
        },
        throughput_msgs_per_sec: None,
        last10s_msgs_per_sec: None,
        latency_ns: Some(LatencySummary {
            p50: hist[0].0,
            p99: hist[0].0,
            max: hist[0].0,
            histogram: hist,
        }),
        lateness: vec![(1000, total_window)],
        lateness_p99_ns: Some(1000),
        last_write_start_ns: Some(WINDOW_END_NS - 1),
        connections_per_sec: None,
        generator_errors: base_generator_errors(),
        syscalls,
        driver,
        cpu_sets: cpu_sets(),
        env: env(),
        cpu: Some(cpu_usage()),
        host_cpu_pressure: None,
        started_at_unix: 1_791_273_600,
        harness_errors: Vec::new(),
        valid: true,
        invalid_reasons: Vec::new(),
        cancelled: false,
    }
}

/// Counted run (§9.1): paced like the 80 % latency record, but under its
/// own `RunKind::Counted` with the syscall-counter evidence the 80 %
/// latency record used to carry.
fn counted_record(server: ServerKind, workers: u32, qos: u8, rep: u32) -> RunRecord {
    let mut rec = latency_record(server, workers, qos, 80, rep);
    rec.kind = RunKind::Counted;
    rec.syscalls = Syscalls {
        method: SyscallMethod::Perf,
        count: Some(PERF_COUNT),
        per_msg: Some(PERF_COUNT as f64 / 4800.0),
        raw: Some(PERF_RAW.to_string()),
        enabled_ns: Some(WINDOW_START_NS),
        disabled_ns: Some(WINDOW_END_NS),
        exit: Some(CounterExit::Signal(2)),
        diagnostics: None,
    };
    rec
}

#[allow(clippy::cast_precision_loss)]
fn connections_record(server: ServerKind, workers: u32, rep: u32) -> RunRecord {
    let cycles = 60_000u64;
    let cps = cycles as f64 / WINDOW_SECS as f64;
    let driver = match server {
        ServerKind::Uring => Some(bench::run_record::DriverFds {
            io_uring_fds: workers,
            eventpoll_fds: 0,
        }),
        ServerKind::Tokio => Some(bench::run_record::DriverFds {
            io_uring_fds: 0,
            eventpoll_fds: 1,
        }),
        ServerKind::Mosquitto => None,
    };
    RunRecord {
        schema: SCHEMA_VERSION,
        profile: Profile::Recorded,
        server,
        workers,
        qos: None,
        kind: RunKind::Connections,
        rate_fraction_pct: None,
        offered_rate: None,
        repetition: rep,
        attempt: 1,
        connections: CONNECTIONS,
        payload_bytes: PAYLOAD_BYTES,
        qos1_window: QOS1_WINDOW,
        warmup_secs: WARMUP_SECS,
        window_secs: WINDOW_SECS,
        drain_grace_secs: DRAIN_GRACE_SECS,
        build: Build::Release,
        origin_ns: ORIGIN_NS,
        window: window_obj(),
        timeline: empty_timeline(),
        cycles_in_window: Some(cycles),
        counts: base_counts(),
        throughput_msgs_per_sec: None,
        last10s_msgs_per_sec: None,
        latency_ns: None,
        lateness: Vec::new(),
        lateness_p99_ns: None,
        last_write_start_ns: None,
        connections_per_sec: Some(cps),
        generator_errors: base_generator_errors(),
        syscalls: base_syscalls(),
        driver,
        cpu_sets: cpu_sets(),
        env: env(),
        cpu: Some(cpu_usage()),
        host_cpu_pressure: None,
        started_at_unix: 1_791_273_600,
        harness_errors: Vec::new(),
        valid: true,
        invalid_reasons: Vec::new(),
        cancelled: false,
    }
}

/// All 45 configurations × 5 repetitions with the brief's recorded fixture.
#[allow(clippy::too_many_lines)]
fn full_recorded_set() -> Vec<RunRecord> {
    let mut out: Vec<RunRecord> = Vec::new();
    for w in RECORDED_WORKERS {
        let servers = servers_for(w, &RECORDED_SERVERS);
        for q in RECORDED_QOS {
            for &s in &servers {
                for rep in 1..=REPETITIONS {
                    out.push(throughput_record(s, w, q, rep));
                    out.push(latency_record(s, w, q, 50, rep));
                    out.push(latency_record(s, w, q, 80, rep));
                    out.push(counted_record(s, w, q, rep));
                }
            }
        }
        for &s in &servers {
            for rep in 1..=REPETITIONS {
                out.push(connections_record(s, w, rep));
            }
        }
    }
    out
}

fn expected_configs() -> Vec<ConfigKey> {
    let mut out = Vec::new();
    for w in RECORDED_WORKERS {
        let servers = servers_for(w, &RECORDED_SERVERS);
        for q in RECORDED_QOS {
            for &s in &servers {
                for kind in [RunKind::Throughput, RunKind::Latency] {
                    let pct = if kind == RunKind::Throughput { None } else { Some(50u32) };
                    out.push(ConfigKey {
                        server: s,
                        workers: w,
                        qos: Some(q),
                        kind,
                        rate_fraction_pct: pct,
                    });
                    let pct = if kind == RunKind::Throughput { None } else { Some(80u32) };
                    out.push(ConfigKey {
                        server: s,
                        workers: w,
                        qos: Some(q),
                        kind,
                        rate_fraction_pct: pct,
                    });
                }
                out.push(ConfigKey {
                    server: s,
                    workers: w,
                    qos: Some(q),
                    kind: RunKind::Counted,
                    rate_fraction_pct: Some(80u32),
                });
            }
        }
        for &s in &servers {
            out.push(ConfigKey {
                server: s,
                workers: w,
                qos: None,
                kind: RunKind::Connections,
                rate_fraction_pct: None,
            });
        }
    }
    out
}

#[test]
fn render_refuses_a_smoke_record_in_a_recorded_doc() {
    let mut records = full_recorded_set();
    records[0].profile = Profile::Smoke;
    match render(&records, false) {
        Err(DocError::SmokeRecordInRecordedDoc(_)) => {}
        other => panic!("expected SmokeRecordInRecordedDoc, got {other:?}"),
    }
}

#[test]
fn render_refuses_a_failed_configuration() {
    let mut records = full_recorded_set();
    // Mark one throughput repetition as invalid with a matching reason: the
    // runner saw three stalled connections, so the stored summary and the
    // `recompute` check agree that the run is invalid.
    let target = records
        .iter_mut()
        .find(|r| r.kind == RunKind::Throughput && r.repetition == 1)
        .expect("at least one throughput rep 1");
    target.generator_errors.stalled = 3;
    target.valid = false;
    target.invalid_reasons = vec![STALLED_3_REASON.to_string()];
    match render(&records, false) {
        Err(DocError::FailedConfigurations(names)) => {
            assert!(!names.is_empty(), "expected non-empty names, got {names:?}");
        }
        other => panic!("expected FailedConfigurations, got {other:?}"),
    }
}

#[test]
fn render_refuses_a_matrix_missing_its_four_worker_rows() {
    let records: Vec<RunRecord> = full_recorded_set()
        .into_iter()
        .filter(|r| r.workers != 4)
        .collect();
    match render(&records, false) {
        Err(DocError::FailedConfigurations(names)) => {
            assert_eq!(names.len(), 18, "expected 18 failed names, got {names:?}");
        }
        other => panic!("expected FailedConfigurations with 18 names, got {other:?}"),
    }
}

#[test]
fn render_refuses_a_debug_build() {
    let mut records = full_recorded_set();
    records[0].build = Build::Debug;
    match render(&records, false) {
        Err(DocError::DebugBuild) => {}
        other => panic!("expected DebugBuild, got {other:?}"),
    }
}

#[test]
fn render_refuses_a_dirty_tree() {
    let mut records = full_recorded_set();
    records[0].env.git_dirty = true;
    match render(&records, false) {
        Err(DocError::DirtyTree) => {}
        other => panic!("expected DirtyTree, got {other:?}"),
    }
}

#[test]
fn render_refuses_mixed_commits() {
    let mut records = full_recorded_set();
    records[1].env.git_sha = "def5678".to_string();
    match render(&records, false) {
        Err(DocError::MixedEnvironment(field)) => {
            assert_eq!(field, "git_sha");
        }
        other => panic!("expected MixedEnvironment(git_sha), got {other:?}"),
    }
}

#[test]
fn render_refuses_mixed_hosts() {
    let mut records = full_recorded_set();
    records[1].env.host = "other-host".to_string();
    match render(&records, false) {
        Err(DocError::MixedEnvironment(field)) => {
            assert_eq!(field, "host");
        }
        other => panic!("expected MixedEnvironment(host), got {other:?}"),
    }
}

#[test]
fn render_refuses_mixed_cpu_models() {
    let mut records = full_recorded_set();
    records[1].env.cpu_model = "AMD Ryzen 9 7950X".to_string();
    match render(&records, false) {
        Err(DocError::MixedEnvironment(field)) => {
            assert_eq!(field, "cpu_model");
        }
        other => panic!("expected MixedEnvironment(cpu_model), got {other:?}"),
    }
}

#[test]
fn render_refuses_mixed_tool_versions() {
    // A resumed run named another `--mosquitto-bin`: the medians would
    // combine two broker builds under one `## Environment` table.
    let mut records = full_recorded_set();
    records[1].env.tools = vec![(
        "/usr/local/sbin/mosquitto".to_string(),
        "mosquitto version 2.0.20".to_string(),
    )];
    match render(&records, false) {
        Err(DocError::MixedEnvironment(field)) => {
            assert_eq!(field, "tools");
        }
        other => panic!("expected MixedEnvironment(tools), got {other:?}"),
    }
}

#[test]
fn render_discloses_the_recorded_tool_versions() {
    let mut records = full_recorded_set();
    let tools = vec![
        ("/usr/bin/perf".to_string(), "perf version 6.8.12".to_string()),
        (
            "/usr/sbin/mosquitto".to_string(),
            "mosquitto version 2.0.18".to_string(),
        ),
    ];
    for rec in &mut records {
        rec.env.tools = tools.clone();
    }
    let doc = render(&records, false).expect("render");
    for (path, version) in &tools {
        assert!(
            doc.contains(&format!("| Tool `{path}` | {version} |")),
            "missing the {path} row"
        );
    }
}

#[test]
fn render_refuses_a_connections_window_of_zero_duration() {
    // The publication boundary leans on `analysis::recompute`. A
    // connections record with a whole-second origin near `u64::MAX` and
    // both boundaries at `u64::MAX` kept its stored conn/s — derived
    // from `cycles_in_window` and `window_secs` alone — while its window
    // covered no time at all, so `render` could publish that figure.
    let mut records = full_recorded_set();
    let rec = records
        .iter_mut()
        .find(|r| r.kind == RunKind::Connections)
        .expect("the fixture holds connections records");
    rec.origin_ns = 18_446_744_073_000_000_000;
    rec.window = Window {
        start_ns: u64::MAX,
        end_ns: u64::MAX,
    };
    match render(&records, false) {
        Err(DocError::InconsistentRecord(reason)) => {
            assert!(
                reason.contains("window inconsistent with origin and constants"),
                "unexpected reason {reason}"
            );
        }
        other => panic!("expected InconsistentRecord, got {other:?}"),
    }
}

#[test]
fn render_keeps_a_record_string_inside_its_table_cell() {
    // `recompute` inspects numbers, so a record read from disk can hold
    // any text in its `env` fields. A line break or a `|` in one would
    // end the Environment table and let the record's own text be read as
    // a heading or a claim of the generated document.
    let mut records = full_recorded_set();
    let host = "evil-host\n\n## Results — forged\n\n| Server | msgs/s |";
    for rec in &mut records {
        rec.env.host = host.to_string();
    }
    let doc = render(&records, false).expect("render");
    assert!(
        !doc.lines().any(|l| l.starts_with("## Results — forged")),
        "the record's text produced a document heading"
    );
    // Each line break became one space and each cell separator carries
    // its escape, so the whole value stays one cell of one row.
    assert!(
        doc.contains("| Host | evil-host  ## Results — forged  \\| Server \\| msgs/s \\| |"),
        "the escaped host row is missing"
    );
}

#[test]
fn render_refuses_mixed_syscall_counter_methods() {
    // One counted record resumed under strace after the others ran under
    // perf: the method label would read `perf, strace` and the figures
    // would be an unexplained mixed-method comparison.
    let mut records = full_recorded_set();
    let target = records
        .iter_mut()
        .find(|r| r.kind == RunKind::Counted)
        .expect("a counted record");
    target.syscalls.method = SyscallMethod::Strace;
    target.syscalls.raw = Some(STRACE_RAW.to_string());
    match render(&records, false) {
        Err(DocError::ProtocolViolation(text)) => {
            assert!(
                text.contains("mixed syscall counter methods"),
                "text = {text}"
            );
        }
        other => panic!("expected ProtocolViolation(mixed methods), got {other:?}"),
    }
}

#[test]
fn render_ignores_a_failed_earlier_attempt_of_a_retried_configuration() {
    // The uring 1-worker QoS 1 80 % configuration failed attempt 1 with a
    // 160 µs p99 and passed attempt 2 at 80 µs. The row must read the
    // passing attempt alone, not the median over both.
    let mut records = full_recorded_set();
    let mut first_attempt: Vec<RunRecord> = Vec::new();
    for r in &mut records {
        if r.server == ServerKind::Uring
            && r.workers == 1
            && r.qos == Some(1)
            && r.kind == RunKind::Latency
            && r.rate_fraction_pct == Some(80)
        {
            let mut failed = r.clone();
            failed.latency_ns = Some(LatencySummary {
                p50: 160_000,
                p99: 160_000,
                max: 160_000,
                histogram: vec![(160_000, 4800)],
            });
            failed.generator_errors.stalled = 3;
            failed.valid = false;
            failed.invalid_reasons = vec![STALLED_3_REASON.to_string()];
            first_attempt.push(failed);
            r.attempt = 2;
        }
    }
    assert_eq!(
        first_attempt.len(),
        usize::try_from(REPETITIONS).expect("fits"),
        "expected one failed record per repetition"
    );
    records.extend(first_attempt);
    let doc = render(&records, false).expect("render ok");
    // 120.0 is the median of the two attempts' p99s in µs.
    assert!(
        !doc.contains("120.0"),
        "the failed attempt's p99 leaked into the table:\n{doc}"
    );
    assert!(
        doc.contains("| uring | 100 | 3.0 % | 95.0 | 40.0 | 40.0 | 80.0 | 1.25 | ok |"),
        "missing the passing attempt's uring row in:\n{doc}"
    );
}

/// AC-14: the uring 1-worker QoS 1 throughput configuration was
/// interrupted mid-attempt-1 (its repetition 2 record is cancelled) and
/// resumed as attempt 2, which completed and passed. `render` must read
/// the resumed, passing attempt and must not refuse on account of the
/// interrupted one still sitting on disk.
#[test]
fn render_publishes_a_configuration_resumed_after_an_interrupted_attempt() {
    let mut records = full_recorded_set();
    for r in &mut records {
        if r.server == ServerKind::Uring
            && r.workers == 1
            && r.qos == Some(1)
            && r.kind == RunKind::Throughput
        {
            r.attempt = 2;
        }
    }
    let mut interrupted_rep1 = throughput_record(ServerKind::Uring, 1, 1, 1);
    interrupted_rep1.attempt = 1;
    records.push(interrupted_rep1);
    let mut interrupted_rep2 = throughput_record(ServerKind::Uring, 1, 1, 2);
    interrupted_rep2.attempt = 1;
    interrupted_rep2.cancelled = true;
    records.push(interrupted_rep2);

    render(&records, false).expect("render ok");
}

#[test]
fn render_refuses_an_inconsistent_record() {
    let mut records = full_recorded_set();
    let target = records
        .iter_mut()
        .find(|r| r.kind == RunKind::Throughput && r.repetition == 1)
        .expect("a throughput rep 1");
    target.throughput_msgs_per_sec = Some(150.0);
    match render(&records, false) {
        Err(DocError::InconsistentRecord(name)) => {
            assert!(name.contains("throughput_msgs_per_sec"), "name = {name}");
        }
        other => panic!("expected InconsistentRecord, got {other:?}"),
    }
}

#[test]
fn render_refuses_an_unsupported_schema() {
    let mut records = full_recorded_set();
    records[0].schema = SCHEMA_VERSION + 1;
    match render(&records, false) {
        Err(DocError::UnsupportedSchema(n)) if n == SCHEMA_VERSION + 1 => {}
        other => panic!("expected UnsupportedSchema({}), got {other:?}", SCHEMA_VERSION + 1),
    }
}

#[test]
fn render_refuses_a_record_of_the_previous_schema() {
    let mut records = full_recorded_set();
    records[0].schema = 1;
    match render(&records, false) {
        Err(DocError::UnsupportedSchema(1)) => {}
        other => panic!("expected UnsupportedSchema(1), got {other:?}"),
    }
}

#[test]
fn render_reports_a_missing_throughput_configuration_as_failed() {
    // Drop every uring 1-worker QoS 1 throughput record.
    let records: Vec<RunRecord> = full_recorded_set()
        .into_iter()
        .filter(|r| !(r.server == ServerKind::Uring && r.workers == 1
            && r.qos == Some(1) && r.kind == RunKind::Throughput))
        .collect();
    match render(&records, false) {
        Err(DocError::FailedConfigurations(names)) => {
            let needle = "throughput";
            assert!(
                names.iter().any(|n| n.contains(needle)),
                "expected a name containing {needle:?} in {names:?}"
            );
        }
        other => panic!("expected FailedConfigurations, got {other:?}"),
    }
}

#[test]
fn render_refuses_a_repeated_repetition_number() {
    // One configuration's repetition 5 relabelled 1.
    let mut records = full_recorded_set();
    let target = records
        .iter_mut()
        .find(|r| r.repetition == 5 && r.kind == RunKind::Throughput)
        .expect("a rep 5 throughput");
    target.repetition = 1;
    match render(&records, false) {
        Err(DocError::FailedConfigurations(names)) => {
            assert!(
                names.iter().any(|n| n == "uring-w1-q1-throughput"),
                "expected FailedConfigurations(uring-w1-q1-throughput), got {names:?}"
            );
        }
        other => panic!("expected FailedConfigurations(uring-w1-q1-throughput), got {other:?}"),
    }
}

#[test]
fn render_refuses_a_latency_rate_not_matched_to_throughput() {
    // One 80% record with offered_rate 81 (and stored fields recomputed
    // consistently: scheduled/window_scheduled derived from rate=81).
    let mut records = full_recorded_set();
    let target = records
        .iter_mut()
        .find(|r| r.kind == RunKind::Latency && r.rate_fraction_pct == Some(80))
        .expect("an 80% latency record");
    let rate = 81u64;
    let (scheduled, window_scheduled) = slot_counts(rate, target.origin_ns, target.window);
    target.offered_rate = Some(rate);
    target.counts.scheduled = Some(scheduled);
    target.counts.window_scheduled = Some(window_scheduled);
    target.counts.offered = scheduled;
    target.counts.delivered = scheduled;
    target.counts.acked = Some(scheduled);
    target.timeline.counts =
        vec![rate; usize::try_from(WARMUP_SECS + WINDOW_SECS).expect("fits")];
    target.latency_ns = Some(LatencySummary {
        p50: 80_000,
        p99: 80_000,
        max: 80_000,
        histogram: vec![(80_000, window_scheduled)],
    });
    target.lateness = vec![(1000, window_scheduled)];
    match render(&records, false) {
        Err(DocError::ProtocolViolation(text)) => {
            assert!(text.contains("offered_rate"), "text = {text}");
        }
        other => panic!("expected ProtocolViolation(offered_rate), got {other:?}"),
    }
}

#[test]
fn render_refuses_counter_evidence_on_a_row_the_matrix_does_not_count() {
    // A 50 % latency record instrumented like the 80 % run: the counter
    // fields are self-consistent (`parse_perf_count(PERF_RAW)` over this
    // record's own deliveries), so `recompute` accepts it, but the
    // runner counts syscalls on the 80 % run only. Its p99 was measured
    // against a traced server and must not be published beside the rest.
    let mut records = full_recorded_set();
    // `derive` divides the count by the timeline's in-window deliveries,
    // which the fixture paces at the matched rate for every window second.
    let deliveries = 50 * WINDOW_SECS;
    let target = records
        .iter_mut()
        .find(|r| r.kind == RunKind::Latency && r.rate_fraction_pct == Some(50))
        .expect("a 50% latency record");
    assert_eq!(
        target
            .timeline
            .sum_secs(target.window.start_sec(), target.window.end_sec()),
        deliveries,
        "the 50% fixture should pace its whole window"
    );
    target.syscalls = Syscalls {
        method: SyscallMethod::Perf,
        count: Some(PERF_COUNT),
        per_msg: Some(PERF_COUNT as f64 / deliveries as f64),
        raw: Some(PERF_RAW.to_string()),
        enabled_ns: Some(WINDOW_START_NS),
        disabled_ns: Some(WINDOW_END_NS),
        exit: Some(CounterExit::Signal(2)),
        diagnostics: None,
    };
    match render(&records, false) {
        Err(DocError::ProtocolViolation(text)) => {
            assert!(
                text.contains("counter evidence") && text.contains("does not count"),
                "text = {text}"
            );
        }
        other => panic!("expected ProtocolViolation(counter evidence), got {other:?}"),
    }

    // The same refusal for a throughput record that only names the
    // method: a valid run the matrix does not count records
    // `SyscallMethod::None`.
    let mut records = full_recorded_set();
    let target = records
        .iter_mut()
        .find(|r| r.kind == RunKind::Throughput)
        .expect("a throughput record");
    target.syscalls.method = SyscallMethod::Strace;
    match render(&records, false) {
        Err(DocError::ProtocolViolation(text)) => {
            assert!(
                text.contains("counter method") && text.contains("does not count"),
                "text = {text}"
            );
        }
        other => panic!("expected ProtocolViolation(counter method), got {other:?}"),
    }

    // (c) an 80 % latency record instrumented like the counted run: a
    // latency record is never counted now that `RunKind::Counted` is its
    // own row.
    let mut records = full_recorded_set();
    let target = records
        .iter_mut()
        .find(|r| r.kind == RunKind::Latency && r.rate_fraction_pct == Some(80))
        .expect("an 80% latency record");
    target.syscalls = Syscalls {
        method: SyscallMethod::Perf,
        count: Some(PERF_COUNT),
        per_msg: Some(PERF_COUNT as f64 / 4800.0),
        raw: Some(PERF_RAW.to_string()),
        enabled_ns: Some(WINDOW_START_NS),
        disabled_ns: Some(WINDOW_END_NS),
        exit: Some(CounterExit::Signal(2)),
        diagnostics: None,
    };
    match render(&records, false) {
        Err(DocError::ProtocolViolation(text)) => {
            assert!(
                text.contains("counter evidence") && text.contains("does not count"),
                "text = {text}"
            );
        }
        other => panic!("expected ProtocolViolation(counter evidence), got {other:?}"),
    }
}

#[test]
fn render_publishes_a_failed_attempt_that_only_names_the_counter_tool() {
    // A run that fails before its window keeps `--syscalls`'s tool as
    // the method with an empty counter block (`runner::finalize_record`).
    // That is not evidence of a counter, so such a failed earlier
    // attempt must not refuse a document whose highest attempt passed.
    let mut records = full_recorded_set();
    let mut first_attempt: Vec<RunRecord> = Vec::new();
    for r in &mut records {
        if r.server == ServerKind::Uring
            && r.workers == 1
            && r.qos == Some(1)
            && r.kind == RunKind::Throughput
        {
            let mut failed = r.clone();
            failed.syscalls.method = SyscallMethod::Perf;
            failed.harness_errors = vec!["scratch directory reused: /tmp/x".to_string()];
            failed.valid = false;
            failed.invalid_reasons = vec!["scratch directory reused: /tmp/x".to_string()];
            first_attempt.push(failed);
            r.attempt = 2;
        }
    }
    assert_eq!(
        first_attempt.len(),
        usize::try_from(REPETITIONS).expect("fits"),
        "expected one failed record per repetition"
    );
    records.extend(first_attempt);
    let doc = render(&records, false).expect("render ok");
    assert!(doc.contains("| uring |"), "missing uring row in:\n{doc}");
}

#[test]
fn render_reports_median_throughput_and_p99_per_server() {
    let records = full_recorded_set();
    let doc = render(&records, false).expect("render ok");
    assert!(
        doc.contains("| uring | 100 | 3.0 % | 95.0 | 40.0 | 40.0 | 80.0 | 1.25 | ok |"),
        "missing uring row in:\n{doc}"
    );
}

#[test]
fn render_marks_every_tokio_driver_proof_not_applicable() {
    // No fd evidence at all (not just a mismatched count) is the
    // no-evidence case the parent review raised: driver_label must
    // mark these rows `n/a`, not guess at the uring-only fd criterion.
    let mut records = full_recorded_set();
    for r in &mut records {
        if r.server == ServerKind::Tokio {
            r.driver = Some(bench::run_record::DriverFds {
                io_uring_fds: 0,
                eventpoll_fds: 0,
            });
        }
    }
    let doc = render(&records, false).expect("render ok");
    let tokio_na_count = doc
        .lines()
        .filter(|l| *l == "| tokio | 100 | 3.0 % | 95.0 | 40.0 | 40.0 | 80.0 | 1.25 | n/a |")
        .count();
    assert_eq!(
        tokio_na_count, 4,
        "expected 4 tokio results rows marked n/a in:\n{doc}"
    );
    let tokio_ok_count = doc
        .lines()
        .filter(|l| l.starts_with("| tokio |") && l.ends_with("| ok |"))
        .count();
    assert_eq!(
        tokio_ok_count, 0,
        "tokio driver proof must never read ok in:\n{doc}"
    );
    let uring_ok_count = doc
        .lines()
        .filter(|l| *l == "| uring | 100 | 3.0 % | 95.0 | 40.0 | 40.0 | 80.0 | 1.25 | ok |")
        .count();
    assert_eq!(
        uring_ok_count, 4,
        "uring driver verdict must stay unchanged in:\n{doc}"
    );
}

#[test]
fn render_refuses_a_recorded_document_with_no_tool_versions() {
    // (a) No record names a tool: the document cannot disclose which
    // binaries produced its figures, so `render` must refuse it.
    let mut records = full_recorded_set();
    for rec in &mut records {
        rec.env.tools = Vec::new();
    }
    match render(&records, false) {
        Err(DocError::NoToolVersions) => {}
        other => panic!("expected NoToolVersions, got {other:?}"),
    }

    // (b) Failure-mode ordering guard (decision Q4): a record set whose
    // tools disagree (one record empty, the rest named) is a
    // `MixedEnvironment("tools")`, not `NoToolVersions` — the mismatch
    // check runs first.
    let mut mixed_records = full_recorded_set();
    mixed_records[0].env.tools = Vec::new();
    match render(&mixed_records, false) {
        Err(DocError::MixedEnvironment(field)) => {
            assert_eq!(field, "tools");
        }
        other => panic!("expected MixedEnvironment(tools), got {other:?}"),
    }
}

#[test]
fn render_refuses_a_recorded_document_with_a_blank_tool_version() {
    // `bench doc` prints a refusal with `{e:?}` (`dispatch_doc`), so the
    // Debug form is what the operator reads; it must name the blank tool.
    let mut records = full_recorded_set();
    for rec in &mut records {
        rec.env.tools = vec![
            ("/usr/bin/perf".to_string(), "perf version 6.8.12".to_string()),
            ("/usr/sbin/mosquitto".to_string(), String::new()),
        ];
    }
    let err = render(&records, false).expect_err("a blank mosquitto version must refuse");
    assert_eq!(format!("{err:?}"), "BlankToolVersion(\"/usr/sbin/mosquitto\")");

    // Two blank entries, the first whitespace-only: the refusal names
    // the first entry in `env.tools` order.
    let mut records = full_recorded_set();
    for rec in &mut records {
        rec.env.tools = vec![
            ("/usr/bin/perf".to_string(), "   ".to_string()),
            ("/usr/sbin/mosquitto".to_string(), String::new()),
        ];
    }
    let err = render(&records, false).expect_err("blank perf and mosquitto versions must refuse");
    assert_eq!(format!("{err:?}"), "BlankToolVersion(\"/usr/bin/perf\")");

    // Ordering guard: one record with a blank version among records
    // that name it is a mixed environment first.
    let mut records = full_recorded_set();
    records[0].env.tools = vec![
        ("/usr/bin/perf".to_string(), "perf version 6.8.12".to_string()),
        ("/usr/sbin/mosquitto".to_string(), String::new()),
    ];
    match render(&records, false) {
        Err(DocError::MixedEnvironment(field)) => assert_eq!(field, "tools"),
        other => panic!("expected MixedEnvironment(tools), got {other:?}"),
    }
}

#[test]
fn render_refuses_a_tool_version_of_tabs_and_newlines() {
    // Whitespace is not only spaces: a version of tabs and newlines is
    // blank after trimming and must be refused like an empty one.
    let mut records = full_recorded_set();
    for rec in &mut records {
        rec.env.tools = vec![("/usr/bin/strace".to_string(), "\t\n  \t".to_string())];
    }
    let err = render(&records, false).expect_err("a tab-and-newline version must refuse");
    assert_eq!(format!("{err:?}"), "BlankToolVersion(\"/usr/bin/strace\")");
}

#[test]
fn render_accepts_a_recorded_document_whose_tool_versions_are_padded() {
    // The check is `trim().is_empty()`, not a comparison with the
    // trimmed value: a padded but non-blank version identifies its
    // binary and must still publish.
    let mut records = full_recorded_set();
    for rec in &mut records {
        rec.env.tools = vec![
            (
                "/usr/bin/perf".to_string(),
                "  perf version 6.8.12  ".to_string(),
            ),
            (
                "/usr/sbin/mosquitto".to_string(),
                "\tmosquitto version 2.0.18\n".to_string(),
            ),
        ];
    }
    let doc = render(&records, false).expect("a padded tool version must render");
    assert!(
        doc.contains("perf version 6.8.12"),
        "the perf row must disclose its version in:\n{doc}"
    );
    assert!(
        doc.contains("mosquitto version 2.0.18"),
        "the mosquitto row must disclose its version in:\n{doc}"
    );
}

#[test]
fn render_refuses_a_blank_tool_version_before_a_protocol_violation() {
    // Ordering guard: the blank-version check sits ahead of the
    // protocol-constant checks, so a record set that breaks both names
    // the unidentified tool, not the repetition.
    let mut records = full_recorded_set();
    for rec in &mut records {
        rec.env.tools = vec![("/usr/sbin/mosquitto".to_string(), String::new())];
    }
    let target = records
        .iter_mut()
        .find(|r| r.repetition == 5 && r.kind == RunKind::Throughput)
        .expect("a rep 5 throughput");
    target.repetition = 1;
    let err = render(&records, false).expect_err("a blank version and a bad repetition must refuse");
    assert_eq!(format!("{err:?}"), "BlankToolVersion(\"/usr/sbin/mosquitto\")");
}

/// `full_recorded_set()` with `edit` applied to every record's `env`.
fn recorded_set_with_env(edit: impl Fn(&mut Env)) -> Vec<RunRecord> {
    let mut records = full_recorded_set();
    for rec in &mut records {
        edit(&mut rec.env);
    }
    records
}

/// `bench doc` prints a refusal with `{e:?}` (`dispatch_doc`), so the
/// Debug form is what the operator reads.
fn assert_recorded_refusal(records: &[RunRecord], expected: &str) {
    let err = render(records, false).expect_err(expected);
    assert_eq!(format!("{err:?}"), expected);
}

#[test]
fn render_refuses_a_recorded_document_with_a_blank_environment_field() {
    assert_recorded_refusal(
        &recorded_set_with_env(|env| env.git_sha = String::new()),
        "BlankEnvironment(\"git_sha\")",
    );
    assert_recorded_refusal(
        &recorded_set_with_env(|env| env.kernel = "   ".to_string()),
        "BlankEnvironment(\"kernel\")",
    );
    // `cell` turns control characters into spaces, so a NUL-only
    // host publishes a blank cell too.
    assert_recorded_refusal(
        &recorded_set_with_env(|env| env.host = "\u{0}".to_string()),
        "BlankEnvironment(\"host\")",
    );
    assert_recorded_refusal(
        &recorded_set_with_env(|env| env.cpu_model = "\t\u{7}\n".to_string()),
        "BlankEnvironment(\"cpu_model\")",
    );

    // Two blank fields: the refusal names the first in the order
    // git_sha, kernel, host, cpu_model.
    assert_recorded_refusal(
        &recorded_set_with_env(|env| {
            env.host = String::new();
            env.cpu_model = String::new();
        }),
        "BlankEnvironment(\"host\")",
    );

    // Ordering guard: one record with a blank host among records
    // that name it is a mixed environment first.
    let mut records = full_recorded_set();
    records[0].env.host = String::new();
    match render(&records, false) {
        Err(DocError::MixedEnvironment(field)) => assert_eq!(field, "host"),
        other => panic!("expected MixedEnvironment(host), got {other:?}"),
    }

    // Ordering guard: every mixed check, `tools` included, runs
    // before the blank check.
    let mut records = recorded_set_with_env(|env| env.host = String::new());
    records[0].env.tools.pop();
    match render(&records, false) {
        Err(DocError::MixedEnvironment(field)) => assert_eq!(field, "tools"),
        other => panic!("expected MixedEnvironment(tools), got {other:?}"),
    }
}

#[test]
fn render_refuses_a_recorded_document_with_a_blank_tool_path() {
    // A blank path with a real version publishes a tool row that
    // names no binary; the refusal names the entry's position.
    assert_recorded_refusal(
        &recorded_set_with_env(|env| {
            env.tools = vec![
                (String::new(), "perf version 6.8.12".to_string()),
                ("/usr/sbin/mosquitto".to_string(), "mosquitto version 2.0.18".to_string()),
            ];
        }),
        "BlankToolPath(0)",
    );
    assert_recorded_refusal(
        &recorded_set_with_env(|env| {
            env.tools = vec![
                ("/usr/bin/perf".to_string(), "perf version 6.8.12".to_string()),
                ("  ".to_string(), "mosquitto version 2.0.18".to_string()),
            ];
        }),
        "BlankToolPath(1)",
    );
    assert_recorded_refusal(
        &recorded_set_with_env(|env| {
            env.tools = vec![("\u{0}".to_string(), "perf version 6.8.12".to_string())];
        }),
        "BlankToolPath(0)",
    );

    // Any blank path wins over any blank version: a version refusal
    // would name an empty path.
    assert_recorded_refusal(
        &recorded_set_with_env(|env| env.tools = vec![(String::new(), String::new())]),
        "BlankToolPath(0)",
    );
    assert_recorded_refusal(
        &recorded_set_with_env(|env| {
            env.tools = vec![
                ("/usr/bin/perf".to_string(), String::new()),
                (String::new(), "mosquitto version 2.0.18".to_string()),
            ];
        }),
        "BlankToolPath(1)",
    );
    // Two blank paths: the first wins.
    assert_recorded_refusal(
        &recorded_set_with_env(|env| {
            env.tools = vec![
                (String::new(), "perf version 6.8.12".to_string()),
                (String::new(), String::new()),
            ];
        }),
        "BlankToolPath(0)",
    );

    // Ordering guard: one record with a blank path among records
    // that name it is a mixed environment first.
    let mut records = full_recorded_set();
    records[0].env.tools[0].0 = String::new();
    match render(&records, false) {
        Err(DocError::MixedEnvironment(field)) => assert_eq!(field, "tools"),
        other => panic!("expected MixedEnvironment(tools), got {other:?}"),
    }
}

#[test]
fn render_refuses_a_tool_version_of_control_characters() {
    // `cell` turns control characters into spaces, so a version of
    // NUL and BEL publishes a blank cell.
    assert_recorded_refusal(
        &recorded_set_with_env(|env| {
            env.tools = vec![("/usr/bin/strace".to_string(), "\u{0}\u{7}".to_string())];
        }),
        "BlankToolVersion(\"/usr/bin/strace\")",
    );
}

#[test]
fn smoke_render_accepts_blank_environment_fields_and_tool_paths() {
    // Both refusals are recorded-mode only: a smoke document is
    // already marked unpublishable.
    let mut records = vec![throughput_record(ServerKind::Uring, 1, 1, 1)];
    records[0].env.git_sha = String::new();
    records[0].env.kernel = String::new();
    records[0].env.host = String::new();
    records[0].env.cpu_model = String::new();
    records[0].env.tools = vec![(String::new(), "perf version 6.8.12".to_string())];
    let doc = render(&records, true).expect("smoke render must accept blank env fields");
    assert!(doc.contains("SMOKE — not publishable"), "doc:\n{doc}");
}

#[test]
fn smoke_render_accepts_a_tool_version_of_control_characters() {
    // The renders-blank predicate sits inside `if !smoke` for the
    // version check too: a smoke document still publishes a version of
    // NUL and BEL.
    let mut records = vec![throughput_record(ServerKind::Uring, 1, 1, 1)];
    records[0].env.tools = vec![("/usr/bin/strace".to_string(), "\u{0}\u{7}".to_string())];
    let doc = render(&records, true).expect("smoke render must accept a control-only version");
    assert!(doc.contains("SMOKE — not publishable"), "doc:\n{doc}");
}

/// `full_recorded_set()` with `edit` applied to every record's `cpu_sets`.
fn recorded_set_with_cpu_sets(edit: impl Fn(&mut CpuSets)) -> Vec<RunRecord> {
    let mut records = full_recorded_set();
    for rec in &mut records {
        edit(&mut rec.cpu_sets);
    }
    records
}

#[test]
fn render_refuses_a_recorded_document_with_a_blank_cpu_set() {
    assert_recorded_refusal(
        &recorded_set_with_cpu_sets(|cpu_sets| cpu_sets.server = String::new()),
        "BlankCpuSet(\"server\")",
    );
    // `cell` turns control characters into spaces, so a NUL-only
    // generator set publishes a blank cell too.
    assert_recorded_refusal(
        &recorded_set_with_cpu_sets(|cpu_sets| cpu_sets.generator = "\u{0}".to_string()),
        "BlankCpuSet(\"generator\")",
    );
    // Two blank fields: the refusal names server first.
    assert_recorded_refusal(
        &recorded_set_with_cpu_sets(|cpu_sets| {
            cpu_sets.server = "  ".to_string();
            cpu_sets.generator = String::new();
        }),
        "BlankCpuSet(\"server\")",
    );

    // Ordering guard: a mixed pair is refused before the blank-set
    // check ever reads the shared pair.
    let mut records = full_recorded_set();
    records[0].cpu_sets.server = String::new();
    match render(&records, false) {
        Err(DocError::ProtocolViolation(text)) => {
            assert!(
                text.starts_with("multiple cpu_sets pairs"),
                "text = {text}"
            );
        }
        other => panic!("expected ProtocolViolation(multiple cpu_sets pairs), got {other:?}"),
    }

    // Ordering guard: the blank-environment check sits ahead of the
    // blank-cpu-set check, so a set that breaks both names the field.
    let mut records = recorded_set_with_env(|env| env.host = String::new());
    for rec in &mut records {
        rec.cpu_sets.server = String::new();
    }
    assert_recorded_refusal(&records, "BlankEnvironment(\"host\")");
}

#[test]
fn smoke_render_accepts_a_blank_cpu_set() {
    // The blank-cpu-set check sits inside `if !smoke`: a smoke document
    // still publishes a blank cpu_sets pair.
    let mut records = vec![throughput_record(ServerKind::Uring, 1, 1, 1)];
    records[0].cpu_sets.server = String::new();
    records[0].cpu_sets.generator = String::new();
    let doc = render(&records, true).expect("smoke render must accept a blank cpu set");
    assert!(doc.contains("SMOKE — not publishable"), "doc:\n{doc}");
}

#[test]
fn render_refuses_a_blank_environment_field_before_a_missing_tool_list() {
    // Refusal order: the blank-environment check sits ahead of
    // `NoToolVersions`, so a set that breaks both names the field whose
    // cell identifies nothing rather than the absent tool list.
    assert_recorded_refusal(
        &recorded_set_with_env(|env| {
            env.host = String::new();
            env.tools = Vec::new();
        }),
        "BlankEnvironment(\"host\")",
    );
}

#[test]
fn render_refuses_a_blank_environment_field_before_a_protocol_violation() {
    // Ordering guard: the blank-environment check sits ahead of the
    // protocol-constant checks, so a record set that breaks both names
    // the unidentified field, not the repetition.
    let mut records = recorded_set_with_env(|env| env.cpu_model = String::new());
    let target = records
        .iter_mut()
        .find(|r| r.repetition == 5 && r.kind == RunKind::Throughput)
        .expect("a rep 5 throughput");
    target.repetition = 1;
    assert_recorded_refusal(&records, "BlankEnvironment(\"cpu_model\")");
}

#[test]
fn render_refuses_a_blank_tool_path_before_a_protocol_violation() {
    // Ordering guard: the blank-path check sits ahead of the
    // protocol-constant checks, so a record set that breaks both names
    // the tool entry, not the repetition.
    let mut records = recorded_set_with_env(|env| {
        env.tools = vec![(String::new(), "perf version 6.8.12".to_string())];
    });
    let target = records
        .iter_mut()
        .find(|r| r.repetition == 5 && r.kind == RunKind::Throughput)
        .expect("a rep 5 throughput");
    target.repetition = 1;
    assert_recorded_refusal(&records, "BlankToolPath(0)");
}

#[test]
fn render_refuses_a_dirty_tree_and_a_debug_build_before_a_blank_environment_field() {
    // Refusal order: provenance decides before disclosure. A dirty tree
    // or a debug build with a blank field names the build, because a
    // fresh run is the remedy for both.
    assert_recorded_refusal(
        &recorded_set_with_env(|env| {
            env.git_dirty = true;
            env.git_sha = String::new();
        }),
        "DirtyTree",
    );

    let mut records = recorded_set_with_env(|env| env.git_sha = String::new());
    records[0].build = Build::Debug;
    assert_recorded_refusal(&records, "DebugBuild");
}

#[test]
fn render_refuses_an_environment_field_of_only_unicode_whitespace() {
    // A no-break space is not a control character, so `cell` keeps it,
    // but it still publishes a cell that identifies nothing: the
    // predicate is `cell(v).trim().is_empty()`, and `trim` removes every
    // Unicode whitespace character.
    assert_recorded_refusal(
        &recorded_set_with_env(|env| env.host = "\u{a0}\u{2003}".to_string()),
        "BlankEnvironment(\"host\")",
    );
}

#[test]
fn render_accepts_environment_fields_that_render_a_non_blank_cell() {
    // No false positive: the check refuses a cell that holds nothing,
    // not one that `cell` escapes or pads. A pipe and a backslash are
    // escaped into visible cell text, so both must still publish.
    let records = recorded_set_with_env(|env| {
        env.git_sha = "  b219eb1  ".to_string();
        env.host = "|".to_string();
        env.cpu_model = "\\".to_string();
        env.tools = vec![(" /usr/bin/perf ".to_string(), "perf version 6.8.12".to_string())];
    });
    let doc = render(&records, false).expect("non-blank env cells must render");
    assert!(
        doc.contains("b219eb1"),
        "the commit row must disclose its value in:\n{doc}"
    );
    assert!(
        doc.contains("/usr/bin/perf"),
        "the perf row must disclose its path in:\n{doc}"
    );
    assert!(
        doc.contains("\\|"),
        "the host cell must escape its pipe in:\n{doc}"
    );
    assert!(
        doc.contains("\\\\"),
        "the cpu_model cell must escape its backslash in:\n{doc}"
    );
}

#[test]
fn render_discloses_the_epic_strace_bias_for_strace_counts() {
    // (a) every counted record resumed under strace: the method
    // label must carry §9.1's own ptrace caveat, not the stale
    // "inflates" wording.
    let mut records = full_recorded_set();
    let mut mutated = 0usize;
    for rec in records
        .iter_mut()
        .filter(|r| r.kind == RunKind::Counted)
    {
        assert_eq!(
            rec.syscalls.method,
            SyscallMethod::Perf,
            "fixture already not perf before mutation"
        );
        rec.syscalls.method = SyscallMethod::Strace;
        rec.syscalls.raw = Some(STRACE_RAW.to_string());
        assert_eq!(rec.syscalls.method, SyscallMethod::Strace, "mutation did not take");
        mutated += 1;
    }
    assert!(mutated > 0, "no 80% latency records found to mutate");

    let doc = render(&records, false).expect("render ok");
    let expected_strace_line = "| Syscall method | strace (ptrace slows each syscall, which raises completions per `io_uring_enter` and flatters io_uring) |";
    assert!(
        doc.lines().any(|l| l == expected_strace_line),
        "missing strace caveat line in:\n{doc}"
    );
    assert!(!doc.contains("inflates"), "stale caveat text found in:\n{doc}");

    // (b) edge — perf counts carry no caveat.
    let perf_doc = render(&full_recorded_set(), false).expect("render ok");
    assert!(
        perf_doc.lines().any(|l| l == "| Syscall method | perf |"),
        "missing plain perf line in:\n{perf_doc}"
    );
    assert!(
        !perf_doc.contains("ptrace"),
        "unexpected ptrace mention in:\n{perf_doc}"
    );
}

#[test]
fn render_qualifies_every_mosquitto_result_row_with_subscriber_receipt() {
    let records = full_recorded_set();
    let doc = render(&records, false).expect("render ok");
    let expected_row =
        "| mosquitto (endpoint = subscriber receipt) | 100 | 3.0 % | 95.0 | 40.0 | 40.0 | 80.0 | 1.25 | n/a |";
    let qualified_count = doc.lines().filter(|l| *l == expected_row).count();
    assert_eq!(
        qualified_count, 2,
        "expected 2 qualified mosquitto results rows (one per QoS) in:\n{doc}"
    );
    let unqualified_count = doc.lines().filter(|l| l.starts_with("| mosquitto | 100 |")).count();
    assert_eq!(
        unqualified_count, 0,
        "unqualified mosquitto results row found in:\n{doc}"
    );
    // Regression guard for decision Q2 (not an AC): the connections row
    // stays plain, not qualified with the subscriber-receipt endpoint.
    let connections_count = doc
        .lines()
        .filter(|l| *l == "| mosquitto | 1000 | 0.0 % |")
        .count();
    assert_eq!(
        connections_count, 1,
        "expected plain mosquitto connections row in:\n{doc}"
    );
}

#[test]
fn render_marks_a_tokio_row_with_no_driver_record_not_applicable() {
    // A record stored before the twin reported any fd count carries
    // `driver: None`. The uring-only proof has nothing to say about it,
    // so the cell must read `n/a` and never the `failed` the deleted
    // tokio arm produced.
    let mut records = full_recorded_set();
    for r in &mut records {
        if r.server == ServerKind::Tokio {
            r.driver = None;
        }
    }
    let doc = render(&records, false).expect("render ok");
    let tokio_na_count = doc
        .lines()
        .filter(|l| *l == "| tokio | 100 | 3.0 % | 95.0 | 40.0 | 40.0 | 80.0 | 1.25 | n/a |")
        .count();
    assert_eq!(
        tokio_na_count, 4,
        "expected 4 tokio results rows marked n/a in:\n{doc}"
    );
    let tokio_failed_count = doc
        .lines()
        .filter(|l| l.starts_with("| tokio |") && l.ends_with("| failed |"))
        .count();
    assert_eq!(
        tokio_failed_count, 0,
        "tokio driver proof must never read failed in:\n{doc}"
    );
}

#[test]
fn render_names_a_failed_mosquitto_configuration_by_its_bare_label() {
    // `config_label` feeds the refusal messages and needs the bare
    // kebab name: the results-row qualification must not leak into it.
    let records: Vec<RunRecord> = full_recorded_set()
        .into_iter()
        .filter(|r| !(r.server == ServerKind::Mosquitto && r.kind == RunKind::Throughput))
        .collect();
    match render(&records, false) {
        Err(DocError::FailedConfigurations(names)) => {
            assert!(
                names.iter().any(|n| n == "mosquitto-w1-q0-throughput"),
                "expected the bare mosquitto configuration label, got {names:?}"
            );
            assert!(
                !names.iter().any(|n| n.contains("endpoint")),
                "the results-row qualification leaked into a configuration label: {names:?}"
            );
        }
        other => panic!("expected FailedConfigurations, got {other:?}"),
    }
}

#[test]
fn smoke_render_accepts_an_empty_tool_set() {
    // The tool-version refusal is recorded-mode only: a smoke document
    // is already marked unpublishable, and `bench doc --smoke` renders
    // sets captured without a tool probe.
    let mut records = vec![throughput_record(ServerKind::Uring, 1, 1, 1)];
    records[0].env.tools = Vec::new();
    let doc = render(&records, true).expect("smoke render must accept an empty tool set");
    assert!(doc.contains("SMOKE — not publishable"), "doc:\n{doc}");
}

#[test]
fn smoke_render_accepts_a_blank_tool_version() {
    // The blank-version refusal is recorded-mode only, like the
    // empty-set refusal.
    let mut records = vec![throughput_record(ServerKind::Uring, 1, 1, 1)];
    records[0].env.tools = vec![("/usr/sbin/mosquitto".to_string(), String::new())];
    let doc = render(&records, true).expect("smoke render must accept a blank tool version");
    assert!(doc.contains("SMOKE — not publishable"), "doc:\n{doc}");
}

#[test]
fn render_ends_with_the_unticked_sign_off_checklist() {
    let records = full_recorded_set();
    let doc = render(&records, false).expect("render ok");
    let expected = [
        "- [ ] Driver proof: every uring run shows io_uring fds = workers and eventpoll fds = 0",
        "- [ ] Spread: every configuration within 10 % over 5 interleaved repetitions",
        "- [ ] Reconciliation: every run reconciled (unresolved 0, duplicates 0, dropped 0 in throughput runs)",
        "- [ ] Environment: host, kernel, CPU sets, AC power and idle state match this disclosure",
        "- [ ] Saturation: in every throughput row the server CPU set, not the generator, is the saturated side",
    ];
    let last_five: Vec<&str> = doc
        .lines()
        .filter(|l| !l.trim().is_empty())
        .rev()
        .take(5)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    assert_eq!(last_five, expected.to_vec());
}

#[test]
fn smoke_render_marks_the_header() {
    // Build a small smoke fixture: just one uring 1-worker throughput run.
    let records = vec![throughput_record(ServerKind::Uring, 1, 1, 1)];
    let doc = render(&records, true).expect("smoke render ok");
    assert!(doc.contains("SMOKE — not publishable"), "doc:\n{doc}");
}

#[test]
fn smoke_render_marks_a_failed_connections_configuration() {
    // The smoke profile runs 2 repetitions, so one invalid repetition
    // leaves the configuration without a passing attempt. The cell must
    // read FAILED, not a conn/s figure taken from the failed set.
    let mut records = vec![
        connections_record(ServerKind::Uring, 1, 1),
        connections_record(ServerKind::Uring, 1, 2),
    ];
    records[1].generator_errors.stalled = 3;
    records[1].valid = false;
    records[1].invalid_reasons = vec![STALLED_3_REASON.to_string()];
    let doc = render(&records, true).expect("smoke render ok");
    assert!(
        doc.contains("| uring | FAILED | FAILED |"),
        "expected a FAILED conn/s row in:\n{doc}"
    );
    assert!(
        !doc.contains("| uring | 1000 |"),
        "a failed connections configuration rendered a figure:\n{doc}"
    );
}

#[test]
fn smoke_render_marks_a_configuration_that_repeats_a_repetition_failed() {
    // Both records claim repetition 1: the attempt never completes its
    // two-repetition smoke set, so the configuration counts as failed.
    let records = vec![
        connections_record(ServerKind::Uring, 1, 1),
        connections_record(ServerKind::Uring, 1, 1),
    ];
    let doc = render(&records, true).expect("smoke render ok");
    let table = doc
        .split("## Connections per second — 1 worker(s)")
        .nth(1)
        .expect("1-worker connections table")
        .split("\n## ")
        .next()
        .unwrap();
    assert!(
        table.contains("| uring | FAILED | FAILED |"),
        "expected a FAILED conn/s row in:\n{table}"
    );
    assert!(
        !table.contains("| uring | 1000 |"),
        "a repeated-repetition configuration rendered a figure:\n{table}"
    );

    // Control: distinct repetitions 1 and 2 render a passing figure.
    let control_records = vec![
        connections_record(ServerKind::Uring, 1, 1),
        connections_record(ServerKind::Uring, 1, 2),
    ];
    let control_doc = render(&control_records, true).expect("smoke render ok");
    let control_table = control_doc
        .split("## Connections per second — 1 worker(s)")
        .nth(1)
        .expect("1-worker connections table")
        .split("\n## ")
        .next()
        .unwrap();
    assert!(
        control_table.contains("| uring | 1000 |"),
        "expected a passing conn/s row in:\n{control_table}"
    );
}

#[test]
fn render_returns_no_records_for_empty_input() {
    match render(&[], false) {
        Err(DocError::NoRecords) => {}
        other => panic!("expected NoRecords, got {other:?}"),
    }
}

#[test]
fn render_succeeds_on_the_full_recorded_fixture() {
    let records = full_recorded_set();
    let doc = render(&records, false).expect("render ok");
    assert!(doc.contains("# Benchmarks"));
    assert!(doc.contains("## Environment"));
    assert!(doc.contains("## Method"));
    // Every expected configuration appears in the document at least once.
    for key in expected_configs() {
        let server = match key.server {
            ServerKind::Uring => "uring",
            ServerKind::Tokio => "tokio",
            ServerKind::Mosquitto => "mosquitto",
        };
        assert!(
            doc.contains(server),
            "document is missing server {server:?} for {key:?}"
        );
    }
}
#[test]
fn render_refuses_a_configuration_outside_the_recorded_matrix() {
    // A self-consistent two-worker throughput configuration. The
    // expected-key loops never visit it, so a published document would
    // hide a measurement it does not disclose.
    let mut records = full_recorded_set();
    let extra: Vec<RunRecord> = records
        .iter()
        .filter(|r| {
            r.server == ServerKind::Uring
                && r.workers == 1
                && r.qos == Some(1)
                && r.kind == RunKind::Throughput
        })
        .map(|r| {
            let mut c = r.clone();
            c.workers = 2;
            c.driver = Some(bench::run_record::DriverFds {
                io_uring_fds: 2,
                eventpoll_fds: 0,
            });
            c
        })
        .collect();
    assert_eq!(
        extra.len(),
        usize::try_from(REPETITIONS).expect("fits"),
        "expected one extra record per repetition"
    );
    records.extend(extra);
    match render(&records, false) {
        Err(DocError::ProtocolViolation(text)) => {
            assert!(
                text.contains("outside the recorded matrix"),
                "text = {text}"
            );
        }
        other => panic!("expected ProtocolViolation(outside the matrix), got {other:?}"),
    }
}

#[test]
fn render_refuses_a_failed_earlier_attempt_without_counter_evidence() {
    // The uring 1-worker QoS 1 counted configuration failed attempt 1
    // with no syscall counter and passed attempt 2 with perf. Counter
    // evidence is required of every counted record, not only the
    // published attempt.
    let mut records = full_recorded_set();
    let mut first_attempt: Vec<RunRecord> = Vec::new();
    for r in &mut records {
        if r.server == ServerKind::Uring
            && r.workers == 1
            && r.qos == Some(1)
            && r.kind == RunKind::Counted
        {
            let mut failed = r.clone();
            failed.syscalls = Syscalls {
                method: SyscallMethod::None,
                count: None,
                per_msg: None,
                raw: None,
                enabled_ns: None,
                disabled_ns: None,
                exit: None,
                diagnostics: None,
            };
            failed.generator_errors.stalled = 3;
            failed.valid = false;
            failed.invalid_reasons = bench::analysis::reconcile(&failed);
            first_attempt.push(failed);
            r.attempt = 2;
        }
    }
    assert_eq!(
        first_attempt.len(),
        usize::try_from(REPETITIONS).expect("fits"),
        "expected one failed record per repetition"
    );
    records.extend(first_attempt);
    match render(&records, false) {
        Err(DocError::ProtocolViolation(text)) => {
            assert!(
                text.contains("counted record without counter evidence"),
                "text = {text}"
            );
        }
        other => panic!(
            "expected ProtocolViolation(counted record without counter evidence), got {other:?}"
        ),
    }
}

#[test]
fn render_refuses_a_failed_earlier_attempt_at_an_unmatched_rate() {
    // The uring 1-worker QoS 1 80 % configuration failed attempt 1 at an
    // offered rate of 81 — self-consistent, but not the matched rate —
    // and passed attempt 2 at the matched rate.
    let mut records = full_recorded_set();
    let rate = 81u64;
    let mut first_attempt: Vec<RunRecord> = Vec::new();
    for r in &mut records {
        if r.server == ServerKind::Uring
            && r.workers == 1
            && r.qos == Some(1)
            && r.kind == RunKind::Latency
            && r.rate_fraction_pct == Some(80)
        {
            let mut failed = r.clone();
            let (scheduled, window_scheduled) =
                slot_counts(rate, failed.origin_ns, failed.window);
            failed.offered_rate = Some(rate);
            failed.counts.scheduled = Some(scheduled);
            failed.counts.window_scheduled = Some(window_scheduled);
            failed.counts.offered = scheduled;
            failed.counts.delivered = scheduled;
            failed.counts.acked = Some(scheduled);
            failed.timeline.counts =
                vec![rate; usize::try_from(WARMUP_SECS + WINDOW_SECS).expect("fits")];
            failed.latency_ns = Some(LatencySummary {
                p50: 80_000,
                p99: 80_000,
                max: 80_000,
                histogram: vec![(80_000, window_scheduled)],
            });
            failed.lateness = vec![(1000, window_scheduled)];
            failed.generator_errors.stalled = 3;
            failed.valid = false;
            failed.invalid_reasons = vec![STALLED_3_REASON.to_string()];
            first_attempt.push(failed);
            r.attempt = 2;
        }
    }
    assert_eq!(
        first_attempt.len(),
        usize::try_from(REPETITIONS).expect("fits"),
        "expected one failed record per repetition"
    );
    records.extend(first_attempt);
    match render(&records, false) {
        Err(DocError::ProtocolViolation(text)) => {
            assert!(text.contains("offered_rate"), "text = {text}");
        }
        other => panic!("expected ProtocolViolation(offered_rate), got {other:?}"),
    }
}

#[test]
fn render_refuses_a_recorded_set_missing_its_counted_runs() {
    // Every `RunKind::Counted` record dropped: §9.1's `syscalls/msg`
    // evidence is its own configuration, so a set without it is as
    // incomplete as a set missing a worker row.
    let records: Vec<RunRecord> = full_recorded_set()
        .into_iter()
        .filter(|r| r.kind != RunKind::Counted)
        .collect();
    match render(&records, false) {
        Err(DocError::FailedConfigurations(names)) => {
            assert_eq!(names.len(), 10, "expected 10 failed names, got {names:?}");
            assert!(
                names.iter().all(|n| n.ends_with("-counted")),
                "expected every name to end with -counted, got {names:?}"
            );
        }
        other => panic!("expected FailedConfigurations with 10 names, got {other:?}"),
    }
}

#[test]
fn render_reads_syscalls_from_the_counted_run_and_p99_from_the_uncounted_run() {
    // The counted run's own count/raw/per_msg feed the table's
    // `syscalls/msg` cell; its own p99 must never leak into the p99
    // column, which stays the uncounted 80 % record's figure.
    let mut records = full_recorded_set();
    let mut mutated = 0usize;
    for r in records.iter_mut().filter(|r| {
        r.server == ServerKind::Uring
            && r.workers == 1
            && r.qos == Some(1)
            && r.kind == RunKind::Counted
    }) {
        r.syscalls.count = Some(12000);
        r.syscalls.raw = Some("12000,,raw_syscalls:sys_enter,60000000000,100.00,,\n".to_string());
        r.syscalls.per_msg = Some(2.5);
        r.latency_ns = Some(LatencySummary {
            p50: 160_000,
            p99: 160_000,
            max: 160_000,
            histogram: vec![(160_000, 4800)],
        });
        mutated += 1;
    }
    assert_eq!(
        mutated,
        usize::try_from(REPETITIONS).expect("fits"),
        "expected one counted record per repetition"
    );

    let doc = render(&records, false).expect("render ok");
    assert!(
        doc.contains("| uring | 100 | 3.0 % | 95.0 | 40.0 | 40.0 | 80.0 | 2.50 | ok |"),
        "missing the counted syscalls/msg row in:\n{doc}"
    );
    assert!(
        !doc.contains("160.0"),
        "the counted run's own p99 leaked into the table:\n{doc}"
    );
}

#[test]
fn render_refuses_a_counted_run_rate_not_matched_to_throughput() {
    // Copy of `render_refuses_a_latency_rate_not_matched_to_throughput`
    // targeting the first `Counted` record: the counted run is paced at
    // the matched rate too, so an unmatched offered_rate refuses it the
    // same way.
    let mut records = full_recorded_set();
    let target = records
        .iter_mut()
        .find(|r| r.kind == RunKind::Counted)
        .expect("a counted record");
    let rate = 81u64;
    let (scheduled, window_scheduled) = slot_counts(rate, target.origin_ns, target.window);
    target.offered_rate = Some(rate);
    target.counts.scheduled = Some(scheduled);
    target.counts.window_scheduled = Some(window_scheduled);
    target.counts.offered = scheduled;
    target.counts.delivered = scheduled;
    target.counts.acked = Some(scheduled);
    target.timeline.counts =
        vec![rate; usize::try_from(WARMUP_SECS + WINDOW_SECS).expect("fits")];
    target.latency_ns = Some(LatencySummary {
        p50: 80_000,
        p99: 80_000,
        max: 80_000,
        histogram: vec![(80_000, window_scheduled)],
    });
    target.lateness = vec![(1000, window_scheduled)];
    target.syscalls.count = Some(PERF_COUNT);
    target.syscalls.per_msg = Some(PERF_COUNT as f64 / (81 * WINDOW_SECS) as f64);
    match render(&records, false) {
        Err(DocError::ProtocolViolation(text)) => {
            assert!(text.contains("offered_rate"), "text = {text}");
        }
        other => panic!("expected ProtocolViolation(offered_rate), got {other:?}"),
    }
}

#[test]
fn render_states_that_latency_and_syscalls_come_from_separate_runs() {
    let records = full_recorded_set();
    let doc = render(&records, false).expect("render ok");
    let expected_line = "Both latency columns come from uncounted runs; `syscalls/msg` comes \
                          from 5 separate counted repetitions at the same matched 80 % rate, \
                          which qualify on `syscalls/msg` spread. The counted runs' p99 stays \
                          in the raw JSON and is not published.";
    assert!(
        doc.lines().any(|l| l == expected_line),
        "missing the counted Method line in:\n{doc}"
    );
}

#[test]
fn smoke_render_reads_syscalls_only_from_counted_runs() {
    // A smoke set that holds no counted run: the syscalls/msg cell and
    // the Method line both read `n/a`.
    let mut records = vec![
        throughput_record(ServerKind::Uring, 1, 1, 1),
        throughput_record(ServerKind::Uring, 1, 1, 2),
        latency_record(ServerKind::Uring, 1, 1, 50, 1),
        latency_record(ServerKind::Uring, 1, 1, 50, 2),
        latency_record(ServerKind::Uring, 1, 1, 80, 1),
        latency_record(ServerKind::Uring, 1, 1, 80, 2),
        connections_record(ServerKind::Uring, 1, 1),
        connections_record(ServerKind::Uring, 1, 2),
    ];
    let doc = render(&records, true).expect("smoke render ok");
    assert!(
        doc.lines()
            .any(|l| l.starts_with("| uring |") && l.ends_with("| 80.0 | n/a | ok |")),
        "expected the uncounted uring row in:\n{doc}"
    );
    let expected_na_line = "Both latency columns come from uncounted runs; `syscalls/msg` is \
                             `n/a` because this record set holds no counted run.";
    assert!(
        doc.lines().any(|l| l == expected_na_line),
        "missing the n/a Method line in:\n{doc}"
    );
    assert!(
        !doc.contains("separate counted repetitions"),
        "unexpected counted Method line in:\n{doc}"
    );

    // Adding the counted run: the syscalls/msg cell and the Method line
    // both switch to the counted figures.
    records.push(counted_record(ServerKind::Uring, 1, 1, 1));
    records.push(counted_record(ServerKind::Uring, 1, 1, 2));
    let doc = render(&records, true).expect("smoke render ok");
    assert!(
        doc.lines()
            .any(|l| l.starts_with("| uring |") && l.ends_with("| 80.0 | 1.25 | ok |")),
        "expected the counted uring row in:\n{doc}"
    );
    let expected_counted_line = "Both latency columns come from uncounted runs; `syscalls/msg` \
                                  comes from 2 separate counted repetitions at the same matched \
                                  80 % rate, which qualify on `syscalls/msg` spread. The counted \
                                  runs' p99 stays in the raw JSON and is not published.";
    assert!(
        doc.lines().any(|l| l == expected_counted_line),
        "missing the counted Method line in:\n{doc}"
    );
    assert!(
        !doc.contains("because this record set holds no counted run"),
        "unexpected n/a Method line in:\n{doc}"
    );
}
