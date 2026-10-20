//! Public-seam tests for `bench::analysis` (task 8 of the
//! benchmark-harness feature): derived metrics, the 30-row reconciliation
//! table, recomputation, percentile/spread/matched_rates, perf/strace
//! parsing, driver-fd counting, interleave ordering, and CPU percent
//! arithmetic (AC-27..AC-34, AC-48, AC-55, AC-56, AC-61).

use std::fs;
use std::os::unix::fs::symlink;
use std::path::PathBuf;

use bench::analysis::{
    config_failure, config_passes, count_driver_fds, cpu_count, cpu_percent, derive, interleave,
    matched_rates, median, metric, parse_cpu_pressure_some_total, parse_perf_count,
    parse_strace_total, percentile, reconcile, recompute,
    spread, GENERATOR_LATENESS_FRACTION, SYSCALL_FRACTION_PCT,
};
use bench::gen::slot_counts;
use bench::record::{Timeline, Window, LATENCY_BUCKET_MAX_NS};
use bench::run_record::{
    Build, ConfigKey, CounterExit, Counts, CpuSets, CpuUsage, DriverFds, Env, GeneratorErrors,
    HostCpuPressure, LatencySummary, Profile, RunKind, RunRecord, ServerKind, SyscallMethod,
    Syscalls, SCHEMA_VERSION,
};

/// Bound on counting a CPU list that names billions of CPUs: far above the
/// arithmetic count's microseconds, far below a CPU-by-CPU walk's billions
/// of steps.
const CPU_COUNT_BOUND: std::time::Duration = std::time::Duration::from_secs(1);

/// Smoke-protocol window: 2 s warm-up + 5 s window (PLAN.md ## Data Model).
const WARMUP_SECS: u64 = 2;
const WINDOW_SECS: u64 = 5;
const CONNECTIONS: u32 = 100;
const QOS1_WINDOW: u32 = 32;
const PAYLOAD_BYTES: u32 = 64;
const DRAIN_GRACE_SECS: u64 = 2;
const COUNTER_BOUNDARY_TOLERANCE_NS: u64 = 10_000_000;
const ONE_SECOND_NS: u64 = 1_000_000_000;

/// Whole-second origin so row 26's multiple-of-1e9 check passes.
const ORIGIN_NS: u64 = 1_700_000_000_000_000_000;
const WINDOW_START_NS: u64 = ORIGIN_NS + WARMUP_SECS * ONE_SECOND_NS;
const WINDOW_END_NS: u64 = WINDOW_START_NS + WINDOW_SECS * ONE_SECOND_NS;

fn window_obj() -> Window {
    Window {
        start_ns: WINDOW_START_NS,
        end_ns: WINDOW_END_NS,
    }
}

#[allow(clippy::too_many_lines)] // fixture builder covers every field the reconciliation table reads from
fn baseline_record(kind: RunKind, server: ServerKind) -> RunRecord {
    let (timeline_counts, delivered, offered, acked, scheduled, window_scheduled, last_write_start_ns) =
        match kind {
            RunKind::Throughput => {
                // 100 deliveries/s across warm-up + window; timeline covers
                // 7 bins so the recorder captures warm-up too.
                let counts = vec![100u64; 7];
                let delivered = 100 * (WARMUP_SECS + WINDOW_SECS);
                (counts, delivered, delivered, Some(delivered), None, None, None)
            }
            RunKind::Latency | RunKind::Counted => {
                let (scheduled, window_scheduled) =
                    slot_counts(200, ORIGIN_NS, window_obj());
                // The slot grid ends `SLOT_GRID_END_LEAD_NS` before the window
                // end, so the run delivers fewer than 200 × 7. The shortfall
                // sits in a warm-up bin to keep the window sum, and with it
                // the stored syscalls/msg, at 1000 deliveries.
                let mut counts = vec![200u64; 7];
                counts[0] -= 200 * (WARMUP_SECS + WINDOW_SECS) - scheduled;
                let delivered = scheduled;
                let last_write_start_ns = Some(WINDOW_END_NS - 1);
                (
                    counts,
                    delivered,
                    delivered,
                    Some(delivered),
                    Some(scheduled),
                    Some(window_scheduled),
                    last_write_start_ns,
                )
            }
            RunKind::Connections => {
                let counts = Vec::new();
                (counts, 0, 0, None, None, None, None)
            }
        };

    let latency_ns = if kind.is_open_loop() {
        Some(LatencySummary {
            p50: 40_000,
            p99: 80_000,
            max: 80_000,
            // Buckets chosen so percentile at 0.99 lands in the second bucket
            // (987 < 989 <= 998): the brief uses `[(40000,990),(80000,10)]`
            // and that has 990 samples already at 40000, which makes p99 == p50.
            histogram: vec![(40_000, 987), (80_000, 11)],
        })
    } else {
        None
    };
    let lateness = if kind.is_open_loop() {
        vec![(1_000, 998)]
    } else {
        Vec::new()
    };
    let lateness_p99_ns = if kind.is_open_loop() {
        Some(1_000)
    } else {
        None
    };
    let (cycles_in_window, connections_per_sec) = if kind == RunKind::Connections {
        (Some(50), Some(10.0))
    } else {
        (None, None)
    };
    let (throughput_msgs_per_sec, last10s_msgs_per_sec) = if kind == RunKind::Throughput {
        // `derive` uses timeline.sum_secs(start, end) / window_secs.
        #[allow(clippy::cast_precision_loss)]
        let whole = (100 * WINDOW_SECS) as f64 / WINDOW_SECS as f64;
        (Some(whole), Some(whole))
    } else {
        (None, None)
    };
    let (qos, rate_fraction_pct, offered_rate) = match kind {
        RunKind::Connections => (None, None, None),
        RunKind::Throughput => (Some(1u8), None, None),
        RunKind::Latency | RunKind::Counted => (Some(1u8), Some(SYSCALL_FRACTION_PCT), Some(200)),
    };
    let (syscalls_method, syscalls_count, syscalls_raw, syscalls_per_msg) = if kind == RunKind::Counted
    {
        (
            SyscallMethod::Perf,
            Some(5_000),
            Some("5000,,raw_syscalls:sys_enter,1000,100.00,,\n".to_string()),
            Some(5.0),
        )
    } else {
        (SyscallMethod::None, None, None, None)
    };
    let driver = if server == ServerKind::Uring {
        Some(DriverFds {
            io_uring_fds: 1,
            eventpoll_fds: 0,
        })
    } else {
        Some(DriverFds {
            io_uring_fds: 0,
            eventpoll_fds: 1,
        })
    };

    RunRecord {
        schema: SCHEMA_VERSION,
        profile: Profile::Smoke,
        server,
        workers: 1,
        qos,
        kind,
        rate_fraction_pct,
        offered_rate,
        repetition: 1,
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
            origin_sec: ORIGIN_NS / 1_000_000_000,
            counts: timeline_counts,
        },
        cycles_in_window,
        counts: Counts {
            scheduled,
            window_scheduled,
            offered,
            acked,
            delivered,
            dropped: Some(0),
            unresolved: 0,
            duplicates: 0,
            foreign: 0,
            latency_over_range: 0,
            clock_anomalies: 0,
        },
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
            method: syscalls_method,
            count: syscalls_count,
            per_msg: syscalls_per_msg,
            raw: syscalls_raw,
            enabled_ns: if kind == RunKind::Counted {
                Some(WINDOW_START_NS)
            } else {
                None
            },
            disabled_ns: if kind == RunKind::Counted {
                Some(WINDOW_END_NS)
            } else {
                None
            },
            exit: if kind == RunKind::Counted {
                Some(CounterExit::Signal(2))
            } else {
                None
            },
            diagnostics: None,
        },
        driver,
        cpu_sets: CpuSets {
            server: "0,2,4,6".to_string(),
            generator: "8-17".to_string(),
        },
        env: Env {
            kernel: "6.18.33.2-microsoft-standard-WSL2".to_string(),
            cpu_model: "Intel(R) Core(TM) i7-14650HX".to_string(),
            host: "test-host".to_string(),
            git_sha: "b219eb1".to_string(),
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
        started_at_unix: 1_791_273_600,
        harness_errors: Vec::new(),
        valid: true,
        invalid_reasons: Vec::new(),
        cancelled: false,
    }
}

fn valid_record(kind: RunKind) -> RunRecord {
    baseline_record(kind, ServerKind::Uring)
}

fn counted_run_record() -> RunRecord {
    valid_record(RunKind::Counted)
}

#[test]
fn a_counted_run_record_is_named_by_its_kind() {
    let record = counted_run_record();
    assert_eq!(record.file_name(), "uring-w1-q1-counted-p80-a1-r1.json");
}

#[test]
fn metric_reads_syscalls_per_message_for_a_counted_run() {
    let record = counted_run_record();
    assert_eq!(metric(&record), Some(5.0));

    // The counted spread ignores p99: these per_msg values are tight
    // (within SPREAD_LIMIT) while their p99s span a 16x range, far
    // above SPREAD_LIMIT, so a p99-based spread would fail this set.
    let per_msg = [5.0, 5.1, 4.9, 5.0, 5.0];
    let p99 = [40_000u64, 80_000, 160_000, 320_000, 640_000];
    let tight_records: Vec<RunRecord> = per_msg
        .iter()
        .zip(p99.iter())
        .zip(1u32..)
        .map(|((&pm, &p), rep)| {
            let mut r = counted_run_record();
            r.syscalls.per_msg = Some(pm);
            r.latency_ns = Some(LatencySummary {
                p50: p / 2,
                p99: p,
                max: p,
                histogram: vec![(p, 1)],
            });
            r.repetition = rep;
            r
        })
        .collect();
    assert!(config_passes(&tight_records, 5, true));

    let wide_per_msg = [5.0, 6.0, 5.0, 5.0, 5.0];
    let wide_records: Vec<RunRecord> = wide_per_msg
        .iter()
        .zip(1u32..)
        .map(|(&pm, rep)| {
            let mut r = counted_run_record();
            r.syscalls.per_msg = Some(pm);
            r.repetition = rep;
            r
        })
        .collect();
    assert!(!config_passes(&wide_records, 5, true));
}

#[test]
#[allow(clippy::too_many_lines)] // exercises every row of the 30-row reconciliation table in one place
fn reconcile_reports_one_reason_per_violated_rule() {
    // Each case: (label, kind, mutator, expected reason)
    type Mutator = Box<dyn Fn(&mut RunRecord)>;
    let cases: Vec<(&str, RunKind, Mutator, String)> = vec![
        // Row 1 — latency: scheduled must equal offered. Mutating offered
        // (and the matching delivered/acked so rows 2/3/13 stay clean) keeps
        // row 27's slot_counts check on `(Some(1398), Some(998))`. Every
        // delivered-count mutation also moves a warm-up timeline bin, so
        // row 29 stays clean and the window sum the metrics use is unchanged.
        (
            "row 1",
            RunKind::Latency,
            Box::new(|r| {
                r.counts.offered = 1397;
                r.counts.delivered = 1397;
                r.counts.acked = Some(1397);
                r.timeline.counts[0] -= 1;
            }),
            "scheduled Some(1398) != offered 1397".to_string(),
        ),
        // Row 2 — throughput/latency: delivered + dropped ≤ offered.
        (
            "row 2",
            RunKind::Throughput,
            Box::new(|r| {
                r.counts.delivered = r.counts.offered + 1;
                r.timeline.counts[0] += 1;
            }),
            "delivered + dropped exceeds offered".to_string(),
        ),
        // Row 3 — throughput/latency: unresolved = 0. Set directly so the
        // offered/delivered/dropped triple stays self-consistent.
        (
            "row 3",
            RunKind::Throughput,
            Box::new(|r| r.counts.unresolved = 1),
            "unresolved 1".to_string(),
        ),
        // Row 4 — all: duplicates = 0.
        (
            "row 4",
            RunKind::Throughput,
            Box::new(|r| r.counts.duplicates = 1),
            "duplicates 1".to_string(),
        ),
        // Row 5 — all: foreign = 0.
        (
            "row 5",
            RunKind::Throughput,
            Box::new(|r| r.counts.foreign = 1),
            "foreign payloads 1".to_string(),
        ),
        // Row 6 — throughput/latency: dropped != Some(0) invalidates. Drop
        // delivered by the same amount so rows 2 and 3 stay clean.
        (
            "row 6",
            RunKind::Throughput,
            Box::new(|r| {
                r.counts.delivered -= 1;
                r.counts.dropped = Some(1);
                r.timeline.counts[0] -= 1;
            }),
            "dropped 1 in a throughput run".to_string(),
        ),
        // Row 7 — throughput: tail and whole within SPREAD_LIMIT.
        (
            "row 7",
            RunKind::Throughput,
            Box::new(|r| {
                r.last10s_msgs_per_sec = Some(200.0);
                r.throughput_msgs_per_sec = Some(100.0);
            }),
            "not sustained: tail Some(200.0) vs window Some(100.0)".to_string(),
        ),
        // Row 8 — uring server: driver proof.
        (
            "row 8",
            RunKind::Throughput,
            Box::new(|r| {
                r.driver = Some(DriverFds {
                    io_uring_fds: 2,
                    eventpoll_fds: 0,
                });
            }),
            "driver proof failed: Some(DriverFds { io_uring_fds: 2, eventpoll_fds: 0 })".to_string(),
        ),
        // Row 9 — all: connect errors = 0.
        (
            "row 9",
            RunKind::Throughput,
            Box::new(|r| r.generator_errors.connect = 1),
            "connect errors 1".to_string(),
        ),
        // Row 10 — all: protocol errors = 0.
        (
            "row 10",
            RunKind::Throughput,
            Box::new(|r| r.generator_errors.protocol = 1),
            "protocol errors 1".to_string(),
        ),
        // Row 11 — latency: missed_slots = 0.
        (
            "row 11",
            RunKind::Latency,
            Box::new(|r| r.generator_errors.missed_slots = 1),
            "missed slots 1".to_string(),
        ),
        // Row 12 — all: stalled = 0.
        (
            "row 12",
            RunKind::Throughput,
            Box::new(|r| r.generator_errors.stalled = 1),
            "stalled 1 (publishes whose write did not complete or whose PUBACK never arrived, plus connection tasks still running at the hard deadline)".to_string(),
        ),
        // Row 13 — QoS 1 throughput/latency: acked = offered.
        (
            "row 13",
            RunKind::Throughput,
            Box::new(|r| r.counts.acked = Some(0)),
            "unacknowledged QoS 1 publishes: acked Some(0) of 700".to_string(),
        ),
        // Row 14 — latency: latency_ns must exist.
        (
            "row 14",
            RunKind::Latency,
            Box::new(|r| r.latency_ns = None),
            "no latency samples in the window".to_string(),
        ),
        // Row 15 — latency: histogram total = window_scheduled. Add one sample to
        // the histogram rather than moving `window_scheduled` so rows 25
        // (lateness total) and 27 (slot_counts check) stay clean.
        (
            "row 15",
            RunKind::Latency,
            Box::new(|r| {
                r.latency_ns = Some(LatencySummary {
                    p50: 40_000,
                    p99: 80_000,
                    max: 80_000,
                    histogram: vec![(40_000, 986), (80_000, 13)],
                });
            }),
            "latency samples 999 != window slots Some(998)".to_string(),
        ),
        // Row 16 — all: latency_over_range = 0.
        (
            "row 16",
            RunKind::Throughput,
            Box::new(|r| r.counts.latency_over_range = 1),
            "latency samples over range 1".to_string(),
        ),
        // Row 17 — all: clock_anomalies = 0.
        (
            "row 17",
            RunKind::Throughput,
            Box::new(|r| r.counts.clock_anomalies = 1),
            "clock anomalies 1".to_string(),
        ),
        // Row 18 — latency: lateness p99 within GENERATOR_LATENESS_FRACTION of latency p99.
        (
            "row 18",
            RunKind::Latency,
            Box::new(|r| r.lateness_p99_ns = Some(20_000)),
            format!(
                "generator lateness p99 Some(20000) ns exceeds {GENERATOR_LATENESS_FRACTION} of latency p99 80000"
            ),
        ),
        // Row 19 — counted syscall run: count must exist.
        (
            "row 19",
            RunKind::Counted,
            Box::new(|r| r.syscalls.count = None),
            "syscall count missing".to_string(),
        ),
        // Row 20 — connections: cycles_in_window > 0.
        (
            "row 20",
            RunKind::Connections,
            Box::new(|r| r.cycles_in_window = Some(0)),
            "no completed connection cycles".to_string(),
        ),
        // Row 21 — latency: lateness_over_range = 0. Reduce the lateness histogram
        // by one so row 25 (lateness + over_range = window_scheduled) still
        // holds after the bump.
        (
            "row 21",
            RunKind::Latency,
            Box::new(|r| {
                r.lateness = vec![(1_000, 997)];
                r.generator_errors.lateness_over_range = 1;
            }),
            "generator lateness over range 1".to_string(),
        ),
        // Row 22 — counted syscall run: boundary times within tolerance.
        (
            "row 22",
            RunKind::Counted,
            Box::new(|r| {
                r.syscalls.enabled_ns = Some(WINDOW_START_NS + COUNTER_BOUNDARY_TOLERANCE_NS + 1);
            }),
            format!(
                "counter boundaries outside tolerance: enabled Some({}) disabled Some({})",
                WINDOW_START_NS + COUNTER_BOUNDARY_TOLERANCE_NS + 1,
                WINDOW_END_NS
            ),
        ),
        // Row 23 — counted syscall run: exit = Code(0) or Signal(SIGINT).
        (
            "row 23",
            RunKind::Counted,
            Box::new(|r| r.syscalls.exit = Some(CounterExit::Code(1))),
            "syscall counter ended with Some(Code(1))".to_string(),
        ),
        // Row 24 — latency: last_write_start_ns < window.end_ns.
        (
            "row 24",
            RunKind::Latency,
            Box::new(|r| r.last_write_start_ns = Some(WINDOW_END_NS)),
            format!("publish write began at or after the window end: {WINDOW_END_NS}"),
        ),
        // Row 25 — latency: lateness total + lateness_over_range = window_scheduled.
        (
            "row 25",
            RunKind::Latency,
            Box::new(|r| r.lateness = vec![(1_000, 997)]),
            "lateness samples 997 != window slots Some(998)".to_string(),
        ),
        // Row 26 — all: window boundaries consistent with origin and warmup_secs/window_secs.
        (
            "row 26",
            RunKind::Throughput,
            Box::new(|r| r.warmup_secs = WARMUP_SECS + 1),
            "window inconsistent with origin and constants".to_string(),
        ),
        // Row 27 — latency: scheduled counts match slot_counts(offered_rate, …).
        (
            "row 27",
            RunKind::Latency,
            Box::new(|r| r.offered_rate = Some(201)),
            "scheduled counts inconsistent with rate Some(201)".to_string(),
        ),
        // Row 28 — each harness error becomes its own reason, verbatim.
        (
            "row 28",
            RunKind::Throughput,
            Box::new(|r| r.harness_errors.push("endpoint exited with 1".to_string())),
            "endpoint exited with 1".to_string(),
        ),
        // Row 29 — all: the timeline must hold exactly `delivered` deliveries.
        // Bumping a warm-up bin leaves the window sum — and with it every
        // stored rate — untouched, so only row 29 fires.
        (
            "row 29",
            RunKind::Throughput,
            Box::new(|r| r.timeline.counts[0] += 1),
            "timeline deliveries 701 != delivered 700".to_string(),
        ),
        // Row 30 — counted syscall run: the window must hold deliveries. Both
        // warm-up bins carry the whole run's 1398 deliveries, so row 29 still
        // sees the stored `delivered` and only row 30 fires.
        (
            "row 30",
            RunKind::Counted,
            Box::new(|r| r.timeline.counts = vec![700, 698, 0, 0, 0, 0, 0]),
            "no deliveries in the window to divide the syscall count by".to_string(),
        ),
    ];

    assert_eq!(cases.len(), 30, "the table must hold exactly 30 rows");

    for (label, kind, mutate, expected) in cases {
        let mut rec = valid_record(kind);
        mutate(&mut rec);
        let reasons = reconcile(&rec);
        assert_eq!(
            reasons,
            vec![expected.clone()],
            "case {label} produced reasons {reasons:?}, expected [{expected:?}]"
        );
    }
}

#[test]
fn reconcile_accepts_a_valid_counted_run() {
    assert_eq!(reconcile(&valid_record(RunKind::Counted)), Vec::<String>::new());
}

#[test]
fn reconcile_applies_every_open_loop_row_to_a_counted_run() {
    type Mutator = Box<dyn Fn(&mut RunRecord)>;
    let cases: Vec<(&str, Mutator, String)> = vec![
        // Row 1 — scheduled must equal offered.
        (
            "row 1",
            Box::new(|r| {
                r.counts.offered = 1397;
                r.counts.delivered = 1397;
                r.counts.acked = Some(1397);
                r.timeline.counts[0] -= 1;
            }),
            "scheduled Some(1398) != offered 1397".to_string(),
        ),
        // Row 2 — delivered + dropped must not exceed offered.
        (
            "row 2",
            Box::new(|r| {
                r.counts.delivered = r.counts.offered + 1;
                r.timeline.counts[0] += 1;
            }),
            "delivered + dropped exceeds offered".to_string(),
        ),
        // Row 3 — unresolved must be zero.
        (
            "row 3",
            Box::new(|r| r.counts.unresolved = 1),
            "unresolved 1".to_string(),
        ),
        // Row 6 — non-zero dropped invalidates.
        (
            "row 6",
            Box::new(|r| {
                r.counts.delivered -= 1;
                r.counts.dropped = Some(1);
                r.timeline.counts[0] -= 1;
            }),
            "dropped 1 in a counted run".to_string(),
        ),
        // Row 11 — no missed slots.
        (
            "row 11",
            Box::new(|r| r.generator_errors.missed_slots = 1),
            "missed slots 1".to_string(),
        ),
        // Row 13 — QoS 1: every offered must be acked.
        (
            "row 13",
            Box::new(|r| r.counts.acked = Some(0)),
            "unacknowledged QoS 1 publishes: acked Some(0) of 1398".to_string(),
        ),
        // Row 14 — latency_ns must be present.
        (
            "row 14",
            Box::new(|r| r.latency_ns = None),
            "no latency samples in the window".to_string(),
        ),
        // Row 15 — histogram total must equal window_scheduled.
        (
            "row 15",
            Box::new(|r| {
                r.latency_ns = Some(LatencySummary {
                    p50: 40_000,
                    p99: 80_000,
                    max: 80_000,
                    histogram: vec![(40_000, 986), (80_000, 13)],
                });
            }),
            "latency samples 999 != window slots Some(998)".to_string(),
        ),
        // Row 18 — lateness p99 within GENERATOR_LATENESS_FRACTION of latency p99.
        (
            "row 18",
            Box::new(|r| r.lateness_p99_ns = Some(20_000)),
            format!(
                "generator lateness p99 Some(20000) ns exceeds {GENERATOR_LATENESS_FRACTION} of latency p99 80000"
            ),
        ),
        // Row 21 — no lateness over-range.
        (
            "row 21",
            Box::new(|r| {
                r.lateness = vec![(1_000, 997)];
                r.generator_errors.lateness_over_range = 1;
            }),
            "generator lateness over range 1".to_string(),
        ),
        // Row 24 — last_write_start_ns must be Some(t < window.end_ns).
        (
            "row 24",
            Box::new(|r| r.last_write_start_ns = Some(WINDOW_END_NS)),
            format!("publish write began at or after the window end: {WINDOW_END_NS}"),
        ),
        // Row 25 — Σ lateness counts + lateness_over_range must equal window_scheduled.
        (
            "row 25",
            Box::new(|r| r.lateness = vec![(1_000, 997)]),
            "lateness samples 997 != window slots Some(998)".to_string(),
        ),
        // Row 27 — scheduled counts must equal slot_counts(offered_rate, origin, window).
        (
            "row 27",
            Box::new(|r| r.offered_rate = Some(201)),
            "scheduled counts inconsistent with rate Some(201)".to_string(),
        ),
    ];

    for (label, mutate, expected) in cases {
        let mut rec = valid_record(RunKind::Counted);
        mutate(&mut rec);
        let reasons = reconcile(&rec);
        assert_eq!(
            reasons,
            vec![expected.clone()],
            "case {label} produced reasons {reasons:?}, expected [{expected:?}]"
        );
    }
}

#[test]
fn reconcile_requires_a_counter_only_on_a_counted_run() {
    assert_eq!(
        reconcile(&valid_record(RunKind::Latency)),
        Vec::<String>::new()
    );

    let mut rec = valid_record(RunKind::Counted);
    rec.syscalls = Syscalls {
        method: SyscallMethod::None,
        count: None,
        per_msg: None,
        raw: None,
        enabled_ns: None,
        disabled_ns: None,
        exit: None,
        diagnostics: None,
    };
    assert_eq!(
        reconcile(&rec),
        vec![
            "syscall count missing".to_string(),
            "counter boundaries outside tolerance: enabled None disabled None".to_string(),
            "syscall counter ended with None".to_string(),
        ]
    );
}

#[test]
fn derive_keeps_the_latency_summary_of_a_counted_run() {
    let d = derive(&valid_record(RunKind::Counted));
    assert_eq!(
        d.latency_ns,
        Some(LatencySummary {
            p50: 40_000,
            p99: 80_000,
            max: 80_000,
            histogram: vec![(40_000, 987), (80_000, 11)],
        })
    );
}

/// AC-10: the counted run's latency summary is computed from its
/// histogram, not copied from the stored percentiles. The stored values
/// here disagree with the histogram, so a `derive` that returned them
/// would pass `derive_keeps_the_latency_summary_of_a_counted_run` and
/// fail this test. The same record's `syscalls/msg` is derived from the
/// raw counter line over the window deliveries (5000 / 1000).
#[test]
fn derive_recomputes_a_counted_runs_latency_from_its_histogram() {
    let mut rec = valid_record(RunKind::Counted);
    rec.latency_ns = Some(LatencySummary {
        p50: 1,
        p99: 2,
        max: 3,
        histogram: vec![(40_000, 987), (80_000, 11)],
    });

    let d = derive(&rec);

    assert_eq!(
        d.latency_ns,
        Some(LatencySummary {
            p50: 40_000,
            p99: 80_000,
            max: 80_000,
            histogram: vec![(40_000, 987), (80_000, 11)],
        })
    );
    assert_eq!(d.syscalls_per_msg, Some(5.0));
}

#[test]
fn reconcile_reports_every_rule_a_record_violates_at_once() {
    let mut rec = baseline_record(RunKind::Latency, ServerKind::Uring);
    rec.counts.duplicates = 2;
    rec.counts.foreign = 1;
    rec.generator_errors.stalled = 1;
    let reasons = reconcile(&rec);
    assert_eq!(
        reasons,
        vec![
            "duplicates 2".to_string(),
            "foreign payloads 1".to_string(),
            "stalled 1 (publishes whose write did not complete or whose PUBACK never arrived, plus connection tasks still running at the hard deadline)".to_string(),
        ]
    );
}

#[test]
fn reconcile_accepts_mosquitto_with_unknown_drops() {
    let mut rec = baseline_record(RunKind::Connections, ServerKind::Mosquitto);
    rec.counts.dropped = None;
    rec.driver = Some(DriverFds {
        io_uring_fds: 0,
        eventpoll_fds: 1,
    });
    assert_eq!(reconcile(&rec), Vec::<String>::new());
}

#[test]
fn reconcile_rejects_a_latency_rate_that_cannot_prove_the_schedule() {
    // No rate: nothing ties the stored slot counts to an offered rate.
    let mut no_rate = valid_record(RunKind::Latency);
    no_rate.offered_rate = None;
    assert_eq!(
        reconcile(&no_rate),
        vec!["no offered rate for a latency run".to_string()]
    );

    // A rate whose schedule exceeds a u64 is reported, not computed.
    let mut unrepresentable = valid_record(RunKind::Latency);
    unrepresentable.offered_rate = Some(u64::MAX);
    assert_eq!(
        reconcile(&unrepresentable),
        vec![format!(
            "offered rate {} schedules more slots than a u64 can hold",
            u64::MAX
        )]
    );
}

#[test]
fn derive_reports_no_throughput_rate_for_a_window_shorter_than_window_secs() {
    let mut rec = valid_record(RunKind::Throughput);
    rec.window = Window {
        start_ns: 0,
        end_ns: 1_000_000_000,
    };
    let d = derive(&rec);
    assert_eq!(d.throughput_msgs_per_sec, None);
    assert_eq!(d.last10s_msgs_per_sec, None);
}

#[test]
fn percentile_reads_the_cumulative_histogram() {
    let histogram = vec![(10u64, 50u64), (20, 49), (30, 1)];
    assert_eq!(percentile(&histogram, 0.5), Some(10));
    assert_eq!(percentile(&histogram, 0.99), Some(20));
    assert_eq!(percentile(&histogram, 1.0), Some(30));
}

#[test]
fn spread_is_range_over_median() {
    let loose_values = vec![100.0, 105.0, 110.0, 95.0, 100.0];
    assert_eq!(spread(&loose_values), Some(0.15));
    let tight_values = vec![100.0, 101.0, 99.0, 102.0, 100.0];
    assert_eq!(spread(&tight_values), Some(0.03));
}

#[test]
fn matched_rates_follow_the_slowest_competitor() {
    let competitors = vec![400_000.0, 250_000.0, 300_000.0];
    assert_eq!(matched_rates(&competitors), Some((125_000, 200_000)));
    // No competitor median: a latency row has no rate to pace at, and
    // `(0, 0)` would pace it at no rate at all.
    assert_eq!(matched_rates(&[]), None);
}

#[test]
fn median_is_the_middle_value_or_the_mean_of_the_middle_two() {
    // Unsorted input: the median sorts first.
    assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
    // Even count: the mean of the middle two, not either neighbour.
    assert_eq!(median(&[1.0, 2.0, 3.0, 4.0]), Some(2.5));
    assert_eq!(median(&[100.0, 102.0]), Some(101.0));
    assert_eq!(median(&[]), None);
}

#[test]
fn spread_has_no_value_without_a_nonzero_median() {
    // An empty repetition set and an all-zero one both divide by a
    // median of 0, which would read as an infinite or undefined spread
    // and pass or fail `config_passes` by accident.
    assert_eq!(spread(&[]), None);
    assert_eq!(spread(&[0.0, 0.0, 0.0]), None);
}

#[test]
fn metric_reads_each_run_kinds_headline_figure() {
    // Throughput → msgs/s, latency → p99 ns, connections → conn/s: the
    // figure `config_passes` measures the spread of.
    let mut throughput = valid_record(RunKind::Throughput);
    throughput.throughput_msgs_per_sec = Some(1234.0);
    assert_eq!(metric(&throughput), Some(1234.0));

    let latency = valid_record(RunKind::Latency);
    let p99 = latency
        .latency_ns
        .as_ref()
        .map(|l| l.p99)
        .expect("latency fixture carries a summary");
    assert_eq!(
        metric(&latency),
        Some(f64::from(u32::try_from(p99).expect("p99 fits in u32")))
    );

    let mut connections = valid_record(RunKind::Connections);
    connections.connections_per_sec = Some(10.0);
    assert_eq!(metric(&connections), Some(10.0));

    // A latency run without samples has no metric at all, so a
    // configuration cannot pass on a spread computed from nothing.
    let mut no_samples = valid_record(RunKind::Latency);
    no_samples.latency_ns = None;
    assert_eq!(metric(&no_samples), None);
}

#[test]
fn percentile_has_no_value_for_an_empty_histogram() {
    assert_eq!(percentile(&[], 0.99), None);
}

#[test]
fn config_passes_requires_every_repetition_valid_and_within_spread() {
    // 5 valid with tight spread (4/100 = 0.04 ≤ SPREAD_LIMIT).
    let tight = [100.0, 102.0, 98.0, 101.0, 99.0];
    let tight_records: Vec<RunRecord> = tight
        .iter()
        .zip(1u32..)
        .map(|(&v, rep)| {
            let mut r = valid_record(RunKind::Throughput);
            r.throughput_msgs_per_sec = Some(v);
            r.repetition = rep;
            r
        })
        .collect();
    assert!(config_passes(&tight_records, 5, true));

    // One invalid record makes the whole configuration fail.
    let mut invalid = valid_record(RunKind::Throughput);
    invalid.throughput_msgs_per_sec = Some(101.0);
    invalid.valid = false;
    let mut mostly_tight = tight_records.clone();
    mostly_tight.push(invalid);
    assert!(!config_passes(&mostly_tight, 5, true));

    // Spread 0.15 fails under enforcement, passes without it.
    let wide = [100.0, 105.0, 110.0, 95.0, 100.0];
    let wide_records: Vec<RunRecord> = wide
        .iter()
        .zip(1u32..)
        .map(|(&v, rep)| {
            let mut r = valid_record(RunKind::Throughput);
            r.throughput_msgs_per_sec = Some(v);
            r.repetition = rep;
            r
        })
        .collect();
    assert!(!config_passes(&wide_records, 5, true));
    assert!(config_passes(&wide_records, 5, false));

    // Wrong repetition count fails regardless of spread.
    let fewer: Vec<RunRecord> = tight_records.iter().take(4).cloned().collect();
    assert!(!config_passes(&fewer, 5, true));
}

#[test]
fn config_passes_refuses_an_attempt_with_a_cancelled_run() {
    // A run cut short by a signal is no repetition: one cancelled run
    // among five otherwise-passing ones must fail the whole attempt.
    let throughputs = [100.0, 102.0, 98.0, 101.0, 99.0];
    let mut runs: Vec<RunRecord> = throughputs
        .iter()
        .zip(1u32..=5)
        .map(|(&v, rep)| {
            let mut r = valid_record(RunKind::Throughput);
            r.throughput_msgs_per_sec = Some(v);
            r.repetition = rep;
            r
        })
        .collect();
    assert!(config_passes(&runs, 5, true));

    let mut cancelled = serde_json::to_value(&runs[4]).expect("serialize the run");
    cancelled
        .as_object_mut()
        .expect("run is a JSON object")
        .insert("cancelled".to_string(), serde_json::Value::Bool(true));
    runs[4] = serde_json::from_value(cancelled).expect("deserialize the cancelled run");
    assert!(!config_passes(&runs, 5, true));
}

#[test]
fn config_passes_refuses_an_attempt_that_repeats_a_repetition() {
    let throughputs = [100.0, 102.0, 98.0, 101.0, 99.0];

    // Repetition 1 appears twice; repetition 5 never appears.
    let bad_reps = [1u32, 1, 2, 3, 4];
    let runs: Vec<RunRecord> = throughputs
        .iter()
        .zip(bad_reps.iter())
        .map(|(&v, &rep)| {
            let mut r = valid_record(RunKind::Throughput);
            r.throughput_msgs_per_sec = Some(v);
            r.repetition = rep;
            r
        })
        .collect();
    assert!(!config_passes(&runs, 5, true));

    // Same metrics, repetitions 1..=5: the attempt is complete.
    let runs: Vec<RunRecord> = throughputs
        .iter()
        .zip(1u32..=5)
        .map(|(&v, rep)| {
            let mut r = valid_record(RunKind::Throughput);
            r.throughput_msgs_per_sec = Some(v);
            r.repetition = rep;
            r
        })
        .collect();
    assert!(config_passes(&runs, 5, true));
}

#[test]
fn config_passes_refuses_an_attempt_whose_repetition_numbers_are_not_one_to_n() {
    // Five records can carry five distinct numbers that are still not the
    // set `1..=5`: a gap above the count, or a zero-based run. Neither is a
    // complete attempt, and neither is caught by counting the records.
    let throughputs = [100.0, 102.0, 98.0, 101.0, 99.0];
    let build = |reps: [u32; 5]| -> Vec<RunRecord> {
        throughputs
            .iter()
            .zip(reps.iter())
            .map(|(&v, &rep)| {
                let mut r = valid_record(RunKind::Throughput);
                r.throughput_msgs_per_sec = Some(v);
                r.repetition = rep;
                r
            })
            .collect()
    };

    // Repetition 5 never ran; a sixth one did.
    assert!(!config_passes(&build([1, 2, 3, 4, 6]), 5, true));
    // Zero-based: repetition 5 is missing again.
    assert!(!config_passes(&build([0, 1, 2, 3, 4]), 5, true));
    // The same five metrics over `1..=5`: the attempt is complete.
    assert!(config_passes(&build([1, 2, 3, 4, 5]), 5, true));
}

#[test]
fn config_passes_refuses_a_repetition_without_its_metric() {
    let mut runs: Vec<RunRecord> = [100.0, 102.0, 98.0, 101.0]
        .iter()
        .zip(1u32..)
        .map(|(&v, rep)| {
            let mut r = valid_record(RunKind::Throughput);
            r.throughput_msgs_per_sec = Some(v);
            r.repetition = rep;
            r
        })
        .collect();
    let mut fifth = valid_record(RunKind::Throughput);
    fifth.repetition = 5;
    fifth.throughput_msgs_per_sec = None;
    runs.push(fifth);
    assert!(!config_passes(&runs, 5, true));

    runs.last_mut().expect("fifth run").throughput_msgs_per_sec = Some(99.0);
    assert!(config_passes(&runs, 5, true));
}

#[test]
fn config_failure_names_the_gate_condition_that_refused_the_attempt() {
    // The verdict `config_passes` reports is `config_failure(..).is_none()`,
    // and the matrix prints the reason for an attempt that did not count:
    // each gate condition must name itself there.
    let build = |values: [Option<f64>; 5], reps: [u32; 5]| -> Vec<RunRecord> {
        values
            .iter()
            .zip(reps.iter())
            .map(|(&v, &rep)| {
                let mut r = valid_record(RunKind::Throughput);
                r.throughput_msgs_per_sec = v;
                r.repetition = rep;
                r
            })
            .collect()
    };
    let tight = [
        Some(100.0),
        Some(102.0),
        Some(98.0),
        Some(101.0),
        Some(99.0),
    ];
    let reps = [1, 2, 3, 4, 5];

    let passing = build(tight, reps);
    assert_eq!(config_failure(&passing, 5, true), None);

    // A cancelled run, read before the repetition set it also breaks.
    let mut cancelled_runs = passing.clone();
    let mut cancelled = serde_json::to_value(&cancelled_runs[4]).expect("serialize the run");
    cancelled
        .as_object_mut()
        .expect("run is a JSON object")
        .insert("cancelled".to_string(), serde_json::Value::Bool(true));
    cancelled_runs[4] = serde_json::from_value(cancelled).expect("deserialize the cancelled run");
    let reason = config_failure(&cancelled_runs, 5, true).expect("a cancelled run must fail");
    assert!(
        reason.contains("repetition 5 was cancelled"),
        "reason must name the cancelled repetition: {reason}"
    );

    // An incomplete repetition set names the numbers it holds.
    let reason =
        config_failure(&build(tight, [1, 2, 3, 4, 6]), 5, true).expect("a gap must fail");
    assert!(
        reason.contains("[1, 2, 3, 4, 6]") && reason.contains("1..=5"),
        "reason must name the repetitions and the set they are not: {reason}"
    );

    // An invalid run names its repetition and the stored reasons.
    let mut invalid_runs = passing.clone();
    invalid_runs[2].valid = false;
    invalid_runs[2].invalid_reasons = vec!["stalled connections 3".to_string()];
    let reason = config_failure(&invalid_runs, 5, true).expect("an invalid run must fail");
    assert!(
        reason.contains("repetition 3 is invalid") && reason.contains("stalled connections 3"),
        "reason must name the invalid repetition and why: {reason}"
    );

    // A missing metric is read only under enforcement.
    let no_metric = build(
        [Some(100.0), Some(102.0), Some(98.0), Some(101.0), None],
        reps,
    );
    let reason = config_failure(&no_metric, 5, true).expect("a missing metric must fail");
    assert!(
        reason.contains("repetition 5 has no"),
        "reason must name the repetition without a metric: {reason}"
    );
    assert_eq!(config_failure(&no_metric, 5, false), None);

    // A spread above the limit names the spread it measured.
    let wide = build(
        [
            Some(100.0),
            Some(105.0),
            Some(110.0),
            Some(95.0),
            Some(100.0),
        ],
        reps,
    );
    let reason = config_failure(&wide, 5, true).expect("a wide spread must fail");
    assert!(
        reason.contains("spread") && reason.contains("0.15"),
        "reason must name the spread it measured: {reason}"
    );
    assert_eq!(config_failure(&wide, 5, false), None);
}

/// Five `valid_record(RunKind::Throughput)` runs with `repetition` 1..=5,
/// `runs[2]` marked invalid with `invalid_reasons: ["stalled connections 3"]`.
fn invalid_pressure_runs() -> Vec<RunRecord> {
    let mut runs: Vec<RunRecord> = (1u32..=5)
        .map(|rep| {
            let mut r = valid_record(RunKind::Throughput);
            r.repetition = rep;
            r
        })
        .collect();
    runs[2].valid = false;
    runs[2].invalid_reasons = vec!["stalled connections 3".to_string()];
    runs
}

#[test]
fn an_invalid_repetition_names_the_host_pressure_it_was_measured_under() {
    let mut runs = invalid_pressure_runs();

    runs[2].host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: 5_000_000_000,
        some_stall_us: 2_885_000,
    });
    assert_eq!(
        config_failure(&runs, 5, true),
        Some(
            "repetition 3 is invalid: stalled connections 3; host CPU pressure 57.7 % over the sampled interval"
                .to_string()
        )
    );

    runs[2].host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: 5_000_000_000,
        some_stall_us: 20_000,
    });
    assert_eq!(
        config_failure(&runs, 5, true),
        Some(
            "repetition 3 is invalid: stalled connections 3; host CPU pressure 0.4 % over the sampled interval"
                .to_string()
        )
    );
}

#[test]
fn an_invalid_repetition_rounds_host_pressure_to_one_decimal_place() {
    let mut runs = invalid_pressure_runs();

    runs[2].host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: 5_000_000_000,
        some_stall_us: 150_000,
    });
    assert_eq!(
        config_failure(&runs, 5, true),
        Some(
            "repetition 3 is invalid: stalled connections 3; host CPU pressure 3.0 % over the sampled interval"
                .to_string()
        )
    );

    runs[2].host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: 5_000_000_000,
        some_stall_us: 150_500,
    });
    assert_eq!(
        config_failure(&runs, 5, true),
        Some(
            "repetition 3 is invalid: stalled connections 3; host CPU pressure 3.0 % over the sampled interval"
                .to_string()
        )
    );

    runs[2].host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: 5_000_000_000,
        some_stall_us: 155_000,
    });
    assert_eq!(
        config_failure(&runs, 5, true),
        Some(
            "repetition 3 is invalid: stalled connections 3; host CPU pressure 3.1 % over the sampled interval"
                .to_string()
        )
    );
}

#[test]
fn an_invalid_repetition_says_when_host_pressure_was_not_measured() {
    let mut runs = invalid_pressure_runs();

    runs[2].host_cpu_pressure = None;
    assert_eq!(
        config_failure(&runs, 5, true),
        Some(
            "repetition 3 is invalid: stalled connections 3; host CPU pressure was not measured"
                .to_string()
        )
    );

    runs[2].host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: 0,
        some_stall_us: 7,
    });
    assert_eq!(
        config_failure(&runs, 5, true),
        Some(
            "repetition 3 is invalid: stalled connections 3; host CPU pressure was not measured"
                .to_string()
        )
    );
}

/// The pressure every run of [`pressure_runs`] carries: 57.7 % of a 5 s
/// interval, a value no other branch of `config_failure` may print.
const WIDE_PRESSURE: HostCpuPressure = HostCpuPressure {
    sampled_ns: 5_000_000_000,
    some_stall_us: 2_885_000,
};

/// Five valid `valid_record(RunKind::Throughput)` runs with the given
/// throughput metrics and repetition numbers, each carrying [`WIDE_PRESSURE`].
fn pressure_runs(values: [f64; 5], reps: [u32; 5]) -> Vec<RunRecord> {
    values
        .iter()
        .zip(reps.iter())
        .map(|(&value, &rep)| {
            let mut r = valid_record(RunKind::Throughput);
            r.throughput_msgs_per_sec = Some(value);
            r.repetition = rep;
            r.host_cpu_pressure = Some(WIDE_PRESSURE);
            r
        })
        .collect()
}

#[test]
fn only_the_invalid_repetition_branch_names_the_host_pressure() {
    // The clause is evidence about an invalid run. The other refusals read no
    // single run's record, so none of them may claim a host condition.
    let tight = [100.0, 102.0, 98.0, 101.0, 99.0];
    let reps = [1, 2, 3, 4, 5];
    assert_eq!(config_failure(&pressure_runs(tight, reps), 5, true), None);

    let gap = config_failure(&pressure_runs(tight, [1, 2, 3, 4, 6]), 5, true)
        .expect("a gap must fail");
    assert!(
        !gap.contains("host CPU pressure"),
        "an incomplete repetition set must name no host pressure: {gap}"
    );

    let wide = config_failure(
        &pressure_runs([100.0, 105.0, 110.0, 95.0, 100.0], reps),
        5,
        true,
    )
    .expect("a wide spread must fail");
    assert!(
        !wide.contains("host CPU pressure"),
        "a wide spread must name no host pressure: {wide}"
    );

    let mut no_metric = pressure_runs(tight, reps);
    no_metric[4].throughput_msgs_per_sec = None;
    let missing = config_failure(&no_metric, 5, true).expect("a missing metric must fail");
    assert!(
        !missing.contains("host CPU pressure"),
        "a missing metric must name no host pressure: {missing}"
    );
}

#[test]
fn an_invalid_repetition_rounds_host_pressure_to_the_nearest_tenth() {
    let mut runs = invalid_pressure_runs();

    // 2.96 %: a clause that cut the digit instead of rounding prints 2.9.
    runs[2].host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: 5_000_000_000,
        some_stall_us: 148_000,
    });
    assert_eq!(
        config_failure(&runs, 5, true),
        Some(
            "repetition 3 is invalid: stalled connections 3; host CPU pressure 3.0 % over the sampled interval"
                .to_string()
        )
    );

    // 0.96 %: the same distinction below one percent.
    runs[2].host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: 5_000_000_000,
        some_stall_us: 48_000,
    });
    assert_eq!(
        config_failure(&runs, 5, true),
        Some(
            "repetition 3 is invalid: stalled connections 3; host CPU pressure 1.0 % over the sampled interval"
                .to_string()
        )
    );

    // 0.04 %: a pressure below the printed precision still prints a value.
    runs[2].host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: 5_000_000_000,
        some_stall_us: 2_000,
    });
    assert_eq!(
        config_failure(&runs, 5, true),
        Some(
            "repetition 3 is invalid: stalled connections 3; host CPU pressure 0.0 % over the sampled interval"
                .to_string()
        )
    );
}

#[test]
fn an_invalid_repetition_ends_with_the_host_pressure_after_every_reason() {
    let mut runs = invalid_pressure_runs();
    runs[2].invalid_reasons = vec![
        "stalled connections 3".to_string(),
        "unresolved 11".to_string(),
    ];
    runs[2].host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: 5_000_000_000,
        some_stall_us: 20_000,
    });
    assert_eq!(
        config_failure(&runs, 5, true),
        Some(
            "repetition 3 is invalid: stalled connections 3; unresolved 11; host CPU pressure 0.4 % over the sampled interval"
                .to_string()
        )
    );
}

#[test]
fn an_invalid_repetition_names_a_zero_a_full_and_an_overflowing_host_pressure() {
    let mut runs = invalid_pressure_runs();

    // No stall over the whole interval.
    runs[2].host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: 5_000_000_000,
        some_stall_us: 0,
    });
    assert_eq!(
        config_failure(&runs, 5, true),
        Some(
            "repetition 3 is invalid: stalled connections 3; host CPU pressure 0.0 % over the sampled interval"
                .to_string()
        )
    );

    // A stall over all of it.
    runs[2].host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: 5_000_000_000,
        some_stall_us: 5_000_000,
    });
    assert_eq!(
        config_failure(&runs, 5, true),
        Some(
            "repetition 3 is invalid: stalled connections 3; host CPU pressure 100.0 % over the sampled interval"
                .to_string()
        )
    );

    // A counter growth no interval could hold: the clause states a number,
    // never `inf`, `NaN` or a panic.
    runs[2].host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: 1,
        some_stall_us: u64::MAX,
    });
    let message = config_failure(&runs, 5, true).expect("an invalid run must fail");
    assert!(
        message.starts_with("repetition 3 is invalid: stalled connections 3; host CPU pressure "),
        "an unbounded pressure must keep the clause's shape: {message}"
    );
    assert!(
        message.ends_with(" % over the sampled interval"),
        "an unbounded pressure must keep the clause's shape: {message}"
    );
    assert!(
        !message.contains("inf") && !message.contains("NaN"),
        "the clause must state a number: {message}"
    );
}

#[test]
fn cpu_pressure_reads_a_some_total_in_sparse_text() {
    // No trailing newline, as a file without one gives it.
    assert_eq!(
        parse_cpu_pressure_some_total("some avg10=0.00 avg60=0.00 avg300=0.00 total=42"),
        Some(42)
    );
    // Only the two fields the counter needs.
    assert_eq!(parse_cpu_pressure_some_total("some total=42\n"), Some(42));
    // A `some` line without a `total=` is unmeasured, never the next line's.
    assert_eq!(
        parse_cpu_pressure_some_total("some avg10=0.00\nfull total=9\n"),
        None
    );
}

#[test]
fn a_some_total_outside_a_u64_is_unmeasured() {
    assert_eq!(
        parse_cpu_pressure_some_total("some avg10=0.00 total=18446744073709551615\n"),
        Some(u64::MAX)
    );
    let cases = [
        "some avg10=0.00 total=18446744073709551616\n",
        "some avg10=0.00 total=\n",
        "some avg10=0.00 total=-1\n",
        "some avg10=0.00 total=1.5\n",
    ];
    for text in cases {
        assert_eq!(parse_cpu_pressure_some_total(text), None, "{text:?}");
    }
}

#[test]
fn perf_count_reads_the_csv_counter() {
    let counted = "31000000,,raw_syscalls:sys_enter,60001234567,100.00,,\n";
    assert_eq!(parse_perf_count(counted), Some(31_000_000));
    let not_counted = "<not counted>,,raw_syscalls:sys_enter,0,0.00,,\n";
    assert_eq!(parse_perf_count(not_counted), None);
}

#[test]
fn strace_total_reads_the_calls_column() {
    let with_errors = "100.00    0.123456           1    123456        12 total";
    assert_eq!(parse_strace_total(with_errors), Some(123_456));
    let without_errors = "100.00    0.123456           1    123456           total";
    assert_eq!(parse_strace_total(without_errors), Some(123_456));
}

#[test]
fn driver_fds_count_uring_and_eventpoll_links() {
    let dir = tempdir_in_target();
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("mkdir");
    for (i, target) in [
        ("0", "anon_inode:[io_uring]"),
        ("1", "anon_inode:[io_uring]"),
        ("2", "anon_inode:[eventfd]"),
        ("3", "socket:[12]"),
    ] {
        symlink(target, dir.join(i)).expect("symlink");
    }
    let counts = count_driver_fds(&dir).expect("count");
    assert_eq!(
        counts,
        DriverFds {
            io_uring_fds: 2,
            eventpoll_fds: 0
        }
    );
    symlink("anon_inode:[eventpoll]", dir.join("4")).expect("symlink");
    let counts = count_driver_fds(&dir).expect("count");
    assert_eq!(
        counts,
        DriverFds {
            io_uring_fds: 2,
            eventpoll_fds: 1
        }
    );
    let _ = fs::remove_dir_all(&dir);
}

fn tempdir_in_target() -> PathBuf {
    let mut p = std::env::temp_dir();
    let unique = format!(
        "bench-driver-fds-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    );
    p.push(unique);
    p
}

#[test]
fn recompute_flags_a_stored_metric_that_disagrees_with_the_raw_timeline() {
    let mut rec = valid_record(RunKind::Throughput);
    rec.throughput_msgs_per_sec = Some(101.0);
    let mismatches = recompute(&rec);
    assert_eq!(mismatches.len(), 1);
    assert!(mismatches[0].starts_with("throughput_msgs_per_sec:"));

    let clean = valid_record(RunKind::Throughput);
    assert!(recompute(&clean).is_empty());
}

#[test]
fn recompute_flags_a_lateness_p99_or_syscall_count_that_disagrees_with_its_raw_data() {
    let mut lateness_mismatch = valid_record(RunKind::Latency);
    lateness_mismatch.lateness_p99_ns = Some(500);
    let mismatches = recompute(&lateness_mismatch);
    assert_eq!(mismatches.len(), 1);
    assert!(mismatches[0].starts_with("lateness_p99_ns:"));

    let mut syscall_mismatch = valid_record(RunKind::Latency);
    syscall_mismatch.syscalls.count = Some(4000);
    let mismatches = recompute(&syscall_mismatch);
    assert!(mismatches.iter().any(|m| m.starts_with("syscalls.count:")));
}

#[test]
fn interleave_is_repetition_major() {
    let configs = vec!["A".to_string(), "B".to_string()];
    let interleaved = interleave(&configs, 2);
    assert_eq!(
        interleaved,
        vec![
            ("A".to_string(), 1u32),
            ("B".to_string(), 1u32),
            ("A".to_string(), 2u32),
            ("B".to_string(), 2u32),
        ]
    );
}

#[test]
fn cpu_percent_scales_ticks_by_the_cpu_set_size() {
    assert_eq!(cpu_count("0,2,4,6"), Some(4));
    assert_eq!(cpu_count("8-17"), Some(10));
    // A CPU list is an affinity mask: a repeated CPU and overlapping ranges
    // name the same CPU once (`taskset -c 0,0` gives the process one CPU).
    assert_eq!(cpu_count("0,0"), Some(1));
    assert_eq!(cpu_count("0-3,2-5"), Some(6));
    assert_eq!(cpu_count("x"), None);
    assert_eq!(cpu_count("5-4"), None);
    // An empty element is unparsable, not an element to skip.
    assert_eq!(cpu_count("0,,2"), None);
    // 22 800 ticks at 100 Hz = 228 CPU-seconds; over a 60 s sampled
    // interval on 4 CPUs that is 95 %. A sampled interval longer than the
    // declared window reports the lower use the ticks prove, not 95 %.
    assert_eq!(cpu_percent(22_800, 100, 60 * ONE_SECOND_NS, 4), Some(95.0));
    assert_eq!(cpu_percent(22_800, 100, 76 * ONE_SECOND_NS, 4), Some(75.0));
    assert_eq!(cpu_percent(22_800, 100, 0, 4), None);
}

#[test]
fn cpu_count_counts_every_cpu_a_stride_names() {
    assert_eq!(cpu_count("0-10:2"), Some(6));
    assert_eq!(cpu_count("0-3:1"), Some(4));
    assert_eq!(cpu_count("1-1:3"), Some(1));
    assert_eq!(cpu_count("0-2:5"), Some(1));
    assert_eq!(cpu_count("0-10:2,1"), Some(7));
    assert_eq!(cpu_count("0-10:2,4-6"), Some(7));
    assert_eq!(cpu_count("0-12:3,0-12:4"), Some(7));
    assert_eq!(cpu_count("0-1000000:2,1-999999:2"), Some(1_000_001));
    // The last stride step would pass u32::MAX.
    assert_eq!(cpu_count("4294967290-4294967295:2"), Some(3));
}

#[test]
fn cpu_count_counts_a_stride_across_billions_of_cpus_without_walking_it() {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        tx.send(cpu_count("0-4000000000:2,0-99")).unwrap();
    });
    assert_eq!(rx.recv_timeout(CPU_COUNT_BOUND), Ok(Some(2_000_000_051)));
}

#[test]
fn cpu_count_counts_each_cpu_once_when_several_strides_name_it() {
    // Three strides over the same range: the union of the multiples of 2, 3
    // and 5 up to 30 is 16 + 11 + 7 − 6 − 4 − 3 + 2 = 23 CPUs.
    assert_eq!(cpu_count("0-30:2,0-30:3,0-30:5"), Some(23));
    // The same stride twice names the same CPUs, so the count is one stride's.
    assert_eq!(cpu_count("0-10:2,0-10:2"), Some(6));
    // Strides that overlap over part of their ranges and start on different
    // CPUs: {0,2,4,6,8,10} ∪ {5,8,11,14} is 9 CPUs.
    assert_eq!(cpu_count("0-10:2,5-15:3"), Some(9));
    // A plain range covering part of a stride: {2,7,12,17} ∪ {0..=6} is 10.
    assert_eq!(cpu_count("2-20:5,0-6"), Some(10));
}

#[test]
fn cpu_count_rejects_a_cpu_number_past_u32() {
    // taskset truncates a larger number to 32 bits (`4294967296` names CPU
    // 0); `cpu_count` does not copy that truncation, for a plain CPU id, a
    // range bound or a stride.
    assert_eq!(cpu_count("4294967296"), None);
    assert_eq!(cpu_count("0-4294967296"), None);
    assert_eq!(cpu_count("0-10:4294967296"), None);
    assert_eq!(cpu_count("0-3,4294967296"), None);
}

#[test]
fn cpu_count_counts_the_whole_cpu_line_without_overflowing() {
    // `0-4294967295` names 2^32 CPUs, one past `u32`: the count saturates
    // instead of overflowing.
    assert_eq!(cpu_count("0-4294967295"), Some(u32::MAX));
}

#[test]
fn cpu_count_rejects_stride_forms_outside_the_documented_grammar() {
    assert_eq!(cpu_count("0-3:0"), None);
    assert_eq!(cpu_count("0:2"), None);
    assert_eq!(cpu_count("0-3:"), None);
    assert_eq!(cpu_count("0-3:x"), None);
    assert_eq!(cpu_count("0-3:2:1"), None);
    assert_eq!(cpu_count("0:2,1"), None);
    assert_eq!(cpu_count("0-3:2:1,4"), None);
    // AC-5 holds wherever the faulty element sits: the same forms after a
    // well-formed element are refused too, and so is a trailing empty one.
    assert_eq!(cpu_count("1,0:2"), None);
    assert_eq!(cpu_count("4,0-3:2:1"), None);
    assert_eq!(cpu_count("0-3,4-7:0"), None);
    assert_eq!(cpu_count("0-3:2,"), None);
}

#[test]
fn derive_matches_stored_fields_for_a_valid_latency_record() {
    let rec = valid_record(RunKind::Latency);
    let d = derive(&rec);
    assert_eq!(d.throughput_msgs_per_sec, None);
    assert_eq!(d.connections_per_sec, None);
    assert_eq!(d.lateness_p99_ns, rec.lateness_p99_ns);
    assert_eq!(d.syscall_count, rec.syscalls.count);
    assert_eq!(d.syscalls_per_msg, rec.syscalls.per_msg);
    assert_eq!(d.unresolved, rec.counts.unresolved);
    assert_eq!(
        d.latency_ns.as_ref().map(|l| l.p99),
        rec.latency_ns.as_ref().map(|l| l.p99)
    );
    assert_eq!(
        rec.key(),
        ConfigKey {
            server: rec.server,
            workers: rec.workers,
            qos: rec.qos,
            kind: rec.kind,
            rate_fraction_pct: rec.rate_fraction_pct,
        }
    );
}

#[test]
fn reconcile_rejects_a_latency_window_whose_start_follows_its_end() {
    // A deserialized window can name a start after its end; row 27 must report
    // it instead of underflowing the slot span inside `slot_counts`.
    let mut rec = valid_record(RunKind::Latency);
    rec.window = Window {
        start_ns: WINDOW_END_NS,
        end_ns: WINDOW_START_NS,
    };
    assert!(reconcile(&rec).contains(&format!(
        "window start {WINDOW_END_NS} is after window end {WINDOW_START_NS}"
    )));
}

/// Whole-second origin whose `origin_ns + warmup_secs × 1e9` leaves the
/// `u64` range, so a saturating row 26 compared `u64::MAX` against
/// `u64::MAX` and accepted the window.
const OVERFLOWING_ORIGIN_NS: u64 = 18_446_744_073_000_000_000;

#[test]
fn reconcile_rejects_a_window_whose_boundaries_overflow_the_constants() {
    // Row 26 computes both boundaries with checked arithmetic: with
    // saturating legs, this record's expected start and end both
    // saturated to `u64::MAX` and matched the stored boundaries, so a
    // connections run reported 10 conn/s over a window of zero duration.
    let mut rec = valid_record(RunKind::Connections);
    rec.origin_ns = OVERFLOWING_ORIGIN_NS;
    rec.window = Window {
        start_ns: u64::MAX,
        end_ns: u64::MAX,
    };
    assert_eq!(
        reconcile(&rec),
        vec!["window inconsistent with origin and constants".to_string()]
    );
    // The derived conn/s still follows the stored `cycles_in_window` and
    // `window_secs`, so nothing but row 26 stands between that figure
    // and a valid verdict.
    assert_eq!(derive(&rec).connections_per_sec, rec.connections_per_sec);
}

#[test]
fn reconcile_rejects_a_window_that_ends_where_it_starts() {
    // A window whose end does not follow its start holds no measurement
    // span for any kind, so row 26 reports it without waiting for the
    // latency-only slot-span check of row 27.
    let mut rec = valid_record(RunKind::Connections);
    rec.window_secs = 0;
    rec.window = Window {
        start_ns: WINDOW_START_NS,
        end_ns: WINDOW_START_NS,
    };
    assert!(reconcile(&rec)
        .contains(&"window inconsistent with origin and constants".to_string()));
}

#[test]
fn reconcile_rejects_histogram_buckets_it_cannot_sum_or_order() {
    // Counts that overflow a u64 are reported, not wrapped.
    let overflowing_buckets = vec![(1u64, u64::MAX), (2, 1)];
    let mut overflowing = valid_record(RunKind::Latency);
    overflowing.latency_ns = Some(LatencySummary {
        p50: 1,
        p99: 1,
        max: 1,
        histogram: overflowing_buckets.clone(),
    });
    assert!(reconcile(&overflowing)
        .contains(&"latency histogram bucket counts overflow a u64".to_string()));
    assert_eq!(percentile(&overflowing_buckets, 0.99), None);

    // Descending buckets name a p99 of 40000 when 11 of the 998 samples sit
    // at 80000, so the buckets are reported instead of read.
    let descending_buckets = vec![(80_000u64, 11u64), (40_000, 987)];
    let mut unsorted = valid_record(RunKind::Latency);
    unsorted.latency_ns = Some(LatencySummary {
        p50: 40_000,
        p99: 40_000,
        max: 40_000,
        histogram: descending_buckets.clone(),
    });
    assert!(reconcile(&unsorted)
        .contains(&"latency histogram buckets out of ascending order".to_string()));
    assert_eq!(percentile(&descending_buckets, 0.99), None);

    // The lateness histogram carries the same rule on its own row.
    let mut unsorted_lateness = valid_record(RunKind::Latency);
    unsorted_lateness.lateness = vec![(2_000, 499), (1_000, 499)];
    assert!(reconcile(&unsorted_lateness)
        .contains(&"lateness histogram buckets out of ascending order".to_string()));
}

#[test]
fn reconcile_rejects_a_histogram_bucket_the_recorder_cannot_hold() {
    // `u64::MAX` keeps the sample total, the over-range count and the stored
    // summary self-consistent, so only the bucket value itself shows that the
    // bounded histogram never recorded it.
    let out_of_range_buckets = vec![(40_000u64, 987u64), (u64::MAX, 11)];
    let mut over = valid_record(RunKind::Latency);
    over.latency_ns = Some(LatencySummary {
        p50: 40_000,
        p99: u64::MAX,
        max: u64::MAX,
        histogram: out_of_range_buckets.clone(),
    });
    over.lateness_p99_ns = Some(u64::MAX / 10);
    assert!(reconcile(&over)
        .contains(&"latency histogram bucket value outside the recorder's range".to_string()));
    assert_eq!(percentile(&out_of_range_buckets, 0.99), None);

    // One nanosecond above the top of the cap bucket is already out of range.
    let mut above_bound = valid_record(RunKind::Latency);
    above_bound.lateness = vec![(LATENCY_BUCKET_MAX_NS + 1, 998)];
    assert!(reconcile(&above_bound)
        .contains(&"lateness histogram bucket value outside the recorder's range".to_string()));

    // The top of the cap bucket itself is what `iter_recorded` reports for a
    // sample at `LATENCY_MAX_NS`, so it must stay acceptable.
    let mut at_bound = valid_record(RunKind::Latency);
    at_bound.lateness = vec![(LATENCY_BUCKET_MAX_NS, 998)];
    at_bound.lateness_p99_ns = Some(LATENCY_BUCKET_MAX_NS);
    assert!(!reconcile(&at_bound)
        .iter()
        .any(|r| r.contains("outside the recorder's range")));

    // A bucket of 0 sits below the histogram's low bound of 1.
    let mut zero_value = valid_record(RunKind::Latency);
    zero_value.lateness = vec![(0, 998)];
    zero_value.lateness_p99_ns = Some(0);
    assert!(reconcile(&zero_value)
        .contains(&"lateness histogram bucket value outside the recorder's range".to_string()));
}

#[test]
fn reconcile_holds_lateness_to_the_exact_fraction_of_the_latency_p99() {
    // 0.10 × 80_001 ns is 8_000.1 ns, so 8_001 ns is 10.0012 % of the tail:
    // over the P-Q1 limit, and accepted while the bound was rounded up.
    let mut over = valid_record(RunKind::Latency);
    over.latency_ns = Some(LatencySummary {
        p50: 40_000,
        p99: 80_001,
        max: 80_001,
        histogram: vec![(40_000, 987), (80_001, 11)],
    });
    over.lateness = vec![(8_001, 998)];
    over.lateness_p99_ns = Some(8_001);
    assert_eq!(
        reconcile(&over),
        vec![format!(
            "generator lateness p99 Some(8001) ns exceeds {GENERATOR_LATENESS_FRACTION} of latency p99 80001"
        )]
    );

    // 8_000 ns is under the same bound and stays valid.
    let mut under = valid_record(RunKind::Latency);
    under.latency_ns = Some(LatencySummary {
        p50: 40_000,
        p99: 80_001,
        max: 80_001,
        histogram: vec![(40_000, 987), (80_001, 11)],
    });
    under.lateness = vec![(8_000, 998)];
    under.lateness_p99_ns = Some(8_000);
    assert_eq!(reconcile(&under), Vec::<String>::new());

    // A lateness p99 exactly at the fraction of a p99 the fraction divides is
    // still within the limit, which is `≤`.
    let mut exact = valid_record(RunKind::Latency);
    exact.lateness = vec![(8_000, 998)];
    exact.lateness_p99_ns = Some(8_000);
    assert_eq!(reconcile(&exact), Vec::<String>::new());
}

#[test]
fn reconcile_rejects_a_timeline_whose_bins_it_cannot_sum() {
    // Bins beyond a u64 would saturate onto `delivered` and pass row 29, so
    // the sum is reported instead of taken.
    let mut rec = valid_record(RunKind::Connections);
    rec.timeline.counts = vec![u64::MAX, 1];
    rec.counts.delivered = u64::MAX;
    assert_eq!(
        reconcile(&rec),
        vec!["timeline deliveries overflow a u64".to_string()]
    );
    assert_eq!(rec.timeline.total(), None);
}

#[test]
fn a_record_keeps_the_host_cpu_pressure_it_was_given() {
    let mut value = serde_json::to_value(valid_record(RunKind::Latency)).expect("serialize the run");
    value
        .as_object_mut()
        .expect("run is a JSON object")
        .insert(
            "host_cpu_pressure".to_string(),
            serde_json::json!({"sampled_ns": 5_000_000_000_u64, "some_stall_us": 20_417_u64}),
        );
    let record: RunRecord = serde_json::from_value(value).expect("deserialize the run");
    let back = serde_json::to_value(&record).expect("serialize the run");
    assert_eq!(
        back["host_cpu_pressure"],
        serde_json::json!({"sampled_ns": 5_000_000_000_u64, "some_stall_us": 20_417_u64})
    );
}

#[test]
fn cpu_pressure_reads_the_some_line_total() {
    let first = "some avg10=1.23 avg60=0.50 avg300=0.23 total=737911155\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n";
    assert_eq!(parse_cpu_pressure_some_total(first), Some(737_911_155));
    let swapped =
        "full avg10=0.00 avg60=0.00 avg300=0.00 total=5\nsome avg10=0.10 avg60=0.20 avg300=0.30 total=9\n";
    assert_eq!(parse_cpu_pressure_some_total(swapped), Some(9));
}

#[test]
fn cpu_pressure_without_a_some_total_is_unmeasured() {
    let cases = [
        "",
        "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
        "some avg10=1.23 avg60=0.50 avg300=0.23\n",
        "some avg10=1.23 avg60=0.50 avg300=0.23 total=abc\n",
        "something avg10=1.23 total=7\n",
    ];
    for text in cases {
        assert_eq!(parse_cpu_pressure_some_total(text), None, "{text:?}");
    }
}

#[test]
fn a_record_written_before_host_pressure_reads_it_as_unmeasured() {
    let mut value = serde_json::to_value(valid_record(RunKind::Latency)).expect("serialize the run");
    let removed = value
        .as_object_mut()
        .expect("run is a JSON object")
        .remove("host_cpu_pressure");
    assert!(removed.is_some(), "host_cpu_pressure was serialized");
    let record: RunRecord = serde_json::from_value(value).expect("deserialize the run");
    assert_eq!(record.host_cpu_pressure, None);
}

#[test]
fn host_cpu_pressure_does_not_change_the_reconciliation_verdict() {
    let baseline = valid_record(RunKind::Latency);
    let mut with_pressure = baseline.clone();
    with_pressure.host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: WINDOW_SECS * ONE_SECOND_NS,
        some_stall_us: 4_500_000,
    });
    assert_eq!(reconcile(&with_pressure), Vec::<String>::new());

    let mut invalid = valid_record(RunKind::Latency);
    invalid.counts.duplicates = 2;
    let before = reconcile(&invalid);
    invalid.host_cpu_pressure = Some(HostCpuPressure {
        sampled_ns: WINDOW_SECS * ONE_SECOND_NS,
        some_stall_us: 4_500_000,
    });
    let after = reconcile(&invalid);
    assert_eq!(before, after);
    assert!(!before.is_empty(), "duplicates=2 must surface as a reconciliation reason");
}
