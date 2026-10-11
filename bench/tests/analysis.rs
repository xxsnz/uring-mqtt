//! Public-seam tests for `bench::analysis` (task 8 of the
//! benchmark-harness feature): derived metrics, the 30-row reconciliation
//! table, recomputation, percentile/spread/matched_rates, perf/strace
//! parsing, driver-fd counting, interleave ordering, and CPU percent
//! arithmetic (AC-27..AC-34, AC-48, AC-55, AC-56, AC-61).

use std::fs;
use std::os::unix::fs::symlink;
use std::path::PathBuf;

use bench::analysis::{
    config_passes, count_driver_fds, cpu_count, cpu_percent, derive, interleave, matched_rates,
    median, metric, parse_perf_count, parse_strace_total, percentile, reconcile, recompute,
    spread, GENERATOR_LATENESS_FRACTION, SYSCALL_FRACTION_PCT,
};
use bench::gen::slot_counts;
use bench::record::{Timeline, Window, LATENCY_BUCKET_MAX_NS};
use bench::run_record::{
    Build, ConfigKey, CounterExit, Counts, CpuSets, CpuUsage, DriverFds, Env, GeneratorErrors,
    LatencySummary, Profile, RunKind, RunRecord, ServerKind, SyscallMethod, Syscalls,
};

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
            RunKind::Latency => {
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

    let latency_ns = if kind == RunKind::Latency {
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
    let lateness = if kind == RunKind::Latency {
        vec![(1_000, 998)]
    } else {
        Vec::new()
    };
    let lateness_p99_ns = if kind == RunKind::Latency {
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
        RunKind::Latency => (Some(1u8), Some(SYSCALL_FRACTION_PCT), Some(200)),
    };
    let (syscalls_method, syscalls_count, syscalls_raw, syscalls_per_msg) = if kind == RunKind::Latency
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
        schema: 1,
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
            enabled_ns: if kind == RunKind::Latency {
                Some(WINDOW_START_NS)
            } else {
                None
            },
            disabled_ns: if kind == RunKind::Latency {
                Some(WINDOW_END_NS)
            } else {
                None
            },
            exit: if kind == RunKind::Latency {
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
        started_at_unix: 1_791_273_600,
        harness_errors: Vec::new(),
        valid: true,
        invalid_reasons: Vec::new(),
    }
}

fn valid_record(kind: RunKind) -> RunRecord {
    baseline_record(kind, ServerKind::Uring)
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
            "stalled connections 1".to_string(),
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
            RunKind::Latency,
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
            RunKind::Latency,
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
            RunKind::Latency,
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
            RunKind::Latency,
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
            "stalled connections 1".to_string(),
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
        .map(|&v| {
            let mut r = valid_record(RunKind::Throughput);
            r.throughput_msgs_per_sec = Some(v);
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
        .map(|&v| {
            let mut r = valid_record(RunKind::Throughput);
            r.throughput_msgs_per_sec = Some(v);
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
