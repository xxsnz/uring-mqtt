//! Derivation, reconciliation and recomputation rules over
//! [`RunRecord`](crate::run_record::RunRecord). `derive`, `reconcile` and
//! `recompute` read no clock and are total over the record they receive: a
//! persisted record can name values no successful run produces, so they
//! report an inconsistency as a reconciliation reason instead of panicking.
//! [`count_driver_fds`] is the one function here that performs I/O; it reads
//! an fd directory and returns the `io::Error` of a failed inspection.
//!
//! The 30 rows of `reconcile` are the spec §9.1 contract; `recompute`
//! re-runs `derive` and `reconcile` against a record's stored summary and
//! returns one mismatch per disagreeing field, so `bench doc` can refuse a
//! record whose summary has drifted from its raw observations.

// Rates, p99s and clock ticks are 64-bit and have to land in `f64` for
// ratios; the matching `f64 → u64` conversions are bounded by the spec
// (window_secs, rate fractions, p99 ns). Accept the loss of precision.
#![allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs;
use std::io;
use std::path::Path;

use crate::gen::{scheduled_slots, slot_counts};
use crate::record::LATENCY_BUCKET_MAX_NS;
use crate::run_record::{
    CounterExit, DriverFds, HostCpuPressure, LatencySummary, RunKind, RunRecord, ServerKind,
    SyscallMethod,
};
// The protocol constants the reconciliation rows read belong to the runner
// (`PLAN.md ## Interface Contracts`, `Protocol constants`). They are used
// here rather than restated so a change to the runner's declared value
// cannot leave the published metric and the refusal rows on the old one.
use crate::runner::{CLEANUP_SURVIVOR_PREFIX, COUNTER_BOUNDARY_TOLERANCE_NS, SUSTAINED_TAIL_SECS};
pub use crate::runner::SYSCALL_FRACTION_PCT;

/// Maximum tolerated relative spread of any configuration's metric over its
/// repetitions (spec §9.1: ≤ 10 %).
pub const SPREAD_LIMIT: f64 = 0.10;

/// Largest allowed generator lateness as a fraction of the latency p99
/// (spec §9.1: generator must not contribute more than 10 % of the latency
/// tail).
pub const GENERATOR_LATENESS_FRACTION: f64 = 0.10;

/// SIGINT as an `i32`; the reconciliation table accepts the runner's
/// `Signal(SIGINT)` ending on its own row.
const SIGINT: i32 = nix::sys::signal::Signal::SIGINT as i32;

/// One second in nanoseconds: the record stores durations in seconds and
/// instants in monotonic nanoseconds.
const ONE_SECOND_NS: u64 = 1_000_000_000;

/// Nanoseconds per microsecond: the kernel's pressure counter counts
/// microseconds, the record's intervals nanoseconds.
const NS_PER_US: u64 = 1_000;

/// Tenths per unit: the host-condition clause prints its percentage
/// rounded to one decimal place.
const PCT_TENTHS: f64 = 10.0;

#[derive(Debug, Clone, PartialEq)]
pub struct Derived {
    pub throughput_msgs_per_sec: Option<f64>,
    pub last10s_msgs_per_sec: Option<f64>,
    pub connections_per_sec: Option<f64>,
    pub latency_ns: Option<LatencySummary>,
    pub lateness_p99_ns: Option<u64>,
    pub syscall_count: Option<u64>,
    pub syscalls_per_msg: Option<f64>,
    pub unresolved: u64,
}

/// Compute every derived field of `record` from its raw observations.
pub fn derive(record: &RunRecord) -> Derived {
    let deliveries = record.timeline.sum_secs(record.window.start_sec(), record.window.end_sec());
    let dropped = record.counts.dropped.unwrap_or(0);
    let unresolved = record
        .counts
        .offered
        .saturating_sub(record.counts.delivered)
        .saturating_sub(dropped);

    let (throughput_msgs_per_sec, last10s_msgs_per_sec) = if record.kind == RunKind::Throughput {
        // A persisted record can hold a window whose seconds contradict
        // `window_secs`. Derive no rate from such a record rather than wrap
        // the tail start or divide by zero; row 7 then reports it as not
        // sustained and row 26 names the inconsistent window.
        let tail = SUSTAINED_TAIL_SECS.min(record.window_secs);
        match record.window.end_sec().checked_sub(tail) {
            Some(tail_start) if tail != 0 => {
                let whole = deliveries as f64 / record.window_secs as f64;
                let tail_deliveries = record
                    .timeline
                    .sum_secs(tail_start, record.window.end_sec());
                let tail_rate = tail_deliveries as f64 / tail as f64;
                (Some(whole), Some(tail_rate))
            }
            _ => (None, None),
        }
    } else {
        (None, None)
    };

    let connections_per_sec = if record.kind == RunKind::Connections {
        record
            .cycles_in_window
            .map(|c| c as f64 / record.window_secs as f64)
    } else {
        None
    };

    let latency_ns = if record.kind.is_open_loop() {
        let histogram = record.latency_ns.as_ref().map(|l| l.histogram.clone());
        histogram.map(|histogram| LatencySummary {
            p50: percentile(&histogram, 0.5).unwrap_or(0),
            p99: percentile(&histogram, 0.99).unwrap_or(0),
            max: percentile(&histogram, 1.0).unwrap_or(0),
            histogram,
        })
    } else {
        None
    };

    let lateness_p99_ns = percentile(&record.lateness, 0.99);

    let syscall_count = match record.syscalls.method {
        SyscallMethod::Perf => record
            .syscalls
            .raw
            .as_deref()
            .and_then(parse_perf_count),
        SyscallMethod::Strace => record
            .syscalls
            .raw
            .as_deref()
            .and_then(parse_strace_total),
        SyscallMethod::None => None,
    };

    let syscalls_per_msg = syscall_count.and_then(|c| {
        if deliveries == 0 {
            None
        } else {
            Some(c as f64 / deliveries as f64)
        }
    });

    Derived {
        throughput_msgs_per_sec,
        last10s_msgs_per_sec,
        connections_per_sec,
        latency_ns,
        lateness_p99_ns,
        syscall_count,
        syscalls_per_msg,
        unresolved,
    }
}

/// One element of the reconciliation decision table. The list order is the
/// order rows appear in PLAN.md, so a record that violates several rows is
/// reported in a stable, reviewable order.
#[allow(clippy::too_many_lines)] // 30 rows of the spec §9.1 contract; splitting them buys nothing
#[allow(unused_parens)] // Row 13 chains `&& (Throughput || open_loop) && ...`; the same shape is kept on rows 2, 3 and 6 so the open-loop/throughput boundary is uniform
pub fn reconcile(record: &RunRecord) -> Vec<String> {
    let mut reasons: Vec<String> = Vec::new();

    let dropped = record.counts.dropped.unwrap_or(0);

    // Row 1 — latency: scheduled count must equal offered.
    if record.kind.is_open_loop()
        && record.counts.scheduled != Some(record.counts.offered)
    {
        reasons.push(format!(
            "scheduled {:?} != offered {}",
            record.counts.scheduled, record.counts.offered
        ));
    }

    // Row 2 — throughput/latency: delivered + dropped must not exceed offered.
    if (record.kind == RunKind::Throughput || record.kind.is_open_loop())
        && record
            .counts
            .delivered
            .saturating_add(dropped)
            > record.counts.offered
    {
        reasons.push("delivered + dropped exceeds offered".to_string());
    }

    // Row 3 — throughput/latency: unresolved must be zero.
    if (record.kind == RunKind::Throughput || record.kind.is_open_loop())
        && record.counts.unresolved != 0
    {
        reasons.push(format!("unresolved {}", record.counts.unresolved));
    }

    // Row 4 — all kinds: no duplicates.
    if record.counts.duplicates != 0 {
        reasons.push(format!("duplicates {}", record.counts.duplicates));
    }

    // Row 5 — all kinds: no foreign payloads.
    if record.counts.foreign != 0 {
        reasons.push(format!("foreign payloads {}", record.counts.foreign));
    }

    // Row 6 — throughput/latency: non-zero dropped invalidates.
    if (record.kind == RunKind::Throughput || record.kind.is_open_loop()) {
        if let Some(n) = record.counts.dropped {
            if n != 0 {
                let kind = kind_label(record.kind);
                reasons.push(format!("dropped {n} in a {kind} run"));
            }
        }
    }

    // Row 7 — throughput: tail and whole-rate must agree within SPREAD_LIMIT.
    if record.kind == RunKind::Throughput {
        let whole = record.throughput_msgs_per_sec;
        let tail = record.last10s_msgs_per_sec;
        let sustained_ok = match (whole, tail) {
            (Some(w), Some(l)) if w > 0.0 => {
                let diff = (l - w).abs() / w;
                diff <= SPREAD_LIMIT
            }
            _ => false,
        };
        if !sustained_ok {
            reasons.push(format!(
                "not sustained: tail {tail:?} vs window {whole:?}"
            ));
        }
    }

    // Row 8 — uring server: driver proof.
    if record.server == ServerKind::Uring
        && record.driver
            != Some(DriverFds {
                io_uring_fds: record.workers,
                eventpoll_fds: 0,
            })
    {
        reasons.push(format!("driver proof failed: {:?}", record.driver));
    }

    // Row 9 — all kinds: no connect errors.
    if record.generator_errors.connect != 0 {
        reasons.push(format!("connect errors {}", record.generator_errors.connect));
    }

    // Row 10 — all kinds: no protocol errors.
    if record.generator_errors.protocol != 0 {
        reasons.push(format!(
            "protocol errors {}",
            record.generator_errors.protocol
        ));
    }

    // Row 11 — latency: no missed slots.
    if record.kind.is_open_loop() && record.generator_errors.missed_slots != 0 {
        reasons.push(format!(
            "missed slots {}",
            record.generator_errors.missed_slots
        ));
    }

    // Row 12 — all kinds: nothing stalled. The counter sums two units:
    // publishes whose write did not complete or whose PUBACK never
    // arrived, and connection tasks still running at the hard deadline.
    if record.generator_errors.stalled != 0 {
        reasons.push(format!(
            "stalled {} (publishes whose write did not complete or whose PUBACK never arrived, plus connection tasks still running at the hard deadline)",
            record.generator_errors.stalled
        ));
    }

    // Row 13 — QoS 1 throughput/latency: every offered must be acked.
    if record.qos == Some(1)
        && (record.kind == RunKind::Throughput || record.kind.is_open_loop())
        && record.counts.acked != Some(record.counts.offered)
    {
        reasons.push(format!(
            "unacknowledged QoS 1 publishes: acked {:?} of {}",
            record.counts.acked, record.counts.offered
        ));
    }

    // Row 14 — latency: latency_ns must be present.
    if record.kind.is_open_loop() && record.latency_ns.is_none() {
        reasons.push("no latency samples in the window".to_string());
    }

    // Row 15 — latency: histogram total must equal window_scheduled. Guarded
    // by `latency_ns.is_some()` so a missing latency record reports row 14
    // alone rather than also this count mismatch.
    if record.kind.is_open_loop() {
        if let Some(latency_ns) = record.latency_ns.as_ref() {
            match histogram_total(&latency_ns.histogram) {
                Err(reason) => reasons.push(format!("latency histogram {reason}")),
                Ok(total) if total != record.counts.window_scheduled.unwrap_or(0) => {
                    reasons.push(format!(
                        "latency samples {total} != window slots {:?}",
                        record.counts.window_scheduled
                    ));
                }
                Ok(_) => {}
            }
        }
    }

    // Row 16 — all kinds: no latency over-range.
    if record.counts.latency_over_range != 0 {
        reasons.push(format!(
            "latency samples over range {}",
            record.counts.latency_over_range
        ));
    }

    // Row 17 — all kinds: no clock anomalies.
    if record.counts.clock_anomalies != 0 {
        reasons.push(format!(
            "clock anomalies {}",
            record.counts.clock_anomalies
        ));
    }

    // Row 18 — latency: lateness p99 within GENERATOR_LATENESS_FRACTION of latency
    // p99. Guarded by `latency_ns.is_some()` so a missing latency record
    // reports row 14 alone rather than also this mismatch against a zero p99.
    if record.kind.is_open_loop() {
        if let Some(latency_ns) = record.latency_ns.as_ref() {
            let latency_p99 = latency_ns.p99;
            // Compare against the exact fraction of the p99, not a bound
            // rounded up to the next whole nanosecond: rounding up accepts a
            // lateness p99 above the fraction P-Q1 fixed the rule at
            // (`lateness p99 ≤ 0.10 × latency p99`). For a latency p99 of
            // 80_001 ns a rounded bound allowed 8_001 ns, which is 10.0012 %.
            let limit = GENERATOR_LATENESS_FRACTION * latency_p99 as f64;
            match record.lateness_p99_ns {
                None => reasons.push(format!(
                    "generator lateness p99 None ns exceeds {GENERATOR_LATENESS_FRACTION} of latency p99 {latency_p99}"
                )),
                Some(p) if p as f64 > limit => reasons.push(format!(
                    "generator lateness p99 {:?} ns exceeds {GENERATOR_LATENESS_FRACTION} of latency p99 {latency_p99}",
                    record.lateness_p99_ns
                )),
                Some(_) => {}
            }
        }
    }

    // Row 19 — counted run: count must exist.
    if is_counted_syscall_run(record) && record.syscalls.count.is_none() {
        reasons.push("syscall count missing".to_string());
    }

    // Row 20 — connections: cycles_in_window must be present and > 0.
    if record.kind == RunKind::Connections {
        match record.cycles_in_window {
            None | Some(0) => reasons.push("no completed connection cycles".to_string()),
            Some(_) => {}
        }
    }

    // Row 21 — latency: no lateness over-range.
    if record.kind.is_open_loop() && record.generator_errors.lateness_over_range != 0 {
        reasons.push(format!(
            "generator lateness over range {}",
            record.generator_errors.lateness_over_range
        ));
    }

    // Row 22 — counted run: boundary times within tolerance.
    if is_counted_syscall_run(record) {
        let start = record.window.start_ns;
        let end = record.window.end_ns;
        let enabled_off = record
            .syscalls
            .enabled_ns
            .is_none_or(|e| e.abs_diff(start) > COUNTER_BOUNDARY_TOLERANCE_NS);
        let disabled_off = record
            .syscalls
            .disabled_ns
            .is_none_or(|d| d.abs_diff(end) > COUNTER_BOUNDARY_TOLERANCE_NS);
        if enabled_off || disabled_off {
            reasons.push(format!(
                "counter boundaries outside tolerance: enabled {:?} disabled {:?}",
                record.syscalls.enabled_ns, record.syscalls.disabled_ns
            ));
        }
    }

    // Row 23 — counted run: exit must be Code(0) or Signal(SIGINT).
    if is_counted_syscall_run(record) {
        let ok = match record.syscalls.exit {
            Some(CounterExit::Code(0)) => true,
            Some(CounterExit::Signal(s)) => s == SIGINT,
            _ => false,
        };
        if !ok {
            reasons.push(format!(
                "syscall counter ended with {:?}",
                record.syscalls.exit
            ));
        }
    }

    // Row 24 — latency: last_write_start_ns must be Some(t < window.end_ns).
    if record.kind.is_open_loop() {
        match record.last_write_start_ns {
            None => reasons
                .push("publish write began at or after the window end: None".to_string()),
            Some(t) if t >= record.window.end_ns => reasons.push(format!(
                "publish write began at or after the window end: {t:?}"
            )),
            Some(_) => {}
        }
    }

    // Row 25 — latency: Σ lateness counts + lateness_over_range must equal window_scheduled.
    if record.kind.is_open_loop() {
        match histogram_total(&record.lateness) {
            Err(reason) => reasons.push(format!("lateness histogram {reason}")),
            Ok(lateness_total) => {
                let total =
                    lateness_total.saturating_add(record.generator_errors.lateness_over_range);
                if total != record.counts.window_scheduled.unwrap_or(0) {
                    reasons.push(format!(
                        "lateness samples {total} != window slots {:?}",
                        record.counts.window_scheduled
                    ));
                }
            }
        }
    }

    // Row 26 — all kinds: window boundaries consistent with origin and warmup_secs / window_secs.
    //
    // Both boundary legs are checked, not saturating: a sum that leaves
    // the `u64` range is not the boundary the constants name, while two
    // saturating legs both landing on `u64::MAX` read as agreement. A
    // record with a whole-second `origin_ns` near `u64::MAX` and both
    // boundaries at `u64::MAX` passed that way, and `derive` then
    // reported a rate over a window of zero duration. A window whose end
    // does not follow its start carries no measurement span for any kind,
    // so it is reported by this row too — the boundaries and the span
    // they must have are one rule, and row 27 only names the latency
    // sub-case it has to reject before `slot_counts`.
    {
        let expected_start = record
            .warmup_secs
            .checked_mul(ONE_SECOND_NS)
            .and_then(|warmup_ns| record.origin_ns.checked_add(warmup_ns));
        let expected_end = record
            .window_secs
            .checked_mul(ONE_SECOND_NS)
            .and_then(|window_ns| record.window.start_ns.checked_add(window_ns));
        let boundaries_ok = expected_start == Some(record.window.start_ns)
            && expected_end == Some(record.window.end_ns);
        let origin_ok = record.origin_ns.is_multiple_of(ONE_SECOND_NS);
        let span_ok = record.window.end_ns > record.window.start_ns;
        if !origin_ok || !boundaries_ok || !span_ok {
            reasons.push("window inconsistent with origin and constants".to_string());
        }
    }

    // Row 27 — latency: scheduled counts must equal slot_counts(offered_rate,
    // origin, window). A record with no rate carries no evidence that its
    // slots follow an offered rate, a window whose start follows its end has
    // no slot span at all, and a rate whose schedule does not fit a `u64`
    // cannot be checked; all three invalidate the run rather than skip the row.
    if record.kind.is_open_loop() {
        match record.offered_rate {
            None => reasons.push("no offered rate for a latency run".to_string()),
            Some(_) if record.window.start_ns > record.window.end_ns => {
                reasons.push(format!(
                    "window start {} is after window end {}",
                    record.window.start_ns, record.window.end_ns
                ));
            }
            Some(rate)
                if scheduled_slots(rate, record.origin_ns, record.window) > u128::from(u64::MAX) =>
            {
                reasons.push(format!(
                    "offered rate {rate} schedules more slots than a u64 can hold"
                ));
            }
            Some(rate) => {
                let (scheduled, window_scheduled) =
                    slot_counts(rate, record.origin_ns, record.window);
                let ok = record.counts.scheduled == Some(scheduled)
                    && record.counts.window_scheduled == Some(window_scheduled);
                if !ok {
                    reasons.push(format!(
                        "scheduled counts inconsistent with rate {:?}",
                        record.offered_rate
                    ));
                }
            }
        }
    }

    // Row 28 — each harness error becomes its own reason, verbatim.
    for err in &record.harness_errors {
        reasons.push(err.clone());
    }

    // Row 29 — all kinds: the delivery timeline must account for exactly the
    // deliveries `counts.delivered` names. The recorder bumps both for the
    // same delivery (`Recorder::record`), so a record whose timeline holds
    // more or fewer deliveries derives a throughput rate and a syscalls/msg
    // denominator from a timeline its own counts contradict.
    match record.timeline.total() {
        None => reasons.push("timeline deliveries overflow a u64".to_string()),
        Some(timeline_total) if timeline_total != record.counts.delivered => {
            reasons.push(format!(
                "timeline deliveries {timeline_total} != delivered {}",
                record.counts.delivered
            ));
        }
        Some(_) => {}
    }

    // Row 30 — counted run: the window must hold deliveries. The
    // syscalls/message denominator is the window's delivery count, so a run
    // whose publishes all arrive outside the window carries a syscall count
    // with nothing to divide it by, and `derive` reports no syscalls/message
    // for a measurement the run exists to take.
    if is_counted_syscall_run(record)
        && record
            .timeline
            .sum_secs(record.window.start_sec(), record.window.end_sec())
            == 0
    {
        reasons.push("no deliveries in the window to divide the syscall count by".to_string());
    }

    reasons
}

/// Walk `record`'s stored summary and return one mismatch string per field
/// that disagrees with what `derive`/`reconcile` compute from the same raw
/// observations.
pub fn recompute(record: &RunRecord) -> Vec<String> {
    let mut mismatches: Vec<String> = Vec::new();
    let d = derive(record);

    if record.throughput_msgs_per_sec != d.throughput_msgs_per_sec {
        mismatches.push(format!(
            "throughput_msgs_per_sec: stored {:?} derived {:?}",
            record.throughput_msgs_per_sec, d.throughput_msgs_per_sec
        ));
    }
    if record.last10s_msgs_per_sec != d.last10s_msgs_per_sec {
        mismatches.push(format!(
            "last10s_msgs_per_sec: stored {:?} derived {:?}",
            record.last10s_msgs_per_sec, d.last10s_msgs_per_sec
        ));
    }
    if record.connections_per_sec != d.connections_per_sec {
        mismatches.push(format!(
            "connections_per_sec: stored {:?} derived {:?}",
            record.connections_per_sec, d.connections_per_sec
        ));
    }

    if record.latency_ns != d.latency_ns {
        mismatches.push(format!(
            "latency_ns: stored {:?} derived {:?}",
            record.latency_ns, d.latency_ns
        ));
    }

    if record.lateness_p99_ns != d.lateness_p99_ns {
        mismatches.push(format!(
            "lateness_p99_ns: stored {:?} derived {:?}",
            record.lateness_p99_ns, d.lateness_p99_ns
        ));
    }

    if record.syscalls.count != d.syscall_count {
        mismatches.push(format!(
            "syscalls.count: stored {:?} derived {:?}",
            record.syscalls.count, d.syscall_count
        ));
    }

    if record.syscalls.per_msg != d.syscalls_per_msg {
        mismatches.push(format!(
            "syscalls.per_msg: stored {:?} derived {:?}",
            record.syscalls.per_msg, d.syscalls_per_msg
        ));
    }

    if record.counts.unresolved != d.unresolved {
        mismatches.push(format!(
            "counts.unresolved: stored {} derived {}",
            record.counts.unresolved, d.unresolved
        ));
    }

    let stored_reasons = &record.invalid_reasons;
    let derived_reasons = reconcile(record);
    if stored_reasons != &derived_reasons {
        mismatches.push(format!(
            "invalid_reasons: stored {stored_reasons:?} derived {derived_reasons:?}"
        ));
    }
    if record.valid != derived_reasons.is_empty() {
        mismatches.push(format!(
            "valid: stored {} derived {}",
            record.valid,
            derived_reasons.is_empty()
        ));
    }

    mismatches
}

/// Total count of `histogram`'s buckets. `Err` names the condition that makes
/// the bucket list unusable. Only a deserialized record reaches here with one:
/// the recorder emits strictly ascending buckets, inside
/// `1..=LATENCY_BUCKET_MAX_NS`, whose counts sum to the run's sample count. A
/// value outside that range names a latency the bounded histogram cannot hold,
/// so the samples at it are not an observation of this run — accepting them
/// would let a record claim any p99 and max its summary likes.
fn histogram_total(histogram: &[(u64, u64)]) -> Result<u64, &'static str> {
    let mut total: u64 = 0;
    let mut previous: Option<u64> = None;
    for &(value, count) in histogram {
        if value == 0 || value > LATENCY_BUCKET_MAX_NS {
            return Err("bucket value outside the recorder's range");
        }
        if previous.is_some_and(|p| value <= p) {
            return Err("buckets out of ascending order");
        }
        previous = Some(value);
        total = total
            .checked_add(count)
            .ok_or("bucket counts overflow a u64")?;
    }
    Ok(total)
}

/// Smallest recorded value whose cumulative count reaches
/// `⌈quantile × total⌉` (never below 1). `None` for an empty histogram, and
/// for one whose buckets [`histogram_total`] rejects: a percentile read off
/// unordered buckets names a value the samples do not support.
pub fn percentile(histogram: &[(u64, u64)], quantile: f64) -> Option<u64> {
    let total = histogram_total(histogram).ok()?;
    if total == 0 {
        return None;
    }
    let rank = ((quantile * total as f64).ceil() as u64).max(1);
    let mut cumulative: u64 = 0;
    for &(value, count) in histogram {
        cumulative = cumulative.saturating_add(count);
        if cumulative >= rank {
            return Some(value);
        }
    }
    None
}

/// Median of `values` (mean of the two middle values when the count is even).
/// `None` when empty.
pub fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("median inputs are finite"));
    let n = sorted.len();
    if n % 2 == 1 {
        Some(sorted[n / 2])
    } else {
        Some(f64::midpoint(sorted[n / 2 - 1], sorted[n / 2]))
    }
}

/// `(max − min) / median` of `values`. `None` when empty or median == 0.
pub fn spread(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let m = median(values)?;
    if m == 0.0 {
        return None;
    }
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for &v in values {
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }
    Some((max - min) / m)
}

/// Primary metric of `record`: throughput → msgs/s, latency → p99 ns,
/// connections → conn/s, counted → syscalls/msg.
pub fn metric(record: &RunRecord) -> Option<f64> {
    match record.kind {
        RunKind::Throughput => record.throughput_msgs_per_sec,
        RunKind::Latency => record.latency_ns.as_ref().map(|l| l.p99 as f64),
        RunKind::Connections => record.connections_per_sec,
        RunKind::Counted => record.syscalls.per_msg,
    }
}

/// Why `runs` are not a complete attempt, in the order
/// [`attempt_complete`] reads the conditions. `None` when they are one.
/// The reason is the diagnostic half of the predicate: both
/// [`attempt_complete`] and [`config_failure`] read it, so one
/// definition decides the verdict and names it.
fn incomplete_reason(runs: &[RunRecord], repetitions: u32) -> Option<String> {
    if let Some(cancelled) = runs.iter().find(|r| r.cancelled) {
        return Some(format!(
            "repetition {} was cancelled",
            cancelled.repetition
        ));
    }
    if let Some(survivor) = runs.iter().find(|r| {
        r.harness_errors
            .iter()
            .any(|e| e.starts_with(CLEANUP_SURVIVOR_PREFIX))
    }) {
        return Some(format!(
            "repetition {} left a child running after cleanup",
            survivor.repetition
        ));
    }
    let mut reps: Vec<u32> = runs.iter().map(|r| r.repetition).collect();
    reps.sort_unstable();
    if !reps.iter().copied().eq(1..=repetitions) {
        return Some(format!("repetitions {reps:?} are not 1..={repetitions}"));
    }
    None
}

/// `true` when `runs` hold the repetition numbers `1..=repetitions`,
/// each exactly once, none of them was cancelled and none left a child
/// running after cleanup — a run cut short by a signal, or one the matrix
/// stopped beside a surviving child, is no repetition. A missing number is a
/// repetition that never ran; a repeated one is a record that ran
/// once and counts twice.
pub fn attempt_complete(runs: &[RunRecord], repetitions: u32) -> bool {
    incomplete_reason(runs, repetitions).is_none()
}

/// Which condition of [`config_passes`] refused `runs`, in the order
/// that gate reads them. `None` when the configuration passes, so
/// `config_failure(..).is_none()` *is* the verdict. A caller that only
/// decides whether to retry reads [`config_passes`]; one that has to
/// tell the operator why an attempt did not count reads this.
pub fn config_failure(
    runs: &[RunRecord],
    repetitions: u32,
    enforce_spread: bool,
) -> Option<String> {
    if let Some(reason) = incomplete_reason(runs, repetitions) {
        return Some(reason);
    }
    if let Some(invalid) = runs.iter().find(|r| !r.valid) {
        return Some(format!(
            "repetition {} is invalid: {}; {}",
            invalid.repetition,
            invalid.invalid_reasons.join("; "),
            host_pressure_note(invalid)
        ));
    }
    if !enforce_spread {
        return None;
    }
    let mut metrics: Vec<f64> = Vec::with_capacity(runs.len());
    for run in runs {
        match metric(run) {
            Some(value) => metrics.push(value),
            None => {
                return Some(format!(
                    "repetition {} has no {:?} metric",
                    run.repetition, run.kind
                ))
            }
        }
    }
    match spread(&metrics) {
        Some(s) if s <= SPREAD_LIMIT => None,
        Some(s) => Some(format!("spread {s:.4} is above the {SPREAD_LIMIT} limit")),
        None => Some("spread is undefined: the metric median is 0".to_string()),
    }
}

/// `true` when `runs` are a complete attempt ([`attempt_complete`]),
/// every run is valid, and (when `enforce_spread` is set) every run has
/// its metric and their spread is within [`SPREAD_LIMIT`].
/// [`config_failure`] names the condition that refused them.
pub fn config_passes(runs: &[RunRecord], repetitions: u32, enforce_spread: bool) -> bool {
    config_failure(runs, repetitions, enforce_spread).is_none()
}

/// Matched latency rates from the slowest throughput competitor:
/// `(floor(0.5 × min), floor(0.8 × min))`. `None` when no competitor
/// reported a rate.
pub fn matched_rates(throughput_medians: &[f64]) -> Option<(u64, u64)> {
    if throughput_medians.is_empty() {
        return None;
    }
    let min = throughput_medians
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min);
    let half = (0.5 * min).floor() as u64;
    let eighty = (0.8 * min).floor() as u64;
    Some((half, eighty))
}

/// `total=` value of the `some` line of `/proc/pressure/cpu`: the
/// microseconds during which at least one runnable task waited for a CPU.
/// `None` when no line whose first field is `some` carries a `total=`
/// field that parses as a `u64`.
pub fn parse_cpu_pressure_some_total(text: &str) -> Option<u64> {
    let line = text
        .lines()
        .find(|line| line.split_whitespace().next() == Some("some"))?;
    line.split_whitespace()
        .find_map(|field| field.strip_prefix("total="))
        .and_then(|value| value.parse::<u64>().ok())
}

/// Counter value of the `raw_syscalls:sys_enter` line of `perf stat -x ,`
/// output. `None` for `<not counted>` / `<not supported>` lines or any
/// malformed input.
pub fn parse_perf_count(stderr: &str) -> Option<u64> {
    for line in stderr.lines() {
        let fields: Vec<&str> = line.split(',').collect();
        if fields.get(2).copied() == Some("raw_syscalls:sys_enter") {
            return fields.first().and_then(|s| s.parse::<u64>().ok());
        }
    }
    None
}

/// `calls` column of the `total` row of `strace -c` output, with or
/// without an `errors` value.
pub fn parse_strace_total(summary: &str) -> Option<u64> {
    let mut last_total: Option<u64> = None;
    for line in summary.lines() {
        let tokens: Vec<&str> = line.split_whitespace().collect();
        if tokens.last().copied() == Some("total") {
            let calls = tokens.get(3).and_then(|s| s.parse::<u64>().ok());
            if calls.is_some() {
                last_total = calls;
            }
        }
    }
    last_total
}

/// Count the `anon_inode:[io_uring]` and `anon_inode:[eventpoll]` entries
/// under `fd_dir`. Each fd entry the kernel lists as `anon_inode:[io_uring]`
/// is one io_uring ring; `anon_inode:[eventpoll]` counts the per-thread epoll
/// instance a multi-threaded runtime opens.
///
/// An fd closed between the listing and its `read_link` is not evidence of a
/// missing ring and is skipped. Every other error reaches the caller: a count
/// taken over entries the inspection could not read is not driver proof.
pub fn count_driver_fds(fd_dir: &Path) -> io::Result<DriverFds> {
    let mut io_uring_fds: u32 = 0;
    let mut eventpoll_fds: u32 = 0;
    for entry in fs::read_dir(fd_dir)? {
        let entry = entry?;
        match fs::read_link(entry.path()) {
            Ok(target) => {
                let s = target.to_string_lossy();
                if s == "anon_inode:[io_uring]" {
                    io_uring_fds = io_uring_fds.saturating_add(1);
                } else if s == "anon_inode:[eventpoll]" {
                    eventpoll_fds = eventpoll_fds.saturating_add(1);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(DriverFds {
        io_uring_fds,
        eventpoll_fds,
    })
}

/// Repetition-major interleaving: run each config in repetition 1, then
/// each config in repetition 2, and so on. The matrix executor iterates
/// this list to keep the run-to-run variance dominated by configuration,
/// not by drift across the run.
pub fn interleave<T: Clone>(configs: &[T], repetitions: u32) -> Vec<(T, u32)> {
    let reps = usize::try_from(repetitions).expect("repetitions fits in usize");
    let mut out: Vec<(T, u32)> = Vec::with_capacity(reps * configs.len());
    for rep in 1..=repetitions {
        for cfg in configs {
            out.push((cfg.clone(), rep));
        }
    }
    out
}

/// Number of distinct CPUs in a taskset list. `"0,2,4,6"` → 4, `"8-17"` → 10,
/// `"0,0"` → 1, `"0-10:2"` → 6. An element is `n`, `a-b`, or taskset's
/// stride form `a-b:s` — every `s`-th CPU from `a` up to `b`, `s ≥ 1`.
/// `None` when any element is outside that grammar — an empty element, as
/// in `"0,,2"`, a stride of 0, a stride on a single CPU (`"0:2"`), a second
/// stride, an empty or non-numeric stride, and a number past `u32`
/// included — or when `b < a`. util-linux's taskset also accepts some of
/// those forms before a comma (`"0:2,1"`); they get `None` here too.
///
/// A CPU list is an affinity mask, so a CPU named twice — directly, through
/// overlapping ranges or through two strides — still gives the process one
/// CPU. Nothing is expanded into a list: a list may name a range far wider
/// than the host has CPUs (taskset accepts `0-4000000000:2`). The CPU line
/// is cut at every element's bounds and each segment is counted by
/// arithmetic; only a segment where two strides overlap with no plain range
/// is walked CPU by CPU.
pub fn cpu_count(set: &str) -> Option<u32> {
    // (first CPU, last CPU, stride); a plain `n` or `a-b` has stride 1.
    let mut elements: Vec<(u32, u32, u32)> = Vec::new();
    for part in set.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return None;
        }
        let (span, stride) = match part.split_once(':') {
            Some((span, s)) => {
                let s: u32 = s.trim().parse().ok()?;
                if s == 0 || !span.contains('-') {
                    return None;
                }
                (span, s)
            }
            None => (part, 1),
        };
        let (a, b) = if let Some((a_s, b_s)) = span.split_once('-') {
            let a: u32 = a_s.trim().parse().ok()?;
            let b: u32 = b_s.trim().parse().ok()?;
            if b < a {
                return None;
            }
            (a, b)
        } else {
            let n: u32 = span.parse().ok()?;
            (n, n)
        };
        elements.push((a, b, stride));
    }
    // Split the CPU line at every element's first CPU and one past its last,
    // so each segment lies wholly inside or wholly outside every element.
    let mut cuts: Vec<u64> = elements
        .iter()
        .flat_map(|&(a, b, _)| [u64::from(a), u64::from(b) + 1])
        .collect();
    cuts.sort_unstable();
    cuts.dedup();
    let mut total: u64 = 0;
    for pair in cuts.windows(2) {
        let (lo, hi) = (pair[0], pair[1]);
        let active: Vec<(u64, u64)> = elements
            .iter()
            .filter(|&&(a, b, _)| u64::from(a) <= lo && hi <= u64::from(b) + 1)
            .map(|&(a, _, s)| (u64::from(a), u64::from(s)))
            .collect();
        total += match active.as_slice() {
            [] => 0,
            // A plain range names every CPU of the segment.
            _ if active.iter().any(|&(_, s)| s == 1) => hi - lo,
            // One stride alone: its CPUs in the segment, counted arithmetically.
            [(a, s)] => {
                let first = a + (lo - a).div_ceil(*s) * s;
                if first < hi { (hi - 1 - first) / s + 1 } else { 0 }
            }
            // Several strides overlap: walk their CPUs in a k-way merge,
            // smallest first, so a CPU two strides share is counted once.
            _ => {
                let mut next: BinaryHeap<Reverse<(u64, u64)>> = active
                    .iter()
                    .map(|&(a, s)| Reverse((a + (lo - a).div_ceil(s) * s, s)))
                    .collect();
                let mut count = 0;
                let mut last = None;
                while let Some(Reverse((cpu, s))) = next.pop() {
                    if cpu >= hi {
                        continue;
                    }
                    if last != Some(cpu) {
                        count += 1;
                        last = Some(cpu);
                    }
                    next.push(Reverse((cpu + s, s)));
                }
                count
            }
        };
    }
    Some(u32::try_from(total).unwrap_or(u32::MAX))
}

/// `100 × ticks / (clk_tck × sampled_secs × cpus)`, where `sampled_secs` is
/// `CpuUsage.sampled_ns` — the interval the two tick samples bracket, not
/// the declared `window_secs`, which a late sample does not cover. `None`
/// when the divisor is 0.
pub fn cpu_percent(ticks: u64, clk_tck: u64, sampled_ns: u64, cpus: u32) -> Option<f64> {
    if clk_tck == 0 || sampled_ns == 0 || cpus == 0 {
        return None;
    }
    let denom = u128::from(clk_tck)
        .saturating_mul(u128::from(sampled_ns))
        .saturating_mul(u128::from(cpus));
    if denom == 0 {
        return None;
    }
    let pct = u128::from(ticks)
        .saturating_mul(100)
        .saturating_mul(u128::from(ONE_SECOND_NS)) as f64
        / denom as f64;
    Some(pct)
}

/// `100 × some_stall_us × NS_PER_US / sampled_ns`: the share of the sampled
/// interval during which at least one runnable task waited for a CPU.
/// `None` when `sampled_ns` is 0.
fn host_cpu_pressure_pct(pressure: &HostCpuPressure) -> Option<f64> {
    if pressure.sampled_ns == 0 {
        return None;
    }
    let stall_ns = u128::from(pressure.some_stall_us).saturating_mul(u128::from(NS_PER_US));
    Some(stall_ns.saturating_mul(100) as f64 / pressure.sampled_ns as f64)
}

/// The host-condition clause [`config_failure`] appends to an invalid
/// repetition: the host CPU pressure over the run's sampled interval,
/// rounded to one decimal place. It states what was measured, not what
/// caused the failure; `bench/README.md` (`## Smoke`) gives the measured
/// reference bands the operator compares it with.
pub fn host_pressure_note(record: &RunRecord) -> String {
    match record.host_cpu_pressure.as_ref().and_then(host_cpu_pressure_pct) {
        Some(pct) => {
            let pct = (pct * PCT_TENTHS).round() / PCT_TENTHS;
            format!("host CPU pressure {pct:.1} % over the sampled interval")
        }
        None => "host CPU pressure was not measured".to_string(),
    }
}

fn kind_label(kind: RunKind) -> &'static str {
    match kind {
        RunKind::Throughput => "throughput",
        RunKind::Latency => "latency",
        RunKind::Connections => "connections",
        RunKind::Counted => "counted",
    }
}

/// The counted run (`RunKind::Counted`) is the only run that carries a syscall counter.
fn is_counted_syscall_run(record: &RunRecord) -> bool {
    record.kind == RunKind::Counted
}
