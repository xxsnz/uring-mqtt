use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

use crate::payload::{monotonic_now_ns, Stamp, TOPIC};

pub const LATENCY_MAX_NS: u64 = 60_000_000_000;
pub const LATENCY_SIGFIG: u8 = 3;

/// An armed origin must be within this distance of the recorder's own clock.
/// The runner reads the same `CLOCK_MONOTONIC` and arms immediately, so a real
/// origin is one to two seconds ahead: §9.1 gives the generator a one-second
/// lead and the `ARM` line rounds the window to whole seconds. The bound keeps
/// an origin from an untrusted `ARM` line from sizing the per-second timeline.
pub const ARM_ORIGIN_SKEW_MAX_NS: u64 = 60_000_000_000;

/// Largest window an `ARM` line may request. §9.1 measures a 60 s window; the
/// hour-long bound leaves room for a longer attended run while keeping the
/// timeline allocation proportional to a run that can actually happen.
pub const WINDOW_MAX_SPAN_NS: u64 = 3_600_000_000_000;

/// `u64` bins one shard's latency histogram holds. `Histogram` rounds
/// [`LATENCY_SIGFIG`] up to 2048 sub-buckets and needs 26 doubling buckets to
/// reach [`LATENCY_MAX_NS`], so one histogram is `27 * 1024` bins whatever it
/// records. `bench/tests/record.rs` asserts this against the pinned
/// hdrhistogram, because [`CONNECTIONS_MAX`] is derived from it.
pub const LATENCY_HISTOGRAM_BINS: u32 = 27 * 1024;

/// Largest bucket value a latency or lateness histogram can report. The
/// recorder clamps a sample above [`LATENCY_MAX_NS`] into its over-range count,
/// but `Histogram::iter_recorded` reports the *top* of each bucket, and
/// [`LATENCY_SIGFIG`] makes the bucket holding [`LATENCY_MAX_NS`] 2^25 ns wide.
/// A bucket value above this bound — or of 0, below the histogram's low bound
/// of 1 — belongs to no run the recorder or the generator can produce.
/// `bench/tests/record.rs` asserts the bound against the pinned hdrhistogram.
pub const LATENCY_BUCKET_MAX_NS: u64 = 60_028_878_847;

/// Latency-histogram bins the recorder may hold when every connection of a run
/// delivers at least once. 32 Mi `u64` bins is 256 MiB: twelve times what the
/// §9.1 run of 100 connections needs, and small enough that even the worst
/// case leaves the endpoint able to write its report.
pub const LATENCY_BINS_BUDGET: u32 = 32 * 1024 * 1024;

/// Largest connection count a recorder may be sized for. Every connection owns
/// a shard, and a shard that records an in-window latency allocates
/// [`LATENCY_HISTOGRAM_BINS`] bins, so the count bounds the histogram memory
/// one run can hold. §9.1 runs 100 connections, so the bound leaves an order
/// of magnitude of slack while keeping a mistyped `--connections` a diagnostic
/// instead of an out-of-memory kill.
pub const CONNECTIONS_MAX: u32 = LATENCY_BINS_BUDGET / LATENCY_HISTOGRAM_BINS;

/// Words in one chunk of the per-connection duplicate bitset. Private: the
/// bitset is an implementation detail of [`Recorder`], and a caller that
/// reasons about chunk size reads [`SEQ_CHUNK_SEQS`].
const SEQ_CHUNK_WORDS: usize = 8;

/// Sequence numbers one chunk of the duplicate bitset covers.
///
/// The bitset is chunked rather than dense because `seq` comes from an
/// unauthenticated payload: a dense bitset indexed by absolute `seq` lets one
/// stamp size an allocation from its own `seq`, and that cost multiplies by the
/// connection count. A chunk is allocated only when a stamp lands in it, so the
/// memory a run holds follows the number of distinct deliveries. A real run
/// numbers `seq` from 0 upwards, so consecutive deliveries share a chunk and
/// the cost stays one word per 64 deliveries.
pub const SEQ_CHUNK_SEQS: u64 = SEQ_CHUNK_WORDS as u64 * 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Window {
    pub start_ns: u64,
    pub end_ns: u64,
}

impl Window {
    pub fn contains(self, ns: u64) -> bool {
        self.start_ns <= ns && ns < self.end_ns
    }

    pub fn start_sec(self) -> u64 {
        self.start_ns / 1_000_000_000
    }

    pub fn end_sec(self) -> u64 {
        self.end_ns / 1_000_000_000
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timeline {
    pub origin_sec: u64,
    pub counts: Vec<u64>,
}

impl Timeline {
    /// Deliveries over every stored bin. Each delivery bumps `delivered` and
    /// exactly one bin, so a record's timeline total must equal its
    /// `counts.delivered`. `None` when the stored bins sum beyond a `u64`:
    /// only a deserialized record reaches that, and saturating the sum would
    /// let it match any `delivered` close to `u64::MAX`.
    pub fn total(&self) -> Option<u64> {
        self.counts
            .iter()
            .try_fold(0u64, |total, &c| total.checked_add(c))
    }

    pub fn sum_secs(&self, from_sec: u64, to_sec: u64) -> u64 {
        // Walk the stored bins, not the requested span: a persisted record can
        // name a range far wider than the bins it holds.
        let len = u64::try_from(self.counts.len()).expect("timeline length fits in u64");
        let first_sec = from_sec.max(self.origin_sec);
        let last_sec = to_sec.min(self.origin_sec.saturating_add(len));
        if last_sec <= first_sec {
            return 0;
        }
        let from_idx =
            usize::try_from(first_sec - self.origin_sec).expect("bin index fits in usize");
        let to_idx = usize::try_from(last_sec - self.origin_sec).expect("bin index fits in usize");
        self.counts[from_idx..to_idx]
            .iter()
            .fold(0u64, |total, &c| total.saturating_add(c))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryReport {
    pub delivered: u64,
    pub duplicates: u64,
    pub foreign: u64,
    pub latency_over_range: u64,
    pub clock_anomalies: u64,
    pub dropped: Option<u64>,
    pub timeline: Timeline,
    pub latency: Vec<(u64, u64)>,
}

#[derive(Debug)]
struct Shard {
    seen: HashMap<u64, [u64; SEQ_CHUNK_WORDS]>,
    delivered: u64,
    duplicates: u64,
    over_range: u64,
    anomalies: u64,
    timeline_origin_sec: Option<u64>,
    timeline: Vec<u64>,
    latency: Option<Histogram<u64>>,
}

/// Allocate one shard's latency histogram. Deferred to the shard's first
/// in-window delivery: a histogram is [`LATENCY_HISTOGRAM_BINS`] bins whatever
/// it records, so allocating one per connection in [`Recorder::new`] would
/// charge the whole run's histogram memory before the endpoint can print
/// `READY`, install its signal handler or write any report.
fn new_latency_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, LATENCY_MAX_NS, LATENCY_SIGFIG)
        .expect("hdrhistogram bounds are valid")
}

impl Shard {
    fn new() -> Self {
        Self {
            seen: HashMap::new(),
            delivered: 0,
            duplicates: 0,
            over_range: 0,
            anomalies: 0,
            timeline_origin_sec: None,
            timeline: Vec::new(),
            latency: None,
        }
    }

    /// Mark `seq` as seen and report whether it already was. Only the chunk
    /// holding `seq` is allocated, so the payload's `seq` value cannot size an
    /// allocation (see [`SEQ_CHUNK_SEQS`]).
    fn set_seq_bit(&mut self, seq: u64) -> bool {
        let chunk = seq / SEQ_CHUNK_SEQS;
        let offset = seq % SEQ_CHUNK_SEQS;
        let idx = usize::try_from(offset / 64).expect("chunk word index fits in usize");
        let mask = 1u64 << (offset % 64);
        let words = self.seen.entry(chunk).or_insert([0u64; SEQ_CHUNK_WORDS]);
        if words[idx] & mask != 0 {
            true
        } else {
            words[idx] |= mask;
            false
        }
    }

    fn bump_timeline(&mut self, now_ns: u64, origin_sec: u64) {
        let now_sec = now_ns / 1_000_000_000;
        let mut base = self.timeline_origin_sec.unwrap_or(origin_sec);
        if now_sec < base {
            // A delivery before the armed origin keeps its own second: grow the
            // timeline downwards instead of folding it into the origin bin.
            let Ok(shift) = usize::try_from(base - now_sec) else { return };
            let mut grown = vec![0u64; shift];
            grown.append(&mut self.timeline);
            self.timeline = grown;
            base = now_sec;
        }
        self.timeline_origin_sec = Some(base);
        let Ok(idx) = usize::try_from(now_sec - base) else { return };
        if self.timeline.len() <= idx {
            self.timeline.resize(idx + 1, 0);
        }
        self.timeline[idx] = self.timeline[idx].saturating_add(1);
    }
}

#[derive(Debug)]
struct Inner {
    armed: OnceLock<(u64, Window)>,
    shards: Vec<Mutex<Shard>>,
    foreign: AtomicU64,
}

#[derive(Clone)]
pub struct Recorder {
    inner: Arc<Inner>,
}

impl Recorder {
    pub fn new(connections: u32) -> Self {
        let connections = usize::try_from(connections).expect("connections fits in usize");
        let mut shards = Vec::with_capacity(connections);
        for _ in 0..connections {
            shards.push(Mutex::new(Shard::new()));
        }
        Self {
            inner: Arc::new(Inner {
                armed: OnceLock::new(),
                shards,
                foreign: AtomicU64::new(0),
            }),
        }
    }

    /// Arm the recorder for one run, reading the same `CLOCK_MONOTONIC` that
    /// later timestamps deliveries. `Err` when already armed, or when the
    /// origin or window is too far from that clock to size the timeline.
    pub fn arm(&self, origin_ns: u64, window: Window) -> Result<(), String> {
        self.arm_at(origin_ns, window, monotonic_now_ns())
    }

    /// [`Recorder::arm`] with the clock reading injected, so a caller that
    /// already holds one does not take a second, and a test can prove the skew
    /// bound without depending on the host's uptime.
    pub fn arm_at(&self, origin_ns: u64, window: Window, now_ns: u64) -> Result<(), String> {
        if origin_ns.abs_diff(now_ns) > ARM_ORIGIN_SKEW_MAX_NS {
            return Err(format!(
                "origin {origin_ns} is more than {ARM_ORIGIN_SKEW_MAX_NS} ns from the clock {now_ns}"
            ));
        }
        if window.start_ns < origin_ns {
            return Err(format!(
                "window start {} precedes origin {origin_ns}",
                window.start_ns
            ));
        }
        if window.start_ns >= window.end_ns {
            return Err(format!(
                "window start {} is not before its end {}",
                window.start_ns, window.end_ns
            ));
        }
        if window.end_ns - origin_ns > WINDOW_MAX_SPAN_NS {
            return Err(format!(
                "window end {} is more than {WINDOW_MAX_SPAN_NS} ns after origin {origin_ns}",
                window.end_ns
            ));
        }
        let origin_sec = origin_ns / 1_000_000_000;
        self.inner
            .armed
            .set((origin_sec, window))
            .map_err(|_| "recorder already armed".to_string())
    }

    /// Record one delivery of the benchmark topic. A caller that also carries
    /// the topic must go through [`Recorder::record_on_topic`] instead.
    pub fn record(&self, payload: &[u8], now_ns: u64) {
        let Some((origin_sec, window)) = self.inner.armed.get().copied() else {
            self.inner.foreign.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let Some(stamp) = Stamp::decode(payload) else {
            self.inner.foreign.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let conn_id = usize::try_from(stamp.conn_id).ok();
        let conn_id = match conn_id {
            Some(c) if c < self.inner.shards.len() => c,
            _ => {
                self.inner.foreign.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        let mut shard = self.inner.shards[conn_id].lock().expect("shard poisoned");
        if shard.set_seq_bit(stamp.seq) {
            shard.duplicates = shard.duplicates.saturating_add(1);
            return;
        }
        shard.delivered = shard.delivered.saturating_add(1);
        shard.bump_timeline(now_ns, origin_sec);
        if stamp.scheduled_ns > now_ns {
            shard.anomalies = shard.anomalies.saturating_add(1);
        } else if window.contains(stamp.scheduled_ns) {
            let latency = now_ns - stamp.scheduled_ns;
            if latency > LATENCY_MAX_NS {
                shard.over_range = shard.over_range.saturating_add(1);
            } else {
                shard
                    .latency
                    .get_or_insert_with(new_latency_histogram)
                    .record(latency.max(1))
                    .expect("latency within histogram bounds");
            }
        }
    }

    /// [`Recorder::record`] for a boundary that sees the topic as well. The
    /// broker invokes its callback once per ingested PUBLISH whatever the
    /// topic, so another topic on the benchmark endpoint is not a benchmark
    /// delivery; counting it as foreign invalidates the run (R2-Q2).
    pub fn record_on_topic(&self, topic: &str, payload: &[u8], now_ns: u64) {
        if topic == TOPIC {
            self.record(payload, now_ns);
        } else {
            self.inner.foreign.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn report(&self) -> DeliveryReport {
        let mut delivered = 0u64;
        let mut duplicates = 0u64;
        let mut over_range = 0u64;
        let mut anomalies = 0u64;
        let mut shard_timelines: Vec<(u64, Vec<u64>)> = Vec::with_capacity(self.inner.shards.len());
        let mut merged: Option<Histogram<u64>> = None;
        for shard in &self.inner.shards {
            let s = shard.lock().expect("shard poisoned");
            delivered = delivered.saturating_add(s.delivered);
            duplicates = duplicates.saturating_add(s.duplicates);
            over_range = over_range.saturating_add(s.over_range);
            anomalies = anomalies.saturating_add(s.anomalies);
            if let Some(shard_origin) = s.timeline_origin_sec {
                shard_timelines.push((shard_origin, s.timeline.clone()));
            }
            // A shard with no in-window delivery holds no histogram, so it
            // contributes nothing to merge.
            if let Some(latency) = s.latency.as_ref() {
                match merged.as_mut() {
                    Some(h) => {
                        h.add(latency).expect("merging same-bounds histograms");
                    }
                    None => {
                        merged = Some(latency.clone());
                    }
                }
            }
        }
        let latency = match merged {
            Some(h) => h
                .iter_recorded()
                .map(|iv| (iv.value_iterated_to(), iv.count_at_value()))
                .collect(),
            None => Vec::new(),
        };
        let armed_origin_sec = self.inner.armed.get().map_or(0, |&(o, _)| o);
        // Shards that saw a delivery before the armed origin carry an earlier
        // origin of their own; the merged timeline starts at the earliest.
        let origin_sec = shard_timelines
            .iter()
            .map(|&(o, _)| o)
            .min()
            .unwrap_or(armed_origin_sec);
        let mut timeline: Vec<u64> = Vec::new();
        for (shard_origin, counts) in shard_timelines {
            let Ok(offset) = usize::try_from(shard_origin - origin_sec) else { continue };
            if timeline.len() < offset + counts.len() {
                timeline.resize(offset + counts.len(), 0);
            }
            for (i, &c) in counts.iter().enumerate() {
                timeline[offset + i] = timeline[offset + i].saturating_add(c);
            }
        }
        DeliveryReport {
            delivered,
            duplicates,
            foreign: self.inner.foreign.load(Ordering::Relaxed),
            latency_over_range: over_range,
            clock_anomalies: anomalies,
            dropped: None,
            timeline: Timeline {
                origin_sec,
                counts: timeline,
            },
            latency,
        }
    }

    pub fn callback(&self) -> uring_mqtt::PublishCallback {
        let rec = self.clone();
        Arc::new(move |p: &uring_mqtt::Publish| {
            rec.record_on_topic(p.topic(), p.payload(), monotonic_now_ns());
        })
    }
}

pub fn parse_arm_line(line: &str) -> Result<(u64, Window), String> {
    let mut tokens = line.split_whitespace();
    let cmd = tokens
        .next()
        .ok_or_else(|| "expected ARM".to_string())?;
    if cmd != "ARM" {
        return Err(format!("expected ARM, got {cmd:?}"));
    }
    let origin_s = tokens.next().ok_or_else(|| "missing origin".to_string())?;
    let start_s = tokens.next().ok_or_else(|| "missing start".to_string())?;
    let end_s = tokens.next().ok_or_else(|| "missing end".to_string())?;
    if tokens.next().is_some() {
        return Err("trailing tokens".to_string());
    }
    let origin_ns: u64 = origin_s
        .parse()
        .map_err(|e| format!("origin: {e}"))?;
    let start_ns: u64 = start_s.parse().map_err(|e| format!("start: {e}"))?;
    let end_ns: u64 = end_s.parse().map_err(|e| format!("end: {e}"))?;
    if start_ns >= end_ns {
        return Err("start must be < end".to_string());
    }
    if !start_ns.is_multiple_of(1_000_000_000) {
        return Err("start must be a multiple of 1e9".to_string());
    }
    if !end_ns.is_multiple_of(1_000_000_000) {
        return Err("end must be a multiple of 1e9".to_string());
    }
    Ok((origin_ns, Window { start_ns, end_ns }))
}
