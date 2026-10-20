//! Load generator (tasks 5 and 6 of the benchmark-harness feature): closed-loop
//! QoS 1/QoS 0 throughput, connections/s, and open-loop latency against
//! `GenConfig`, reporting `GenReport`. Signatures and the public shape match
//! `features/benchmark-harness/PLAN.md` `## Data Model` and
//! `## Interface Contracts`.

use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::StreamExt;
use hdrhistogram::Histogram;
use rmqtt_codec::error::DecodeError;
use rmqtt_codec::v3::{Codec, Packet};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_util::codec::FramedRead;
use tokio_util::task::{AbortOnDropHandle, TaskTracker};

use crate::payload::{monotonic_now_ns, Stamp};
use crate::record::{Window, CONNECTIONS_MAX, LATENCY_MAX_NS, LATENCY_SIGFIG};
use crate::wire;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Throughput,
    Latency { rate: u64 },
    Connections,
}

#[derive(Debug, Clone)]
pub struct GenConfig {
    pub addr: SocketAddr,
    pub mode: Mode,
    pub qos: u8,
    pub connections: u32,
    pub qos1_window: u32,
    pub origin_ns: u64,
    pub window: Window,
    pub drain_grace: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenReport {
    pub scheduled: Option<u64>,
    pub window_scheduled: Option<u64>,
    pub offered: u64,
    pub acked: Option<u64>,
    pub missed_slots: u64,
    pub connect_errors: u64,
    pub protocol_errors: u64,
    pub stalled: u64,
    pub lateness_over_range: u64,
    pub last_write_start_ns: Option<u64>,
    pub cycles_in_window: Option<u64>,
    pub lateness: Vec<(u64, u64)>,
}

/// Shared counters every connection task increments. Held in an `Arc` so the
/// per-task clones share the same atomic cells; the post-loop report reads them.
struct Counters {
    offered: AtomicU64,
    acked: AtomicU64,
    connect_errors: AtomicU64,
    protocol_errors: AtomicU64,
    stalled: AtomicU64,
    cycles_in_window: AtomicU64,
    /// Latency-mode-only: window-due slots whose send lateness exceeded
    /// `LATENCY_MAX_NS` (the histogram cap).
    lateness_over_range: AtomicU64,
    /// Latency-mode-only: slots a QoS 1 connection could not allocate a packet
    /// id for, plus slots whose batch was dropped after `window.end_ns` (a
    /// backpressured writer whose next write would have begun at or after the
    /// window end).
    missed_slots: AtomicU64,
    /// Latency-mode-only: monotonic time of the start of the last publish
    /// write across every connection; 0 means no write has started yet.
    last_write_start_ns: AtomicU64,
    /// Latency-mode-only: one independently locked histogram per connection id
    /// (size = connections), each built with
    /// `new_with_bounds(1, LATENCY_MAX_NS, LATENCY_SIGFIG)`.
    /// `run_latency_connection` records into the entry it owns; `build_report`
    /// merges them after every task has finished. Empty in the modes that
    /// record no lateness, so they allocate no histogram at all.
    lateness_histograms: Vec<Mutex<Histogram<u64>>>,
    /// Every session reader started for this run. A reader counts into
    /// these cells until its future is dropped, so `join_and_report`
    /// closes and waits on this tracker before it builds the report.
    readers: TaskTracker,
}

impl Counters {
    /// `mode` decides the lateness histograms: each one is
    /// `LATENCY_HISTOGRAM_BINS` `u64` bins whatever it records, so only the
    /// mode that records lateness pays for them. Throughput and connections
    /// runs would otherwise allocate `connections ×` that much before their
    /// first connect, which `validate`'s `CONNECTIONS_MAX` bound then has to
    /// cover for a run that never records a sample.
    fn new(connections: u32, mode: Mode) -> Self {
        let conn_count = match mode {
            Mode::Latency { .. } => usize::try_from(connections).expect("connections fits in usize"),
            Mode::Throughput | Mode::Connections => 0,
        };
        let mut hist = Vec::with_capacity(conn_count);
        for _ in 0..conn_count {
            hist.push(Mutex::new(
                Histogram::new_with_bounds(1, LATENCY_MAX_NS, LATENCY_SIGFIG)
                    .expect("hdrhistogram bounds are valid"),
            ));
        }
        Self {
            offered: AtomicU64::new(0),
            acked: AtomicU64::new(0),
            connect_errors: AtomicU64::new(0),
            protocol_errors: AtomicU64::new(0),
            stalled: AtomicU64::new(0),
            cycles_in_window: AtomicU64::new(0),
            lateness_over_range: AtomicU64::new(0),
            missed_slots: AtomicU64::new(0),
            last_write_start_ns: AtomicU64::new(0),
            lateness_histograms: hist,
            readers: TaskTracker::new(),
        }
    }

    fn add_offered(&self, n: u64) {
        self.offered.fetch_add(n, Ordering::Relaxed);
    }
    fn add_acked(&self, n: u64) {
        self.acked.fetch_add(n, Ordering::Relaxed);
    }
    fn add_connect_errors(&self, n: u64) {
        self.connect_errors.fetch_add(n, Ordering::Relaxed);
    }
    fn add_protocol_errors(&self, n: u64) {
        self.protocol_errors.fetch_add(n, Ordering::Relaxed);
    }
    fn add_stalled(&self, n: u64) {
        self.stalled.fetch_add(n, Ordering::Relaxed);
    }
    fn add_cycles(&self, n: u64) {
        self.cycles_in_window.fetch_add(n, Ordering::Relaxed);
    }
    fn add_lateness_over_range(&self, n: u64) {
        self.lateness_over_range.fetch_add(n, Ordering::Relaxed);
    }
    fn add_missed_slots(&self, n: u64) {
        self.missed_slots.fetch_add(n, Ordering::Relaxed);
    }

    /// Start a session reader on this run's tracker. Dropping the
    /// returned handle aborts the reader, and `join_and_report` waits
    /// for the tracker, so no reader outlives the report.
    fn spawn_reader<F>(&self, reader: F) -> AbortOnDropHandle<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        AbortOnDropHandle::new(self.readers.spawn(reader))
    }

    /// CAS loop: keep the greatest `start_ns` ever passed, never overwrite a
    /// non-zero with a smaller value. 0 is the unset sentinel.
    fn update_last_write_start(&self, start_ns: u64) {
        let mut current = self.last_write_start_ns.load(Ordering::Relaxed);
        while start_ns > current {
            match self.last_write_start_ns.compare_exchange(
                current,
                start_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// Record one lateness sample into this connection's histogram. Each
    /// connection id owns its own `Mutex`, so a sample never waits on another
    /// connection's recording.
    fn record_lateness(&self, conn_id: u32, value_ns: u64) {
        let idx = usize::try_from(conn_id).expect("conn_id fits in usize");
        let cell = self
            .lateness_histograms
            .get(idx)
            .expect("latency mode sized one histogram cell per connection id");
        let mut hist = cell.lock().unwrap_or_else(PoisonError::into_inner);
        hist.record(value_ns).expect("latency within histogram bounds");
    }

    /// Merge every per-connection lateness histogram into one and emit its
    /// `(value, count)` pairs ascending. Called only after every connection
    /// task has finished, so no lock is still held.
    fn finalize_lateness(&self) -> Vec<(u64, u64)> {
        let mut merged: Option<Histogram<u64>> = None;
        for cell in &self.lateness_histograms {
            let hist = cell.lock().unwrap_or_else(PoisonError::into_inner);
            match merged.as_mut() {
                Some(m) => {
                    m.add(&*hist).expect("merging same-bounds histograms");
                }
                None => {
                    merged = Some(hist.clone());
                }
            }
        }
        match merged {
            Some(h) => h
                .iter_recorded()
                .map(|iv| (iv.value_iterated_to(), iv.count_at_value()))
                .collect(),
            None => Vec::new(),
        }
    }
}

/// Bitset over packet ids 1..=65535 with a forward-only allocation cursor.
/// Bit `k` of the bitset maps to packet id `k + 1`, so id 0 (the MQTT v3.1.1
/// "no packet id" sentinel) is not represented. `allocate` returns the lowest
/// unused id at or after the cursor; `release` clears the bit and pulls the
/// cursor back so the slot is reused on the next `allocate`.
pub struct PacketIds {
    bits: [u64; 1024],
    cursor: usize,
}

impl PacketIds {
    #[must_use]
    pub fn new() -> Self {
        Self {
            bits: [0u64; 1024],
            cursor: 0,
        }
    }

    pub fn allocate(&mut self) -> Option<NonZeroU16> {
        let start = self.cursor;
        for offset in 0..self.bits.len() {
            let idx = (start + offset) % self.bits.len();
            let word = self.bits[idx];
            if word == u64::MAX {
                continue;
            }
            // First zero bit = position of the first free id in this word.
            let bit = (!word).trailing_zeros() as usize;
            let id_offset = idx * 64 + bit;
            // The bitset holds 65536 bits; only bits 0..=65534 cover valid
            // packet ids (1..=65535). Skip the unused last bit so an
            // empty-top word never yields id 65536.
            if id_offset > 65534 {
                continue;
            }
            self.bits[idx] |= 1u64 << bit;
            self.cursor = (idx + 1) % self.bits.len();
            return NonZeroU16::new(u16::try_from(id_offset + 1).expect("id in 1..=65535"));
        }
        None
    }

    pub fn release(&mut self, id: u16) -> bool {
        if id == 0 {
            return false;
        }
        let id_offset = usize::from(id) - 1;
        let idx = id_offset / 64;
        let bit = id_offset % 64;
        let mask = 1u64 << bit;
        let was_set = self.bits[idx] & mask != 0;
        self.bits[idx] &= !mask;
        // Pull the cursor back so the next `allocate` reuses this slot.
        self.cursor = idx;
        was_set
    }
}

impl Default for PacketIds {
    fn default() -> Self {
        Self::new()
    }
}

pub const BATCH: usize = 64;
pub const IO_TIMEOUT_SECS: u64 = 5;
/// How long `join_and_report` waits, after the connection tasks are
/// aborted and joined, for the aborted readers' futures to be dropped. A
/// reader blocked inside a synchronous call an abort cannot interrupt
/// would otherwise hold the generator forever; the wait ends and the run
/// fails with that cause instead.
///
/// It is the same budget `run_generator`'s `shutdown_timeout` already
/// spends on the same thing — dropping task futures a cancel cannot
/// interrupt — so the two share it: `run_generator` hands the shutdown
/// only what this wait left (`run_generator`). The hard deadline the
/// drain gets is therefore the whole of AC-24's `window.end_ns +
/// drain_grace + IO_TIMEOUT_SECS`, unchanged, and `run_generator`'s own
/// worst case stays the one it had before the wait existed.
const READER_STOP_GRACE: Duration = Duration::from_secs(1);
/// How long before a due slot the pacer stops sleeping and yields. Private:
/// the pacer in this file is its only reader, and no caller can act on it.
const SPIN_THRESHOLD_NS: u64 = 2_000_000;
/// Write-buffer reservation per batched PUBLISH: fixed header, topic and a
/// `Stamp` payload all fit well inside this, so one batch needs no regrowth.
const PUBLISH_BYTES: usize = 96;
/// How far before `window.end_ns` the latency slot grid ends. A slot due
/// just before the window end has no time left to be written before the
/// AC-26 gate closes, so any wake-up jitter would count it as missed. The
/// lead gives every scheduled slot this much lateness budget instead.
pub const SLOT_GRID_END_LEAD_NS: u64 = 10_000_000;

/// End of the latency slot grid: `window.end_ns` less
/// [`SLOT_GRID_END_LEAD_NS`], never before `window.start_ns`.
pub fn slot_grid_end_ns(window: Window) -> u64 {
    window
        .end_ns
        .saturating_sub(SLOT_GRID_END_LEAD_NS)
        .max(window.start_ns)
}

/// Global slot count from the origin to the slot grid end, as a `u128`.
/// [`slot_counts`] reports the same number as a `u64` and panics above
/// `u64::MAX`, so every caller that cannot trust `rate` asks this first.
pub fn scheduled_slots(rate: u64, origin_ns: u64, window: Window) -> u128 {
    u128::from(slot_grid_end_ns(window).saturating_sub(origin_ns))
        .saturating_mul(u128::from(rate))
        .div_ceil(1_000_000_000)
}

pub fn slot_counts(rate: u64, origin_ns: u64, window: Window) -> (u64, u64) {
    let end_ns = u128::from(slot_grid_end_ns(window).saturating_sub(origin_ns))
        .saturating_mul(u128::from(rate));
    let scheduled = u64::try_from(end_ns.div_ceil(1_000_000_000))
        .expect("scheduled fits in u64 (slot_counts bound)");
    let start_ns = u128::from(window.start_ns.saturating_sub(origin_ns))
        .saturating_mul(u128::from(rate));
    let start_scheduled = u64::try_from(start_ns.div_ceil(1_000_000_000))
        .expect("start_scheduled fits in u64");
    let window_scheduled = scheduled - start_scheduled;
    (scheduled, window_scheduled)
}

pub fn run_generator(config: &GenConfig) -> Result<GenReport, String> {
    validate(config)?;
    let now_ns = monotonic_now_ns();
    if now_ns > config.origin_ns {
        return Err("generator started after its origin".to_string());
    }
    // `validate` refused any grace whose nanosecond count overflows a u64.
    let drain_grace_ns = u64::try_from(config.drain_grace.as_nanos())
        .expect("drain_grace fits in u64 (validate bound)");
    let hard_deadline_ns = config
        .window
        .end_ns
        .saturating_add(drain_grace_ns);
    let origin_lead = Duration::from_nanos(config.origin_ns - now_ns);

    let workers = std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .map_err(|e| format!("build tokio runtime: {e}"))?;

    let report = runtime.block_on(async move {
        let counters = Arc::new(Counters::new(config.connections, config.mode));
        tokio::time::sleep(origin_lead).await;
        let mut set = JoinSet::new();
        for conn_id in 0..config.connections {
            let task = ConnectionTask {
                conn_id,
                config: config.clone(),
                counters: Arc::clone(&counters),
            };
            set.spawn(run_connection(task));
        }
        // Read the clock here, after the origin sleep: `now_ns` was taken
        // before it, so reusing it would push this deadline later than
        // `window.end + drain_grace + IO_TIMEOUT_SECS` by the whole origin
        // lead (AC-24).
        let hard_deadline = tokio::time::Instant::now()
            + Duration::from_nanos(hard_deadline_ns.saturating_sub(monotonic_now_ns()))
            + Duration::from_secs(IO_TIMEOUT_SECS);
        // Every bound inside `join_and_report` is a Tokio timer, and a
        // timer needs a runtime worker to fire it. The watchdog answers
        // from its own thread when no worker is left to do that. Its
        // budget is `join_and_report`'s own worst case — the hard
        // deadline plus the reader grace — and the `biased` select polls
        // `join_and_report` first, so an answer that path can still give
        // always wins.
        let (watchdog_done, expired) = spawn_progress_watchdog(
            hard_deadline.saturating_duration_since(tokio::time::Instant::now())
                + READER_STOP_GRACE,
        );
        let outcome = tokio::select! {
            biased;
            result = join_and_report(&mut set, hard_deadline, config, &counters) => result,
            _ = expired => Err(format!(
                "the generator made no progress in the {} s after its hard deadline: a task \
                 blocked inside a synchronous call holds its runtime worker, and a runtime \
                 whose workers are all held fires no timer",
                READER_STOP_GRACE.as_secs()
            )),
        };
        drop(watchdog_done);
        outcome
    });

    // `join_and_report`'s reader wait and this shutdown are one
    // `READER_STOP_GRACE` budget for dropping task futures a cancel cannot
    // interrupt, so the shutdown gets only what that wait left. Without
    // this the two would stack and `run_generator` would answer later than
    // it did before the reader wait existed.
    let past_hard_deadline = Duration::from_nanos(
        monotonic_now_ns().saturating_sub(hard_deadline_ns),
    )
    .saturating_sub(Duration::from_secs(IO_TIMEOUT_SECS));
    runtime.shutdown_timeout(READER_STOP_GRACE.saturating_sub(past_hard_deadline));
    report
}

/// Start a watchdog that answers the run from outside the runtime once
/// `budget` passes. Returns the sender whose drop ends it and the
/// receiver that resolves on expiry.
///
/// `join_and_report` bounds the drain and the reader-stop wait with Tokio
/// timers; the cancellation joins between them have no bound of their own,
/// and this watchdog is the only one over them. In a multi-thread runtime
/// only a worker thread drives the timer driver: `block_on` polls the root
/// future on the calling thread and parks it, nothing more (tokio 1.53.2
/// `runtime/park.rs:284`). A task blocked inside a synchronous call an
/// abort cannot interrupt — an `eprintln!` to a stderr whose consumer
/// stopped reading — holds the worker that was polling it. On a host with
/// one CPU the generator has one worker, so that single blocked task
/// stops every one of those timers at once and no bound inside the
/// runtime can expire.
///
/// This watchdog needs no worker. It waits on its own OS thread, and on
/// expiry its `oneshot` send wakes the root future directly, so
/// `block_on` returns, `run_generator` answers with a cause and the
/// bounded printer in `main` reports it. Dropping the returned sender
/// ends the thread at once, so a run that answered on its own leaves
/// nothing sleeping behind it.
fn spawn_progress_watchdog(
    budget: Duration,
) -> (std::sync::mpsc::Sender<()>, tokio::sync::oneshot::Receiver<()>) {
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let (expired_tx, expired_rx) = tokio::sync::oneshot::channel::<()>();
    std::thread::spawn(move || {
        if matches!(
            done_rx.recv_timeout(budget),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ) {
            let _ = expired_tx.send(());
        }
    });
    (done_tx, expired_rx)
}

/// Join every connection task by `hard_deadline` and build the report.
/// Tasks still running at the deadline are aborted, joined and counted
/// `stalled`; a task that had finished by then is not, collected or not.
/// A task that panicked fails the run instead, once every other task has
/// been aborted and joined: it leaves counters no report may be built
/// from (`bench gen` then exits 1 with no report, and the runner's
/// harness errors reject the record through reconcile row 28).
/// Every session reader has stopped before the report is built, or
/// `READER_STOP_GRACE` passed after the abort and the run fails with
/// that cause.
async fn join_and_report(
    set: &mut JoinSet<()>,
    hard_deadline: tokio::time::Instant,
    config: &GenConfig,
    counters: &Counters,
) -> Result<GenReport, String> {
    let drain = async {
        while let Some(res) = set.join_next().await {
            if let Err(join_err) = res {
                if join_err.is_panic() {
                    return Err(format!("connection task panicked: {join_err}"));
                }
            }
        }
        Ok(())
    };
    // The drain gets the whole hard deadline: a connection is entitled to
    // every instant before it — `open_session` alone may spend
    // `IO_TIMEOUT_SECS` on a withheld CONNACK — and a connection that
    // ends by then is its own outcome, never a stall (AC-1). The reader
    // wait below is budgeted after the deadline instead, out of the
    // shutdown allowance `run_generator` already held.
    let mut failure = match tokio::time::timeout_at(hard_deadline, drain).await {
        Ok(Err(cause)) => Some(cause),
        Ok(Ok(())) | Err(_) => None,
    };
    // The set's length at the deadline would also count tasks that
    // finished but were not collected yet — `join_next` yields once the
    // cooperative-scheduling budget is spent — so only a join that
    // reports the abort cancelled a running task is a stall. An abort
    // that reaches a task that already finished leaves its `Ok`.
    let mut still_running: u64 = 0;
    set.abort_all();
    while let Some(res) = set.join_next().await {
        if let Err(join_err) = res {
            if join_err.is_panic() {
                if failure.is_none() {
                    failure = Some(format!("connection task panicked: {join_err}"));
                }
            } else {
                still_running += 1;
            }
        }
    }
    // Every connection task has ended, so every reader is finished or
    // aborted (its `AbortOnDropHandle` was dropped). Wait until each
    // reader's future is dropped: a reader still mid-poll could
    // otherwise count while the report reads the counters. An abort
    // cannot interrupt a reader blocked inside a synchronous call — an
    // `eprintln!` to a stderr whose consumer stopped reading — so the
    // wait ends after `READER_STOP_GRACE` and its expiry fails the run:
    // no report may be built while a reader can still count. This wait
    // is a Tokio timer, so it expires only while a worker is left to
    // fire it; `spawn_progress_watchdog` answers for the run when the
    // blocked readers hold them all.
    counters.readers.close();
    let readers_stopped = tokio::time::timeout(READER_STOP_GRACE, counters.readers.wait())
        .await
        .is_ok();
    if let Some(cause) = failure {
        return Err(cause);
    }
    if !readers_stopped {
        return Err(format!(
            "session readers did not stop in the {} s after their connections were aborted",
            READER_STOP_GRACE.as_secs()
        ));
    }
    counters.add_stalled(still_running);
    Ok(build_report(config, counters))
}

fn validate(config: &GenConfig) -> Result<(), String> {
    if config.qos > 1 {
        return Err(format!("qos must be 0 or 1, got {}", config.qos));
    }
    // Same bound and wording as `run_server` (`src/server.rs:54`): a latency
    // run sizes one lateness histogram per connection, so the count bounds the
    // memory the generator holds. A mistyped value must be refused here —
    // past this point an out-of-memory kill leaves the runner with a timeout
    // instead of a report.
    if config.connections == 0 || config.connections > CONNECTIONS_MAX {
        return Err(format!(
            "connections must be between 1 and {CONNECTIONS_MAX}, got {}",
            config.connections
        ));
    }
    // A QoS 1 connection has `u16::MAX` packet ids, so a larger window holds
    // permits no packet can use, and a zero window never sends.
    if config.qos == 1 && !(1..=u32::from(u16::MAX)).contains(&config.qos1_window) {
        return Err(format!(
            "qos1_window must be between 1 and {}, got {}",
            u16::MAX,
            config.qos1_window
        ));
    }
    if let Mode::Latency { rate } = config.mode {
        if rate == 0 {
            return Err("latency rate must be at least 1".to_string());
        }
        // `slot_counts` reports the schedule as a u64; a rate high enough to
        // overflow it would panic there instead of returning a report. The
        // bound is representability, not a chosen rate ceiling.
        let scheduled = scheduled_slots(rate, config.origin_ns, config.window);
        if scheduled > u128::from(u64::MAX) {
            return Err(format!(
                "latency rate {rate} schedules {scheduled} slots over this window, \
                 more than a u64 can hold"
            ));
        }
    }
    if config.window.start_ns >= config.window.end_ns {
        return Err("window start must be before window end".to_string());
    }
    // Every slot and every deadline is an offset from the origin, so a
    // window starting before it has no schedule: `slot_counts` saturates
    // its start offset to zero and `run_generator` would sleep to the
    // origin long after `window.end + drain_grace`, returning accounting
    // for a window that had already passed.
    if config.window.start_ns < config.origin_ns {
        return Err(format!(
            "window start {} must be at or after the origin {}",
            config.window.start_ns, config.origin_ns
        ));
    }
    // `run_generator` and every per-connection deadline add this grace to
    // `window.end_ns` in nanoseconds. A `Duration` built from seconds can
    // hold more nanoseconds than a `u64`, so the value is refused here:
    // past this point the conversion would panic before `GenReport` is
    // written and the runner would see a signal instead of a report.
    if u64::try_from(config.drain_grace.as_nanos()).is_err() {
        return Err(format!(
            "drain grace {}s is more nanoseconds than a u64 can hold",
            config.drain_grace.as_secs()
        ));
    }
    Ok(())
}

fn build_report(config: &GenConfig, counters: &Counters) -> GenReport {
    let mut scheduled = None;
    let mut window_scheduled = None;
    if let Mode::Latency { rate } = config.mode {
        let (s, w) = slot_counts(rate, config.origin_ns, config.window);
        scheduled = Some(s);
        window_scheduled = Some(w);
    }
    // `acked` is a QoS 1 publishing-mode field only (`PLAN.md ## Data Model`):
    // QoS 0 has no PUBACK, and connections mode publishes nothing.
    let acked = match config.mode {
        Mode::Throughput | Mode::Latency { .. } if config.qos == 1 => {
            Some(counters.acked.load(Ordering::Relaxed))
        }
        Mode::Throughput | Mode::Latency { .. } | Mode::Connections => None,
    };
    let cycles_in_window = match config.mode {
        Mode::Connections => Some(counters.cycles_in_window.load(Ordering::Relaxed)),
        _ => None,
    };
    let last_write_start_ns = counters.last_write_start_ns.load(Ordering::Relaxed);
    GenReport {
        scheduled,
        window_scheduled,
        offered: counters.offered.load(Ordering::Relaxed),
        acked,
        missed_slots: counters.missed_slots.load(Ordering::Relaxed),
        connect_errors: counters.connect_errors.load(Ordering::Relaxed),
        protocol_errors: counters.protocol_errors.load(Ordering::Relaxed),
        stalled: counters.stalled.load(Ordering::Relaxed),
        lateness_over_range: counters.lateness_over_range.load(Ordering::Relaxed),
        last_write_start_ns: if last_write_start_ns == 0 {
            None
        } else {
            Some(last_write_start_ns)
        },
        cycles_in_window,
        lateness: counters.finalize_lateness(),
    }
}

struct ConnectionTask {
    conn_id: u32,
    config: GenConfig,
    counters: Arc<Counters>,
}

async fn run_connection(task: ConnectionTask) {
    let conn_id = task.conn_id;
    let result = match task.config.mode {
        Mode::Throughput => run_throughput_connection(task).await,
        Mode::Connections => run_connections_mode(task).await,
        Mode::Latency { .. } => run_latency_connection(task).await,
    };
    if let Err(e) = result {
        eprintln!("gen: connection {conn_id}: {e}");
    }
}

async fn run_throughput_connection(task: ConnectionTask) -> Result<(), String> {
    let client_id = format!("bench-p{}", task.conn_id);
    let (read_half, write_half, deadline_instant) =
        match open_session(&task.config, &client_id).await {
            Ok(v) => v,
            Err(OpenError::Connect(reason)) => {
                task.counters.add_connect_errors(1);
                eprintln!("gen: connection {}: {reason}", task.conn_id);
                return Ok(());
            }
            Err(OpenError::Protocol(reason)) => {
                task.counters.add_connect_errors(1);
                task.counters.add_protocol_errors(1);
                // The counters say a CONNACK invalidated the run; only this
                // line says which reply did it.
                eprintln!("gen: connection {}: {reason}", task.conn_id);
                return Ok(());
            }
        };

    match task.config.qos {
        0 => run_qos0_throughput(read_half, write_half, task, deadline_instant).await,
        _ => run_qos1_throughput(read_half, write_half, task, deadline_instant).await,
    }
}

/// Wait for CONNACK and split the stream into a framed reader + write half.
/// `deadline_instant` is the wall-clock equivalent of `window.end_ns + drain_grace`.
enum OpenError {
    /// Which handshake step failed and why: the operation, then the OS
    /// error or the timeout. Never carries payload bytes.
    Connect(String),
    /// Why the reply is not a valid CONNACK for this CONNECT. Every reason
    /// is a fixed short text with at most one integer — the codec's
    /// non-`Io` `DecodeError`s and `ConnectAckReason` both print a
    /// constant — so the diagnostic a caller prints stays bounded.
    Protocol(String),
}

async fn open_session(
    config: &GenConfig,
    client_id: &str,
) -> Result<(OwnedReadHalf, OwnedWriteHalf, tokio::time::Instant), OpenError> {
    let connect_fut = TcpStream::connect(config.addr);
    let stream = match tokio::time::timeout(Duration::from_secs(IO_TIMEOUT_SECS), connect_fut).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            return Err(OpenError::Connect(format!(
                "connect {}: {e}",
                config.addr
            )));
        }
        Err(_) => {
            return Err(OpenError::Connect(format!(
                "connect {}: timed out after {IO_TIMEOUT_SECS} s",
                config.addr
            )));
        }
    };
    if let Err(e) = stream.set_nodelay(true) {
        return Err(OpenError::Connect(format!("set TCP_NODELAY: {e}")));
    }
    let (read_half, mut write_half) = stream.into_split();
    let connect_bytes = wire::encode_connect(client_id);
    match tokio::time::timeout(
        Duration::from_secs(IO_TIMEOUT_SECS),
        write_half.write_all(&connect_bytes),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            return Err(OpenError::Connect(format!("write CONNECT: {e}")));
        }
        Err(_) => {
            return Err(OpenError::Connect(format!(
                "write CONNECT: timed out after {IO_TIMEOUT_SECS} s"
            )));
        }
    }
    let mut framed = FramedRead::new(read_half, Codec::new(wire::INBOUND_REMAINING_LENGTH_MAX));
    let (connack, remaining_length) = match tokio::time::timeout(
        Duration::from_secs(IO_TIMEOUT_SECS),
        framed.next(),
    )
    .await
    {
        Ok(Some(Ok((Packet::ConnectAck(ack), remaining_length)))) => (ack, remaining_length),
        Ok(Some(Ok(_))) => {
            return Err(OpenError::Protocol(
                "first packet was not a CONNACK".to_string(),
            ))
        }
        // A read failure is the transport's; a frame the codec rejects
        // — reserved flag bits, a return code MQTT 3.1.1 does not
        // define — is the broker's malformed reply, counted like every
        // other malformed frame (`qos1_reader_task` splits the same way).
        Ok(Some(Err(DecodeError::Io(e)))) => {
            return Err(OpenError::Connect(format!("read CONNACK: {e}")))
        }
        Ok(Some(Err(e))) => {
            return Err(OpenError::Protocol(format!("CONNACK decode failed: {e}")))
        }
        Ok(None) => {
            return Err(OpenError::Connect("peer closed before CONNACK".to_string()))
        }
        Err(_) => {
            return Err(OpenError::Connect(format!(
                "read CONNACK: timed out after {IO_TIMEOUT_SECS} s"
            )))
        }
    };
    // The pinned codec checks only that a CONNACK body holds at least
    // two bytes (`rmqtt-codec` 0.2.2 `v3/decode.rs:93`) and ignores the
    // rest, so a longer body would pass every check below.
    if remaining_length != wire::CONNACK_REMAINING_LENGTH {
        return Err(OpenError::Protocol(format!(
            "CONNACK remaining length was {remaining_length}, expected {}",
            wire::CONNACK_REMAINING_LENGTH
        )));
    }
    if !matches!(
        connack.return_code,
        rmqtt_codec::v3::ConnectAckReason::ConnectionAccepted
    ) {
        return Err(OpenError::Protocol(format!(
            "CONNACK refused the session: {:?}",
            connack.return_code
        )));
    }
    // `wire::encode_connect` always sets CleanSession, so
    // [MQTT-3.2.2-1] requires Session Present 0; a broker claiming a
    // stored session is not answering this CONNECT.
    if connack.session_present {
        return Err(OpenError::Protocol(
            "CONNACK claimed Session Present for a CleanSession CONNECT".to_string(),
        ));
    }
    let now = monotonic_now_ns();
    let hard_deadline_ns = config
        .window
        .end_ns
        .saturating_add(u64::try_from(config.drain_grace.as_nanos()).unwrap_or(0));
    let deadline_instant = tokio::time::Instant::now()
        + Duration::from_nanos(hard_deadline_ns.saturating_sub(now).max(1));
    // Bytes read past the CONNACK sit in the codec's buffer, which
    // `into_inner` discards. Before its first PUBLISH this client is owed
    // no frame — CleanSession, no subscription — so any such byte, a
    // whole frame or part of one, is the broker's violation. Refusing it
    // here covers bytes that arrive with the CONNACK on every publishing
    // path, before any reader starts.
    let read_ahead = framed.read_buffer().len();
    if read_ahead > 0 {
        return Err(OpenError::Protocol(format!(
            "CONNACK was followed by {read_ahead} unsolicited bytes"
        )));
    }
    let read_half = framed.into_inner();
    Ok((read_half, write_half, deadline_instant))
}

/// The cause a throughput publish write reports when it did not
/// complete, worded as `latency_send_batch` words it; `None` when the
/// write completed.
fn publish_write_failure(
    result: Result<std::io::Result<()>, tokio::time::error::Elapsed>,
) -> Option<String> {
    match result {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(format!("publish write failed: {e}")),
        Err(_) => Some("publish write did not finish by the hard deadline".to_string()),
    }
}

/// The cause a session teardown reports when its DISCONNECT write or
/// its write-half shutdown failed, worded as `publish_write_failure`
/// words a publish; `None` when both completed. `disconnect` is `None`
/// when no DISCONNECT was written (a publish write had failed).
fn teardown_failure(
    disconnect: Option<Result<std::io::Result<()>, tokio::time::error::Elapsed>>,
    shutdown: std::io::Result<()>,
) -> Option<String> {
    let mut causes = Vec::new();
    match disconnect {
        None | Some(Ok(Ok(()))) => {}
        Some(Ok(Err(e))) => causes.push(format!("DISCONNECT write failed: {e}")),
        Some(Err(_)) => {
            causes.push("DISCONNECT write did not finish by the hard deadline".to_string());
        }
    }
    if let Err(e) = shutdown {
        causes.push(format!("write-half shutdown failed: {e}"));
    }
    if causes.is_empty() { None } else { Some(causes.join("; ")) }
}

/// A connection's result: its publish or pacer failure, then its
/// teardown failure, joined with `; ` when both happened.
fn session_result(failure: Option<String>, teardown: Option<String>) -> Result<(), String> {
    match (failure, teardown) {
        (None, None) => Ok(()),
        (Some(cause), None) | (None, Some(cause)) => Err(cause),
        (Some(first), Some(then)) => Err(format!("{first}; {then}")),
    }
}

async fn run_qos0_throughput(
    read_half: OwnedReadHalf,
    write_half: OwnedWriteHalf,
    task: ConnectionTask,
    deadline_instant: tokio::time::Instant,
) -> Result<(), String> {
    let reader_handle =
        task.counters
            .spawn_reader(qos0_reader_task(task.conn_id, read_half, Arc::clone(&task.counters)));
    run_qos0_throughput_with_reader(reader_handle, write_half, task, deadline_instant).await
}

/// [`run_qos0_throughput`] once its session reader is running: publish
/// until the window ends, then tear down. The reader's handle is taken
/// rather than spawned here, so a test can hand in a reader that
/// panics and observe both places its panic is re-raised.
async fn run_qos0_throughput_with_reader(
    reader_handle: AbortOnDropHandle<()>,
    mut write_half: OwnedWriteHalf,
    task: ConnectionTask,
    deadline_instant: tokio::time::Instant,
) -> Result<(), String> {
    let mut next_seq: u64 = 0;
    let mut write_buf = Vec::with_capacity(BATCH * PUBLISH_BYTES);
    while monotonic_now_ns() < task.config.window.end_ns {
        write_buf.clear();
        for _ in 0..BATCH {
            let stamp = Stamp {
                conn_id: task.conn_id,
                seq: next_seq,
                scheduled_ns: monotonic_now_ns(),
            };
            next_seq = next_seq.saturating_add(1);
            let payload = stamp.encode();
            wire::encode_publish(&mut write_buf, 0, None, &payload);
        }
        let batch_len = u64::try_from(BATCH).expect("BATCH fits in u64");
        // A batch is offered once its write begins: publishes of a
        // partly written batch can be delivered, so counting only
        // completed writes would let `delivered` exceed `offered`.
        task.counters.add_offered(batch_len);
        let write_result =
            tokio::time::timeout_at(deadline_instant, write_half.write_all(&write_buf)).await;
        if let Some(cause) = publish_write_failure(write_result) {
            // QoS 0 has no acknowledgement to resolve a publish by, so
            // every publish of a batch whose write did not complete is
            // unresolved.
            task.counters.add_stalled(batch_len);
            // Stop the reader before the connection ends: a dropped
            // handle only requests the abort, and a reader in the middle
            // of a poll can still count a protocol error after the
            // connection returns.
            reader_handle.abort();
            reraise_reader_panic(reader_handle.await);
            return Err(cause);
        }
    }
    // Bounded by the hard deadline, not `IO_TIMEOUT_SECS` after it: the
    // time AC-24 allows past the deadline belongs to `run_generator`'s
    // backstop, which counts an unfinished connection `stalled`
    // (`qos1_drain_and_disconnect` bounds its DISCONNECT the same way).
    let disconnect =
        tokio::time::timeout_at(deadline_instant, write_half.write_all(&wire::DISCONNECT))
            .await;
    let shutdown = write_half.shutdown().await;
    // A peer that keeps its socket open after DISCONNECT never sends
    // the EOF the reader waits for, so an unbounded join would outlast
    // the deadline and leave the backstop to count the whole
    // connection `stalled`. The reader can still count a protocol
    // error, so the aborted handle is awaited before the report is
    // built (as in `qos1_drain_and_disconnect`).
    join_reader_by(reader_handle, deadline_instant).await;
    session_result(None, teardown_failure(Some(disconnect), shutdown))
}

async fn run_qos1_throughput(
    read_half: OwnedReadHalf,
    mut write_half: OwnedWriteHalf,
    task: ConnectionTask,
    deadline_instant: tokio::time::Instant,
) -> Result<(), String> {
    let semaphore = Arc::new(Semaphore::new(
        usize::try_from(task.config.qos1_window).expect("qos1_window fits in usize"),
    ));
    let packet_ids = Arc::new(std::sync::Mutex::new(PacketIds::new()));
    let in_flight: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let reader_handle = task.counters.spawn_reader(qos1_reader_task(
        task.conn_id,
        read_half,
        Arc::clone(&task.counters),
        Arc::clone(&semaphore),
        Arc::clone(&packet_ids),
        Arc::clone(&in_flight),
    ));

    let mut next_seq: u64 = 0;
    let mut write_buf: Vec<u8> = Vec::with_capacity(BATCH * PUBLISH_BYTES);
    let window_end_instant = tokio::time::Instant::now()
        + Duration::from_nanos(
            task.config
                .window
                .end_ns
                .saturating_sub(monotonic_now_ns())
                .max(1),
        );
    let mut write_failure: Option<String> = None;
    while monotonic_now_ns() < task.config.window.end_ns {
        let Ok(Ok(permit)) = tokio::time::timeout_at(
            window_end_instant,
            Arc::clone(&semaphore).acquire_owned(),
        )
        .await
        else {
            break;
        };
        let permits: Vec<OwnedSemaphorePermit> = fill_batch(permit, &semaphore);
        let Some((alloc_ids, payload_len)) = encode_qos1_batch(
            &task,
            &permits,
            &packet_ids,
            &mut next_seq,
            &mut write_buf,
        ) else {
            drop(permits);
            continue;
        };
        let batch_len = u64::try_from(alloc_ids.len()).expect("batch size fits in u64");
        // Submitted before the write: the reader decrements `in_flight`
        // and adds a permit per accepted PUBACK, and a PUBACK for this
        // batch's first PUBLISH can arrive before `write_all` returns.
        task.counters.add_offered(batch_len);
        in_flight.fetch_add(batch_len, Ordering::Relaxed);
        for permit in permits {
            permit.forget();
        }
        let write_result =
            tokio::time::timeout_at(deadline_instant, write_half.write_all(&write_buf[..payload_len]))
                .await;
        if let Some(cause) = publish_write_failure(write_result) {
            // The batch stays in flight: a PUBACK for a PUBLISH that was
            // written still resolves its id, and the drain counts the
            // rest as `stalled`.
            write_failure = Some(cause);
            break;
        }
    }
    qos1_drain_and_disconnect(
        &mut write_half,
        &in_flight,
        deadline_instant,
        &task.counters,
        reader_handle,
        write_failure,
    )
    .await
}

/// Carry a session reader's panic into the connection task that joined
/// it. `run_connection` only prints a connection's `Err`, so a reader
/// panic returned as an error would leave the run reconciling;
/// re-raised, it panics the connection task and `join_and_report`
/// fails the run with its message. A reader ended by `abort` carries no
/// evidence.
fn reraise_reader_panic(joined: Result<(), tokio::task::JoinError>) {
    if let Err(join_err) = joined {
        if join_err.is_panic() {
            std::panic::resume_unwind(join_err.into_panic());
        }
    }
}

/// Join a session reader by `deadline`, abort it when it is still
/// running then, and re-raise its panic ([`reraise_reader_panic`]).
async fn join_reader_by(mut handle: AbortOnDropHandle<()>, deadline: tokio::time::Instant) {
    let joined = match tokio::time::timeout_at(deadline, &mut handle).await {
        Ok(joined) => joined,
        Err(_) => {
            handle.abort();
            handle.await
        }
    };
    reraise_reader_panic(joined);
}

/// Drain every in-flight packet id up to the deadline, then write
/// DISCONNECT unless a publish write failed (`write_failure`) (a
/// partial PUBLISH may be on the wire), shut the writer half down and
/// end the reader. Ids still in flight at the deadline count in
/// `stalled`, read only after the reader has stopped, so no PUBACK is
/// counted both `acked` and `stalled`. Returns the connection's
/// result: the publish failure, then any teardown failure.
async fn qos1_drain_and_disconnect(
    write_half: &mut OwnedWriteHalf,
    in_flight: &AtomicU64,
    deadline_instant: tokio::time::Instant,
    counters: &Counters,
    reader_handle: AbortOnDropHandle<()>,
    write_failure: Option<String>,
) -> Result<(), String> {
    let drain_fut = async {
        while in_flight.load(Ordering::Relaxed) > 0 {
            tokio::task::yield_now().await;
        }
    };
    let drained = tokio::time::timeout_at(deadline_instant, drain_fut).await.is_ok();
    let mut reader_handle = Some(reader_handle);
    if !drained || write_failure.is_some() {
        // Stop the reader before reading what is left: a PUBACK it
        // counted after that read would be both `acked` and `stalled`.
        // The aborted handle resolves once the reader is no longer
        // running — at most the end of the poll it is in.
        if let Some(handle) = reader_handle.take() {
            handle.abort();
            reraise_reader_panic(handle.await);
        }
        let leaked = in_flight.load(Ordering::Relaxed);
        if leaked > 0 {
            counters.add_stalled(leaked);
        }
    }
    // Bounded by the hard deadline, not `IO_TIMEOUT_SECS` after it: the
    // time AC-24 allows past the deadline belongs to `run_generator`'s
    // backstop, which counts an unfinished connection `stalled`, so
    // spending it here would count leftovers twice (`latency_teardown`
    // bounds its DISCONNECT the same way).
    let disconnect = if write_failure.is_some() {
        None
    } else {
        Some(
            tokio::time::timeout_at(deadline_instant, write_half.write_all(&wire::DISCONNECT))
                .await,
        )
    };
    let shutdown = write_half.shutdown().await;
    if let Some(handle) = reader_handle {
        // A peer that keeps its socket open after DISCONNECT never sends
        // the EOF the reader waits for, so an unbounded join would
        // outlast the deadline and leave the backstop to count the whole
        // connection `stalled`. The reader can still count a protocol
        // error, so the aborted handle is awaited before the report is
        // built (as in `latency_teardown`).
        join_reader_by(handle, deadline_instant).await;
    }
    session_result(write_failure, teardown_failure(disconnect, shutdown))
}

/// Reader task: pull PUBACKs, release packet ids, retry the semaphore. Other
/// packet types count as protocol errors and the connection is dropped on
/// transport errors. Anything the codec rejects — an oversized or malformed
/// frame — counts as a protocol error too, so a broker that answers with
/// garbage cannot leave a run reconciling as valid; a transport error ends
/// the reader with a diagnostic only, exactly as in `qos1_latency_reader`.
async fn qos1_reader_task(
    conn_id: u32,
    read_half: OwnedReadHalf,
    counters: Arc<Counters>,
    semaphore: Arc<Semaphore>,
    packet_ids: Arc<std::sync::Mutex<PacketIds>>,
    in_flight: Arc<AtomicU64>,
) {
    let mut framed = FramedRead::new(read_half, Codec::new(wire::INBOUND_REMAINING_LENGTH_MAX));
    // One diagnostic per reader: protocol_errors keeps the count.
    let mut rejection_reported = false;
    while let Some(item) = framed.next().await {
        match item {
            Ok((Packet::PublishAck { packet_id }, _consumed)) => {
                let id = packet_id.get();
                let released = {
                    let mut ids = packet_ids.lock().expect("packet ids poisoned");
                    ids.release(id)
                };
                if released {
                    counters.add_acked(1);
                    semaphore.add_permits(1);
                    in_flight.fetch_sub(1, Ordering::Relaxed);
                } else {
                    counters.add_protocol_errors(1);
                    if !rejection_reported {
                        rejection_reported = true;
                        eprintln!("gen: connection {conn_id}: QoS 1 reader rejected a PUBACK for packet id {id}, which is not in flight");
                    }
                }
            }
            Ok((packet, _consumed)) => {
                counters.add_protocol_errors(1);
                if !rejection_reported {
                    rejection_reported = true;
                    eprintln!(
                        "gen: connection {conn_id}: QoS 1 reader rejected an unexpected {}",
                        wire::packet_kind(&packet)
                    );
                }
            }
            Err(DecodeError::Io(e)) => {
                eprintln!("gen: connection {conn_id}: QoS 1 reader read failed: {e}");
                return;
            }
            Err(e) => {
                counters.add_protocol_errors(1);
                eprintln!("gen: connection {conn_id}: QoS 1 reader decode failed: {e}");
                return;
            }
        }
    }
}

/// QoS 0 session reader: a QoS 0 publisher subscribes to nothing, so
/// every frame the broker sends is a protocol error. Anything the codec
/// rejects — an oversized or malformed frame — is one too, and counting
/// it keeps a broker that answers with garbage from reconciling as a
/// valid run (`qos1_latency_reader` splits the same two cases); a
/// transport error ends the reader with a diagnostic only.
async fn qos0_reader_task(conn_id: u32, read_half: OwnedReadHalf, counters: Arc<Counters>) {
    let mut framed = FramedRead::new(read_half, Codec::new(wire::INBOUND_REMAINING_LENGTH_MAX));
    // One diagnostic per reader: protocol_errors keeps the count.
    let mut rejection_reported = false;
    while let Some(item) = framed.next().await {
        match item {
            Ok((packet, _)) => {
                counters.add_protocol_errors(1);
                if !rejection_reported {
                    rejection_reported = true;
                    eprintln!(
                        "gen: connection {conn_id}: QoS 0 reader rejected an unexpected {}",
                        wire::packet_kind(&packet)
                    );
                }
            }
            Err(DecodeError::Io(e)) => {
                eprintln!("gen: connection {conn_id}: QoS 0 reader read failed: {e}");
                return;
            }
            Err(e) => {
                counters.add_protocol_errors(1);
                eprintln!("gen: connection {conn_id}: QoS 0 reader decode failed: {e}");
                return;
            }
        }
    }
}

/// Fill a batch of permits by topping up with `try_acquire_owned` up to BATCH.
fn fill_batch(
    first: OwnedSemaphorePermit,
    semaphore: &Arc<Semaphore>,
) -> Vec<OwnedSemaphorePermit> {
    let mut permits = vec![first];
    for _ in 1..BATCH {
        match Arc::clone(semaphore).try_acquire_owned() {
            Ok(p) => permits.push(p),
            Err(_) => break,
        }
    }
    permits
}

/// Encode one batch of QoS 1 PUBLISHes. Returns `Some((ids, payload_len))` on
/// success, `None` when no ids were allocated (or allocation failed). Each
/// `Stamp::encode` stamps `scheduled_ns` with the current monotonic clock.
fn encode_qos1_batch(
    task: &ConnectionTask,
    permits: &[OwnedSemaphorePermit],
    packet_ids: &Arc<std::sync::Mutex<PacketIds>>,
    next_seq: &mut u64,
    write_buf: &mut Vec<u8>,
) -> Option<(Vec<NonZeroU16>, usize)> {
    write_buf.clear();
    let mut alloc_ids: Vec<NonZeroU16> = Vec::with_capacity(permits.len());
    for _permit in permits {
        let id = {
            let mut ids = packet_ids.lock().expect("packet ids poisoned");
            ids.allocate()
        };
        let Some(id) = id else {
            break;
        };
        alloc_ids.push(id);
        let stamp = Stamp {
            conn_id: task.conn_id,
            seq: *next_seq,
            scheduled_ns: monotonic_now_ns(),
        };
        *next_seq = next_seq.saturating_add(1);
        let payload = stamp.encode();
        wire::encode_publish(write_buf, 1, Some(id), &payload);
    }
    if alloc_ids.is_empty() {
        return None;
    }
    let payload_len = write_buf.len();
    Some((alloc_ids, payload_len))
}

/// How a connection cycle's socket ended after the generator sent DISCONNECT.
enum CloseOutcome {
    /// The broker sent its FIN, after `unexpected` bytes it should not have
    /// sent at all.
    Closed { unexpected: u64 },
    /// The broker reset the connection instead of closing it cleanly, after
    /// `unexpected` bytes it should not have sent at all. The generator still
    /// did not close first, so the cycle costs no TIME_WAIT.
    Reset { unexpected: u64 },
    /// The read failed for a reason that does not prove the broker ended the
    /// connection — ENOMEM, EIO, an interrupted read. The cycle's close is
    /// unproven, so it cannot be counted as clean.
    Failed,
}

/// Does this read error prove the peer tore the connection down? ECONNRESET
/// is the broker's RST, and ECONNABORTED is the same teardown reported from
/// the local end; neither leaves a client-side TIME_WAIT, which is the one
/// property the close loop needs. Every other kind says the read failed, not
/// that the broker closed.
fn ends_the_connection(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
    )
}

async fn run_connections_mode(task: ConnectionTask) -> Result<(), String> {
    // One CONNACK diagnostic per task, not per cycle: a broker that answers
    // every cycle the same way would otherwise print one line per connection
    // it accepts, and this mode opens thousands of them.
    let mut connack_reported = false;
    while monotonic_now_ns() < task.config.window.end_ns {
        let cycle_client = format!("bench-c{}", task.conn_id);
        let Ok(Ok(mut stream)) = tokio::time::timeout(
            Duration::from_secs(IO_TIMEOUT_SECS),
            TcpStream::connect(task.config.addr),
        )
        .await
        else {
            task.counters.add_connect_errors(1);
            continue;
        };
        if stream.set_nodelay(true).is_err() {
            drop(stream);
            task.counters.add_connect_errors(1);
            continue;
        }
        let connect_bytes = wire::encode_connect(&cycle_client);
        if !matches!(
            tokio::time::timeout(
                Duration::from_secs(IO_TIMEOUT_SECS),
                stream.write_all(&connect_bytes),
            )
            .await,
            Ok(Ok(()))
        ) {
            drop(stream);
            task.counters.add_connect_errors(1);
            continue;
        }
        let mut buf = [0u8; 4];
        let read_result = tokio::time::timeout(
            Duration::from_secs(IO_TIMEOUT_SECS),
            stream.read_exact(&mut buf),
        )
        .await;
        match read_result {
            Ok(Ok(_)) => {}
            _ => {
                drop(stream);
                task.counters.add_connect_errors(1);
                continue;
            }
        }
        // The cycle completes when the CONNACK is in hand, so the rate is
        // timestamped here and not before the await: a read that starts
        // inside the window and completes after `window.end_ns` is not a
        // connection the broker accepted inside the window.
        let connack_at = monotonic_now_ns();
        // buf[2] is the acknowledge-flags byte: reserved bits must be 0, and Session Present must be 0 for the CleanSession CONNECT this cycle sent ([MQTT-3.2.2-1]).
        if buf[0] != 0x20 || buf[1] != 0x02 || buf[2] != 0x00 || buf[3] != 0x00 {
            drop(stream);
            task.counters.add_protocol_errors(1);
            // `protocol_errors` says a CONNACK invalidated the run; only
            // this line says which reply did it (`open_session` carries the
            // same detail on the other modes' paths).
            if !connack_reported {
                connack_reported = true;
                eprintln!(
                    "gen: connection {}: CONNACK was not 20 02 00 00: {:02x?}",
                    task.conn_id, buf
                );
            }
            continue;
        }
        if task.config.window.contains(connack_at) {
            task.counters.add_cycles(1);
        }
        if !matches!(
            tokio::time::timeout(
                Duration::from_secs(IO_TIMEOUT_SECS),
                stream.write_all(&wire::DISCONNECT),
            )
            .await,
            Ok(Ok(()))
        ) {
            drop(stream);
            task.counters.add_protocol_errors(1);
            continue;
        }
        // Read until the broker's FIN before dropping the stream, so the
        // broker is the side that closes first. A generator that closes
        // first leaves one TIME_WAIT socket per cycle, which exhausts the
        // host's ephemeral port range in about a second and makes every
        // later `connect` fail with EADDRNOTAVAIL — the run would then
        // measure the port range instead of the broker. Only EOF or a peer
        // teardown proves the broker closed; bytes after DISCONNECT, a read
        // error of any other kind, and a broker that never closes are all
        // protocol errors.
        let mut fin = [0u8; 1];
        let close = tokio::time::timeout(Duration::from_secs(IO_TIMEOUT_SECS), async {
            let mut unexpected = 0u64;
            loop {
                match stream.read(&mut fin).await {
                    // FIN: this cycle leaves no client-side TIME_WAIT.
                    Ok(0) => return CloseOutcome::Closed { unexpected },
                    // The broker must send nothing after DISCONNECT. Keep
                    // reading so it still closes first, and report the
                    // violation once the close arrives.
                    Ok(_) => unexpected = unexpected.saturating_add(1),
                    // A reset instead of a FIN also ends the connection
                    // without a client-side TIME_WAIT, but it must not erase
                    // the bytes already read after DISCONNECT. Any other
                    // read error leaves the close unproven.
                    Err(e) if ends_the_connection(e.kind()) => {
                        return CloseOutcome::Reset { unexpected }
                    }
                    Err(_) => return CloseOutcome::Failed,
                }
            }
        })
        .await;
        drop(stream);
        match close {
            Ok(
                CloseOutcome::Closed { unexpected: 0 }
                | CloseOutcome::Reset { unexpected: 0 },
            ) => {}
            // Bytes after DISCONNECT — before a FIN or before a reset — a
            // read error that does not prove the broker closed, or a broker
            // that never closed. `protocol_errors` is the counter the
            // never-closed case already uses, and every one of these cycles
            // has to invalidate the run (decision R2-Q2).
            Ok(
                CloseOutcome::Closed { .. }
                | CloseOutcome::Reset { .. }
                | CloseOutcome::Failed,
            )
            | Err(_) => {
                task.counters.add_protocol_errors(1);
            }
        }
    }
    Ok(())
}

// Reference the QoS 1 throughput writer's `permit.forget()` path: nothing
// else in this module calls `OwnedSemaphorePermit` outside of the writer's
// `permit.forget()` and `for _permit in &permits` consumers, so the import
// stays in use.

/// Wait until CLOCK_MONOTONIC reaches `due_ns`. Sleeps for `remaining -
/// SPIN_THRESHOLD_NS` when that is positive, then yields until the deadline
/// — `tokio::time::sleep` has millisecond granularity, so without the spin
/// a 50 us slot would land at the next millisecond boundary and skew the
/// lateness histogram.
async fn pacer_wait(due_ns: u64) {
    loop {
        let now = monotonic_now_ns();
        if now >= due_ns {
            return;
        }
        let remaining = due_ns - now;
        if remaining > SPIN_THRESHOLD_NS {
            tokio::time::sleep(Duration::from_nanos(remaining - SPIN_THRESHOLD_NS)).await;
        } else {
            tokio::task::yield_now().await;
        }
    }
}

/// `due_ns` for the global slot `j` of `conn_id` in a latency run whose total
/// rate is `rate` slots/s and origin is `origin_ns`. Pure u128 arithmetic —
/// the slot index can exceed u64 ns windows, but `j * 1e9 / rate` is always
/// representable in u128, and `try_from` to u64 only fails after ~584 years.
fn slot_due_ns(conn_id: u32, slot_k: u64, connections: u32, rate: u64, origin_ns: u64) -> u64 {
    let j = u128::from(conn_id) + u128::from(slot_k) * u128::from(connections);
    let offset_ns = j.saturating_mul(1_000_000_000) / u128::from(rate);
    origin_ns.saturating_add(
        u64::try_from(offset_ns).expect("slot offset fits in u64 (under ~584 years)"),
    )
}

/// Open-loop latency pacer for one connection. Reads its own slots from the
/// global sequence `j = conn_id + k * connections`, paces each due time with
/// `pacer_wait`, batches up to `BATCH` due slots per `write_all`, releases the
/// batch's packet ids when a write would begin at or after the window end,
/// records `send_ns − due_ns` for sent slots whose due lies inside the
/// measurement window, and counts packet-id exhaustion (QoS 1) or
/// post-window-write (backpressure) as `missed_slots`.
async fn run_latency_connection(task: ConnectionTask) -> Result<(), String> {
    let Mode::Latency { rate } = task.config.mode else {
        return Ok(());
    };
    let conn_id = task.conn_id;
    let config = task.config.clone();
    let counters = Arc::clone(&task.counters);

    let Some((mut write_half, deadline_instant, packet_ids, in_flight, reader_handle)) =
        latency_open_session(&config, &counters, conn_id).await?
    else {
        return Ok(());
    };

    let pacer_result = latency_pacer(
        PacerState {
            conn_id,
            config: &config,
            counters: &counters,
            rate,
            deadline_instant,
            packet_ids: &packet_ids,
            in_flight: &in_flight,
        },
        &mut write_half,
    )
    .await;

    latency_teardown(
        &config,
        &counters,
        deadline_instant,
        &in_flight,
        reader_handle,
        &mut write_half,
        pacer_result,
    )
    .await
}

/// Outcome of opening a latency-mode session.
type OpenedSession = (
    OwnedWriteHalf,
    tokio::time::Instant,
    Arc<std::sync::Mutex<PacketIds>>,
    Arc<AtomicU64>,
    Option<AbortOnDropHandle<()>>,
);

/// Open CONNECT, return `(write_half, deadline_instant, packet_ids, in_flight,
/// reader_handle)` on success. `Ok(None)` is the connect-error / protocol-
/// error path (counters already bumped); `Err` is an unexpected failure.
/// `reader_handle` is present for both QoS levels.
async fn latency_open_session(
    config: &GenConfig,
    counters: &Arc<Counters>,
    conn_id: u32,
) -> Result<Option<OpenedSession>, String> {
    let client_id = format!("bench-p{conn_id}");
    let (read_half, write_half, deadline_instant) = match open_session(config, &client_id).await {
        Ok(v) => v,
        Err(OpenError::Connect(reason)) => {
            counters.add_connect_errors(1);
            eprintln!("gen: connection {conn_id}: {reason}");
            return Ok(None);
        }
        Err(OpenError::Protocol(reason)) => {
            counters.add_connect_errors(1);
            counters.add_protocol_errors(1);
            // The counters say a CONNACK invalidated the run; only this
            // line says which reply did it.
            eprintln!("gen: connection {conn_id}: {reason}");
            return Ok(None);
        }
    };
    let packet_ids = Arc::new(std::sync::Mutex::new(PacketIds::new()));
    let in_flight: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let reader_handle = if config.qos == 1 {
        let reader_packet_ids = Arc::clone(&packet_ids);
        let reader_in_flight = Arc::clone(&in_flight);
        let reader_counters = Arc::clone(counters);
        Some(counters.spawn_reader(qos1_latency_reader(
            conn_id,
            read_half,
            reader_counters,
            reader_packet_ids,
            reader_in_flight,
        )))
    } else {
        Some(counters.spawn_reader(qos0_reader_task(
            conn_id,
            read_half,
            Arc::clone(counters),
        )))
    };
    Ok(Some((write_half, deadline_instant, packet_ids, in_flight, reader_handle)))
}

/// Borrowed inputs for `latency_pacer` — keeps the function below
/// `clippy::too_many_lines` and the borrow checker happy.
struct PacerState<'a> {
    conn_id: u32,
    config: &'a GenConfig,
    counters: &'a Arc<Counters>,
    rate: u64,
    deadline_instant: tokio::time::Instant,
    packet_ids: &'a Arc<std::sync::Mutex<PacketIds>>,
    in_flight: &'a Arc<AtomicU64>,
}

async fn latency_pacer(state: PacerState<'_>, write_half: &mut OwnedWriteHalf) -> Result<(), String> {
    let PacerState {
        conn_id,
        config,
        counters,
        rate,
        deadline_instant,
        packet_ids,
        in_flight,
    } = state;
    let mut next_slot_k: u64 = 0;
    let mut next_seq: u64 = 0;
    let mut batch: Vec<(Option<NonZeroU16>, u64)> = Vec::with_capacity(BATCH);
    let mut write_buf: Vec<u8> = Vec::with_capacity(BATCH * PUBLISH_BYTES);

    'pacer: loop {
        let due_ns = slot_due_ns(conn_id, next_slot_k, config.connections, rate, config.origin_ns);
        // This connection's own due times are nondecreasing (a high rate puts
        // several slots in one nanosecond), so a due time at or after the
        // slot grid end means it has no window slot left. Waiting for
        // it would outlast the hard deadline and report a false `stalled`.
        if due_ns >= slot_grid_end_ns(config.window) {
            break 'pacer;
        }
        pacer_wait(due_ns).await;
        if monotonic_now_ns() >= config.window.end_ns {
            count_remaining_missed(counters, conn_id, next_slot_k, config, rate);
            break 'pacer;
        }
        batch.clear();
        latency_collect_batch(
            counters,
            conn_id,
            config,
            rate,
            packet_ids,
            &mut next_slot_k,
            &mut batch,
        );
        if batch.is_empty() {
            continue 'pacer;
        }
        // Encode before the write gate, so no encoding work sits between the
        // recorded start and the socket write. The gate itself lives inside
        // the future `latency_send_batch` polls (AC-26).
        write_buf.clear();
        latency_encode_batch(conn_id, config, &batch, &mut next_seq, &mut write_buf);
        let outcome = latency_send_batch(SendBatchInputs {
            counters,
            conn_id,
            config,
            write_half,
            deadline_instant,
            in_flight,
            batch: &batch,
            write_buf: &write_buf,
        })
        .await;
        match outcome {
            SendOutcome::Sent => {}
            SendOutcome::WindowEnded => {
                latency_drop_batch(
                    counters,
                    conn_id,
                    config,
                    rate,
                    packet_ids,
                    &batch,
                    next_slot_k,
                );
                break 'pacer;
            }
            SendOutcome::Failed(cause) => {
                // The socket is unusable, so every later due batch would fail
                // the same way. Retrying until the window ends would repeat
                // the failed write and its diagnostic once per due batch —
                // thousands of synchronous stderr writes at an accepted high
                // rate, and a block inside `eprintln!` if nothing drains
                // stderr would hold this task past the hard deadline (AC-24).
                // The batch is already counted `offered`; the slots it never
                // reached are counted arithmetically, and the cause travels
                // out through `run_connection`'s per-connection diagnostic.
                count_remaining_missed(counters, conn_id, next_slot_k, config, rate);
                return Err(cause);
            }
        }
    }
    Ok(())
}

/// Borrowed inputs for `latency_send_batch` — extracted so the function
/// stays below the `clippy::too_many_arguments` cap.
struct SendBatchInputs<'a> {
    counters: &'a Arc<Counters>,
    conn_id: u32,
    config: &'a GenConfig,
    write_half: &'a mut OwnedWriteHalf,
    deadline_instant: tokio::time::Instant,
    in_flight: &'a Arc<AtomicU64>,
    batch: &'a [(Option<NonZeroU16>, u64)],
    write_buf: &'a [u8],
}

/// Fill `batch` with up to `BATCH` slots whose due has already passed,
/// stopping at the window end or when the next slot is not due yet.
/// QoS 1 may exhaust packet ids mid-batch — `skip_past_due_missed` then
/// accounts for every already-due slot at once and this returns.
fn latency_collect_batch(
    counters: &Arc<Counters>,
    conn_id: u32,
    config: &GenConfig,
    rate: u64,
    packet_ids: &Arc<std::sync::Mutex<PacketIds>>,
    next_slot_k: &mut u64,
    batch: &mut Vec<(Option<NonZeroU16>, u64)>,
) {
    while batch.len() < BATCH {
        let due = slot_due_ns(conn_id, *next_slot_k, config.connections, rate, config.origin_ns);
        if due >= slot_grid_end_ns(config.window) {
            return;
        }
        if due > monotonic_now_ns() {
            return;
        }
        if config.qos == 1 {
            let id = {
                let mut ids = packet_ids.lock().expect("packet ids poisoned");
                ids.allocate()
            };
            let Some(id) = id else {
                // Every id of this connection is in flight, so no already-due
                // slot can be sent. Retrying one slot at a time would scan
                // `PacketIds` once per slot with no `.await` in between, and a
                // high rate leaves billions of due slots — enough to hold this
                // task past the hard deadline (AC-23, AC-24).
                skip_past_due_missed(counters, conn_id, config, rate, next_slot_k);
                return;
            };
            batch.push((Some(id), due));
        } else {
            batch.push((None, due));
        }
        *next_slot_k = next_slot_k.saturating_add(1);
    }
}

/// Release the QoS 1 packet ids held by `batch` (a no-op for QoS 0).
fn latency_release_batch_ids(
    config: &GenConfig,
    packet_ids: &Arc<std::sync::Mutex<PacketIds>>,
    batch: &[(Option<NonZeroU16>, u64)],
) {
    if config.qos != 1 {
        return;
    }
    let mut pids = packet_ids.lock().expect("packet ids poisoned");
    for (id_opt, _) in batch {
        if let Some(id) = id_opt {
            pids.release(id.get());
        }
    }
}

/// Release the QoS 1 packet ids in `batch`, count its slots plus every
/// remaining window-due slot of this connection as missed.
fn latency_drop_batch(
    counters: &Arc<Counters>,
    conn_id: u32,
    config: &GenConfig,
    rate: u64,
    packet_ids: &Arc<std::sync::Mutex<PacketIds>>,
    batch: &[(Option<NonZeroU16>, u64)],
    next_slot_k: u64,
) {
    latency_release_batch_ids(config, packet_ids, batch);
    counters.add_missed_slots(u64::try_from(batch.len()).expect("batch size fits in u64"));
    count_remaining_missed(counters, conn_id, next_slot_k, config, rate);
}

/// Encode `batch` into `write_buf`, stamping each slot with its own scheduled
/// due time. Kept apart from `latency_send_batch` so the caller can read the
/// clock immediately before the write (AC-26).
fn latency_encode_batch(
    conn_id: u32,
    config: &GenConfig,
    batch: &[(Option<NonZeroU16>, u64)],
    next_seq: &mut u64,
    write_buf: &mut Vec<u8>,
) {
    for (id_opt, due) in batch {
        let stamp = Stamp {
            conn_id,
            seq: *next_seq,
            scheduled_ns: *due,
        };
        *next_seq = next_seq.saturating_add(1);
        let payload = stamp.encode();
        let packet_id = if config.qos == 1 {
            Some(id_opt.expect("QoS 1 batch entry carries a packet id"))
        } else {
            None
        };
        wire::encode_publish(write_buf, config.qos, packet_id, &payload);
    }
}

/// What `latency_send_batch` leaves for `latency_pacer` to do next.
enum SendOutcome {
    /// The batch went out; keep pacing.
    Sent,
    /// The write gate read the window end before the first write, so nothing
    /// went out and the batch still holds its QoS 1 packet ids.
    WindowEnded,
    /// The write failed or did not finish by the hard deadline. The batch is
    /// already counted `offered` — its QoS 1 ids stay in flight for the
    /// teardown drain, a QoS 0 batch is counted `stalled` — but the socket
    /// is unusable, so the caller must stop pacing. Carries the cause.
    Failed(String),
}

/// Gate the window end, count the batch as offered (and, for QoS 1, in flight), then perform one
/// `write_all` of the already encoded `write_buf` under the deadline, and update the counters (`lateness`,
/// `lateness_over_range`, and `stalled` for a failed QoS 0 batch).
async fn latency_send_batch(inputs: SendBatchInputs<'_>) -> SendOutcome {
    let SendBatchInputs {
        counters,
        conn_id,
        config,
        write_half,
        deadline_instant,
        in_flight,
        batch,
        write_buf,
    } = inputs;
    let sent = u64::try_from(batch.len()).expect("batch size fits in u64");
    // The gate is the first thing the write future does, so the clock read,
    // the recorded start and this batch's first `write` all happen in one poll
    // with no await between them. Reading the clock in the caller instead
    // leaves the task descheduleable after the check and before the write,
    // which lets a write begin at or after `window.end_ns` behind a recorded
    // start inside it — the write is then permitted until `deadline_instant`
    // while `last_write_start_ns` keeps the earlier value (AC-26).
    let gated_write = async {
        let start = monotonic_now_ns();
        if start >= config.window.end_ns {
            return None;
        }
        counters.update_last_write_start(start);
        // Submitted once the gate passes, before the write: a PUBACK for
        // this batch's first PUBLISH can arrive before `write_all`
        // returns, and a partly written batch can be delivered.
        counters.add_offered(sent);
        if config.qos == 1 {
            in_flight.fetch_add(sent, Ordering::Relaxed);
        }
        Some(write_half.write_all(write_buf).await)
    };
    let write_result = tokio::time::timeout_at(deadline_instant, gated_write).await;
    match write_result {
        Ok(None) => SendOutcome::WindowEnded,
        Ok(Some(Ok(()))) => {
            let send_ns = monotonic_now_ns();
            for (_, due) in batch {
                if *due >= config.window.start_ns && *due < config.window.end_ns {
                    let value = send_ns.saturating_sub(*due);
                    if value > LATENCY_MAX_NS {
                        counters.add_lateness_over_range(1);
                    } else {
                        counters.record_lateness(conn_id, value.max(1));
                    }
                }
            }
            SendOutcome::Sent
        }
        Ok(Some(Err(e))) => {
            // QoS 1 ids stay in flight for `latency_teardown`'s drain to
            // resolve; QoS 0 has no acknowledgement, so the batch is
            // unresolved.
            if config.qos != 1 {
                counters.add_stalled(sent);
            }
            SendOutcome::Failed(format!("publish write failed: {e}"))
        }
        Err(_) => {
            // QoS 1 ids stay in flight for `latency_teardown`'s drain to
            // resolve; QoS 0 has no acknowledgement, so the batch is
            // unresolved.
            if config.qos != 1 {
                counters.add_stalled(sent);
            }
            SendOutcome::Failed(
                "publish write did not finish by the hard deadline".to_string(),
            )
        }
    }
}

/// Drain in-flight QoS 1 ids up to the deadline (stopping the reader before the leftovers are counted), write DISCONNECT unless `pacer_result` is already an error (a partial PUBLISH may be on the wire), shut the writer down, and rejoin the reader task.
///
/// `pacer_result` is the pacing loop's own outcome. The returned result is
/// this connection's combined failure: `pacer_result`'s cause, the
/// teardown's cause, or both joined by `session_result`.
async fn latency_teardown(
    config: &GenConfig,
    counters: &Arc<Counters>,
    deadline_instant: tokio::time::Instant,
    in_flight: &Arc<AtomicU64>,
    mut reader_handle: Option<AbortOnDropHandle<()>>,
    write_half: &mut OwnedWriteHalf,
    pacer_result: Result<(), String>,
) -> Result<(), String> {
    if config.qos == 1 {
        let drain_fut = async {
            while in_flight.load(Ordering::Relaxed) > 0 {
                tokio::task::yield_now().await;
            }
        };
        if tokio::time::timeout_at(deadline_instant, drain_fut).await.is_err() {
            // Stop the reader before reading what is left: a PUBACK it
            // counted after that read would be both `acked` and
            // `stalled`. The aborted handle resolves once the reader is
            // no longer running — at most the end of the poll it is in.
            if let Some(handle) = reader_handle.take() {
                handle.abort();
                reraise_reader_panic(handle.await);
            }
            let leaked = in_flight.load(Ordering::Relaxed);
            if leaked > 0 {
                counters.add_stalled(leaked);
            }
        }
    }
    // Everything this connection does ends at the hard deadline
    // (`window.end + drain_grace`, `PLAN.md:33`), teardown included: the
    // `IO_TIMEOUT_SECS` that AC-24 allows after it belongs to `run_generator`'s
    // backstop, which counts an abandoned task as `stalled`. Spending it here
    // instead would race that backstop and double-count slots the pacer has
    // already accounted for.
    let disconnect = if pacer_result.is_err() {
        None
    } else {
        Some(
            tokio::time::timeout_at(deadline_instant, write_half.write_all(&wire::DISCONNECT))
                .await,
        )
    };
    let shutdown = write_half.shutdown().await;
    if let Some(handle) = reader_handle {
        // A peer that holds its socket open while withholding PUBACK never
        // sends the EOF the reader is waiting for, so an unbounded join would
        // outlast the deadline above. Dropping the handle would abort the
        // reader without waiting for it, so abort and await what the bounded join leaves running.
        join_reader_by(handle, deadline_instant).await;
    }
    session_result(pacer_result.err(), teardown_failure(disconnect, shutdown))
}

/// Count the window-due slots of this connection from `start_k` onward,
/// excluding those already processed (so callers that have advanced past
/// them need to subtract first). QoS 0's misses still increment the counter
/// here — the brief's "count … in missed_slots" wording is mode-agnostic on
/// this path, and `missed_slots` is a uint64 either way.
///
/// The count is arithmetic, not a scan: a high rate can leave hundreds of
/// millions of slots, and stepping over them one at a time would hold this
/// task well past the hard deadline, leaving the run with no report.
fn count_remaining_missed(
    counters: &Arc<Counters>,
    conn_id: u32,
    start_k: u64,
    config: &GenConfig,
    rate: u64,
) {
    let own_count = own_slot_count(conn_id, config.connections, window_due_j_count(config, rate));
    let remaining = own_count.saturating_sub(u128::from(start_k));
    // A rate high enough to overflow a u64 slot count saturates rather than
    // panicking: any non-zero `missed_slots` already invalidates the run.
    counters.add_missed_slots(u64::try_from(remaining).unwrap_or(u64::MAX));
}

/// Count every window-due slot of this connection from `*next_slot_k` whose
/// due time has already passed as missed, and advance past them. The current
/// slot is always one of them, so the caller's loop still makes progress.
///
/// Like `count_remaining_missed`, the count is arithmetic rather than a scan:
/// this runs when QoS 1 packet ids are exhausted, where a high rate leaves
/// billions of unsendable due slots.
fn skip_past_due_missed(
    counters: &Arc<Counters>,
    conn_id: u32,
    config: &GenConfig,
    rate: u64,
    next_slot_k: &mut u64,
) {
    // Inverse of `slot_due_ns`: `floor(j × 1e9 / rate) ≤ elapsed` holds
    // exactly while `j < ceil((elapsed + 1) × rate / 1e9)`.
    let elapsed_ns = u128::from(monotonic_now_ns().saturating_sub(config.origin_ns));
    let past_due_j_count = elapsed_ns
        .saturating_add(1)
        .saturating_mul(u128::from(rate))
        .div_ceil(1_000_000_000);
    let connections = config.connections;
    let bound_k = own_slot_count(conn_id, connections, past_due_j_count)
        .min(own_slot_count(conn_id, connections, window_due_j_count(config, rate)));
    let skipped = bound_k.saturating_sub(u128::from(*next_slot_k));
    if skipped == 0 {
        return;
    }
    counters.add_missed_slots(u64::try_from(skipped).unwrap_or(u64::MAX));
    *next_slot_k = u64::try_from(bound_k).unwrap_or(u64::MAX);
}

/// Global slot count from the origin to the slot grid end — `slot_counts`'
/// `scheduled`, so the warm-up slots between the origin and `window.start_ns`
/// are included. A global slot index `j` is due in that span exactly while
/// `j < j_count`.
fn window_due_j_count(config: &GenConfig, rate: u64) -> u128 {
    scheduled_slots(rate, config.origin_ns, config.window)
}

/// How many of this connection's own slots `k` sit below a global slot bound.
/// A connection owns `j = conn_id + k × connections` (`slot_due_ns`), so
/// `j < j_bound` holds for `k < ceil((j_bound − conn_id) / connections)`.
fn own_slot_count(conn_id: u32, connections: u32, j_bound: u128) -> u128 {
    j_bound
        .saturating_sub(u128::from(conn_id))
        .div_ceil(u128::from(connections))
}

/// QoS 1 latency reader: pull PUBACKs, release packet ids, decrement the
/// in-flight counter. Mirrors the throughput reader minus the semaphore
/// (latency mode has no `qos1_window`). A transport error ends the reader with
/// a diagnostic; anything the codec rejects, and anything that isn't a
/// well-formed PUBACK, is a protocol error. An EOF with ids still outstanding
/// is reported too — `latency_teardown`'s drain already counts those as
/// `stalled`, but the cause only shows up here.
async fn qos1_latency_reader(
    conn_id: u32,
    read_half: OwnedReadHalf,
    counters: Arc<Counters>,
    packet_ids: Arc<std::sync::Mutex<PacketIds>>,
    in_flight: Arc<AtomicU64>,
) {
    let mut framed = FramedRead::new(read_half, Codec::new(wire::INBOUND_REMAINING_LENGTH_MAX));
    // One diagnostic per reader: protocol_errors keeps the count.
    let mut rejection_reported = false;
    while let Some(item) = framed.next().await {
        match item {
            Ok((Packet::PublishAck { packet_id }, _consumed)) => {
                let id = packet_id.get();
                let released = {
                    let mut ids = packet_ids.lock().expect("packet ids poisoned");
                    ids.release(id)
                };
                if released {
                    counters.add_acked(1);
                    in_flight.fetch_sub(1, Ordering::Relaxed);
                } else {
                    counters.add_protocol_errors(1);
                    if !rejection_reported {
                        rejection_reported = true;
                        eprintln!("gen: connection {conn_id}: QoS 1 reader rejected a PUBACK for packet id {id}, which is not in flight");
                    }
                }
            }
            Ok((packet, _consumed)) => {
                counters.add_protocol_errors(1);
                if !rejection_reported {
                    rejection_reported = true;
                    eprintln!(
                        "gen: connection {conn_id}: QoS 1 reader rejected an unexpected {}",
                        wire::packet_kind(&packet)
                    );
                }
            }
            Err(DecodeError::Io(e)) => {
                eprintln!("gen: connection {conn_id}: QoS 1 reader read failed: {e}");
                return;
            }
            Err(e) => {
                counters.add_protocol_errors(1);
                eprintln!("gen: connection {conn_id}: QoS 1 reader decode failed: {e}");
                return;
            }
        }
    }
    let outstanding = in_flight.load(Ordering::Relaxed);
    if outstanding > 0 {
        eprintln!(
            "gen: connection {conn_id}: QoS 1 reader saw EOF with {outstanding} ids outstanding"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{
        join_and_report, latency_teardown, qos1_drain_and_disconnect, qos1_latency_reader,
        qos1_reader_task, run_qos0_throughput, run_qos0_throughput_with_reader, run_qos1_throughput,
        session_result, spawn_progress_watchdog, teardown_failure, wire, ConnectionTask, Counters,
        GenConfig, Mode, PacketIds, Window, READER_STOP_GRACE,
    };
    use std::io::ErrorKind;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::Semaphore;
    use tokio::task::JoinSet;
    use tokio_util::task::AbortOnDropHandle;

    /// Deadline handed to the teardown under test — long enough that the
    /// clean path never meets it.
    const TEARDOWN_DEADLINE: Duration = Duration::from_secs(2);
    /// Ids left in flight for the leftover tests: a batch the peer never
    /// acknowledged.
    const LEFTOVER_IDS: u64 = 3;
    /// Deadline handed to a teardown whose drain cannot finish — short
    /// enough to meet inside a test, far longer than one scheduler poll.
    const LEFTOVER_DEADLINE: Duration = Duration::from_millis(200);
    /// Time the leftover tests give a reader that was never stopped to count
    /// the late PUBACKs after they reached the client socket.
    const ABORT_GRACE: Duration = Duration::from_millis(400);
    /// Bytes of the PUBACKs a leftover test's peer sends: one four-byte
    /// PUBACK for each of the `LEFTOVER_IDS` (3) ids.
    const LEFTOVER_PUBACK_BYTES: usize = 12;
    /// Bound on the late PUBACKs being written and then seen unread on the
    /// client socket.
    const STIMULUS_BOUND: Duration = Duration::from_secs(2);
    /// Poll interval while a test waits for those bytes to arrive.
    const PEEK_POLL: Duration = Duration::from_millis(10);
    /// Message of the panic the `join_and_report` tests plant in a
    /// connection task.
    const CONNECTION_PANIC: &str = "connection panic fixture";

    /// A connection task body that panics. Synchronous: an `async fn`
    /// with no `.await` fails `clippy::unused_async`.
    fn panicking_connection() {
        panic!("{CONNECTION_PANIC}");
    }

    /// Message of the panic the reader-panic tests plant in a reader task.
    const READER_PANIC: &str = "reader panic fixture";

    /// A reader task body that panics. Synchronous for the same
    /// `clippy::unused_async` reason as `panicking_connection`.
    fn panicking_reader() {
        panic!("{READER_PANIC}");
    }

    /// Counts its own drop, so a test can prove a task's future was
    /// destroyed (aborted and joined) rather than detached.
    struct DropFlag(Arc<AtomicU64>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A task body that never finishes and holds a `DropFlag` from the
    /// moment it is created — captured, not built on first poll, so a
    /// task aborted before it ever ran still counts.
    fn held_until_dropped(
        dropped: &Arc<AtomicU64>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let flag = DropFlag(Arc::clone(dropped));
        async move {
            let _flag = flag;
            std::future::pending::<()>().await;
        }
    }

    /// Message of the panic a task plants in the drop of its own future —
    /// one panic `join_and_report` can see after `abort_all`, since
    /// tokio turns a panic while a cancelled future is destroyed into a
    /// panicking `JoinError` (1.53.2 `runtime/task/harness.rs:502-507`).
    const DROP_PANIC: &str = "connection drop panic fixture";
    /// Tasks spawned by the clean-drain test; more than one, so a report
    /// that counted collected tasks as stalled would be visible.
    const FINISHED_TASKS: usize = 3;
    /// Tasks the uncollected-task test lets finish before the deadline:
    /// more than tokio's cooperative budget of 128 joins per poll
    /// (tokio 1.53.2 `task/coop/mod.rs:116`), so one poll of the drain
    /// cannot collect them all.
    const UNCOLLECTED_TASKS: u64 = 200;
    /// Poll interval while that test waits for its tasks to finish.
    const FINISH_POLL: Duration = Duration::from_millis(10);
    /// Window and connection deadline of a connection that must still be
    /// running when a short hard deadline passes.
    const RUNNING_SPAN: Duration = Duration::from_secs(60);
    /// How long the mid-poll test's reader may block before it gives up,
    /// so a broken wait fails the test instead of hanging it.
    const RELEASE_WATCHDOG: Duration = Duration::from_secs(10);
    /// Slack on top of `READER_STOP_GRACE` for the answer to an expired
    /// hard deadline held by a reader no abort can stop: a reader wait
    /// longer than its own grace fails this bound instead of only
    /// lengthening the run.
    const EXPIRED_DEADLINE_ANSWER: Duration = Duration::from_millis(500);
    /// When a task of the late-finish test ends: after the deadline a
    /// reserve of `READER_STOP_GRACE` would have cut the drain at, and
    /// before the hard deadline the test gives `join_and_report`.
    const LATE_FINISH: Duration = Duration::from_millis(500);
    /// How long the blocked-worker test holds the runtime's only worker
    /// inside one synchronous call. Longer than every bound that test
    /// asserts, so the worker is still held when they are measured.
    const WORKER_HOLD: Duration = Duration::from_secs(10);
    /// The Tokio timer the blocked-worker test races the watchdog
    /// against. It is the shorter of the two, so a run that fired it
    /// would beat the watchdog and fail the test.
    const HELD_WORKER_TIMER: Duration = Duration::from_millis(100);
    /// The watchdog budget of the blocked-worker test, deliberately
    /// longer than `HELD_WORKER_TIMER`.
    const HELD_WORKER_WATCHDOG: Duration = Duration::from_millis(400);

    /// A guard that panics while it is dropped.
    struct PanicOnDrop;

    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("{DROP_PANIC}");
        }
    }

    /// A task body that never finishes and panics when its future is
    /// destroyed, so the panic reaches `join_and_report` only in the join
    /// that follows `abort_all` — the path a task still running at the
    /// hard deadline takes.
    fn panics_when_dropped() -> impl std::future::Future<Output = ()> + Send + 'static {
        let guard = PanicOnDrop;
        async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        }
    }

    /// A throughput configuration for tests that call `join_and_report`
    /// directly; no connection is opened from it.
    fn join_test_config() -> GenConfig {
        GenConfig {
            addr: "127.0.0.1:1".parse().expect("parse addr"),
            mode: Mode::Throughput,
            qos: 1,
            connections: 1,
            qos1_window: 1,
            origin_ns: 0,
            window: Window {
                start_ns: 0,
                end_ns: 0,
            },
            drain_grace: Duration::ZERO,
        }
    }
    /// Bytes per write while a test fills a socket's send buffer.
    const FILL_CHUNK: usize = 65_536;
    /// How long one chunk's `write_all` may take before the send buffer
    /// counts as full. A timeout proves only that the chunk did not finish
    /// in that time — some of its bytes may have been written.
    const FILL_STALL: Duration = Duration::from_millis(100);
    /// Watchdog over the whole fill, so a peer that keeps draining
    /// fails the test instead of hanging it.
    const FILL_WATCHDOG: Duration = Duration::from_secs(10);
    /// Scheduling tolerance past a teardown deadline — far below
    /// `IO_TIMEOUT_SECS`, so a wait bounded by that timeout fails it.
    const TEARDOWN_TOLERANCE: Duration = Duration::from_secs(1);
    /// A PINGRESP — any frame a QoS 0 publisher's reader counts as a
    /// protocol error.
    const PINGRESP: [u8; 2] = [0xD0, 0x00];

    /// Publish failure handed to a teardown under test.
    const PUBLISH_FAILURE_FIXTURE: &str = "publish write failed: fixture";

    /// Fill `write_half`'s send buffer against a peer that never reads:
    /// write `FILL_CHUNK`s until one does not finish within `FILL_STALL`,
    /// then prove the condition the test needs — a two-byte write (the
    /// size of DISCONNECT) does not finish within `FILL_STALL` either.
    /// Kernel back-pressure, not tokio's cached readiness, decides both.
    async fn fill_send_buffer(write_half: &mut tokio::net::tcp::OwnedWriteHalf) {
        let chunk = vec![0u8; FILL_CHUNK];
        tokio::time::timeout(FILL_WATCHDOG, async {
            while matches!(
                tokio::time::timeout(FILL_STALL, write_half.write_all(&chunk)).await,
                Ok(Ok(()))
            ) {}
        })
        .await
        .expect("the send buffer must fill within FILL_WATCHDOG");
        assert!(
            tokio::time::timeout(FILL_STALL, write_half.write_all(&wire::DISCONNECT))
                .await
                .is_err(),
            "a two-byte write must block once the send buffer is full"
        );
    }

    async fn connected_pair() -> (
        tokio::net::tcp::OwnedReadHalf,
        tokio::net::tcp::OwnedWriteHalf,
        TcpStream,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let client = TcpStream::connect(addr).await.expect("connect");
        let (peer, _) = listener.accept().await.expect("accept");
        let (read_half, write_half) = client.into_split();
        (read_half, write_half, peer)
    }

    async fn peer_bytes(mut peer: TcpStream) -> Vec<u8> {
        let mut buf = Vec::new();
        tokio::time::timeout(TEARDOWN_DEADLINE * 2, peer.read_to_end(&mut buf))
            .await
            .expect("peer bytes must arrive in time")
            .expect("peer read must succeed");
        buf
    }

    #[tokio::test]
    async fn qos1_drain_writes_disconnect_only_when_no_write_failed() {
        for write_failed in [false, true] {
            let (read_half, mut write_half, peer) = connected_pair().await;
            let in_flight = AtomicU64::new(0);
            let counters = Counters::new(1, Mode::Throughput);
            let reader_handle = AbortOnDropHandle::new(tokio::spawn(async {}));
            let _ = qos1_drain_and_disconnect(
                &mut write_half,
                &in_flight,
                tokio::time::Instant::now() + TEARDOWN_DEADLINE,
                &counters,
                reader_handle,
                write_failed.then(|| PUBLISH_FAILURE_FIXTURE.to_string()),
            )
            .await;
            drop(write_half);
            drop(read_half);
            let bytes = peer_bytes(peer).await;
            if write_failed {
                assert!(
                    bytes.is_empty(),
                    "no DISCONNECT must follow a failed write, got {bytes:?}"
                );
            } else {
                assert_eq!(
                    bytes,
                    wire::DISCONNECT.to_vec(),
                    "DISCONNECT must follow a clean drain"
                );
            }
        }
    }

    #[tokio::test]
    async fn latency_teardown_writes_disconnect_only_when_no_write_failed() {
        for write_failed in [false, true] {
            let (_read_half, mut write_half, peer) = connected_pair().await;
            let config = GenConfig {
                addr: "127.0.0.1:1".parse().expect("parse addr"),
                mode: Mode::Latency { rate: 1 },
                qos: 0,
                connections: 1,
                qos1_window: 1,
                origin_ns: 0,
                window: Window {
                    start_ns: 0,
                    end_ns: 0,
                },
                drain_grace: Duration::ZERO,
            };
            let counters = Arc::new(Counters::new(1, config.mode));
            let in_flight = Arc::new(AtomicU64::new(0));
            let _ = latency_teardown(
                &config,
                &counters,
                tokio::time::Instant::now() + TEARDOWN_DEADLINE,
                &in_flight,
                None,
                &mut write_half,
                if write_failed {
                    Err(PUBLISH_FAILURE_FIXTURE.to_string())
                } else {
                    Ok(())
                },
            )
            .await;
            drop(write_half);
            let bytes = peer_bytes(peer).await;
            if write_failed {
                assert!(
                    bytes.is_empty(),
                    "no DISCONNECT must follow a failed write, got {bytes:?}"
                );
            } else {
                assert_eq!(
                    bytes,
                    wire::DISCONNECT.to_vec(),
                    "DISCONNECT must follow a clean teardown"
                );
            }
        }
    }

    #[tokio::test]
    async fn latency_teardown_reports_a_disconnect_it_could_not_write_after_any_pacer_failure() {
        // Case 1: a full send buffer blocks DISCONNECT past the
        // deadline; the teardown's own cause must reach the caller.
        {
            let (_read_half, mut write_half, peer) = connected_pair().await;
            fill_send_buffer(&mut write_half).await;
            let config = GenConfig {
                addr: "127.0.0.1:1".parse().expect("parse addr"),
                mode: Mode::Latency { rate: 1 },
                qos: 0,
                connections: 1,
                qos1_window: 1,
                origin_ns: 0,
                window: Window {
                    start_ns: 0,
                    end_ns: 0,
                },
                drain_grace: Duration::ZERO,
            };
            let counters = Arc::new(Counters::new(1, config.mode));
            let in_flight = Arc::new(AtomicU64::new(0));
            let finished = tokio::time::timeout(
                LEFTOVER_DEADLINE + TEARDOWN_TOLERANCE,
                latency_teardown(
                    &config,
                    &counters,
                    tokio::time::Instant::now() + LEFTOVER_DEADLINE,
                    &in_flight,
                    None,
                    &mut write_half,
                    Ok(()),
                ),
            )
            .await;
            assert!(
                matches!(&finished, Ok(Err(cause)) if cause == "DISCONNECT write did not finish by the hard deadline"),
                "the teardown must report the unwritten DISCONNECT, got {finished:?}"
            );
            drop(write_half);
            drop(peer);
        }

        // Case 2: an earlier pacer failure must survive teardown and
        // no DISCONNECT must follow it on a clear buffer.
        {
            let (_read_half, mut write_half, peer) = connected_pair().await;
            let config = GenConfig {
                addr: "127.0.0.1:1".parse().expect("parse addr"),
                mode: Mode::Latency { rate: 1 },
                qos: 0,
                connections: 1,
                qos1_window: 1,
                origin_ns: 0,
                window: Window {
                    start_ns: 0,
                    end_ns: 0,
                },
                drain_grace: Duration::ZERO,
            };
            let counters = Arc::new(Counters::new(1, config.mode));
            let in_flight = Arc::new(AtomicU64::new(0));
            let finished = tokio::time::timeout(
                TEARDOWN_DEADLINE,
                latency_teardown(
                    &config,
                    &counters,
                    tokio::time::Instant::now() + TEARDOWN_DEADLINE,
                    &in_flight,
                    None,
                    &mut write_half,
                    Err(PUBLISH_FAILURE_FIXTURE.to_string()),
                ),
            )
            .await;
            assert!(
                matches!(&finished, Ok(Err(cause)) if cause == PUBLISH_FAILURE_FIXTURE),
                "the pacer's cause must survive teardown, got {finished:?}"
            );
            drop(write_half);
            let bytes = peer_bytes(peer).await;
            assert!(
                bytes.is_empty(),
                "no DISCONNECT must follow a pacer failure, got {bytes:?}"
            );
        }
    }

    /// AC-10 — `teardown_failure` joins DISCONNECT and shutdown causes
    /// the same way `publish_write_failure` joins a publish's. tokio
    /// never surfaces a shutdown `NotConnected` (`stream.rs:1129-1133`),
    /// so the dual-cause arm is exercised only here.
    #[tokio::test]
    async fn teardown_failure_names_every_step_that_failed() {
        let elapsed = tokio::time::timeout(Duration::ZERO, std::future::pending::<std::io::Result<()>>())
            .await
            .expect_err("a zero timeout elapses");
        assert_eq!(teardown_failure(Some(Ok(Ok(()))), Ok(())), None);
        assert_eq!(teardown_failure(None, Ok(())), None);
        assert_eq!(
            teardown_failure(Some(Err(elapsed)), Ok(())),
            Some("DISCONNECT write did not finish by the hard deadline".to_string())
        );
        assert_eq!(
            teardown_failure(
                Some(Ok(Err(std::io::Error::from(ErrorKind::BrokenPipe)))),
                Err(std::io::Error::from(ErrorKind::NotConnected)),
            ),
            Some("DISCONNECT write failed: broken pipe; write-half shutdown failed: not connected".to_string())
        );
        assert_eq!(
            teardown_failure(None, Err(std::io::Error::from(ErrorKind::NotConnected))),
            Some("write-half shutdown failed: not connected".to_string())
        );
    }

    /// AC-10 — `session_result` joins a publish failure and a teardown
    /// failure the same way `run_connection` would: as the connection's
    /// `Err`, with `; ` between them when both happened.
    #[test]
    fn a_publish_failure_and_a_teardown_failure_are_both_reported() {
        assert_eq!(session_result(None, None), Ok(()));
        assert_eq!(
            session_result(Some("publish write failed: broken pipe".to_string()), None),
            Err("publish write failed: broken pipe".to_string())
        );
        assert_eq!(
            session_result(None, Some("write-half shutdown failed: not connected".to_string())),
            Err("write-half shutdown failed: not connected".to_string())
        );
        assert_eq!(
            session_result(
                Some("publish write failed: broken pipe".to_string()),
                Some("write-half shutdown failed: not connected".to_string()),
            ),
            Err(
                "publish write failed: broken pipe; write-half shutdown failed: not connected"
                    .to_string()
            )
        );
    }

    /// The peer's side of a leftover test: PUBACKs for the ids still in
    /// flight, written only when `send` is called — after the teardown
    /// under test returned — so the drain meets its deadline with every
    /// id in flight. `sender` yields the write's result and the peer, which
    /// the test keeps open until it has observed the client socket.
    struct LatePubacks {
        send: tokio::sync::oneshot::Sender<()>,
        sender: tokio::task::JoinHandle<std::io::Result<TcpStream>>,
    }

    /// Allocate the ids a teardown's drain finds still in flight, and arm
    /// the peer to acknowledge them late ([`LatePubacks`]). A teardown that
    /// detaches the production reader instead of aborting and awaiting it
    /// reads those PUBACKs and counts them after the report is built.
    fn leftover_ids_acked_late(
        mut peer: TcpStream,
    ) -> (Arc<std::sync::Mutex<PacketIds>>, LatePubacks) {
        let mut ids = PacketIds::new();
        let mut pubacks = Vec::new();
        for _ in 0..LEFTOVER_IDS {
            let id = ids.allocate().expect("packet id available").get();
            pubacks.extend_from_slice(&[0x40, 0x02, (id >> 8) as u8, (id & 0xFF) as u8]);
        }
        let (send, go) = tokio::sync::oneshot::channel::<()>();
        let sender = tokio::spawn(async move {
            let _ = go.await;
            peer.write_all(&pubacks).await?;
            Ok(peer)
        });
        (Arc::new(std::sync::Mutex::new(ids)), LatePubacks { send, sender })
    }

    /// Send the late PUBACKs, prove they were written, and wait until every
    /// byte of them sits unread on the client socket `write_half` belongs
    /// to. Returns the peer so the caller keeps it open while it observes.
    /// A reader still running would consume them, so the wait would fail.
    async fn deliver_late_pubacks(
        late: LatePubacks,
        write_half: &tokio::net::tcp::OwnedWriteHalf,
    ) -> TcpStream {
        late.send.send(()).expect("the peer task waits for the late PUBACKs signal");
        let peer = tokio::time::timeout(STIMULUS_BOUND, late.sender)
            .await
            .expect("the late PUBACKs must be written within STIMULUS_BOUND")
            .expect("the peer task must not panic")
            .expect("the late PUBACK write must succeed");
        let mut buf = [0u8; LEFTOVER_PUBACK_BYTES];
        tokio::time::timeout(STIMULUS_BOUND, async {
            loop {
                let seen = write_half.as_ref().peek(&mut buf).await.expect("peek the client socket");
                if seen == LEFTOVER_PUBACK_BYTES {
                    return;
                }
                tokio::time::sleep(PEEK_POLL).await;
            }
        })
        .await
        .expect("every late PUBACK byte must sit unread on the client socket: none arrived, or a reader still running consumed them");
        peer
    }

    #[tokio::test]
    async fn qos1_drain_counts_the_leftovers_as_stalled_and_leaves_no_reader_running() {
        let (read_half, mut write_half, peer) = connected_pair().await;
        let in_flight = Arc::new(AtomicU64::new(LEFTOVER_IDS));
        let counters = Arc::new(Counters::new(1, Mode::Throughput));
        let (packet_ids, late) = leftover_ids_acked_late(peer);
        let semaphore = Arc::new(Semaphore::new(0));
        let reader_handle = AbortOnDropHandle::new(tokio::spawn(qos1_reader_task(
            0,
            read_half,
            Arc::clone(&counters),
            Arc::clone(&semaphore),
            Arc::clone(&packet_ids),
            Arc::clone(&in_flight),
        )));

        let _ = qos1_drain_and_disconnect(
            &mut write_half,
            &in_flight,
            tokio::time::Instant::now() + LEFTOVER_DEADLINE,
            &counters,
            reader_handle,
            None,
        )
        .await;

        assert_eq!(
            counters.stalled.load(Ordering::Relaxed),
            LEFTOVER_IDS,
            "every id still in flight at the deadline counts once in stalled"
        );
        let acked_when_counted = counters.acked.load(Ordering::Relaxed);
        let _peer = deliver_late_pubacks(late, &write_half).await;
        tokio::time::sleep(ABORT_GRACE).await;
        assert_eq!(
            counters.acked.load(Ordering::Relaxed),
            acked_when_counted,
            "the teardown must leave no reader running: a detached reader keeps counting PUBACKs after the report is built"
        );
    }

    #[tokio::test]
    async fn latency_teardown_counts_the_leftovers_as_stalled_and_leaves_no_reader_running() {
        let (read_half, mut write_half, peer) = connected_pair().await;
        let config = GenConfig {
            addr: "127.0.0.1:1".parse().expect("parse addr"),
            mode: Mode::Latency { rate: 1 },
            qos: 1,
            connections: 1,
            qos1_window: 1,
            origin_ns: 0,
            window: Window {
                start_ns: 0,
                end_ns: 0,
            },
            drain_grace: Duration::ZERO,
        };
        let counters = Arc::new(Counters::new(1, config.mode));
        let in_flight = Arc::new(AtomicU64::new(LEFTOVER_IDS));
        let (packet_ids, late) = leftover_ids_acked_late(peer);
        let reader_handle = AbortOnDropHandle::new(tokio::spawn(qos1_latency_reader(
            0,
            read_half,
            Arc::clone(&counters),
            Arc::clone(&packet_ids),
            Arc::clone(&in_flight),
        )));

        let _ = latency_teardown(
            &config,
            &counters,
            tokio::time::Instant::now() + LEFTOVER_DEADLINE,
            &in_flight,
            Some(reader_handle),
            &mut write_half,
            Ok(()),
        )
        .await;

        assert_eq!(
            counters.stalled.load(Ordering::Relaxed),
            LEFTOVER_IDS,
            "every id still in flight at the deadline counts once in stalled"
        );
        let acked_when_counted = counters.acked.load(Ordering::Relaxed);
        let _peer = deliver_late_pubacks(late, &write_half).await;
        tokio::time::sleep(ABORT_GRACE).await;
        assert_eq!(
            counters.acked.load(Ordering::Relaxed),
            acked_when_counted,
            "the teardown must leave no reader running: a detached reader keeps counting PUBACKs after the report is built"
        );
    }

    #[tokio::test]
    async fn qos1_drain_carries_a_reader_panic_into_the_connection_task() {
        for (ids_in_flight, deadline) in [(0, TEARDOWN_DEADLINE), (LEFTOVER_IDS, LEFTOVER_DEADLINE)] {
            let (read_half, mut write_half, peer) = connected_pair().await;
            let joined = tokio::spawn(async move {
                let in_flight = AtomicU64::new(ids_in_flight);
                let counters = Counters::new(1, Mode::Throughput);
                let reader_handle = AbortOnDropHandle::new(tokio::spawn(async { panicking_reader() }));
                let _ = qos1_drain_and_disconnect(
                    &mut write_half,
                    &in_flight,
                    tokio::time::Instant::now() + deadline,
                    &counters,
                    reader_handle,
                    None,
                )
                .await;
            })
            .await;
            drop(read_half);
            drop(peer);
            let join_err =
                joined.expect_err("a reader panic must panic the connection task that joined it");
            assert!(join_err.is_panic());
            assert!(
                join_err.to_string().contains(READER_PANIC),
                "the connection task must panic with the reader's message: {join_err}"
            );
        }
    }

    #[tokio::test]
    async fn latency_teardown_carries_a_reader_panic_into_the_connection_task() {
        for (ids_in_flight, deadline) in [(0, TEARDOWN_DEADLINE), (LEFTOVER_IDS, LEFTOVER_DEADLINE)] {
            let (read_half, mut write_half, peer) = connected_pair().await;
            let config = GenConfig {
                addr: "127.0.0.1:1".parse().expect("parse addr"),
                mode: Mode::Latency { rate: 1 },
                qos: 1,
                connections: 1,
                qos1_window: 1,
                origin_ns: 0,
                window: Window {
                    start_ns: 0,
                    end_ns: 0,
                },
                drain_grace: Duration::ZERO,
            };
            let joined = tokio::spawn(async move {
                let counters = Arc::new(Counters::new(1, config.mode));
                let in_flight = Arc::new(AtomicU64::new(ids_in_flight));
                let _ = latency_teardown(
                    &config,
                    &counters,
                    tokio::time::Instant::now() + deadline,
                    &in_flight,
                    Some(AbortOnDropHandle::new(tokio::spawn(async { panicking_reader() }))),
                    &mut write_half,
                    Ok(()),
                )
                .await;
            })
            .await;
            drop(read_half);
            drop(peer);
            let join_err =
                joined.expect_err("a reader panic must panic the connection task that joined it");
            assert!(join_err.is_panic());
            assert!(
                join_err.to_string().contains(READER_PANIC),
                "the connection task must panic with the reader's message: {join_err}"
            );
        }
    }

    /// AC-8 — a reader panic must panic the QoS 0 throughput connection
    /// task on both reader-join sites: the clean teardown's bounded
    /// `join_reader_by` and the failed-publish-write site's abort-and-reraise.
    /// Without the `is_finished()` wait the failed-write case can `abort()`
    /// a reader tokio never polled and the join returns a cancellation,
    /// not the panic.
    #[tokio::test]
    async fn qos0_throughput_carries_a_reader_panic_into_the_connection_task() {
        for (window_end_ns, deadline, fill) in [
            (0u64, TEARDOWN_DEADLINE, false),
            (u64::MAX, LEFTOVER_DEADLINE, true),
        ] {
            let (read_half, mut write_half, peer) = connected_pair().await;
            if fill {
                fill_send_buffer(&mut write_half).await;
            }
            let config = GenConfig {
                addr: "127.0.0.1:1".parse().expect("parse addr"),
                mode: Mode::Throughput,
                qos: 0,
                connections: 1,
                qos1_window: 1,
                origin_ns: 0,
                window: Window {
                    start_ns: 0,
                    end_ns: window_end_ns,
                },
                drain_grace: Duration::ZERO,
            };
            let counters = Arc::new(Counters::new(1, Mode::Throughput));
            let task = ConnectionTask {
                conn_id: 0,
                config,
                counters: Arc::clone(&counters),
            };
            let reader = AbortOnDropHandle::new(tokio::spawn(async { panicking_reader() }));
            tokio::time::timeout(TEARDOWN_DEADLINE, async {
                while !reader.is_finished() {
                    tokio::time::sleep(FINISH_POLL).await;
                }
            })
            .await
            .expect("the panicking reader must finish before the teardown starts");
            let joined = tokio::spawn(async move {
                let _ = run_qos0_throughput_with_reader(
                    reader,
                    write_half,
                    task,
                    tokio::time::Instant::now() + deadline,
                )
                .await;
            })
            .await;
            drop(read_half);
            drop(peer);
            let join_err =
                joined.expect_err("a reader panic must panic the connection task that joined it");
            assert!(join_err.is_panic());
            assert!(
                join_err.to_string().contains(READER_PANIC),
                "the connection task must panic with the reader's message: {join_err}"
            );
        }
    }

    #[tokio::test]
    async fn qos0_teardown_stops_waiting_at_its_deadline_when_disconnect_cannot_be_written() {
        let (read_half, mut write_half, mut peer) = connected_pair().await;
        fill_send_buffer(&mut write_half).await;
        let config = GenConfig {
            addr: "127.0.0.1:1".parse().expect("parse addr"),
            mode: Mode::Throughput,
            qos: 0,
            connections: 1,
            qos1_window: 1,
            origin_ns: 0,
            window: Window {
                start_ns: 0,
                end_ns: 0,
            },
            drain_grace: Duration::ZERO,
        };
        let counters = Arc::new(Counters::new(1, Mode::Throughput));
        let task = ConnectionTask {
            conn_id: 0,
            config,
            counters: Arc::clone(&counters),
        };
        let deadline = tokio::time::Instant::now() + LEFTOVER_DEADLINE;
        let finished = tokio::time::timeout(
            LEFTOVER_DEADLINE + TEARDOWN_TOLERANCE,
            run_qos0_throughput(read_half, write_half, task, deadline),
        )
        .await;
        assert!(
            matches!(&finished, Ok(Err(cause)) if cause == "DISCONNECT write did not finish by the hard deadline"),
            "the teardown must stop waiting at its deadline and report the unwritten DISCONNECT"
        );
        assert_eq!(counters.stalled.load(Ordering::Relaxed), 0);
        peer.write_all(&PINGRESP).await.expect("peer write");
        tokio::time::sleep(ABORT_GRACE).await;
        assert_eq!(
            counters.protocol_errors.load(Ordering::Relaxed),
            0,
            "the teardown must leave no reader running: a detached reader counts the frame after the report is built"
        );
    }

    #[tokio::test]
    async fn qos1_teardown_reports_a_disconnect_it_could_not_write() {
        let (read_half, mut write_half, _peer) = connected_pair().await;
        fill_send_buffer(&mut write_half).await;
        let config = GenConfig {
            addr: "127.0.0.1:1".parse().expect("parse addr"),
            mode: Mode::Throughput,
            qos: 1,
            connections: 1,
            qos1_window: 1,
            origin_ns: 0,
            window: Window {
                start_ns: 0,
                end_ns: 0,
            },
            drain_grace: Duration::ZERO,
        };
        let counters = Arc::new(Counters::new(1, Mode::Throughput));
        let task = ConnectionTask {
            conn_id: 0,
            config,
            counters: Arc::clone(&counters),
        };
        let deadline = tokio::time::Instant::now() + LEFTOVER_DEADLINE;
        let finished = tokio::time::timeout(
            LEFTOVER_DEADLINE + TEARDOWN_TOLERANCE,
            run_qos1_throughput(read_half, write_half, task, deadline),
        )
        .await;
        assert!(
            matches!(&finished, Ok(Err(cause)) if cause == "DISCONNECT write did not finish by the hard deadline"),
            "the teardown must report the unwritten DISCONNECT"
        );
        assert_eq!(counters.stalled.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn qos0_teardown_writes_disconnect_and_ends_when_the_peer_closes() {
        let (read_half, write_half, peer) = connected_pair().await;
        let config = GenConfig {
            addr: "127.0.0.1:1".parse().expect("parse addr"),
            mode: Mode::Throughput,
            qos: 0,
            connections: 1,
            qos1_window: 1,
            origin_ns: 0,
            window: Window {
                start_ns: 0,
                end_ns: 0,
            },
            drain_grace: Duration::ZERO,
        };
        let counters = Arc::new(Counters::new(1, Mode::Throughput));
        let task = ConnectionTask {
            conn_id: 0,
            config,
            counters: Arc::clone(&counters),
        };
        let deadline = tokio::time::Instant::now() + TEARDOWN_DEADLINE;
        let teardown = tokio::spawn(run_qos0_throughput(read_half, write_half, task, deadline));
        let bytes = peer_bytes(peer).await;
        assert_eq!(
            bytes,
            wire::DISCONNECT.to_vec(),
            "a clean teardown must write exactly DISCONNECT, then EOF"
        );
        let joined = tokio::time::timeout(TEARDOWN_DEADLINE, teardown)
            .await
            .expect("the teardown must end once the peer closed");
        assert!(matches!(joined, Ok(Ok(()))));
    }

    #[tokio::test]
    async fn join_and_report_fails_the_run_with_the_cause_of_a_panicked_task() {
        let config = join_test_config();
        let counters = Counters::new(1, config.mode);
        let dropped = Arc::new(AtomicU64::new(0));
        let mut set = JoinSet::new();
        set.spawn(held_until_dropped(&dropped));
        set.spawn(async { panicking_connection() });
        let cause = join_and_report(
            &mut set,
            tokio::time::Instant::now() + TEARDOWN_DEADLINE,
            &config,
            &counters,
        )
        .await
        .expect_err("a panicked connection task must fail the run, not build a report");
        assert!(
            cause.starts_with("connection task panicked: "),
            "the cause must start with the panic prefix: {cause}"
        );
        assert!(
            cause.contains(CONNECTION_PANIC),
            "the cause must carry the panic's message: {cause}"
        );
        assert!(set.is_empty(), "every spawned task must be joined");
        assert_eq!(
            dropped.load(Ordering::Relaxed),
            1,
            "the pending task's future must be destroyed before join_and_report returns"
        );
        assert_eq!(
            counters.stalled.load(Ordering::Relaxed),
            0,
            "a panic is not a stall"
        );
    }

    #[tokio::test]
    async fn join_and_report_counts_the_tasks_still_running_at_the_deadline_as_stalled() {
        let config = join_test_config();
        let counters = Counters::new(1, config.mode);
        let dropped = Arc::new(AtomicU64::new(0));
        let mut set = JoinSet::new();
        set.spawn(held_until_dropped(&dropped));
        set.spawn(held_until_dropped(&dropped));
        set.spawn(async {});
        let report = join_and_report(
            &mut set,
            tokio::time::Instant::now() + LEFTOVER_DEADLINE,
            &config,
            &counters,
        )
        .await
        .expect("no task panicked");
        assert_eq!(report.stalled, 2, "each task still running at the deadline counts once");
        assert!(set.is_empty());
        assert_eq!(
            dropped.load(Ordering::Relaxed),
            2,
            "both tasks still running must be aborted and joined before the report is built"
        );
    }

    /// AC-3, AC-4 — the deadline and a panic together: a task still running
    /// at the hard deadline whose panic surfaces only in the join after
    /// `abort_all` fails the run, and the leftovers it would have been
    /// counted for never reach `stalled`.
    #[tokio::test]
    async fn join_and_report_fails_the_run_when_a_task_aborted_at_the_deadline_panics() {
        let config = join_test_config();
        let counters = Counters::new(1, config.mode);
        let dropped = Arc::new(AtomicU64::new(0));
        let mut set = JoinSet::new();
        set.spawn(held_until_dropped(&dropped));
        set.spawn(panics_when_dropped());
        let cause = join_and_report(
            &mut set,
            tokio::time::Instant::now() + LEFTOVER_DEADLINE,
            &config,
            &counters,
        )
        .await
        .expect_err("a panic seen after abort_all must fail the run, not build a report");
        assert!(
            cause.starts_with("connection task panicked: "),
            "the cause must start with the panic prefix: {cause}"
        );
        assert!(
            cause.contains(DROP_PANIC),
            "the cause must carry the panic's message: {cause}"
        );
        assert!(set.is_empty(), "every spawned task must be joined");
        assert_eq!(
            dropped.load(Ordering::Relaxed),
            1,
            "the other task still running must be aborted and joined too"
        );
        assert_eq!(
            counters.stalled.load(Ordering::Relaxed),
            0,
            "a run that failed on a panic must count no leftover as stalled"
        );
    }

    /// AC-4 — the count added to `stalled` is what was still running at the
    /// deadline, so a drain that finished before it builds a report with
    /// nothing stalled.
    #[tokio::test]
    async fn join_and_report_reports_no_stall_when_every_task_finished() {
        let config = join_test_config();
        let counters = Counters::new(1, config.mode);
        let mut set = JoinSet::new();
        for _ in 0..FINISHED_TASKS {
            set.spawn(async {});
        }
        let report = join_and_report(
            &mut set,
            tokio::time::Instant::now() + TEARDOWN_DEADLINE,
            &config,
            &counters,
        )
        .await
        .expect("no task panicked");
        assert_eq!(
            report.stalled, 0,
            "a task that finished before the deadline is not stalled"
        );
        assert_eq!(
            counters.stalled.load(Ordering::Relaxed),
            0,
            "a clean drain must add nothing to stalled"
        );
        assert!(set.is_empty());
    }

    /// AC-1 — a task that finished before the abort but has not been
    /// collected yet (the drain yields before `join_next` reaches it) must
    /// not be counted stalled; only the task still running when
    /// `abort_all` ran may be. The deadline here has already passed when
    /// the tasks are spawned, so the drain never collects any of them.
    #[tokio::test]
    async fn join_and_report_counts_only_the_tasks_still_running_when_the_deadline_passes() {
        let config = join_test_config();
        let counters = Counters::new(1, config.mode);
        let finished = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let hard_deadline = tokio::time::Instant::now();
        let mut set = JoinSet::new();
        set.spawn(held_until_dropped(&dropped));
        for _ in 0..UNCOLLECTED_TASKS {
            let finished = Arc::clone(&finished);
            set.spawn(async move {
                finished.fetch_add(1, Ordering::Relaxed);
            });
        }
        tokio::time::timeout(TEARDOWN_DEADLINE, async {
            loop {
                tokio::time::sleep(FINISH_POLL).await;
                if finished.load(Ordering::Relaxed) == UNCOLLECTED_TASKS {
                    break;
                }
            }
        })
        .await
        .expect("every short task must finish");
        let report = join_and_report(&mut set, hard_deadline, &config, &counters)
            .await
            .expect("no task panicked");
        assert_eq!(
            report.stalled, 1,
            "only the task still running when the abort ran is stalled"
        );
        assert!(set.is_empty());
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }

    /// AC-3 — a reader started with a plain `tokio::spawn` is only
    /// detached, not stopped, when its connection task is aborted; it can
    /// still read the peer's PINGRESP and count a protocol error after
    /// `join_and_report` has already built a report with `protocol_errors == 0`.
    #[tokio::test]
    async fn join_and_report_builds_the_report_after_every_reader_has_stopped() {
        use super::monotonic_now_ns;

        let (read_half, write_half, mut peer) = connected_pair().await;
        let now_ns = monotonic_now_ns();
        let config = GenConfig {
            addr: "127.0.0.1:1".parse().expect("parse addr"),
            mode: Mode::Throughput,
            qos: 0,
            connections: 1,
            qos1_window: 1,
            origin_ns: 0,
            window: Window {
                start_ns: now_ns,
                end_ns: now_ns
                    + u64::try_from(RUNNING_SPAN.as_nanos()).expect("span fits in u64"),
            },
            drain_grace: Duration::ZERO,
        };
        let counters = Arc::new(Counters::new(1, Mode::Throughput));
        let task = ConnectionTask {
            conn_id: 0,
            config: config.clone(),
            counters: Arc::clone(&counters),
        };
        let mut set = JoinSet::new();
        let connection_deadline = tokio::time::Instant::now() + RUNNING_SPAN;
        set.spawn(async move {
            let _ = run_qos0_throughput(read_half, write_half, task, connection_deadline).await;
        });

        let report = join_and_report(
            &mut set,
            tokio::time::Instant::now() + LEFTOVER_DEADLINE,
            &config,
            &counters,
        )
        .await
        .expect("no task panicked");
        assert_eq!(
            report.stalled, 1,
            "the connection blocked on a full send buffer must be stalled at the deadline"
        );

        let _ = peer.write_all(&PINGRESP).await;
        tokio::time::sleep(ABORT_GRACE).await;
        assert_eq!(
            counters.protocol_errors.load(Ordering::Relaxed),
            report.protocol_errors,
            "no reader may count after the report is built"
        );
        assert_eq!(report.protocol_errors, 0);
    }

    /// AC-3 — `join_and_report` must wait for a reader that is still
    /// inside one poll, not only abort it: on a multi-thread runtime an
    /// `AbortOnDropHandle` alone cannot stop a reader mid-poll, so the
    /// report may only be built once the tracker's `wait` resolves.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn join_and_report_waits_for_a_reader_still_inside_a_poll() {
        let config = join_test_config();
        let counters = Arc::new(Counters::new(1, config.mode));
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let mut set = JoinSet::new();
        let conn_counters = Arc::clone(&counters);
        set.spawn(async move {
            let reader_counters = Arc::clone(&conn_counters);
            let _reader = conn_counters.spawn_reader(async move {
                let _ = entered_tx.send(());
                let _ = release_rx.recv_timeout(RELEASE_WATCHDOG);
                reader_counters.add_protocol_errors(1);
            });
            std::future::pending::<()>().await;
        });
        tokio::time::timeout(TEARDOWN_DEADLINE, entered_rx)
            .await
            .expect("the reader must start")
            .expect("entered sender kept");

        let join_counters = Arc::clone(&counters);
        let reporting = tokio::spawn(async move {
            let mut set = set;
            join_and_report(
                &mut set,
                tokio::time::Instant::now() + LEFTOVER_DEADLINE,
                &config,
                &join_counters,
            )
            .await
        });
        tokio::time::sleep(LEFTOVER_DEADLINE + ABORT_GRACE).await;
        assert!(
            !reporting.is_finished(),
            "the report must wait for a reader still inside a poll"
        );

        release_tx.send(()).expect("reader waiting");
        let report = tokio::time::timeout(TEARDOWN_DEADLINE, reporting)
            .await
            .expect("the report must follow the reader's exit")
            .expect("reporting task joined")
            .expect("no task panicked");
        assert_eq!(
            report.protocol_errors, 1,
            "the reader's last count must be in the report"
        );
        assert_eq!(report.stalled, 1);
    }

    /// AC-1 — a connection that ends inside the last `READER_STOP_GRACE`
    /// before the hard deadline keeps its own outcome: reserving the
    /// reader-stop time out of the drain would abort it early and turn
    /// its counted error into a stall.
    #[tokio::test]
    async fn join_and_report_lets_a_task_finish_in_the_last_grace_before_the_deadline() {
        let config = join_test_config();
        let counters = Arc::new(Counters::new(1, config.mode));
        let mut set = JoinSet::new();
        let late_counters = Arc::clone(&counters);
        set.spawn(async move {
            tokio::time::sleep(LATE_FINISH).await;
            late_counters.add_connect_errors(1);
        });

        let report = join_and_report(
            &mut set,
            tokio::time::Instant::now() + READER_STOP_GRACE,
            &config,
            &counters,
        )
        .await
        .expect("no task panicked");
        assert_eq!(
            report.stalled, 0,
            "a task that ended before the hard deadline is not a stall"
        );
        assert_eq!(
            report.connect_errors, 1,
            "the outcome it reached before the hard deadline must be in the report"
        );
    }

    /// AC-3, AC-24 — a reader an abort cannot destroy must not hold the
    /// generator open with no answer: the wait ends `READER_STOP_GRACE`
    /// after the abort and fails the run, because a report may not be
    /// built while a reader can still count. The elapsed bound is that
    /// grace plus slack, so a longer wait fails the test rather than
    /// only lengthening the run.
    #[tokio::test]
    async fn join_and_report_fails_when_a_reader_outlives_its_grace() {
        let config = join_test_config();
        let counters = Arc::new(Counters::new(1, config.mode));
        // Holding the handle keeps the abort from being requested at
        // all, which stands in for the reader an abort cannot
        // interrupt: a write to a stderr nobody reads.
        let _reader = counters.spawn_reader(std::future::pending::<()>());

        // A hard deadline that has already passed leaves the drain no
        // time, so the run costs the reader grace and nothing else.
        let mut set = JoinSet::new();
        let started = std::time::Instant::now();
        let cause = join_and_report(
            &mut set,
            tokio::time::Instant::now(),
            &config,
            &counters,
        )
        .await
        .expect_err("a reader that cannot be stopped must fail the run");
        assert_eq!(
            cause,
            format!(
                "session readers did not stop in the {} s after their connections were aborted",
                READER_STOP_GRACE.as_secs()
            )
        );
        let elapsed = started.elapsed();
        assert!(
            elapsed < READER_STOP_GRACE + EXPIRED_DEADLINE_ANSWER,
            "the reader wait must end one grace after the abort, took {elapsed:?}"
        );
    }

    /// AC-24 — a task blocked inside a synchronous call holds the worker
    /// that polls it, and a host with one CPU gives the generator one
    /// worker. Every Tokio timer then stops, including the ones
    /// `join_and_report` bounds itself with, so only a watchdog outside
    /// the runtime can still answer. The `tokio::time::sleep` here is the
    /// shorter of the two waits: if timers still fired it would win.
    #[test]
    fn the_watchdog_answers_while_a_blocked_task_holds_the_only_worker() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build the one-worker runtime");
        let (holding_tx, holding_rx) = std::sync::mpsc::channel::<()>();

        let watchdog_answered = runtime.block_on(async move {
            tokio::spawn(async move {
                let _ = holding_tx.send(());
                std::thread::sleep(WORKER_HOLD);
            });
            // The worker cannot park again until this poll returns, so
            // once the task reports itself running no timer can fire.
            holding_rx
                .recv_timeout(RELEASE_WATCHDOG)
                .expect("the only worker must be running the blocking task");

            let (watchdog_done, expired) = spawn_progress_watchdog(HELD_WORKER_WATCHDOG);
            let answered = tokio::select! {
                biased;
                () = tokio::time::sleep(HELD_WORKER_TIMER) => false,
                _ = expired => true,
            };
            drop(watchdog_done);
            answered
        });

        // The held worker outlives the test, so the runtime is not
        // dropped: that drop would wait for it.
        runtime.shutdown_timeout(Duration::ZERO);
        assert!(
            watchdog_answered,
            "the watchdog must answer when a blocked task holds every worker"
        );
    }

    /// The watchdog must leave nothing behind when the run answers on its
    /// own: dropping the sender ends its thread before the budget.
    #[test]
    fn the_watchdog_sender_ends_the_thread_before_its_budget() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build the current-thread runtime");
        let started = std::time::Instant::now();
        runtime.block_on(async {
            let (watchdog_done, expired) = spawn_progress_watchdog(WORKER_HOLD);
            drop(watchdog_done);
            expired
                .await
                .expect_err("a watchdog its sender ended must not report expiry");
        });
        let elapsed = started.elapsed();
        assert!(
            elapsed < EXPIRED_DEADLINE_ANSWER,
            "the watchdog must end with its sender, took {elapsed:?}"
        );
    }
}