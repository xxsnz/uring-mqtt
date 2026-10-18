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
        let drained = tokio::time::timeout_at(hard_deadline, drain).await;
        let stalled_now = set.len() as u64;
        if drained.is_err() {
            set.abort_all();
            while set.join_next().await.is_some() {}
            counters.add_stalled(stalled_now);
        }
        Ok(build_report(config, &counters))
    });

    runtime.shutdown_timeout(Duration::from_secs(1));
    report
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
            Err(OpenError::Connect) => {
                task.counters.add_connect_errors(1);
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
    Connect,
    /// Why the reply is not a valid CONNACK for this CONNECT. Every reason
    /// is built from a fixed set of short texts — the codec's non-`Io`
    /// `DecodeError`s and `ConnectAckReason` both print a constant — so the
    /// diagnostic a caller prints stays bounded.
    Protocol(String),
}

async fn open_session(
    config: &GenConfig,
    client_id: &str,
) -> Result<(OwnedReadHalf, OwnedWriteHalf, tokio::time::Instant), OpenError> {
    let connect_fut = TcpStream::connect(config.addr);
    let Ok(Ok(stream)) =
        tokio::time::timeout(Duration::from_secs(IO_TIMEOUT_SECS), connect_fut).await
    else {
        return Err(OpenError::Connect);
    };
    if stream.set_nodelay(true).is_err() {
        return Err(OpenError::Connect);
    }
    let (read_half, mut write_half) = stream.into_split();
    let connect_bytes = wire::encode_connect(client_id);
    if !matches!(
        tokio::time::timeout(
            Duration::from_secs(IO_TIMEOUT_SECS),
            write_half.write_all(&connect_bytes),
        )
        .await,
        Ok(Ok(()))
    ) {
        return Err(OpenError::Connect);
    }
    let mut framed = FramedRead::new(read_half, Codec::new(wire::INBOUND_REMAINING_LENGTH_MAX));
    let connack = match tokio::time::timeout(
        Duration::from_secs(IO_TIMEOUT_SECS),
        framed.next(),
    )
    .await
    {
        Ok(Some(Ok((Packet::ConnectAck(ack), _)))) => ack,
        Ok(Some(Ok(_))) => {
            return Err(OpenError::Protocol(
                "first packet was not a CONNACK".to_string(),
            ))
        }
        // A frame the codec rejects — reserved flag bits, a return code
        // MQTT 3.1.1 does not define — is the broker's malformed reply,
        // counted like every other malformed frame; a read failure is
        // the transport's (`qos1_reader_task` splits the same way).
        Ok(Some(Err(e))) if !matches!(e, DecodeError::Io(_)) => {
            return Err(OpenError::Protocol(format!("CONNACK decode failed: {e}")))
        }
        _ => return Err(OpenError::Connect),
    };
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
    // The CONNACK read consumed 4 bytes from `read_half`; the codec's read
    // buffer is empty unless a partial next frame already arrived. Returning
    // the half lets the session reader rebuild its own `FramedRead`.
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

async fn run_qos0_throughput(
    read_half: OwnedReadHalf,
    mut write_half: OwnedWriteHalf,
    task: ConnectionTask,
    deadline_instant: tokio::time::Instant,
) -> Result<(), String> {
    let counters = Arc::clone(&task.counters);
    let conn_id = task.conn_id;
    let reader_handle = tokio::spawn(async move {
        let mut framed = FramedRead::new(read_half, Codec::new(wire::INBOUND_REMAINING_LENGTH_MAX));
        while let Some(item) = framed.next().await {
            match item {
                // A QoS 0 publisher subscribes to nothing, so every frame
                // the broker sends is a protocol error. Anything the codec
                // rejects — an oversized or malformed frame — is one too, and
                // counting it keeps a broker that answers with garbage from
                // reconciling as a valid run (`qos1_latency_reader` splits the
                // same two cases).
                Ok(_) => counters.add_protocol_errors(1),
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
    });

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
            // handle only detaches it, and a detached reader keeps the
            // socket and can still count a protocol error after the
            // report is built.
            reader_handle.abort();
            let _ = reader_handle.await;
            return Err(cause);
        }
    }
    let _ = tokio::time::timeout(
        Duration::from_secs(IO_TIMEOUT_SECS),
        write_half.write_all(&wire::DISCONNECT),
    )
    .await;
    let _ = write_half.shutdown().await;
    let _ = reader_handle.await;
    Ok(())
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
    let reader_handle = tokio::spawn(qos1_reader_task(
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
        write_failure.is_some(),
    )
    .await;
    write_failure.map_or(Ok(()), Err)
}

/// Drain every in-flight packet id up to the deadline, then write
/// DISCONNECT unless a publish write failed (a partial PUBLISH may be on
/// the wire), shut the writer half down and end the reader. Ids still in
/// flight at the deadline count in `stalled`, read only after the reader
/// has stopped, so no PUBACK is counted both `acked` and `stalled`.
async fn qos1_drain_and_disconnect(
    write_half: &mut OwnedWriteHalf,
    in_flight: &AtomicU64,
    deadline_instant: tokio::time::Instant,
    counters: &Counters,
    reader_handle: tokio::task::JoinHandle<()>,
    write_failed: bool,
) {
    let drain_fut = async {
        while in_flight.load(Ordering::Relaxed) > 0 {
            tokio::task::yield_now().await;
        }
    };
    let drained = tokio::time::timeout_at(deadline_instant, drain_fut).await.is_ok();
    let mut reader_handle = Some(reader_handle);
    if !drained || write_failed {
        // Stop the reader before reading what is left: a PUBACK it
        // counted after that read would be both `acked` and `stalled`.
        // The aborted handle resolves once the reader is no longer
        // running — at most the end of the poll it is in.
        if let Some(handle) = reader_handle.take() {
            handle.abort();
            let _ = handle.await;
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
    if !write_failed {
        let _ =
            tokio::time::timeout_at(deadline_instant, write_half.write_all(&wire::DISCONNECT))
                .await;
    }
    let _ = write_half.shutdown().await;
    if let Some(mut handle) = reader_handle {
        // A peer that keeps its socket open after DISCONNECT never sends
        // the EOF the reader waits for, so an unbounded join would
        // outlast the deadline and leave the backstop to count the whole
        // connection `stalled`. The reader can still count a protocol
        // error, so the aborted handle is awaited before the report is
        // built (as in `latency_teardown`).
        if tokio::time::timeout_at(deadline_instant, &mut handle).await.is_err() {
            handle.abort();
            let _ = handle.await;
        }
    }
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
                }
            }
            Ok(_) => counters.add_protocol_errors(1),
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
        pacer_result.is_err(),
    )
    .await;

    pacer_result
}

/// Outcome of opening a latency-mode session.
type OpenedSession = (
    OwnedWriteHalf,
    tokio::time::Instant,
    Arc<std::sync::Mutex<PacketIds>>,
    Arc<AtomicU64>,
    Option<tokio::task::JoinHandle<()>>,
);

/// Open CONNECT, return `(write_half, deadline_instant, packet_ids, in_flight,
/// reader_handle)` on success. `Ok(None)` is the connect-error / protocol-
/// error path (counters already bumped); `Err` is an unexpected failure.
async fn latency_open_session(
    config: &GenConfig,
    counters: &Arc<Counters>,
    conn_id: u32,
) -> Result<Option<OpenedSession>, String> {
    let client_id = format!("bench-p{conn_id}");
    let (read_half, write_half, deadline_instant) = match open_session(config, &client_id).await {
        Ok(v) => v,
        Err(OpenError::Connect) => {
            counters.add_connect_errors(1);
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
        Some(tokio::spawn(qos1_latency_reader(
            conn_id,
            read_half,
            reader_counters,
            reader_packet_ids,
            reader_in_flight,
        )))
    } else {
        drop(read_half);
        None
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

/// Drain in-flight QoS 1 ids up to the deadline (stopping the reader before the leftovers are counted), write DISCONNECT unless a publish write failed (a partial PUBLISH may be on the wire), shut the writer down, and rejoin the reader task (QoS 1 only).
async fn latency_teardown(
    config: &GenConfig,
    counters: &Arc<Counters>,
    deadline_instant: tokio::time::Instant,
    in_flight: &Arc<AtomicU64>,
    mut reader_handle: Option<tokio::task::JoinHandle<()>>,
    write_half: &mut OwnedWriteHalf,
    write_failed: bool,
) {
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
                let _ = handle.await;
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
    if !write_failed {
        let _ = tokio::time::timeout_at(deadline_instant, write_half.write_all(&wire::DISCONNECT))
            .await;
    }
    let _ = write_half.shutdown().await;
    if let Some(mut handle) = reader_handle {
        // A peer that holds its socket open while withholding PUBACK never
        // sends the EOF the reader is waiting for, so an unbounded join would
        // outlast the deadline above. Dropping the handle would only detach
        // the reader, so abort what the bounded join leaves running.
        if tokio::time::timeout_at(deadline_instant, &mut handle).await.is_err() {
            handle.abort();
            let _ = handle.await;
        }
    }
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
                }
            }
            Ok(_) => {
                counters.add_protocol_errors(1);
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
        latency_teardown, qos1_drain_and_disconnect, qos1_latency_reader, qos1_reader_task, wire,
        Counters, GenConfig, Mode, PacketIds, Window,
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::Semaphore;

    /// Deadline handed to the teardown under test — long enough that the
    /// clean path never meets it.
    const TEARDOWN_DEADLINE: Duration = Duration::from_secs(2);
    /// Ids left in flight for the leftover tests: a batch the peer never
    /// acknowledged.
    const LEFTOVER_IDS: u64 = 3;
    /// Deadline handed to a teardown whose drain cannot finish — short
    /// enough to meet inside a test, far longer than one scheduler poll.
    const LEFTOVER_DEADLINE: Duration = Duration::from_millis(200);
    /// How long the leftover tests hold their peer's PUBACKs back — past
    /// `LEFTOVER_DEADLINE`, so the drain meets the deadline with every id
    /// still in flight and the teardown has already returned.
    const PUBACK_DELAY: Duration = Duration::from_millis(400);
    /// Time the leftover tests give a reader that was never stopped to count
    /// after the teardown returned — long enough to cover `PUBACK_DELAY` and
    /// the polls that follow it.
    const ABORT_GRACE: Duration = Duration::from_millis(400);

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
            let reader_handle = tokio::spawn(async {});
            qos1_drain_and_disconnect(
                &mut write_half,
                &in_flight,
                tokio::time::Instant::now() + TEARDOWN_DEADLINE,
                &counters,
                reader_handle,
                write_failed,
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
            latency_teardown(
                &config,
                &counters,
                tokio::time::Instant::now() + TEARDOWN_DEADLINE,
                &in_flight,
                None,
                &mut write_half,
                write_failed,
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

    /// Allocate the ids a teardown's drain finds still in flight, and hand
    /// the peer a task that writes one PUBACK per id only after the drain
    /// deadline has passed. A teardown that detaches the production reader
    /// instead of aborting and awaiting it counts those PUBACKs after the
    /// report is built, which these tests make visible.
    fn leftover_ids_acked_late(mut peer: TcpStream) -> Arc<std::sync::Mutex<PacketIds>> {
        let mut ids = PacketIds::new();
        let mut pubacks = Vec::new();
        for _ in 0..LEFTOVER_IDS {
            let id = ids.allocate().expect("packet id available").get();
            pubacks.extend_from_slice(&[0x40, 0x02, (id >> 8) as u8, (id & 0xFF) as u8]);
        }
        tokio::spawn(async move {
            tokio::time::sleep(PUBACK_DELAY).await;
            // A teardown that stopped its reader has already dropped the
            // read half, so this write can draw a reset; the test's
            // assertion is what the reader did, not what the peer wrote.
            let _ = peer.write_all(&pubacks).await;
        });
        Arc::new(std::sync::Mutex::new(ids))
    }

    #[tokio::test]
    async fn qos1_drain_counts_the_leftovers_as_stalled_and_leaves_no_reader_running() {
        let (read_half, mut write_half, peer) = connected_pair().await;
        let in_flight = Arc::new(AtomicU64::new(LEFTOVER_IDS));
        let counters = Arc::new(Counters::new(1, Mode::Throughput));
        let packet_ids = leftover_ids_acked_late(peer);
        let semaphore = Arc::new(Semaphore::new(0));
        let reader_handle = tokio::spawn(qos1_reader_task(
            0,
            read_half,
            Arc::clone(&counters),
            Arc::clone(&semaphore),
            Arc::clone(&packet_ids),
            Arc::clone(&in_flight),
        ));

        qos1_drain_and_disconnect(
            &mut write_half,
            &in_flight,
            tokio::time::Instant::now() + LEFTOVER_DEADLINE,
            &counters,
            reader_handle,
            false,
        )
        .await;

        assert_eq!(
            counters.stalled.load(Ordering::Relaxed),
            LEFTOVER_IDS,
            "every id still in flight at the deadline counts once in stalled"
        );
        let acked_when_counted = counters.acked.load(Ordering::Relaxed);
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
        let packet_ids = leftover_ids_acked_late(peer);
        let reader_handle = tokio::spawn(qos1_latency_reader(
            0,
            read_half,
            Arc::clone(&counters),
            Arc::clone(&packet_ids),
            Arc::clone(&in_flight),
        ));

        latency_teardown(
            &config,
            &counters,
            tokio::time::Instant::now() + LEFTOVER_DEADLINE,
            &in_flight,
            Some(reader_handle),
            &mut write_half,
            false,
        )
        .await;

        assert_eq!(
            counters.stalled.load(Ordering::Relaxed),
            LEFTOVER_IDS,
            "every id still in flight at the deadline counts once in stalled"
        );
        let acked_when_counted = counters.acked.load(Ordering::Relaxed);
        tokio::time::sleep(ABORT_GRACE).await;
        assert_eq!(
            counters.acked.load(Ordering::Relaxed),
            acked_when_counted,
            "the teardown must leave no reader running: a detached reader keeps counting PUBACKs after the report is built"
        );
    }
}