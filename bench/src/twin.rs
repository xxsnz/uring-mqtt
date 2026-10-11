//! Tokio twin over the runtime seam (PLAN.md `## Interface Contracts`):
//! per-worker `current_thread` tokio runtimes running the crate's own
//! `serve_session` and event channel, with the uring server's accept,
//! drain, processor and lifecycle shape.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::codec::Framed;

use monoio_codec::{Decoder as MonoioDecoder, Decoded as MonoioDecoded};
use uring_mqtt::runtime_seam::{
    event_channel, EventReceiver, HandshakeIo, MqttDecoder, MqttEncoder, MqttPacket, PacketIo,
    ProtocolVersion, serve_session, SessionOutcome, TimedOut, Timer, VersionDecoder,
    MAX_TIMEOUT_SECS,
};
use uring_mqtt::{BrokerConfig, MaxInboundPacketSize, PublishCallback};

/// Pause after a failed `accept` before retrying, so a persistent error
/// cannot spin the worker. Mirrors the shipped worker's
/// `ACCEPT_ERROR_BACKOFF` (`src/broker/worker.rs:92`).
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// `Timer` adapter over `tokio::time::timeout`.
pub struct TokioTimer;

impl Timer for TokioTimer {
    async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output, TimedOut> {
        tokio::time::timeout(duration, future)
            .await
            .map_err(|_| TimedOut)
    }
}

/// Private `tokio_util` `Decoder` adapter over the crate's `VersionDecoder`.
struct VersionCodec(VersionDecoder);

impl tokio_util::codec::Decoder for VersionCodec {
    type Item = ProtocolVersion;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match <VersionDecoder as MonoioDecoder>::decode(&mut self.0, src) {
            Ok(MonoioDecoded::Some(version)) => Ok(Some(version)),
            Ok(MonoioDecoded::Insufficient | MonoioDecoded::InsufficientAtLeast(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// Private `tokio_util` `Decoder`/`Encoder` adapter over the crate's packet
/// codecs.
struct PacketCodec {
    protocol_version: ProtocolVersion,
    decoder: MqttDecoder,
    encoder: MqttEncoder,
}

impl PacketCodec {
    fn new(version: ProtocolVersion, max_inbound_packet_size: MaxInboundPacketSize) -> Self {
        Self {
            protocol_version: version,
            decoder: MqttDecoder::new(version, max_inbound_packet_size),
            encoder: MqttEncoder::new(version),
        }
    }

    fn version(&self) -> ProtocolVersion {
        self.protocol_version
    }
}

impl tokio_util::codec::Decoder for PacketCodec {
    type Item = (MqttPacket, u32);
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match <MqttDecoder as MonoioDecoder>::decode(&mut self.decoder, src) {
            Ok(MonoioDecoded::Some(item)) => Ok(Some(item)),
            Ok(MonoioDecoded::Insufficient | MonoioDecoded::InsufficientAtLeast(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl tokio_util::codec::Encoder<MqttPacket> for PacketCodec {
    type Error = io::Error;

    fn encode(&mut self, item: MqttPacket, dst: &mut BytesMut) -> Result<(), Self::Error> {
        <MqttEncoder as monoio_codec::Encoder<MqttPacket>>::encode(&mut self.encoder, item, dst)
    }
}

/// `HandshakeIo` adapter: a `Framed<TcpStream, VersionCodec>`.
pub struct TokioHandshake {
    framed: Framed<TcpStream, VersionCodec>,
}

impl TokioHandshake {
    pub fn new(stream: TcpStream, max_inbound_packet_size: MaxInboundPacketSize) -> Self {
        Self {
            framed: Framed::new(stream, VersionCodec(VersionDecoder::new(max_inbound_packet_size))),
        }
    }
}

impl HandshakeIo for TokioHandshake {
    type Packets = TokioPackets;

    fn next_version(&mut self) -> impl Future<Output = Option<io::Result<ProtocolVersion>>> {
        self.framed.next()
    }

    fn into_packets(
        self,
        version: ProtocolVersion,
        max_inbound_packet_size: MaxInboundPacketSize,
    ) -> Self::Packets {
        TokioPackets {
            framed: self.framed.map_codec(|_| PacketCodec::new(version, max_inbound_packet_size)),
        }
    }
}

/// `PacketIo` adapter: a `Framed<TcpStream, PacketCodec>`.
pub struct TokioPackets {
    framed: Framed<TcpStream, PacketCodec>,
}

impl PacketIo for TokioPackets {
    fn next_packet(&mut self) -> impl Future<Output = Option<io::Result<(MqttPacket, u32)>>> {
        self.framed.next()
    }

    async fn send_and_flush(&mut self, packet: MqttPacket) -> io::Result<()> {
        // `SinkExt::send` is feed + flush, so this is the one flush per reply
        // the shipped `send_and_flush` performs (`handler.rs:531`).
        SinkExt::send(&mut self.framed, packet).await
    }

    fn read_buffer_is_empty(&self) -> bool {
        self.framed.read_buffer().is_empty()
    }

    fn version(&self) -> ProtocolVersion {
        self.framed.codec().version()
    }
}

/// Per-worker connection-capacity bookkeeping.
struct WorkerState {
    active: usize,
    limit: usize,
}

impl WorkerState {
    fn new(limit: usize) -> Self {
        Self { active: 0, limit }
    }

    fn at_limit(&self) -> bool {
        self.active >= self.limit
    }

    fn add(&mut self) {
        self.active += 1;
    }

    fn remove(&mut self) {
        self.active -= 1;
    }
}

/// Per-worker startup report: one message once the listener has been bound,
/// the event channel and processor spawned, and the worker is ready to
/// serve. A setup failure never travels this channel — every failing step
/// precedes the report — so its cause comes back from the worker thread,
/// which `start` joins before returning `Err`.
type StartupReport = ();

/// Start the tokio twin: one worker per `config.num_workers` (or available
/// parallelism), each its own `current_thread` runtime serving the crate's
/// own `serve_session` over this module's adapters.
pub fn start(config: BrokerConfig, callback: PublishCallback) -> Result<TwinHandle, String> {
    let workers = config.num_workers.unwrap_or_else(|| {
        std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
    });
    if workers == 0 {
        return Err("num_workers must be at least 1".into());
    }
    if config.backlog < 0 {
        return Err("backlog must not be negative".into());
    }
    let backlog = u32::try_from(config.backlog).map_err(|_| "backlog out of range".to_string())?;
    let bind_addr: SocketAddr = config
        .bind_addr
        .parse()
        .map_err(|e: std::net::AddrParseError| format!("bind_addr parse: {e}"))?;

    let (startup_tx, startup_rx) = std::sync::mpsc::channel::<StartupReport>();
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let shutdown_tx = Arc::new(shutdown_tx);
    let dropped: Vec<Arc<AtomicU64>> =
        (0..workers).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let dropped_for_handle = dropped.clone();

    let mut thread_handles: Vec<std::thread::JoinHandle<Result<(), String>>> = Vec::new();
    let mut spawn_failed: Option<String> = None;
    for worker_id in 0..workers {
        let config = config.clone();
        let callback = callback.clone();
        let startup_tx = startup_tx.clone();
        let shutdown_tx = shutdown_tx.clone();
        let dropped = dropped[worker_id].clone();
        match std::thread::Builder::new()
            .name(format!("twin-worker-{worker_id}"))
            .spawn(move || worker_thread(worker_id, config, callback, bind_addr, backlog, shutdown_tx, startup_tx, dropped))
        {
            Ok(handle) => thread_handles.push(handle),
            Err(e) => {
                spawn_failed = Some(format!("worker {worker_id} thread spawn: {e}"));
                break;
            }
        }
    }

    // Drop the main thread's clone of the startup sender before receiving so
    // a worker that failed before reporting only closes the channel once
    // every sender is gone.
    drop(startup_tx);

    // Collect startup reports: a `RecvError` (a worker died before
    // reporting) triggers shutdown + join. It means every sender is gone, so
    // no further report can arrive.
    let first_err: Option<String> = spawn_failed;
    let mut startup_senders_gone = false;
    for _ in 0..thread_handles.len() {
        if startup_rx.recv().is_err() {
            startup_senders_gone = true;
            break;
        }
    }

    if first_err.is_some() || startup_senders_gone {
        let _ = shutdown_tx.send_replace(true);
        // A worker whose setup failed returns the specific cause (`bind`,
        // `listen`, runtime build) from its thread instead of reporting it,
        // because the failing step precedes the startup report. Keep that
        // cause rather than the generic channel-closed message.
        let mut joined_err: Option<String> = None;
        for (worker_id, handle) in thread_handles.into_iter().enumerate() {
            match handle.join() {
                Ok(Err(e)) => {
                    if joined_err.is_none() {
                        joined_err = Some(e);
                    }
                }
                Err(_) => {
                    if joined_err.is_none() {
                        joined_err = Some(format!("worker {worker_id} panicked during startup"));
                    }
                }
                Ok(Ok(())) => {}
            }
        }
        return Err(first_err.or(joined_err).unwrap_or_else(|| {
            "worker died before reporting startup".to_string()
        }));
    }

    Ok(TwinHandle {
        handles: Some(thread_handles),
        dropped: dropped_for_handle,
        shutdown: TwinShutdown { tx: shutdown_tx },
    })
}

#[allow(clippy::too_many_arguments)]
fn worker_thread(
    worker_id: usize,
    config: BrokerConfig,
    callback: PublishCallback,
    bind_addr: SocketAddr,
    backlog: u32,
    shutdown_tx: Arc<tokio::sync::watch::Sender<bool>>,
    startup_tx: std::sync::mpsc::Sender<StartupReport>,
    dropped: Arc<AtomicU64>,
) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("worker {worker_id}: tokio runtime build: {e}"))?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, worker_main(
        worker_id,
        config,
        callback,
        bind_addr,
        backlog,
        shutdown_tx,
        startup_tx,
        dropped,
    ))
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn worker_main(
    worker_id: usize,
    config: BrokerConfig,
    callback: PublishCallback,
    bind_addr: SocketAddr,
    backlog: u32,
    shutdown_tx: Arc<tokio::sync::watch::Sender<bool>>,
    startup_tx: std::sync::mpsc::Sender<StartupReport>,
    dropped: Arc<AtomicU64>,
) -> Result<(), String> {
    // Listener: SO_REUSEPORT so workers co-bind for kernel-side balancing.
    let socket = if bind_addr.is_ipv4() {
        TcpSocket::new_v4()
    } else {
        TcpSocket::new_v6()
    }
    .map_err(|e| format!("worker {worker_id}: TcpSocket: {e}"))?;
    socket
        .set_reuseaddr(true)
        .map_err(|e| format!("worker {worker_id}: set_reuseaddr: {e}"))?;
    socket
        .set_reuseport(true)
        .map_err(|e| format!("worker {worker_id}: set_reuseport: {e}"))?;
    socket
        .bind(bind_addr)
        .map_err(|e| format!("worker {worker_id}: bind: {e}"))?;
    let listener = socket
        .listen(backlog)
        .map_err(|e| format!("worker {worker_id}: listen: {e}"))?;

    let (event_tx, event_rx) = event_channel(worker_id, dropped);

    let callback_for_processor = callback.clone();
    let mut processor: JoinHandle<()> = tokio::task::spawn_local(async move {
        process_events(event_rx, callback_for_processor).await;
    });

    if startup_tx.send(()).is_err() {
        return Err(format!("worker {worker_id}: startup receiver dropped"));
    }
    drop(startup_tx);

    let shutdown_rx = shutdown_tx.subscribe();
    let ingest_deadline = Duration::from_secs(config.ingest_deadline_secs.min(MAX_TIMEOUT_SECS));
    let max_connections = config.max_connections_per_worker;
    // A session task reports a refusal, a protocol violation or a non-routine
    // error as `Some(message)`.
    let mut sessions: JoinSet<Option<String>> = JoinSet::new();
    let mut state = WorkerState::new(max_connections);
    let mut failure: Option<String> = None;
    let mut session_failure: Option<String> = None;
    let mut accept_failure: Option<String> = None;
    let mut processor_joined = false;
    let mut shutdown_requested = shutdown_rx;

    loop {
        tokio::select! {
            biased;
            _ = shutdown_requested.wait_for(|v| *v) => break,
            r = &mut processor => {
                processor_joined = true;
                match r {
                    Ok(()) => {
                        failure = Some("event processor ended".to_string());
                    }
                    Err(e) if e.is_panic() => {
                        failure = Some(format!("event processor panicked: {e}"));
                    }
                    Err(e) => {
                        failure = Some(format!("event processor ended: {e}"));
                    }
                }
                let _ = shutdown_tx.send_replace(true);
                break;
            }
            res = listener.accept() => {
                // Reap any finished sessions first — a finished task stays
                // in the JoinSet until joined, so `sessions.len()` alone
                // would over-count.
                while let Some(joined) = sessions.try_join_next() {
                    if state.active > 0 {
                        state.remove();
                    }
                    note_session_result(joined, &mut session_failure, &mut failure, &shutdown_tx);
                }
                if failure.is_some() {
                    break;
                }
                match res {
                    Ok((stream, peer)) => {
                        if state.at_limit() {
                            drop(stream);
                            continue;
                        }
                        if stream.set_nodelay(true).is_err() {
                            drop(stream);
                            continue;
                        }
                        state.add();
                        let event_tx = event_tx.clone();
                        let connection_timeout_secs = config.connection_timeout_secs;
                        let idle_timeout_secs = config.idle_timeout_secs;
                        let max_inbound_packet_size = config.max_inbound_packet_size;
                        sessions.spawn_local(async move {
                            let outcome = serve_session::<TokioHandshake, TokioTimer>(
                                TokioHandshake::new(stream, max_inbound_packet_size),
                                peer,
                                event_tx,
                                connection_timeout_secs,
                                idle_timeout_secs,
                                max_inbound_packet_size,
                                ingest_deadline,
                            )
                            .await;
                            // A routine disconnect (hangup, reset, broken
                            // pipe, timeout) is the client's own doing, so it
                            // stays unreported, as in the shipped worker
                            // (`src/broker/worker.rs:582-586`). Any other
                            // error is server-side evidence the run needs.
                            //
                            // A refusal and a protocol violation are evidence
                            // too: the shipped worker counts both
                            // (`note_refused` / `note_violation`,
                            // `src/broker/worker.rs:579,581`), and decisions
                            // Q15 and R2-Q2 make a refused connect or a
                            // protocol error invalidate the run. The twin has
                            // no logger, so `join` is where an operator can
                            // still tell rejected traffic from a quiet server.
                            match outcome {
                                Ok(SessionOutcome::Served) => None,
                                Ok(SessionOutcome::Refused) => {
                                    Some(format!("session {peer}: connect refused"))
                                }
                                Ok(SessionOutcome::Violation) => {
                                    Some(format!("session {peer}: protocol violation"))
                                }
                                Err(e) if e.is_routine_disconnect() => None,
                                Err(e) => Some(format!("session {peer}: {e}")),
                            }
                        });
                    }
                    Err(e) => {
                        // An accept error once shutdown is requested is the
                        // dropped listener, not evidence — the shipped worker
                        // breaks there too (`src/broker/worker.rs:594-596`).
                        if *shutdown_tx.borrow() {
                            break;
                        }
                        // A recoverable per-connection error must not end the
                        // benchmark server, and decision Q1 keeps the twin's
                        // accept policy identical to the shipped worker's:
                        // back off `ACCEPT_ERROR_BACKOFF` and keep accepting
                        // (`src/broker/worker.rs:595-601`). Shutdown stays
                        // responsive — the next loop iteration enters the
                        // biased select whose first branch is the watcher.
                        //
                        // The shipped worker logs the cause; the twin has no
                        // logger, so the first cause is kept as worker
                        // evidence and reported by `join`. A listener that
                        // keeps failing therefore cannot leave a healthy
                        // looking server behind, and a single recoverable
                        // error still invalidates the run it happened in
                        // (decision R2-Q2) without stopping the accept loop.
                        if accept_failure.is_none() {
                            accept_failure = Some(format!("accept: {e}"));
                        }
                        tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                    }
                }
            }
        }
    }

    // Drain: drop the listener, drop the master sender so the processor
    // eventually sees the channel end. Bound by `drain_timeout_secs`.
    drop(listener);

    let drain_secs = config.drain_timeout_secs.min(MAX_TIMEOUT_SECS);
    let drain_outcome = tokio::time::timeout(Duration::from_secs(drain_secs), async {
        while let Some(joined) = sessions.join_next().await {
            if state.active > 0 {
                state.remove();
            }
            note_session_result(joined, &mut session_failure, &mut failure, &shutdown_tx);
        }
    })
    .await;

    if drain_outcome.is_err() || failure.is_some() {
        sessions.abort_all();
        while let Some(joined) = sessions.join_next().await {
            if state.active > 0 {
                state.remove();
            }
            note_session_result(joined, &mut session_failure, &mut failure, &shutdown_tx);
        }
    }

    drop(event_tx);

    // Always consume the processor's result unless the select branch above
    // already took it. `is_finished()` is also true for a task that panicked
    // and has not been joined, so testing it here would discard the
    // `JoinError` of a callback that panicked while shutdown was pending —
    // the worker would report `Ok(())` for a failed run.
    if !processor_joined {
        match processor.await {
            Ok(()) => {}
            Err(e) if e.is_panic() => {
                if failure.is_none() {
                    failure = Some(format!("event processor panicked: {e}"));
                }
                let _ = shutdown_tx.send_replace(true);
            }
            Err(e) => {
                if failure.is_none() {
                    failure = Some(format!("event processor ended: {e}"));
                }
            }
        }
    }

    // An accept error and a session refusal, violation or non-routine error
    // never stop the twin — the shipped worker also keeps serving — but each
    // is server-side evidence that invalidates the run, so `join` reports it
    // when nothing worse happened. The accept error wins: it means the worker
    // stopped taking clients, while a session outcome concerns one client.
    if failure.is_none() {
        failure = accept_failure.or(session_failure);
    }

    failure.map_or(Ok(()), Err)
}

/// Record one joined session task: a refusal, a protocol violation or a
/// non-routine session error in `session_failure`, a panic in `failure` plus
/// a shutdown broadcast. A cancelled task (abort during drain) carries no
/// evidence.
fn note_session_result(
    joined: Result<Option<String>, tokio::task::JoinError>,
    session_failure: &mut Option<String>,
    failure: &mut Option<String>,
    shutdown_tx: &tokio::sync::watch::Sender<bool>,
) {
    match joined {
        Ok(Some(e)) => {
            if session_failure.is_none() {
                *session_failure = Some(e);
            }
        }
        Err(e) if e.is_panic() => {
            if failure.is_none() {
                *failure = Some(format!("session panicked: {e}"));
            }
            let _ = shutdown_tx.send_replace(true);
        }
        // A clean session, and a task cancelled by `abort_all` during drain,
        // carry no evidence.
        Ok(None) | Err(_) => {}
    }
}

/// Mirror of `broker::worker::process_events` adapted for the bench
/// callback signature. The channel is `local_sync::mpsc::unbounded`, so
/// `recv().await` polls until the sender is dropped.
async fn process_events(mut rx: EventReceiver, callback: PublishCallback) {
    while let Some(publish) = rx.recv().await {
        callback(&publish);
    }
}

/// A clonable shutdown trigger for every twin worker.
#[derive(Clone)]
pub struct TwinShutdown {
    tx: Arc<tokio::sync::watch::Sender<bool>>,
}

impl TwinShutdown {
    pub fn shutdown(&self) {
        let _ = self.tx.send_replace(true);
    }
}

/// Reader of the twin's lifetime drop total that outlives the
/// [`TwinHandle`] it was cloned from, so the total stays readable after
/// [`join`](TwinHandle::join) consumes the handle and whatever that join
/// reports. Mirrors `DroppedPublishesHandle` (`src/broker/mod.rs:304`).
#[derive(Clone)]
pub struct TwinDroppedPublishes {
    counters: Vec<Arc<AtomicU64>>,
}

impl TwinDroppedPublishes {
    /// The same total a successful [`TwinHandle::join`] returns, read now.
    pub fn total(&self) -> u64 {
        self.counters.iter().map(|d| d.load(Ordering::Relaxed)).sum()
    }
}

/// Join handle over every twin worker thread.
pub struct TwinHandle {
    handles: Option<Vec<std::thread::JoinHandle<Result<(), String>>>>,
    dropped: Vec<Arc<AtomicU64>>,
    shutdown: TwinShutdown,
}

impl TwinHandle {
    pub fn shutdown_handle(&self) -> TwinShutdown {
        self.shutdown.clone()
    }

    /// Clone a reader of the drop total that outlives this handle, so the
    /// final total stays readable after [`join`](Self::join) consumes it —
    /// including when that join reports a worker error instead of the total.
    pub fn dropped_publishes_handle(&self) -> TwinDroppedPublishes {
        TwinDroppedPublishes {
            counters: self.dropped.clone(),
        }
    }

    /// Returns `Ok(total dropped publishes)` once every worker thread has
    /// exited, or the first worker error.
    pub fn join(mut self) -> Result<u64, String> {
        let handles = self
            .handles
            .take()
            .expect("twin handles already joined by Drop");
        let mut first_err: Option<String> = None;
        for (id, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Err(_) => {
                    if first_err.is_none() {
                        first_err = Some(format!("twin worker {id} panicked"));
                    }
                }
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(format!("twin worker {id}: {e}"));
                    }
                }
                Ok(Ok(())) => {}
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(self.dropped.iter().map(|d| d.load(Ordering::Relaxed)).sum())
    }
}

impl Drop for TwinHandle {
    /// Mirrors `BrokerHandle::Drop` (`src/broker/mod.rs:404`): shutdown,
    /// then join any handles still held. Results are discarded — the
    /// handle has been moved out of by the time an embedder could observe
    /// them.
    fn drop(&mut self) {
        self.shutdown.shutdown();
        if let Some(handles) = self.handles.take() {
            for handle in handles {
                let _ = handle.join();
            }
        }
    }
}