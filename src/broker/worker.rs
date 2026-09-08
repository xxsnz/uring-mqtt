use monoio::io::{AsyncReadRent, Canceller};
use monoio::net::{ListenerConfig, TcpListener};
use std::cell::Cell;
use std::rc::Rc;

use super::handler::handle_client;
use super::{BrokerConfig, Event, EventCallback};
use crate::error::Error;
use crate::pool::BufferPool;

/// Per-worker state (thread-local, no Send/Sync required)
struct WorkerState {
    worker_id: usize,
    active_connections: Cell<usize>,
    max_connections: usize,
    refused_connects: Cell<u64>,
    #[allow(dead_code)] // Reserved for future buffer reuse optimization
    buffer_pool: BufferPool,
}

impl WorkerState {
    fn new(worker_id: usize, max_connections: usize) -> Self {
        Self {
            worker_id,
            active_connections: Cell::new(0),
            max_connections,
            refused_connects: Cell::new(0),
            buffer_pool: BufferPool::new(2048, max_connections / 4),
        }
    }

    fn try_acquire(&self) -> bool {
        let current = self.active_connections.get();
        if current >= self.max_connections {
            return false;
        }
        self.active_connections.set(current + 1);
        true
    }

    fn release(&self) {
        self.active_connections
            .set(self.active_connections.get() - 1);
    }

    fn active_count(&self) -> usize {
        self.active_connections.get()
    }

    fn id(&self) -> usize {
        self.worker_id
    }

    fn note_refused(&self) {
        let total = self.refused_connects.get() + 1;
        self.refused_connects.set(total);
        if should_log_count(total) {
            tracing::warn!(
                "Worker {}: {} refused connects total",
                self.worker_id,
                total
            );
        }
    }
}

/// Maximum number of undelivered events buffered between worker and callback.
pub(crate) const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Cadence of warn-level logs driven by lifetime counters (drops, refusals).
const COUNT_LOG_EVERY: u64 = 100;

/// True when `total` is the first occurrence or a positive multiple of `COUNT_LOG_EVERY`.
fn should_log_count(total: u64) -> bool {
    total == 1 || (total > 0 && total.is_multiple_of(COUNT_LOG_EVERY))
}

/// Shared bookkeeping between sender clones and the receiver.
struct EventChannelState {
    depth: Cell<usize>, // events currently queued
    dropped: Cell<u64>, // lifetime overflow drops (never reset)
}

#[derive(Clone)]
pub(crate) struct EventSender {
    tx: local_sync::mpsc::unbounded::Tx<Event>,
    state: Rc<EventChannelState>,
    worker_id: usize,
}

impl EventSender {
    pub(crate) fn send(&self, event: Event) {
        if self.tx.is_closed() {
            tracing::debug!(
                "worker {}: event receiver closed, event discarded",
                self.worker_id
            );
            return;
        }
        if self.state.depth.get() >= EVENT_CHANNEL_CAPACITY {
            let total = self.state.dropped.get() + 1;
            self.state.dropped.set(total);
            if should_log_count(total) {
                tracing::warn!(
                    "worker {}: event channel full, dropped event ({} dropped total)",
                    self.worker_id,
                    total
                );
            }
            return;
        }
        match self.tx.send(event) {
            Ok(()) => {
                self.state.depth.set(self.state.depth.get() + 1);
            }
            Err(_) => {
                tracing::debug!(
                    "worker {}: event receiver closed, event discarded",
                    self.worker_id
                );
            }
        }
    }
}

pub(crate) struct EventReceiver {
    rx: local_sync::mpsc::unbounded::Rx<Event>,
    state: Rc<EventChannelState>,
}

impl EventReceiver {
    pub(crate) async fn recv(&mut self) -> Option<Event> {
        let item = self.rx.recv().await;
        if item.is_some() {
            self.state.depth.set(self.state.depth.get() - 1);
        }
        item
    }
}

/// Build a new bounded facade over a `local-sync` unbounded channel.
pub(crate) fn event_channel(worker_id: usize) -> (EventSender, EventReceiver) {
    let state = Rc::new(EventChannelState {
        depth: Cell::new(0),
        dropped: Cell::new(0),
    });
    let (tx, rx) = local_sync::mpsc::unbounded::channel::<Event>();
    (
        EventSender {
            tx,
            state: state.clone(),
            worker_id,
        },
        EventReceiver { rx, state },
    )
}

/// Run a worker thread with its own io_uring event loop.
///
/// ALL fallible setup (bind, `WorkerState`, event channel, event-processor
/// spawn) precedes the success report. `ready_tx` is dropped immediately after
/// reporting so the supervisor can detect a sibling that panicked before its
/// own report (panic → drop → channel disconnect). Blocking `go_rx.recv()` is
/// safe — nothing else progresses on this runtime until the accept loop
/// starts.
#[allow(clippy::too_many_lines)] // shutdown/drain machinery (watcher, cancelable_accept, done-channel, abort select, deadline force-close) pushes the body past the pedantic 100-line boundary; mirrors handler.rs's identical allowance
pub async fn run_worker(
    worker_id: usize,
    config: BrokerConfig,
    callback: Option<EventCallback>,
    ready_tx: std::sync::mpsc::Sender<(usize, Result<(), Error>)>,
    go_rx: std::sync::mpsc::Receiver<bool>,
    shutdown_signal: std::os::unix::net::UnixStream,
) -> Result<(), Error> {
    tracing::info!("Worker {} starting", worker_id);

    // Bind must stay on the worker thread (monoio per-thread-driver fd
    // registration; SO_REUSEPORT lets workers co-bind for kernel load
    // balancing).
    let listener_config = ListenerConfig::new()
        .reuse_addr(true)
        .reuse_port(true)
        .backlog(config.backlog);

    let listener = match TcpListener::bind_with_config(&config.bind_addr, &listener_config) {
        Ok(l) => l,
        Err(e) => {
            let _ = ready_tx.send((worker_id, Err(e.into())));
            drop(ready_tx);
            return Ok(());
        }
    };

    // EOF on this stream is the shutdown signal — the watcher task below
    // owns the converted stream and sets `shutdown_requested` on any read
    // completion. Conversion sits after the bind and before the ready
    // report, mirroring the bind-error arm.
    let shutdown_stream = match monoio::net::UnixStream::from_std(shutdown_signal) {
        Ok(s) => s,
        Err(e) => {
            let _ = ready_tx.send((worker_id, Err(e.into())));
            drop(ready_tx);
            return Ok(());
        }
    };

    tracing::info!(
        "Worker {} listening on {} (max {} connections)",
        worker_id,
        config.bind_addr,
        config.max_connections_per_worker
    );

    // Thread-local worker state (Rc, not Arc - no Send/Sync needed)
    let state = Rc::new(WorkerState::new(
        worker_id,
        config.max_connections_per_worker,
    ));

    // Thread-local event channel
    let (event_tx, event_rx) = event_channel(worker_id);

    // Spawn event processor task
    let callback_clone = callback.clone();
    monoio::spawn(async move {
        process_events(event_rx, callback_clone).await;
    });

    // ALL setup complete: report success and drop the sender. The drop is
    // load-bearing — a sibling that panicked before its report shows up as a
    // channel disconnect only when every sender clone is gone.
    let _ = ready_tx.send((worker_id, Ok(())));
    drop(ready_tx);

    if let Ok(false) | Err(_) = go_rx.recv() {
        tracing::info!("Worker {} aborting: startup failed elsewhere", worker_id);
        return Ok(());
    }
    // Ok(true) falls through to the accept loop.

    // Per-worker shutdown machinery: done-channel (sender drops are the
    // drain signal), abort semaphore (close() broadcasts the force-close),
    // and the shared `shutdown_requested` flag the watcher sets on EOF.
    let (done_tx, mut done_rx) = local_sync::mpsc::unbounded::channel::<()>();
    let abort = Rc::new(local_sync::semaphore::Semaphore::new(0));
    let shutdown_requested = Rc::new(Cell::new(false));

    // Watcher: any completion on the shutdown stream (EOF, byte, error)
    // flips the flag and cancels the in-flight accept. The accept loop
    // awaits cancellation to completion — never drops a future mid-accept,
    // which would leak its fd (monoio uring/lifecycle.rs:74).
    let canceller = Canceller::new();
    let accept_cancel = canceller.handle();
    {
        let flag = shutdown_requested.clone();
        let mut signal = shutdown_stream;
        monoio::spawn(async move {
            let (_res, _buf) = signal.read(vec![0u8; 1]).await;
            flag.set(true);
            let _ = canceller.cancel();
        });
    }

    // Accept loop
    let mut connection_counter: u64 = 0;

    loop {
        match listener.cancelable_accept(accept_cancel.clone()).await {
            Ok((stream, addr)) => {
                if shutdown_requested.get() {
                    drop(stream);
                    break;
                }
                if !state.try_acquire() {
                    tracing::warn!(
                        "Worker {}: connection limit reached, rejecting {}",
                        worker_id,
                        addr
                    );
                    drop(stream);
                    continue;
                }

                connection_counter += 1;
                if connection_counter.is_multiple_of(100) {
                    tracing::info!(
                        "Worker {}: {} active connections (total accepted: {})",
                        state.id(),
                        state.active_count(),
                        connection_counter
                    );
                }

                // Set TCP_NODELAY for low latency
                let _ = stream.set_nodelay(true);

                let state_clone = state.clone();
                let event_tx_clone = event_tx.clone();
                let connection_timeout = config.connection_timeout_secs;
                let idle_timeout = config.idle_timeout_secs;
                let done_tx_clone = done_tx.clone();
                let abort_clone = abort.clone();

                // Spawn connection handler (local task, stays on this core).
                // The `biased` select gives the abort semaphore priority so a
                // force-close can wake a parked handler without racing a
                // slow I/O completion.
                monoio::spawn(async move {
                    monoio::select! {
                        biased;
                        res = handle_client(stream, event_tx_clone, connection_timeout, idle_timeout) => {
                            match res {
                                Ok(super::handler::SessionOutcome::Refused) => state_clone.note_refused(),
                                Ok(super::handler::SessionOutcome::Served) => {}
                                Err(e) => {
                                    if !e.is_routine_disconnect() {
                                        tracing::debug!("Client {} error: {:?}", addr, e);
                                    }
                                }
                            }
                        }
                        _ = abort_clone.acquire() => {}
                    }
                    state_clone.release();
                    drop(done_tx_clone);
                });
            }
            Err(e) => {
                if shutdown_requested.get() {
                    break;
                }
                tracing::error!("Worker {}: accept error: {}", worker_id, e);
                monoio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }

    // Shutdown path: stop accepting, drop the master done-sender, drain.
    tracing::info!(
        "Worker {} shutting down: draining {} active connections",
        worker_id,
        state.active_count()
    );
    drop(listener);
    drop(done_tx);
    let drain_secs = config
        .drain_timeout_secs
        .min(crate::broker::handshake::MAX_TIMEOUT_SECS);
    let graceful = monoio::time::timeout(std::time::Duration::from_secs(drain_secs), async {
        while done_rx.recv().await.is_some() {}
    })
    .await;
    if graceful.is_err() {
        tracing::warn!(
            "Worker {} drain deadline expired: force-closing {} connections",
            worker_id,
            state.active_count()
        );
        abort.close();
        while done_rx.recv().await.is_some() {}
    }
    Ok(())
}

/// Process events from the local channel.
async fn process_events(mut rx: EventReceiver, callback: Option<EventCallback>) {
    while let Some(event) = rx.recv().await {
        if let Some(ref cb) = callback {
            cb(event);
        } else {
            tracing::trace!("Event: {:?}", event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::supervise_startup;
    use std::future::Future;
    use std::io::{Read, Write};
    use std::net::TcpListener as StdTcpListener;
    use std::pin::pin;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::Duration;

    impl EventSender {
        pub(crate) fn dropped_total(&self) -> u64 {
            self.state.dropped.get()
        }
    }

    impl WorkerState {
        pub(crate) fn refused_total(&self) -> u64 {
            self.refused_connects.get()
        }
    }

    struct FlagWaker {
        flag: AtomicBool,
    }

    impl Wake for FlagWaker {
        fn wake(self: Arc<Self>) {
            self.flag.store(true, AtomicOrdering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.flag.store(true, AtomicOrdering::SeqCst);
        }
    }

    fn flag_waker() -> (Arc<FlagWaker>, Waker) {
        let w = Arc::new(FlagWaker {
            flag: AtomicBool::new(false),
        });
        let waker = Waker::from(w.clone());
        (w, waker)
    }

    fn poll_once<F: Future>(fut: &mut std::pin::Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        let mut ctx = Context::from_waker(waker);
        fut.as_mut().poll(&mut ctx)
    }

    /// Try to receive the next event with a fresh future on each call. The
    /// `recv` future returned by `local_sync` is single-shot, and the borrow
    /// checker forces a fresh borrow of `rx` per call to release the previous
    /// one before another `rx.recv()` can be issued.
    fn try_recv(rx: &mut EventReceiver, waker: &Waker) -> Poll<Option<Event>> {
        let mut fut = pin!(rx.recv());
        poll_once(&mut fut, waker)
    }

    #[test]
    fn event_yielded_when_channel_not_full() {
        let (tx, mut rx) = event_channel(0);
        tx.send(Event::SensorV1 {
            temperature: 2500,
            pressure: 1013,
        });
        let (waker_arc, waker) = flag_waker();
        match try_recv(&mut rx, &waker) {
            Poll::Ready(Some(Event::SensorV1 {
                temperature,
                pressure,
            })) => {
                assert_eq!(temperature, 2500);
                assert_eq!(pressure, 1013);
            }
            other => panic!("expected Ready(Some), got {other:?}"),
        }
        // Drain closes the channel: all senders dropped, receiver yields None.
        drop(tx);
        assert!(matches!(try_recv(&mut rx, &waker), Poll::Ready(None)));
        assert!(!waker_arc.flag.load(AtomicOrdering::SeqCst));
    }

    #[test]
    fn channel_accepts_exactly_capacity_and_drops_the_rest() {
        let (tx1, mut rx) = event_channel(0);
        let tx2 = tx1.clone();
        let total = EVENT_CHANNEL_CAPACITY + 3;
        for i in 0..total {
            let t = i16::try_from(i).expect("fixture index fits i16");
            if i % 2 == 0 {
                tx1.send(Event::SensorV1 {
                    temperature: t,
                    pressure: u16::try_from(i).expect("fixture index fits u16"),
                });
            } else {
                tx2.send(Event::SensorV1 {
                    temperature: t,
                    pressure: u16::try_from(i).expect("fixture index fits u16"),
                });
            }
        }
        assert_eq!(tx1.dropped_total(), 3);

        // Drop both senders so recv can eventually yield None.
        drop(tx1);
        drop(tx2);

        let (_, waker) = flag_waker();
        let mut received = Vec::with_capacity(EVENT_CHANNEL_CAPACITY);
        for _ in 0..EVENT_CHANNEL_CAPACITY {
            match try_recv(&mut rx, &waker) {
                Poll::Ready(Some(Event::SensorV1 { temperature, .. })) => {
                    received.push(temperature);
                }
                other => panic!("expected Ready(Some), got {other:?}"),
            }
        }
        // Channel now empty: must return None.
        assert!(matches!(try_recv(&mut rx, &waker), Poll::Ready(None)));

        assert_eq!(received.len(), EVENT_CHANNEL_CAPACITY);
        // First EVENT_CHANNEL_CAPACITY distinct indices, in send order.
        for (idx, temp) in received.iter().enumerate() {
            assert_eq!(
                *temp,
                i16::try_from(idx).expect("fixture index fits i16"),
                "received index {idx} out of order"
            );
        }
    }

    #[test]
    fn sender_recovers_after_drain() {
        let (tx, mut rx) = event_channel(0);
        let (_, waker) = flag_waker();

        // Fill to capacity; one extra is dropped.
        for i in 0..EVENT_CHANNEL_CAPACITY {
            tx.send(Event::SensorV1 {
                temperature: i16::try_from(i).expect("fits i16"),
                pressure: 0,
            });
        }
        tx.send(Event::SensorV1 {
            temperature: 9999,
            pressure: 0,
        });
        assert_eq!(tx.dropped_total(), 1);

        // Drain everything via manual polls.
        for _ in 0..EVENT_CHANNEL_CAPACITY {
            match try_recv(&mut rx, &waker) {
                Poll::Ready(Some(_)) => {}
                other => panic!("expected Ready(Some), got {other:?}"),
            }
        }

        // Sender recovers; the next send succeeds and is deliverable.
        tx.send(Event::SensorV1 {
            temperature: 1234,
            pressure: 7,
        });
        match try_recv(&mut rx, &waker) {
            Poll::Ready(Some(Event::SensorV1 {
                temperature,
                pressure,
            })) => {
                assert_eq!(temperature, 1234);
                assert_eq!(pressure, 7);
            }
            other => panic!("expected Ready(Some), got {other:?}"),
        }
        drop(tx);
    }

    #[test]
    fn recv_pending_is_woken_by_send() {
        let (tx, mut rx) = event_channel(0);
        let (waker_arc, waker) = flag_waker();

        // Empty channel: poll must return Pending, flag stays false.
        assert!(matches!(try_recv(&mut rx, &waker), Poll::Pending));
        assert!(
            !waker_arc.flag.load(AtomicOrdering::SeqCst),
            "waker fired without cause"
        );

        tx.send(Event::SensorV1 {
            temperature: 2500,
            pressure: 1013,
        });
        assert!(
            waker_arc.flag.load(AtomicOrdering::SeqCst),
            "send did not wake pending recv"
        );

        // Re-poll with the now-woken context: must yield the queued event.
        match try_recv(&mut rx, &waker) {
            Poll::Ready(Some(Event::SensorV1 {
                temperature,
                pressure,
            })) => {
                assert_eq!(temperature, 2500);
                assert_eq!(pressure, 1013);
            }
            other => panic!("expected Ready(Some), got {other:?}"),
        }
    }

    #[test]
    fn dropping_last_sender_wakes_pending_recv() {
        let (tx, mut rx) = event_channel(0);
        let (waker_arc, waker) = flag_waker();

        assert!(matches!(try_recv(&mut rx, &waker), Poll::Pending));
        assert!(!waker_arc.flag.load(AtomicOrdering::SeqCst));

        drop(tx);
        assert!(
            waker_arc.flag.load(AtomicOrdering::SeqCst),
            "dropping the only sender did not wake pending recv"
        );
        assert!(matches!(try_recv(&mut rx, &waker), Poll::Ready(None)));
    }

    #[test]
    fn should_log_count_cadence() {
        assert!(should_log_count(1));
        assert!(should_log_count(100));
        assert!(should_log_count(200));
        assert!(!should_log_count(0));
        assert!(!should_log_count(2));
        assert!(!should_log_count(99));
        assert!(!should_log_count(101));
        assert!(!should_log_count(150));
    }

    /// AC-19 — `try_acquire` honours the cap and `release` decrements it. A
    /// no-op `Cell` rewrite (e.g. setter removed) fails the exact counts.
    #[test]
    fn connection_limit_is_enforced_and_released() {
        let state = WorkerState::new(0, 2);
        assert!(state.try_acquire());
        assert!(state.try_acquire());
        assert!(!state.try_acquire());
        assert_eq!(state.active_count(), 2);
        state.release();
        assert_eq!(state.active_count(), 1);
        assert!(state.try_acquire());
        assert_eq!(state.active_count(), 2);
    }

    /// AC-24 — `note_refused` increments the lifetime counter exactly once per
    /// call. The warn-line cadence predicate is AC-5's already-tested
    /// `should_log_count`; the warn wiring is counter-asserted only.
    #[test]
    fn note_refused_increments_lifetime_total() {
        let state = WorkerState::new(0, 2);
        assert_eq!(state.refused_total(), 0);
        state.note_refused();
        state.note_refused();
        state.note_refused();
        assert_eq!(state.refused_total(), 3);
    }

    /// AC-2 + AC-16 + AC-22 — bind happens on the worker thread (the std
    /// listener can't co-bind), the worker parks on `go_rx` after reporting
    /// (drops its sender), the port is released after the worker thread is
    /// joined.
    #[test]
    fn worker_binds_reports_parks_and_releases_on_abort() {
        // Find a free port without co-binding reuse_port.
        let std_pick = StdTcpListener::bind("127.0.0.1:0").expect("std bind free");
        let addr: std::net::SocketAddr = std_pick.local_addr().expect("local_addr");
        drop(std_pick);

        let config = BrokerConfig::new(addr.to_string());
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        let (res_tx, res_rx) = std::sync::mpsc::channel();
        let (sig_main, sig_worker) = std::os::unix::net::UnixStream::pair().expect("socketpair");
        let _sig_main = sig_main;

        let worker_thread = std::thread::Builder::new()
            .name("worker-park-test".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("runtime build");
                let r = rt.block_on(run_worker(0, config, None, ready_tx, go_rx, sig_worker));
                let _ = res_tx.send(r);
            })
            .expect("spawn worker");

        // 1. Worker reports readiness.
        match ready_rx.recv_timeout(Duration::from_secs(5)) {
            Ok((0, Ok(()))) => {}
            other => panic!("expected (0, Ok(())), got {other:?}"),
        }

        // 2. Parked worker dropped its sender; recv sees disconnect.
        match ready_rx.recv_timeout(Duration::from_secs(1)) {
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {}
            other => panic!("expected Disconnected, got {other:?}"),
        }

        // 3. The worker really owns the port: a plain std bind (no reuse_port)
        // collides with its reuse_port-enabled bind.
        assert!(
            StdTcpListener::bind(addr).is_err(),
            "second bind should fail while worker holds the port"
        );

        // 4. AC-22 probe — a TCP client connects (kernel backlog), writes
        // V3_CONNECT_TEST, and reads with a 500ms timeout. The worker is
        // parked, never entered the accept loop, so no CONNACK arrives.
        let connect_bytes: [u8; 18] = [
            0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x00, 0x00, 0x3C, 0x00, 0x04,
            b't', b'e', b's', b't',
        ];
        let mut last_err: Option<std::io::Error> = None;
        let mut stream = None;
        for _ in 0..20 {
            match std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50)) {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
        let mut stream =
            stream.unwrap_or_else(|| panic!("failed to connect within retry window: {last_err:?}"));
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .expect("set_read_timeout");
        stream.write_all(&connect_bytes).expect("write CONNECT");
        let mut buf = [0u8; 4];
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => {} // EOF or timeout — no CONNACK while parked
            Ok(n) => panic!("unexpected {n} bytes while worker parked: {:?}", &buf[..n]),
        }
        drop(stream);

        // 5. Abort the parked worker.
        go_tx.send(false).expect("go send");

        // 6. Worker exits cleanly with Ok(()).
        match res_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => {}
            other => panic!("expected Ok(()), got {other:?}"),
        }

        // 7. Join + retry-bind: runtime teardown closes the fd, port becomes
        // available again. monoio's SharedFd defers the close, so a small
        // bounded retry is needed.
        worker_thread.join().expect("worker join");
        let mut rebound = false;
        for _ in 0..20 {
            if StdTcpListener::bind(addr).is_ok() {
                rebound = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(rebound, "port did not release after worker join");
    }

    /// AC-2 companion — the go channel's sender disappearing (supervisor gone)
    /// is the other abort trigger beside an explicit `false`. The parked worker
    /// must treat the disconnect as an abort and return `Ok(())`, never panic
    /// or park forever.
    #[test]
    fn worker_aborts_when_go_sender_is_dropped() {
        let std_pick = StdTcpListener::bind("127.0.0.1:0").expect("std bind free");
        let addr: std::net::SocketAddr = std_pick.local_addr().expect("local_addr");
        drop(std_pick);

        let config = BrokerConfig::new(addr.to_string());
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<bool>();
        let (res_tx, res_rx) = std::sync::mpsc::channel();
        let (sig_main, sig_worker) = std::os::unix::net::UnixStream::pair().expect("socketpair");
        let _sig_main = sig_main;

        let worker_thread = std::thread::Builder::new()
            .name("worker-go-drop-test".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("runtime build");
                let r = rt.block_on(run_worker(3, config, None, ready_tx, go_rx, sig_worker));
                let _ = res_tx.send(r);
            })
            .expect("spawn worker");

        match ready_rx.recv_timeout(Duration::from_secs(5)) {
            Ok((3, Ok(()))) => {}
            other => panic!("expected (3, Ok(())), got {other:?}"),
        }

        // Supervisor vanished without a decision.
        drop(go_tx);

        match res_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => {}
            other => panic!("expected Ok(()), got {other:?}"),
        }
        worker_thread.join().expect("worker join");
    }

    /// AC-1 partial-failure path through PRODUCTION `supervise_startup`: one
    /// worker's bind collides, the other is healthy. The helper must fan out
    /// `false` to BOTH go-senders — the healthy worker aborts cleanly.
    #[test]
    fn mixed_bind_failure_aborts_healthy_worker() {
        // P1 is held by a std listener (no reuse_port → collides with the
        // worker's reuse_port-enabled bind). P2 is genuinely free.
        let p1_linger = StdTcpListener::bind("127.0.0.1:0").expect("std bind p1");
        let p1: std::net::SocketAddr = p1_linger.local_addr().expect("p1 addr");
        let p2_pick = StdTcpListener::bind("127.0.0.1:0").expect("std bind p2 pick");
        let p2: std::net::SocketAddr = p2_pick.local_addr().expect("p2 addr");
        drop(p2_pick);

        let (ready_tx_main, ready_rx) = std::sync::mpsc::channel();
        let (go_a_tx, go_a_rx) = std::sync::mpsc::channel();
        let (go_b_tx, go_b_rx) = std::sync::mpsc::channel();
        let (res_a_tx, res_a_rx) = std::sync::mpsc::channel();
        let (res_b_tx, res_b_rx) = std::sync::mpsc::channel();

        // Main drops its ready_tx — supervise only sees the workers' clones.
        let ready_a = ready_tx_main.clone();
        let ready_b = ready_tx_main.clone();
        drop(ready_tx_main);

        let cfg_a = BrokerConfig::new(p2.to_string());
        let cfg_b = BrokerConfig::new(p1.to_string());

        let (sig_a_main, sig_a_worker) =
            std::os::unix::net::UnixStream::pair().expect("socketpair a");
        let (sig_b_main, sig_b_worker) =
            std::os::unix::net::UnixStream::pair().expect("socketpair b");
        let _sig_a_main = sig_a_main;
        let _sig_b_main = sig_b_main;

        let thread_a = std::thread::Builder::new()
            .name("mixed-healthy".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("rt a");
                let r = rt.block_on(run_worker(0, cfg_a, None, ready_a, go_a_rx, sig_a_worker));
                let _ = res_a_tx.send(r);
            })
            .expect("spawn a");
        let thread_b = std::thread::Builder::new()
            .name("mixed-failing".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("rt b");
                let r = rt.block_on(run_worker(1, cfg_b, None, ready_b, go_b_rx, sig_b_worker));
                let _ = res_b_tx.send(r);
            })
            .expect("spawn b");

        // Run PRODUCTION supervise_startup on a harness thread and ship the
        // result back over a channel with a bounded wait.
        let go_txs = vec![go_a_tx, go_b_tx];
        let (sup_tx, sup_rx) = std::sync::mpsc::channel();
        let sup_thread = std::thread::Builder::new()
            .name("mixed-supervise".into())
            .spawn(move || {
                let r = supervise_startup(&ready_rx, &go_txs, 2);
                let _ = sup_tx.send(r);
            })
            .expect("spawn sup");

        let sup_result = sup_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("sup timeout");
        assert!(
            sup_result.is_err(),
            "expected Err from supervise, got {sup_result:?}"
        );

        // Both worker threads exit within 5s each — A aborted via the
        // production `false` fan-out; B reported its bind error and returned.
        match res_a_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("a timeout")
        {
            Ok(()) => {}
            other => panic!("worker A expected Ok(()), got {other:?}"),
        }
        match res_b_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("b timeout")
        {
            Ok(()) => {}
            other => panic!("worker B expected Ok(()), got {other:?}"),
        }

        thread_a.join().expect("join a");
        thread_b.join().expect("join b");
        sup_thread.join().expect("join sup");
        drop(p1_linger);
    }

    /// AC-17 ordering discriminator — with setup BEFORE report, a worker's
    /// setup panic (BufferPool overflow) drops its sender unreported. With
    /// setup AFTER report, the panic would happen after a successful report
    /// and the survivor would enter its endless accept loop, hanging the
    /// test on the bounded wait.
    #[test]
    fn setup_panic_before_report_aborts_startup() {
        let p1_pick = StdTcpListener::bind("127.0.0.1:0").expect("std bind p1");
        let p1: std::net::SocketAddr = p1_pick.local_addr().expect("p1");
        drop(p1_pick);
        let p2_pick = StdTcpListener::bind("127.0.0.1:0").expect("std bind p2");
        let p2: std::net::SocketAddr = p2_pick.local_addr().expect("p2");
        drop(p2_pick);

        let (ready_tx_main, ready_rx) = std::sync::mpsc::channel();
        let (go_a_tx, go_a_rx) = std::sync::mpsc::channel();
        let (go_b_tx, go_b_rx) = std::sync::mpsc::channel();
        let (res_a_tx, res_a_rx) = std::sync::mpsc::channel();
        let (res_b_tx, res_b_rx) = std::sync::mpsc::channel();

        let ready_a = ready_tx_main.clone();
        let ready_b = ready_tx_main.clone();
        drop(ready_tx_main);

        // Worker A: sane config.
        let cfg_a = BrokerConfig::new(p1.to_string());
        // Worker B: triggers BufferPool::new → VecDeque::with_capacity(usize::MAX/4)
        // panic during WorkerState construction (pre-report).
        let cfg_b = BrokerConfig::new(p2.to_string()).max_connections_per_worker(usize::MAX);

        let (sig_a_main, sig_a_worker) =
            std::os::unix::net::UnixStream::pair().expect("socketpair a");
        let (sig_b_main, sig_b_worker) =
            std::os::unix::net::UnixStream::pair().expect("socketpair b");
        let _sig_a_main = sig_a_main;
        let _sig_b_main = sig_b_main;

        let thread_a = std::thread::Builder::new()
            .name("panic-test-healthy".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("rt a");
                let r = rt.block_on(run_worker(0, cfg_a, None, ready_a, go_a_rx, sig_a_worker));
                let _ = res_a_tx.send(r);
            })
            .expect("spawn a");
        let thread_b = std::thread::Builder::new()
            .name("panic-test-panicker".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("rt b");
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    rt.block_on(run_worker(1, cfg_b, None, ready_b, go_b_rx, sig_b_worker))
                }));
                let _ = res_b_tx.send(r);
            })
            .expect("spawn b");

        let go_txs = vec![go_a_tx, go_b_tx];
        let (sup_tx, sup_rx) = std::sync::mpsc::channel();
        let sup_thread = std::thread::Builder::new()
            .name("panic-test-supervise".into())
            .spawn(move || {
                let r = supervise_startup(&ready_rx, &go_txs, 2);
                let _ = sup_tx.send(r);
            })
            .expect("spawn sup");

        let sup_result = sup_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("sup timeout");
        assert!(sup_result.is_err(), "expected Err, got {sup_result:?}");

        // A aborted cleanly via the false fan-out.
        match res_a_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("a timeout")
        {
            Ok(()) => {}
            other => panic!("worker A expected Ok(()), got {other:?}"),
        }
        // B's thread panicked; the catch_unwind yields Err.
        match res_b_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("b timeout")
        {
            Err(_) => {} // catch_unwind caught a panic — expected
            Ok(Ok(())) => panic!("worker B should have panicked, not returned Ok"),
            Ok(Err(e)) => panic!("worker B should have panicked, not returned {e:?}"),
        }

        thread_a.join().expect("join a");
        let _ = thread_b.join(); // panic payload — don't assert success
        sup_thread.join().expect("join sup");
    }

    // -- graceful-shutdown tests (task 2) ---------------------------------
    //
    // MQTT CONNECT/CONNACK fixtures, copied byte-for-byte from
    // `src/broker/mod.rs:466` (per PLAN.md — fixtures are test-module-local
    // and not importable). These are the only legal handshake for v3
    // CONNACK `[0x20,0x02,0x00,0x00]`.
    const V3_CONNECT_BYTES: [u8; 18] = [
        0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x00, 0x00, 0x3C, 0x00, 0x04, b't',
        b'e', b's', b't',
    ];
    const V3_CONNACK_BYTES: [u8; 4] = [0x20, 0x02, 0x00, 0x00];
    const PINGREQ_BYTES: [u8; 2] = [0xC0, 0x00];
    const PINGRESP_BYTES: [u8; 2] = [0xD0, 0x00];

    /// Drain a fixed-length MQTT reply from `stream`, padding with zero bytes
    /// if the read returns short. Treats any read error other than
    /// `WouldBlock`/`TimedOut` as fatal — those two mean "no bytes yet".
    fn read_exact_or_zero(
        stream: &mut std::net::TcpStream,
        buf: &mut [u8],
    ) -> std::io::Result<usize> {
        let mut read_total = 0usize;
        while read_total < buf.len() {
            match stream.read(&mut buf[read_total..]) {
                Ok(0) => return Ok(0),
                Ok(n) => read_total += n,
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    if read_total == 0 {
                        return Err(e);
                    }
                    // Zero the unread tail so the assertion sees the expected
                    // payload — the wire read was short, not absent.
                    for slot in &mut buf[read_total..] {
                        *slot = 0;
                    }
                    return Ok(read_total);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(read_total)
    }

    /// AC-1 — EOF on the shutdown signal stream must end the worker. The
    /// harness drops the main end of the socketpair after `go(true)`; the
    /// worker observes EOF and returns `Ok(())`. A retry-bind oracle proves
    /// the listener's fd was released (matching the abort test shape).
    #[test]
    fn shutdown_signal_stops_accept_and_worker_returns() {
        let std_pick = StdTcpListener::bind("127.0.0.1:0").expect("std bind free");
        let addr: std::net::SocketAddr = std_pick.local_addr().expect("local_addr");
        drop(std_pick);

        let config = BrokerConfig::new(addr.to_string());
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        let (res_tx, res_rx) = std::sync::mpsc::channel();
        let (sig_main, sig_worker) = std::os::unix::net::UnixStream::pair().expect("socketpair");

        let worker_thread = std::thread::Builder::new()
            .name("shutdown-eof-test".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("runtime build");
                let r = rt.block_on(run_worker(0, config, None, ready_tx, go_rx, sig_worker));
                let _ = res_tx.send(r);
            })
            .expect("spawn worker");

        // Worker reports readiness.
        match ready_rx.recv_timeout(Duration::from_secs(5)) {
            Ok((0, Ok(()))) => {}
            other => panic!("expected (0, Ok(())), got {other:?}"),
        }

        // Green light: worker enters the accept loop.
        go_tx.send(true).expect("go true");

        // Signal: drop the main end of the socketpair → worker reads EOF.
        drop(sig_main);

        // Worker must return Ok(()) within 5s of the signal.
        match res_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => {}
            other => panic!("expected Ok(Ok(())), got {other:?}"),
        }

        // Port released: plain std bind (no reuse_port) succeeds after a
        // bounded retry window for monoio's deferred fd close.
        worker_thread.join().expect("worker join");
        let mut rebound = false;
        for _ in 0..20 {
            if StdTcpListener::bind(addr).is_ok() {
                rebound = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(rebound, "port did not release after worker shutdown");
    }

    /// AC-3 (+ AC-1 during-drain refusal) — once shutdown is signaled, the
    /// worker stops accepting new connections (poll-connect fails) but keeps
    /// serving the held one (PINGREQ→PINGRESP round-trip), then exits cleanly
    /// when the held client disconnects.
    #[test]
    fn worker_serves_active_connection_during_drain_and_refuses_new_ones() {
        let std_pick = StdTcpListener::bind("127.0.0.1:0").expect("std bind free");
        let addr: std::net::SocketAddr = std_pick.local_addr().expect("local_addr");
        drop(std_pick);

        // 30s drain so the test is bounded by the client drop, not the timer.
        let config = BrokerConfig::new(addr.to_string()).drain_timeout_secs(30);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        let (res_tx, res_rx) = std::sync::mpsc::channel();
        let (sig_main, sig_worker) = std::os::unix::net::UnixStream::pair().expect("socketpair");

        let worker_thread = std::thread::Builder::new()
            .name("drain-serves-held-test".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("runtime build");
                let r = rt.block_on(run_worker(0, config, None, ready_tx, go_rx, sig_worker));
                let _ = res_tx.send(r);
            })
            .expect("spawn worker");

        match ready_rx.recv_timeout(Duration::from_secs(5)) {
            Ok((0, Ok(()))) => {}
            other => panic!("expected (0, Ok(())), got {other:?}"),
        }
        go_tx.send(true).expect("go true");

        // Connect, complete CONNECT/CONNACK — held client established.
        let mut client = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2))
            .expect("client connect");
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("client read timeout");
        client
            .set_write_timeout(Some(Duration::from_secs(2)))
            .expect("client write timeout");
        client.write_all(&V3_CONNECT_BYTES).expect("write CONNECT");
        let mut connack = [0u8; 4];
        let n = read_exact_or_zero(&mut client, &mut connack).expect("read CONNACK");
        assert_eq!(n, 4, "short CONNACK read");
        assert_eq!(connack, V3_CONNACK_BYTES);

        // Signal: drop the main end.
        drop(sig_main);

        // Probe: new connects must be refused (listener dropped). Up to
        // 40×50ms — gives the worker time to drop the listener after EOF.
        let mut refused = false;
        for _ in 0..40 {
            if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_err() {
                refused = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(refused, "worker kept accepting after shutdown signal");

        // Held client still served: PINGREQ → PINGRESP.
        client.write_all(&PINGREQ_BYTES).expect("write PINGREQ");
        let mut pingresp = [0u8; 2];
        let n = read_exact_or_zero(&mut client, &mut pingresp).expect("read PINGRESP");
        assert_eq!(n, 2, "short PINGRESP read");
        assert_eq!(pingresp, PINGRESP_BYTES);

        // Drop the held client → drain completes on the last disconnect.
        drop(client);

        match res_rx.recv_timeout(Duration::from_secs(2)) {
            Ok(Ok(())) => {}
            other => panic!("expected Ok(Ok(())), got {other:?}"),
        }

        worker_thread.join().expect("worker join");
    }

    /// AC-4 — when the drain deadline expires with stragglers still held,
    /// every straggler must see EOF or reset on its socket within a bounded
    /// window after the worker returns. Two stragglers exercise the abort
    /// semaphore's broadcast (single-waker implementations hang on the
    /// second).
    #[test]
    fn worker_force_closes_all_idle_connections_at_drain_deadline() {
        let std_pick = StdTcpListener::bind("127.0.0.1:0").expect("std bind free");
        let addr: std::net::SocketAddr = std_pick.local_addr().expect("local_addr");
        drop(std_pick);

        // 1s drain so the test is bounded by the timer.
        let config = BrokerConfig::new(addr.to_string()).drain_timeout_secs(1);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        let (res_tx, res_rx) = std::sync::mpsc::channel();
        let (sig_main, sig_worker) = std::os::unix::net::UnixStream::pair().expect("socketpair");

        let worker_thread = std::thread::Builder::new()
            .name("drain-force-close-test".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("runtime build");
                let r = rt.block_on(run_worker(0, config, None, ready_tx, go_rx, sig_worker));
                let _ = res_tx.send(r);
            })
            .expect("spawn worker");

        match ready_rx.recv_timeout(Duration::from_secs(5)) {
            Ok((0, Ok(()))) => {}
            other => panic!("expected (0, Ok(())), got {other:?}"),
        }
        go_tx.send(true).expect("go true");

        // Two clients, both CONNECT/CONNACK — establish and hold.
        let mut clients = Vec::new();
        for _ in 0..2 {
            let mut c = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2))
                .expect("client connect");
            c.set_read_timeout(Some(Duration::from_secs(2)))
                .expect("read timeout");
            c.set_write_timeout(Some(Duration::from_secs(2)))
                .expect("write timeout");
            c.write_all(&V3_CONNECT_BYTES).expect("write CONNECT");
            let mut connack = [0u8; 4];
            let n = read_exact_or_zero(&mut c, &mut connack).expect("read CONNACK");
            assert_eq!(n, 4, "short CONNACK read");
            assert_eq!(connack, V3_CONNACK_BYTES);
            clients.push(c);
        }

        // Signal shutdown.
        drop(sig_main);

        // Worker must return Ok(()) within 5s (deadline=1s + slack).
        match res_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => {}
            other => panic!("expected Ok(Ok(())), got {other:?}"),
        }

        // Both held clients must observe EOF or reset — WouldBlock/TimedOut
        // means the fd leaked (the abort semaphore did not wake this waiter).
        // Give each read the full 2s window after the worker result lands
        // (the runtime drop releases fds on the worker thread, which
        // precedes the harness `res_tx` send).
        for (idx, c) in clients.iter_mut().enumerate() {
            let mut buf = [0u8; 8];
            match c.read(&mut buf) {
                Ok(0) => {} // EOF — clean force-close
                Ok(n) => panic!("client {idx} read {n} bytes after force-close"),
                Err(e) => match e.kind() {
                    std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionAborted => {} // reset — fine
                    other_kind => panic!(
                        "client {idx} read returned {other_kind:?} after force-close: \
                         connection not force-closed: fds leaked"
                    ),
                },
            }
        }

        worker_thread.join().expect("worker join");
    }

    /// Regression guard for the drain clamp — absurdly large drain values
    /// must not panic when converted to a `Duration`. The value here fits in
    /// `Instant` (so monoio does not take its far-future fallback) but
    /// overflows millisecond arithmetic.
    #[test]
    fn absurd_drain_timeout_is_clamped_not_panicking() {
        let std_pick = StdTcpListener::bind("127.0.0.1:0").expect("std bind free");
        let addr: std::net::SocketAddr = std_pick.local_addr().expect("local_addr");
        drop(std_pick);

        // Fits Instant (~u64::MAX/1_000 seconds), overflows any ×1_000
        // millisecond arithmetic that would panic instead of clamping.
        let absurd_secs: u64 = u64::MAX / 1_000 + 1;
        let config = BrokerConfig::new(addr.to_string()).drain_timeout_secs(absurd_secs);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        let (res_tx, res_rx) = std::sync::mpsc::channel();
        let (sig_main, sig_worker) = std::os::unix::net::UnixStream::pair().expect("socketpair");

        let worker_thread = std::thread::Builder::new()
            .name("drain-clamp-test".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("runtime build");
                let r = rt.block_on(run_worker(0, config, None, ready_tx, go_rx, sig_worker));
                let _ = res_tx.send(r);
            })
            .expect("spawn worker");

        match ready_rx.recv_timeout(Duration::from_secs(5)) {
            Ok((0, Ok(()))) => {}
            other => panic!("expected (0, Ok(())), got {other:?}"),
        }
        go_tx.send(true).expect("go true");

        // Hold a connection open so the drain actually polls the timer.
        let mut client = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2))
            .expect("client connect");
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("read timeout");
        client
            .set_write_timeout(Some(Duration::from_secs(2)))
            .expect("write timeout");
        client.write_all(&V3_CONNECT_BYTES).expect("write CONNECT");
        let mut connack = [0u8; 4];
        let n = read_exact_or_zero(&mut client, &mut connack).expect("read CONNACK");
        assert_eq!(n, 4, "short CONNACK read");
        assert_eq!(connack, V3_CONNACK_BYTES);

        // Signal shutdown.
        drop(sig_main);

        // Probe: new connects refused (drain entered, timer polled, no panic).
        let mut refused = false;
        for _ in 0..40 {
            if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_err() {
                refused = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(refused, "worker kept accepting after shutdown signal");

        // Drop the held client — drain completes.
        drop(client);

        // A panic (instead of a clean return) surfaces as a channel
        // disconnect and fails this match.
        match res_rx.recv_timeout(Duration::from_secs(2)) {
            Ok(Ok(())) => {}
            other => panic!("expected Ok(Ok(())), got {other:?}"),
        }

        worker_thread.join().expect("worker join");
    }

    /// AC-1, level-persistence — the shutdown signal fired BEFORE the worker
    /// reached its accept loop must still stop it. The main end is dropped
    /// while the worker is parked on `go_rx`, so the EOF is already pending
    /// when the watcher issues its first read. An edge-triggered design that
    /// only reacts to a signal arriving after loop entry parks forever here
    /// and fails the bounded worker-result wait.
    #[test]
    fn shutdown_signaled_before_accept_loop_still_stops_worker() {
        let std_pick = StdTcpListener::bind("127.0.0.1:0").expect("std bind free");
        let addr: std::net::SocketAddr = std_pick.local_addr().expect("local_addr");
        drop(std_pick);

        let config = BrokerConfig::new(addr.to_string());
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        let (res_tx, res_rx) = std::sync::mpsc::channel();
        let (sig_main, sig_worker) = std::os::unix::net::UnixStream::pair().expect("socketpair");

        let worker_thread = std::thread::Builder::new()
            .name("shutdown-before-go-test".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("runtime build");
                let r = rt.block_on(run_worker(0, config, None, ready_tx, go_rx, sig_worker));
                let _ = res_tx.send(r);
            })
            .expect("spawn worker");

        match ready_rx.recv_timeout(Duration::from_secs(5)) {
            Ok((0, Ok(()))) => {}
            other => panic!("expected (0, Ok(())), got {other:?}"),
        }

        // Signal BEFORE the green light: the worker is still parked on the
        // startup barrier and has not spawned its watcher yet.
        drop(sig_main);
        go_tx.send(true).expect("go true");

        match res_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => {}
            other => panic!("expected Ok(Ok(())), got {other:?}"),
        }

        worker_thread.join().expect("worker join");
        let mut rebound = false;
        for _ in 0..20 {
            if StdTcpListener::bind(addr).is_ok() {
                rebound = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(rebound, "port did not release after worker shutdown");
    }

    /// AC-4 boundary (decision Q3 / pre-mortem ruling 1) — `drain_timeout_secs(0)`
    /// is the documented way for an embedder to close immediately. A held
    /// straggler must be force-closed without waiting out any grace period,
    /// and the worker must still return `Ok(())`. The 3s upper bound is far
    /// below the 5s default, so an implementation ignoring the configured
    /// zero fails it; the client read must be EOF/reset, never a timeout.
    #[test]
    fn zero_drain_timeout_force_closes_straggler_immediately() {
        let std_pick = StdTcpListener::bind("127.0.0.1:0").expect("std bind free");
        let addr: std::net::SocketAddr = std_pick.local_addr().expect("local_addr");
        drop(std_pick);

        let config = BrokerConfig::new(addr.to_string()).drain_timeout_secs(0);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        let (res_tx, res_rx) = std::sync::mpsc::channel();
        let (sig_main, sig_worker) = std::os::unix::net::UnixStream::pair().expect("socketpair");

        let worker_thread = std::thread::Builder::new()
            .name("drain-zero-timeout-test".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("runtime build");
                let r = rt.block_on(run_worker(0, config, None, ready_tx, go_rx, sig_worker));
                let _ = res_tx.send(r);
            })
            .expect("spawn worker");

        match ready_rx.recv_timeout(Duration::from_secs(5)) {
            Ok((0, Ok(()))) => {}
            other => panic!("expected (0, Ok(())), got {other:?}"),
        }
        go_tx.send(true).expect("go true");

        let mut client = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2))
            .expect("client connect");
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("read timeout");
        client
            .set_write_timeout(Some(Duration::from_secs(2)))
            .expect("write timeout");
        client.write_all(&V3_CONNECT_BYTES).expect("write CONNECT");
        let mut connack = [0u8; 4];
        let n = read_exact_or_zero(&mut client, &mut connack).expect("read CONNACK");
        assert_eq!(n, 4, "short CONNACK read");
        assert_eq!(connack, V3_CONNACK_BYTES);

        // Clock started BEFORE the signal so descheduling cannot fake a pass.
        let t0 = std::time::Instant::now();
        drop(sig_main);

        match res_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => {}
            other => panic!("expected Ok(Ok(())), got {other:?}"),
        }
        let dt = t0.elapsed();
        assert!(
            dt <= Duration::from_secs(3),
            "zero drain timeout still waited {dt:?}; a grace period was applied"
        );

        // The straggler must be force-closed: EOF or reset, never a timeout
        // (a timeout means the fd leaked open).
        let mut buf = [0u8; 8];
        match client.read(&mut buf) {
            Ok(0) => {}
            Ok(n) => panic!("client read {n} bytes after force-close"),
            Err(e) => match e.kind() {
                std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionAborted => {}
                other_kind => panic!(
                    "client read returned {other_kind:?} after force-close: \
                     connection not force-closed: fds leaked"
                ),
            },
        }

        worker_thread.join().expect("worker join");
    }
}
