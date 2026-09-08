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
pub async fn run_worker(
    worker_id: usize,
    config: BrokerConfig,
    callback: Option<EventCallback>,
    ready_tx: std::sync::mpsc::Sender<(usize, Result<(), Error>)>,
    go_rx: std::sync::mpsc::Receiver<bool>,
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

    // Accept loop
    let mut connection_counter: u64 = 0;

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
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

                // Spawn connection handler (local task, stays on this core)
                monoio::spawn(async move {
                    match handle_client(stream, event_tx_clone, connection_timeout, idle_timeout)
                        .await
                    {
                        Ok(super::handler::SessionOutcome::Refused) => state_clone.note_refused(),
                        Ok(super::handler::SessionOutcome::Served) => {}
                        Err(e) => {
                            if !e.is_routine_disconnect() {
                                tracing::debug!("Client {} error: {:?}", addr, e);
                            }
                        }
                    }
                    state_clone.release();
                });
            }
            Err(e) => {
                tracing::error!("Worker {}: accept error: {}", worker_id, e);
                monoio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
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

        let worker_thread = std::thread::Builder::new()
            .name("worker-park-test".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("runtime build");
                let r = rt.block_on(run_worker(0, config, None, ready_tx, go_rx));
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

        let worker_thread = std::thread::Builder::new()
            .name("worker-go-drop-test".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("runtime build");
                let r = rt.block_on(run_worker(3, config, None, ready_tx, go_rx));
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

        let thread_a = std::thread::Builder::new()
            .name("mixed-healthy".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("rt a");
                let r = rt.block_on(run_worker(0, cfg_a, None, ready_a, go_a_rx));
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
                let r = rt.block_on(run_worker(1, cfg_b, None, ready_b, go_b_rx));
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

        let thread_a = std::thread::Builder::new()
            .name("panic-test-healthy".into())
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("rt a");
                let r = rt.block_on(run_worker(0, cfg_a, None, ready_a, go_a_rx));
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
                    rt.block_on(run_worker(1, cfg_b, None, ready_b, go_b_rx))
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
}
