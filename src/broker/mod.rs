mod handler;
pub(crate) mod handshake;
pub(crate) mod worker;

use crate::error::Error;
use std::sync::Arc;

/// Default `drain_timeout_secs` for `BrokerConfig::new`. Five seconds gives
/// active clients time to disconnect cleanly while bounding the worker's
/// teardown window.
pub(crate) const DEFAULT_DRAIN_TIMEOUT_SECS: u64 = 5;

/// Configuration for the MQTT broker.
#[derive(Clone)]
pub struct BrokerConfig {
    /// Address to bind to (e.g., "0.0.0.0:1883")
    pub bind_addr: String,
    /// Maximum connections per worker
    pub max_connections_per_worker: usize,
    /// Connection handshake timeout in seconds
    pub connection_timeout_secs: u64,
    /// Idle timeout in seconds
    pub idle_timeout_secs: u64,
    /// Grace period in seconds for draining active connections after shutdown
    /// is signaled; stragglers are force-closed when it expires.
    pub drain_timeout_secs: u64,
    /// Number of worker threads (defaults to CPU count)
    pub num_workers: Option<usize>,
    /// TCP listen backlog
    pub backlog: i32,
}

impl BrokerConfig {
    pub fn new(bind_addr: impl Into<String>) -> Self {
        Self {
            bind_addr: bind_addr.into(),
            max_connections_per_worker: 1000,
            connection_timeout_secs: 10,
            idle_timeout_secs: 300,
            drain_timeout_secs: DEFAULT_DRAIN_TIMEOUT_SECS,
            num_workers: None,
            backlog: 1024,
        }
    }

    pub fn max_connections_per_worker(mut self, max: usize) -> Self {
        self.max_connections_per_worker = max;
        self
    }

    pub fn connection_timeout_secs(mut self, secs: u64) -> Self {
        self.connection_timeout_secs = secs;
        self
    }

    pub fn idle_timeout_secs(mut self, secs: u64) -> Self {
        self.idle_timeout_secs = secs;
        self
    }

    pub fn drain_timeout_secs(mut self, secs: u64) -> Self {
        self.drain_timeout_secs = secs;
        self
    }

    pub fn num_workers(mut self, workers: usize) -> Self {
        self.num_workers = Some(workers);
        self
    }

    pub fn backlog(mut self, backlog: i32) -> Self {
        self.backlog = backlog;
        self
    }
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self::new("0.0.0.0:1883")
    }
}

/// Event emitted by the broker when sensor data is received.
#[derive(Debug, Clone, Copy)]
pub enum Event {
    /// Sensor data received (temperature in 0.01°C, pressure in hPa)
    SensorV1 { temperature: i16, pressure: u16 },
}

/// Callback type for handling events per worker.
pub type EventCallback = Arc<dyn Fn(Event) + Send + Sync>;

/// Cloneable cross-thread shutdown trigger. Signal = take + drop the
/// main-side socketpair ends (EOF is the level-persistent wake).
///
/// One-shot and per broker instance: after the first [`shutdown`](Self::shutdown)
/// every clone is a permanent no-op. A restarted server needs its own
/// handle, and process-global signal handlers must be re-targeted by the
/// embedder.
#[derive(Clone)]
pub struct ShutdownHandle {
    signal_ends: std::sync::Arc<std::sync::Mutex<Option<Vec<std::os::unix::net::UnixStream>>>>,
}

impl ShutdownHandle {
    /// Signal shutdown to every worker owned by the parent broker. Idempotent:
    /// subsequent calls (same handle, any clone, any thread, concurrent or
    /// not) are no-ops.
    pub fn shutdown(&self) {
        let taken = self.take_signal_ends();
        if taken.is_some() {
            tracing::info!("shutdown signaled to workers");
        }
        drop(taken);
    }

    /// Take the main-side signal ends, leaving `None` behind. Returns them
    /// undropped so the caller decides what to log before the EOF lands.
    fn take_signal_ends(&self) -> Option<Vec<std::os::unix::net::UnixStream>> {
        // Guard's scope ends here — drop happens before the ends are
        // dropped, so no concurrent lock holder can re-observe the Option.
        self.signal_ends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

/// Running ingest server. Returned only after startup succeeded; owns the
/// worker lifecycle through shutdown. Send. NOT Clone.
pub struct BrokerHandle {
    trigger: ShutdownHandle,
    handles: Vec<std::thread::JoinHandle<Result<(), Error>>>,
}

impl BrokerHandle {
    /// Clone the shutdown trigger so another thread (e.g. a signal-handler
    /// caller) can request shutdown independently of this handle.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.trigger.clone()
    }

    /// Convenience: trigger shutdown via this handle's own trigger. Idempotent.
    pub fn shutdown(&self) {
        self.trigger.shutdown();
    }

    /// True while every worker thread is still running. Goes false as soon
    /// as any worker exits — after a panic, a shutdown, or a drain — so an
    /// embedder holding this handle can notice a dead ingest server without
    /// blocking in [`join`](Self::join). It reports worker death, not health.
    pub fn is_running(&self) -> bool {
        !self
            .handles
            .iter()
            .any(std::thread::JoinHandle::is_finished)
    }

    /// Wait for every worker thread to exit, aggregating their terminal
    /// results. Does NOT itself signal shutdown — call [`shutdown`](Self::shutdown)
    /// (or drop the handle) first if the embedder wants the workers to stop.
    /// A worker that exits unexpectedly signals shutdown to its siblings from
    /// its own thread, so this call returns rather than blocking on a live
    /// sibling's accept loop.
    ///
    /// The first worker error wins; a thread panic surfaces as
    /// [`Error::Worker("worker thread panicked".into())`].
    ///
    /// # Panics
    /// Panics if invoked from inside a worker thread or from within an event
    /// callback on a worker thread — the call would self-join.
    pub fn join(mut self) -> Result<(), Error> {
        let mut worker_err: Option<Error> = None;
        for handle in self.handles.drain(..) {
            match handle.join() {
                Err(_) => {
                    tracing::error!("Worker thread panicked");
                    worker_err =
                        worker_err.or(Some(Error::Worker("worker thread panicked".into())));
                }
                Ok(Err(e)) => worker_err = worker_err.or(Some(e)),
                Ok(Ok(())) => {}
            }
        }
        worker_err.map_or(Ok(()), Err)
    }
}

impl Drop for BrokerHandle {
    /// Signal shutdown and join every remaining worker. Results are logged
    /// at warn and discarded — the handle has already been moved out of by
    /// the time an embedder could observe them. Blocks up to the drain
    /// deadline plus scheduling slack.
    fn drop(&mut self) {
        self.trigger.shutdown();
        for handle in self.handles.drain(..) {
            match handle.join() {
                Err(_) => tracing::warn!("BrokerHandle::drop: worker thread panicked"),
                Ok(Err(e)) => tracing::warn!("BrokerHandle::drop: worker returned error: {e}"),
                Ok(Ok(())) => {}
            }
        }
    }
}

/// Fail-fast supervision for one worker thread. Dropped on normal return
/// and on unwind alike, so a panicking worker signals shutdown to its
/// siblings instead of leaving them parked in `cancelable_accept`.
struct WorkerExitGuard {
    worker_id: usize,
    post_go: worker::PostGoFlag,
    trigger: ShutdownHandle,
}

impl Drop for WorkerExitGuard {
    /// Runs on normal return and on unwind alike. A worker that never
    /// reached the go barrier leaves the signal untouched, so both
    /// startup-error paths keep their exact behaviour.
    fn drop(&mut self) {
        if !self.post_go.is_armed() {
            return;
        }
        let taken = self.trigger.take_signal_ends();
        if taken.is_some() {
            tracing::error!(
                "worker {} exited before shutdown was signaled; shutting down remaining workers",
                self.worker_id
            );
        }
        drop(taken);
    }
}

/// High-performance MQTT broker using Monoio io_uring runtime.
pub struct MqttBroker;

impl MqttBroker {
    /// Start the MQTT ingest server. Returns once every worker has passed
    /// its startup barrier (runtime built, listener bound, state spawned) or
    /// an `Err` if any worker failed startup — the first error wins and the
    /// preserved join-and-return path is followed before the error escapes.
    pub fn start(config: BrokerConfig) -> Result<BrokerHandle, Error> {
        Self::start_with_callback(config, None)
    }

    /// Start the MQTT ingest server with an optional event callback.
    ///
    /// # Callback contract
    /// The callback runs on worker threads. It must return promptly: blocking
    /// stalls that worker's event processing AND its shutdown. A panic inside
    /// the callback unwinds the worker thread (monoio's task harness does not
    /// catch it) and surfaces from [`BrokerHandle::join`] as
    /// [`Error::Worker(_)`]. Never call [`BrokerHandle::join`] or drop a
    /// [`BrokerHandle`] from inside an event callback. That panic also signals
    /// shutdown to the remaining workers, so the whole ingest server stops
    /// rather than running with a dead worker. This relies on the panic
    /// unwinding: an application built with the `panic = "abort"` profile
    /// setting terminates at the panic site instead, with no shutdown signal
    /// and no drain.
    pub fn start_with_callback(
        config: BrokerConfig,
        callback: Option<EventCallback>,
    ) -> Result<BrokerHandle, Error> {
        let num_workers = config.num_workers.unwrap_or_else(|| {
            std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
        });

        if num_workers == 0 {
            return Err(Error::Worker("num_workers must be at least 1".into()));
        }

        tracing::info!(
            "Starting MQTT broker on {} with {} workers",
            config.bind_addr,
            num_workers
        );

        // Create all per-worker shutdown socketpairs before any thread exists,
        // so a `?` here is clean (no in-flight workers to join on failure).
        // The worker-end moves into the worker thread; the main-end stays on
        // this thread and is owned by the returned `BrokerHandle` until it
        // signals shutdown (the handle's `Drop` calls `shutdown` on drop).
        let mut worker_ends = Vec::with_capacity(num_workers);
        let mut signal_ends = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let (main_end, worker_end) =
                std::os::unix::net::UnixStream::pair().map_err(Error::Io)?;
            signal_ends.push(main_end);
            worker_ends.push(worker_end);
        }
        let trigger = ShutdownHandle {
            signal_ends: std::sync::Arc::new(std::sync::Mutex::new(Some(signal_ends))),
        };

        // Startup barrier: every worker's fallible setup (runtime build, bind,
        // state/channel/processor spawn) completes before we fan out go/abort.
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let mut go_txs: Vec<std::sync::mpsc::Sender<bool>> = Vec::with_capacity(num_workers);
        let mut handles: Vec<std::thread::JoinHandle<Result<(), Error>>> =
            Vec::with_capacity(num_workers);

        for worker_id in 0..num_workers {
            let config = config.clone();
            let callback = callback.clone();
            let trigger = trigger.clone();
            let (go_tx, go_rx) = std::sync::mpsc::channel();
            go_txs.push(go_tx);
            let ready_tx = ready_tx.clone();
            let worker_end = worker_ends.remove(0);

            match std::thread::Builder::new()
                .name(format!("mqtt-worker-{worker_id}"))
                .spawn(move || -> Result<(), Error> {
                    let post_go = worker::PostGoFlag::unarmed();
                    // Declared before `runtime` so it drops after it: the guard is the
                    // last thing to run on this thread, on return and on unwind alike.
                    let _exit_guard = WorkerExitGuard {
                        worker_id,
                        post_go: post_go.clone(),
                        trigger,
                    };
                    let mut runtime = match monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                        .enable_timer()
                        .build()
                    {
                        Ok(rt) => rt,
                        Err(e) => {
                            let _ = ready_tx.send((
                                worker_id,
                                Err(Error::Worker(format!(
                                    "worker {worker_id}: runtime build failed: {e}"
                                ))),
                            ));
                            return Ok(());
                        }
                    };
                    runtime.block_on(worker::run_worker(
                        worker_id, config, callback, ready_tx, go_rx, worker_end, post_go,
                    ))
                }) {
                Ok(h) => handles.push(h),
                Err(e) => {
                    for tx in &go_txs {
                        let _ = tx.send(false);
                    }
                    for h in handles {
                        if let Err(panic) = h.join() {
                            tracing::error!("Worker thread panicked during abort: {panic:?}");
                        }
                    }
                    return Err(Error::Worker(format!(
                        "failed to spawn worker thread {worker_id}: {e}"
                    )));
                }
            }
        }
        drop(ready_tx);

        if let Err(e) = supervise_startup(&ready_rx, &go_txs, num_workers) {
            for h in handles {
                if let Err(panic) = h.join() {
                    tracing::error!("Worker thread panicked during abort: {panic:?}");
                }
            }
            return Err(e);
        }

        Ok(BrokerHandle { trigger, handles })
    }

    /// Run the MQTT broker with the given configuration. Equivalent to
    /// `start(config)?.join()` — blocks until every worker exits.
    ///
    /// # Arguments
    /// * `config` - Broker configuration
    ///
    /// # Returns
    /// Returns when all workers have exited (typically never in normal operation)
    pub fn run(config: BrokerConfig) -> Result<(), Error> {
        Self::run_with_callback(config, None)
    }

    /// Run the MQTT broker with an event callback. Equivalent to
    /// `start_with_callback(config, callback)?.join()`.
    ///
    /// # Arguments
    /// * `config` - Broker configuration
    /// * `callback` - Optional callback invoked for each event (called from worker thread)
    pub fn run_with_callback(
        config: BrokerConfig,
        callback: Option<EventCallback>,
    ) -> Result<(), Error> {
        Self::start_with_callback(config, callback)?.join()
    }
}

/// Collect per-worker startup reports and fan out a single go/abort decision to
/// every go-sender. Returns `Ok(())` once every worker has reported success
/// (decision = `true`); returns the first `Err` reported, or a generic
/// premature-disconnect error if the ready channel closes with workers missing
/// (decision = `false` in both error cases). Send errors on the go side are
/// ignored — the worker may have already exited.
fn supervise_startup(
    ready_rx: &std::sync::mpsc::Receiver<(usize, Result<(), Error>)>,
    go_txs: &[std::sync::mpsc::Sender<bool>],
    num_workers: usize,
) -> Result<(), Error> {
    let mut count = 0;
    loop {
        match ready_rx.recv() {
            Ok((_, Ok(()))) => {
                count += 1;
                if count == num_workers {
                    for tx in go_txs {
                        let _ = tx.send(true);
                    }
                    return Ok(());
                }
            }
            Ok((id, Err(e))) => {
                tracing::warn!("worker {id} failed startup: {e}");
                for tx in go_txs {
                    let _ = tx.send(false);
                }
                return Err(e);
            }
            Err(std::sync::mpsc::RecvError) => {
                for tx in go_txs {
                    let _ = tx.send(false);
                }
                return Err(Error::Worker(
                    "worker exited before reporting startup result".into(),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::time::{Duration, Instant};

    /// Bind a std listener to an ephemeral port and return the port. Single
    /// attempt only — the tiny race against another process grabbing the port
    /// is accepted.
    fn find_free_port() -> u16 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
        l.local_addr().expect("local_addr").port()
    }

    /// v3 CONNECT: ka=60, id "test", flags 0x00 (copy of the handler test
    /// module's fixture — those constants are test-module-local).
    const V3_CONNECT_TEST: [u8; 18] = [
        0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x00, 0x00, 0x3C, 0x00, 0x04, b't',
        b'e', b's', b't',
    ];
    /// v3 CONNECT: empty id, clean_session=false → decoder-level
    /// `InvalidClientId` refusal.
    const V3_CONNECT_EMPTY_NO_CLEAN: [u8; 14] = [
        0x10, 0x0C, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x00, 0x00, 0x3C, 0x00, 0x00,
    ];

    /// Connect to a broker that may still be starting up: ≤ 20 × 50 ms.
    fn connect_with_retry(addr: std::net::SocketAddr) -> std::net::TcpStream {
        for _ in 0..20 {
            if let Ok(s) = std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50)) {
                return s;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("failed to connect within retry window");
    }

    #[test]
    fn test_broker_config_new() {
        let config = BrokerConfig::new("127.0.0.1:1883");

        assert_eq!(config.bind_addr, "127.0.0.1:1883");
        assert_eq!(config.max_connections_per_worker, 1000);
        assert_eq!(config.connection_timeout_secs, 10);
        assert_eq!(config.idle_timeout_secs, 300);
        assert_eq!(config.drain_timeout_secs, 5);
        assert!(config.num_workers.is_none());
        assert_eq!(config.backlog, 1024);
    }

    #[test]
    fn test_broker_config_default() {
        let config = BrokerConfig::default();

        assert_eq!(config.bind_addr, "0.0.0.0:1883");
    }

    #[test]
    fn test_broker_config_builder_pattern() {
        let config = BrokerConfig::new("0.0.0.0:1883")
            .max_connections_per_worker(500)
            .connection_timeout_secs(5)
            .idle_timeout_secs(120)
            .drain_timeout_secs(20)
            .num_workers(4)
            .backlog(512);

        assert_eq!(config.max_connections_per_worker, 500);
        assert_eq!(config.connection_timeout_secs, 5);
        assert_eq!(config.idle_timeout_secs, 120);
        assert_eq!(config.drain_timeout_secs, 20);
        assert_eq!(config.num_workers, Some(4));
        assert_eq!(config.backlog, 512);
    }

    #[test]
    fn test_broker_config_clone() {
        let config1 = BrokerConfig::new("localhost:1883").num_workers(2);
        let config2 = config1.clone();

        assert_eq!(config1.bind_addr, config2.bind_addr);
        assert_eq!(config1.num_workers, config2.num_workers);
    }

    #[test]
    fn test_event_sensor_v1() {
        let event = Event::SensorV1 {
            temperature: 2500, // 25.00°C
            pressure: 1013,    // 1013 hPa
        };

        match event {
            Event::SensorV1 {
                temperature,
                pressure,
            } => {
                assert_eq!(temperature, 2500);
                assert_eq!(pressure, 1013);
            }
        }
    }

    #[test]
    fn test_event_clone_copy() {
        let event1 = Event::SensorV1 {
            temperature: 100,
            pressure: 500,
        };

        // Test Copy - event1 remains valid after assignment
        let event2 = event1;
        let event3 = event1;
        let event4 = event1;

        match (event2, event3, event4) {
            (
                Event::SensorV1 {
                    temperature: t2, ..
                },
                Event::SensorV1 {
                    temperature: t3, ..
                },
                Event::SensorV1 {
                    temperature: t4, ..
                },
            ) => {
                assert_eq!(t2, 100);
                assert_eq!(t3, 100);
                assert_eq!(t4, 100);
            }
        }
    }

    #[test]
    fn test_event_debug() {
        let event = Event::SensorV1 {
            temperature: -500,
            pressure: 900,
        };

        let debug_str = format!("{event:?}");
        assert!(debug_str.contains("SensorV1"));
        assert!(debug_str.contains("-500"));
        assert!(debug_str.contains("900"));
    }

    /// AC-1 — when a worker's listener bind fails, `MqttBroker::run` MUST
    /// return `Err`. Holds a std listener on a free port (no SO_REUSEPORT,
    /// so the worker's reuse_port-enabled bind collides with EADDRINUSE).
    #[test]
    fn run_returns_err_when_bind_fails() {
        let port = find_free_port();
        let addr = format!("127.0.0.1:{port}");
        let occupier = std::net::TcpListener::bind(&addr).expect("std bind");
        let occupier_port = occupier.local_addr().expect("local_addr").port();
        assert_eq!(occupier_port, port);

        let config = BrokerConfig::new(addr).num_workers(2);
        let (tx, rx) = std::sync::mpsc::channel();
        let _h = std::thread::spawn(move || {
            let _ = tx.send(MqttBroker::run(config));
        });
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Err(Error::Io(ref e))) if e.kind() == std::io::ErrorKind::AddrInUse => {}
            other => panic!("expected Err(Io(AddrInUse)), got {other:?}"),
        }
        drop(occupier);
    }

    /// AC-17 public contract — `.max_connections_per_worker(usize::MAX)`
    /// panics in `BufferPool::new` during worker setup, before any report.
    /// `run` must surface that as `Err(Error::Worker(_))`.
    #[test]
    fn run_returns_err_when_worker_setup_panics() {
        let port = find_free_port();
        let addr = format!("127.0.0.1:{port}");
        let config = BrokerConfig::new(addr)
            .num_workers(1)
            .max_connections_per_worker(usize::MAX);
        let (tx, rx) = std::sync::mpsc::channel();
        let _h = std::thread::spawn(move || {
            let _ = tx.send(MqttBroker::run(config));
        });
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Err(Error::Worker(_))) => {}
            other => panic!("expected Err(Worker), got {other:?}"),
        }
    }

    /// AC-20 — zero workers is rejected up front, before any thread spawn.
    #[test]
    fn zero_workers_is_config_error() {
        let config = BrokerConfig::new("127.0.0.1:0").num_workers(0);
        match MqttBroker::run_with_callback(config, None) {
            Err(Error::Worker(msg)) => {
                assert!(
                    msg.contains("worker") || msg.contains("at least 1"),
                    "message should mention workers, got: {msg}"
                );
            }
            other => panic!("expected Err(Worker), got {other:?}"),
        }
    }

    /// AC-3 + AC-18 — after `supervise_startup` sends `true`, a real client
    /// completes CONNECT → CONNACK and the broker's callback receives the
    /// decoded PUBLISH event through the new event channel. A drain-and-
    /// discard processor fails this. The detached broker thread runs until
    /// process exit (graceful shutdown is F1.4's responsibility).
    #[test]
    fn run_delivers_publish_event_to_callback_after_go() {
        let port = find_free_port();
        let addr_str = format!("127.0.0.1:{port}");
        let addr: std::net::SocketAddr = addr_str.parse().expect("parse addr");
        let (cb_tx, cb_rx) = std::sync::mpsc::channel::<Event>();

        let config = BrokerConfig::new(addr_str).num_workers(1);
        let cb: EventCallback = std::sync::Arc::new(move |event: Event| {
            let _ = cb_tx.send(event);
        });
        let broker_handle = std::thread::Builder::new()
            .name("broker-publish-callback".into())
            .spawn(move || {
                let _ = MqttBroker::run_with_callback(config, Some(cb));
            })
            .expect("spawn broker");

        let v3_connect: [u8; 18] = [
            0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x00, 0x00, 0x3C, 0x00, 0x04,
            b't', b'e', b's', b't',
        ];
        let publish: [u8; 9] = [0x30, 0x07, 0x00, 0x01, b't', 0x09, 0xC4, 0x03, 0xF5];

        let mut stream = None;
        for _ in 0..20 {
            match std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50)) {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(_) => std::thread::sleep(Duration::from_millis(50)),
            }
        }
        let mut stream = stream.expect("connect within retry window");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set_read_timeout");
        stream.write_all(&v3_connect).expect("write CONNECT");
        let mut connack = [0u8; 4];
        stream.read_exact(&mut connack).expect("read CONNACK");
        assert_eq!(connack, [0x20, 0x02, 0x00, 0x00]);
        stream.write_all(&publish).expect("write PUBLISH");

        match cb_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Event::SensorV1 {
                temperature,
                pressure,
            }) => {
                assert_eq!(temperature, 2500);
                assert_eq!(pressure, 1013);
            }
            other => panic!("expected Event::SensorV1 {{ 2500, 1013 }}, got {other:?}"),
        }

        // Detached broker thread keeps running until process exit — F1.4 owns
        // graceful shutdown. We drop the JoinHandle so it isn't joined here.
        drop(broker_handle);
    }

    /// AC-19 + AC-24 wiring — the accept loop's `SessionOutcome::Refused` arm
    /// must still release the connection slot. With `max_connections_per_worker(1)`
    /// a refused CONNECT would permanently exhaust the worker if `release()`
    /// were skipped on the refusal path: the next client would be dropped
    /// without a CONNACK.
    #[test]
    fn refused_connection_releases_its_worker_slot() {
        let port = find_free_port();
        let addr_str = format!("127.0.0.1:{port}");
        let addr: std::net::SocketAddr = addr_str.parse().expect("parse addr");

        let config = BrokerConfig::new(addr_str)
            .num_workers(1)
            .max_connections_per_worker(1);
        let broker_handle = std::thread::Builder::new()
            .name("broker-refusal-slot".into())
            .spawn(move || {
                let _ = MqttBroker::run(config);
            })
            .expect("spawn broker");

        // Client 1 is refused; reading to EOF proves the handler task finished,
        // which is what releases the slot.
        let mut refused = connect_with_retry(addr);
        refused
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set_read_timeout");
        refused
            .write_all(&V3_CONNECT_EMPTY_NO_CLEAN)
            .expect("write empty-id CONNECT");
        let mut connack = [0u8; 4];
        refused.read_exact(&mut connack).expect("read refusal");
        assert_eq!(connack, [0x20, 0x02, 0x00, 0x02]);
        let mut tail = [0u8; 1];
        assert_eq!(refused.read(&mut tail).expect("read to eof"), 0);
        drop(refused);

        // Client 2 must be served on the freed slot.
        let mut served = connect_with_retry(addr);
        served
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set_read_timeout");
        served
            .write_all(&V3_CONNECT_TEST)
            .expect("write valid CONNECT");
        let mut connack = [0u8; 4];
        served
            .read_exact(&mut connack)
            .expect("read CONNACK on the released slot");
        assert_eq!(connack, [0x20, 0x02, 0x00, 0x00]);

        // Detached broker thread — graceful shutdown is F1.4's scope.
        drop(broker_handle);
    }

    // AC-21 — supervise_startup is the production fan-out; these threadless
    // tests prove its decision reaches EVERY go-sender in every branch.

    #[test]
    fn supervise_all_ok_sends_go_to_every_worker() {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go1_tx, go1_rx) = std::sync::mpsc::channel();
        let (go2_tx, go2_rx) = std::sync::mpsc::channel();
        let go_txs = vec![go1_tx, go2_tx];

        ready_tx.send((0, Ok(()))).expect("send 0");
        ready_tx.send((1, Ok(()))).expect("send 1");
        drop(ready_tx);

        assert!(matches!(supervise_startup(&ready_rx, &go_txs, 2), Ok(())));
        assert_eq!(go1_rx.try_recv(), Ok(true));
        assert_eq!(go2_rx.try_recv(), Ok(true));
    }

    #[test]
    fn supervise_first_error_aborts_every_worker() {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go1_tx, go1_rx) = std::sync::mpsc::channel();
        let (go2_tx, go2_rx) = std::sync::mpsc::channel();
        let go_txs = vec![go1_tx, go2_tx];

        ready_tx.send((0, Ok(()))).expect("send 0 ok");
        ready_tx
            .send((1, Err(Error::Worker("bind failed".into()))))
            .expect("send 1 err");
        drop(ready_tx);

        match supervise_startup(&ready_rx, &go_txs, 2) {
            Err(Error::Worker(msg)) => assert_eq!(msg, "bind failed"),
            other => panic!("expected Err(Worker(\"bind failed\")), got {other:?}"),
        }
        assert_eq!(go1_rx.try_recv(), Ok(false));
        assert_eq!(go2_rx.try_recv(), Ok(false));
    }

    /// AC-21 robustness — a worker that already exited leaves a dead go
    /// receiver behind. The fan-out must ignore that send error and still
    /// deliver the decision to every remaining sender, in both the success and
    /// the failure branch.
    #[test]
    fn supervise_tolerates_a_dead_go_receiver() {
        // Success branch.
        {
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let (dead_tx, dead_rx) = std::sync::mpsc::channel();
            let (live_tx, live_rx) = std::sync::mpsc::channel();
            drop(dead_rx);
            let go_txs = vec![dead_tx, live_tx];

            ready_tx.send((0, Ok(()))).expect("send 0");
            ready_tx.send((1, Ok(()))).expect("send 1");
            drop(ready_tx);

            assert!(matches!(supervise_startup(&ready_rx, &go_txs, 2), Ok(())));
            assert_eq!(live_rx.try_recv(), Ok(true));
        }

        // Failure branch.
        {
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let (dead_tx, dead_rx) = std::sync::mpsc::channel();
            let (live_tx, live_rx) = std::sync::mpsc::channel();
            drop(dead_rx);
            let go_txs = vec![dead_tx, live_tx];

            ready_tx
                .send((0, Err(Error::Worker("bind failed".into()))))
                .expect("send 0 err");
            drop(ready_tx);

            assert!(supervise_startup(&ready_rx, &go_txs, 2).is_err());
            assert_eq!(live_rx.try_recv(), Ok(false));
        }
    }

    #[test]
    fn supervise_premature_disconnect_aborts_every_worker() {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go1_tx, go1_rx) = std::sync::mpsc::channel();
        let (go2_tx, go2_rx) = std::sync::mpsc::channel();
        let (go3_tx, go3_rx) = std::sync::mpsc::channel();
        let go_txs = vec![go1_tx, go2_tx, go3_tx];

        // Only one Ok report for num_workers=3, then drop — premature
        // disconnect.
        ready_tx.send((0, Ok(()))).expect("send 0 ok");
        drop(ready_tx);

        match supervise_startup(&ready_rx, &go_txs, 3) {
            Err(Error::Worker(_)) => {}
            other => panic!("expected Err(Worker), got {other:?}"),
        }
        assert_eq!(go1_rx.try_recv(), Ok(false));
        assert_eq!(go2_rx.try_recv(), Ok(false));
        assert_eq!(go3_rx.try_recv(), Ok(false));
    }

    // ─────────────────── T3 public-API lifecycle tests ───────────────────
    //
    // Every test below runs its WHOLE handle-owning body — `start`, client
    // sockets, assertions, `join`/`drop` — inside ONE spawned harness thread
    // that sends its outcome over a channel. The outer test only does
    // `recv_timeout` (10–15s) and asserts on the received outcome. A panic
    // inside the harness thread disconnects the channel and fails the outer
    // `recv_timeout` bounded, so an assert-unwind reaching a blocking `Drop`
    // can never hang the suite. The outer thread never owns a `BrokerHandle`.
    // Every client socket read sets a read timeout first.

    /// AC-5 — once `MqttBroker::start` returns `Ok`, a client must complete
    /// CONNECT → CONNACK on the first connect attempt (no retry loop).
    #[test]
    fn client_connects_without_retry_after_start_returns() {
        let (tx, rx) = std::sync::mpsc::channel::<Result<(), Error>>();
        let _h = std::thread::Builder::new()
            .name("t3-ac5-start-returns".into())
            .spawn(move || {
                let outcome = (|| -> Result<(), Error> {
                    let port = find_free_port();
                    let addr_str = format!("127.0.0.1:{port}");
                    let addr: std::net::SocketAddr = addr_str.parse().expect("parse addr");
                    let config = BrokerConfig::new(addr_str).num_workers(1);

                    let handle = MqttBroker::start(config)?;
                    // SINGLE connect attempt — no retry loop.
                    let mut client =
                        std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2))
                            .map_err(Error::Io)?;
                    client
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .map_err(Error::Io)?;
                    client.write_all(&V3_CONNECT_TEST).map_err(Error::Io)?;
                    let mut connack = [0u8; 4];
                    client.read_exact(&mut connack).map_err(Error::Io)?;
                    assert_eq!(connack, [0x20, 0x02, 0x00, 0x00]);
                    drop(client);

                    handle.shutdown();
                    handle.join()
                })();
                let _ = tx.send(outcome);
            })
            .expect("spawn harness");
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok(())) => {}
            other => panic!("expected Ok(()) from harness, got {other:?}"),
        }
    }

    /// AC-2 — `BrokerHandle::join` returns no earlier than drain completes
    /// and no later than `drain_timeout_secs` plus scheduling slack. A held
    /// connection pins the drain to the full 1s deadline; the lower bound
    /// defeats a detaching `join` (which would return instantly).
    #[test]
    fn shutdown_then_join_returns_within_drain_deadline() {
        let (tx, rx) =
            std::sync::mpsc::channel::<(Result<(), Error>, Duration, std::io::Result<usize>)>();
        let _h = std::thread::Builder::new()
            .name("t3-ac2-shutdown-join-deadline".into())
            .spawn(move || {
                let outcome = (|| -> Result<(), Error> {
                    let port = find_free_port();
                    let addr_str = format!("127.0.0.1:{port}");
                    let addr: std::net::SocketAddr = addr_str.parse().expect("parse addr");
                    let config = BrokerConfig::new(addr_str)
                        .drain_timeout_secs(1)
                        .num_workers(1);

                    let handle = MqttBroker::start(config)?;

                    let mut client =
                        std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2))
                            .map_err(Error::Io)?;
                    client
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .map_err(Error::Io)?;
                    client.write_all(&V3_CONNECT_TEST).map_err(Error::Io)?;
                    let mut connack = [0u8; 4];
                    client.read_exact(&mut connack).map_err(Error::Io)?;
                    assert_eq!(connack, [0x20, 0x02, 0x00, 0x00]);

                    // Clock started BEFORE the shutdown signal so descheduling
                    // cannot fake a lower-bound failure.
                    let t0 = Instant::now();
                    handle.shutdown();
                    let join_result = handle.join();
                    let dt = t0.elapsed();

                    // Now read with a 2s timeout — a still-open connection
                    // proves the worker force-closed it after the drain
                    // deadline expired.
                    client
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .map_err(Error::Io)?;
                    let mut tail = [0u8; 1];
                    let read_outcome = client.read(&mut tail);

                    let _ = tx.send((join_result, dt, read_outcome));
                    Ok(())
                })();
                if outcome.is_err() {
                    // Send a sentinel so the outer recv_timeout doesn't block.
                    let _ = tx.send((
                        Err(Error::Worker("harness setup failed".into())),
                        Duration::ZERO,
                        Err(std::io::Error::other("harness setup failed")),
                    ));
                }
            })
            .expect("spawn harness");
        let Ok((join_result, dt, read_outcome)) = rx.recv_timeout(Duration::from_secs(15)) else {
            panic!("harness did not report within 15s")
        };
        match join_result {
            Ok(()) => {}
            Err(e) => panic!("join returned Err: {e}"),
        }
        assert!(
            dt >= Duration::from_millis(900),
            "join returned too fast ({dt:?}); a detaching join or instant drop would fail this bound"
        );
        assert!(
            dt <= Duration::from_secs(4),
            "join returned too slow ({dt:?}); upper bound is 4s with 3s slack"
        );
        match read_outcome {
            Ok(0) => {} // EOF — force-closed
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ) => {}
            other => {
                panic!("held client did not observe EOF/reset within bounded window: {other:?}")
            }
        }
    }

    /// AC-6 — bind collision on any worker yields `Err(Error::Io(AddrInUse))`.
    /// Occupier pattern (mirrors `run_returns_err_when_bind_fails`); the
    /// bounded harness-thread `recv_timeout` is the join-on-error-path guard.
    #[test]
    fn start_returns_err_when_bind_fails() {
        let port = find_free_port();
        let addr = format!("127.0.0.1:{port}");
        let occupier = std::net::TcpListener::bind(&addr).expect("std bind");

        let (tx, rx) = std::sync::mpsc::channel::<Result<(), Error>>();
        let config = BrokerConfig::new(addr).num_workers(2);
        let _h = std::thread::Builder::new()
            .name("t3-ac6-start-bind-fails".into())
            .spawn(move || {
                // An unexpected `Ok` handle is dropped HERE, inside the
                // harness thread — the outcome crossing the channel is
                // handle-free, so a blocking `Drop` can never run on the
                // test thread after `recv_timeout` returned.
                let outcome = match MqttBroker::start(config) {
                    Ok(handle) => {
                        drop(handle);
                        Ok(())
                    }
                    Err(e) => Err(e),
                };
                let _ = tx.send(outcome);
            })
            .expect("spawn harness");
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Err(Error::Io(ref e))) if e.kind() == std::io::ErrorKind::AddrInUse => {}
            Ok(Err(other)) => panic!("expected Err(Io(AddrInUse)), got Err({other:?})"),
            Ok(Ok(())) => {
                panic!("expected Err(Io(AddrInUse)), got Ok(_) — handle dropped inside harness")
            }
            _ => panic!("harness did not report within 10s"),
        }
        drop(occupier);
    }

    /// AC-7 — repeated `shutdown` on the same handle, on clones, and on
    /// concurrent threads is a no-op (no panic, no error, no second effect).
    #[test]
    fn repeated_and_concurrent_shutdown_calls_are_noops() {
        let (tx, rx) = std::sync::mpsc::channel::<Result<(), Error>>();
        let _h = std::thread::Builder::new()
            .name("t3-ac7-repeated-shutdown".into())
            .spawn(move || {
                let outcome = (|| -> Result<(), Error> {
                    let port = find_free_port();
                    let addr_str = format!("127.0.0.1:{port}");
                    let config = BrokerConfig::new(addr_str).num_workers(1);

                    let handle = MqttBroker::start(config)?;

                    let trig1 = handle.shutdown_handle();
                    let trig2 = handle.shutdown_handle();

                    // Two std threads call `.shutdown()` simultaneously on
                    // different clones.
                    let t1 = std::thread::spawn(move || trig1.shutdown());
                    let t2 = std::thread::spawn(move || trig2.shutdown());
                    t1.join().expect("thread 1 join");
                    t2.join().expect("thread 2 join");

                    // A third call on the handle's own trigger after the first
                    // two fired is also a no-op.
                    handle.shutdown();

                    handle.join()
                })();
                let _ = tx.send(outcome);
            })
            .expect("spawn harness");
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok(())) => {}
            other => panic!("expected Ok(()) from harness, got {other:?}"),
        }
    }

    /// AC-8 — dropping a `BrokerHandle` signals shutdown, joins every worker
    /// before returning, and frees the port for an immediate restart. The
    /// lower bound defeats a fast-drop-detach; the upper bound catches a
    /// hang. The held client's read after drop proves the force-close
    /// completed (EOF/reset, not a timeout).
    #[test]
    fn dropping_handle_shuts_server_down_and_allows_restart() {
        let (tx, rx) =
            std::sync::mpsc::channel::<(Duration, std::io::Result<usize>, Result<(), Error>)>();
        let _h = std::thread::Builder::new()
            .name("t3-ac8-drop-restart".into())
            .spawn(move || {
                let outcome = (|| -> Result<(), Error> {
                    let port = find_free_port();
                    let addr_str = format!("127.0.0.1:{port}");
                    let addr: std::net::SocketAddr = addr_str.parse().expect("parse addr");
                    let config = BrokerConfig::new(addr_str)
                        .drain_timeout_secs(1)
                        .num_workers(1);

                    let handle = MqttBroker::start(config.clone())?;

                    let mut client =
                        std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2))
                            .map_err(Error::Io)?;
                    client
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .map_err(Error::Io)?;
                    client.write_all(&V3_CONNECT_TEST).map_err(Error::Io)?;
                    let mut connack = [0u8; 4];
                    client.read_exact(&mut connack).map_err(Error::Io)?;
                    assert_eq!(connack, [0x20, 0x02, 0x00, 0x00]);

                    let t0 = Instant::now();
                    drop(handle);
                    let dt = t0.elapsed();

                    client
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .map_err(Error::Io)?;
                    let mut tail = [0u8; 1];
                    let read_outcome = client.read(&mut tail);
                    drop(client);

                    // Restart on the SAME port (production listeners use
                    // reuse_addr + reuse_port — a plain std rebind would
                    // false-fail on post-close TCP states).
                    let restart_handle = MqttBroker::start(config)?;
                    let mut restart_client =
                        std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2))
                            .map_err(Error::Io)?;
                    restart_client
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .map_err(Error::Io)?;
                    restart_client
                        .write_all(&V3_CONNECT_TEST)
                        .map_err(Error::Io)?;
                    let mut restart_connack = [0u8; 4];
                    restart_client
                        .read_exact(&mut restart_connack)
                        .map_err(Error::Io)?;
                    assert_eq!(restart_connack, [0x20, 0x02, 0x00, 0x00]);
                    drop(restart_client);
                    restart_handle.shutdown();
                    let restart_result = restart_handle.join();

                    let _ = tx.send((dt, read_outcome, restart_result));
                    Ok(())
                })();
                if outcome.is_err() {
                    let _ = tx.send((
                        Duration::ZERO,
                        Err(std::io::Error::other("harness setup failed")),
                        Err(Error::Worker("harness setup failed".into())),
                    ));
                }
            })
            .expect("spawn harness");
        let Ok((dt, read_outcome, restart_result)) = rx.recv_timeout(Duration::from_secs(15))
        else {
            panic!("harness did not report within 15s")
        };
        assert!(
            dt >= Duration::from_millis(900),
            "Drop returned too fast ({dt:?}); a no-op Drop would fail this bound"
        );
        assert!(
            dt <= Duration::from_secs(4),
            "Drop returned too slow ({dt:?}); upper bound is 4s with 3s slack"
        );
        match read_outcome {
            Ok(0) => {} // EOF — force-closed
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ) => {}
            other => {
                panic!("held client did not observe EOF/reset within bounded window: {other:?}")
            }
        }
        match restart_result {
            Ok(()) => {}
            Err(e) => panic!("restarted broker's join returned Err: {e}"),
        }
    }

    #[test]
    fn armed_exit_guard_closes_the_signal_ends_and_logs_once() {
        let logs = crate::broker::handler::tests::capture_logs();
        let (main_end, mut peer) = std::os::unix::net::UnixStream::pair().expect("socketpair");
        peer.set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set read timeout");
        let trigger = ShutdownHandle {
            signal_ends: std::sync::Arc::new(std::sync::Mutex::new(Some(vec![main_end]))),
        };
        let post_go = worker::PostGoFlag::unarmed();
        post_go.arm();
        let guard = WorkerExitGuard {
            worker_id: 7,
            post_go,
            trigger: trigger.clone(),
        };
        drop(guard);

        let mut byte = [0u8; 1];
        assert_eq!(peer.read(&mut byte).expect("signal peer read"), 0);
        let logs = String::from_utf8(logs.lock().expect("log lock").clone()).expect("log UTF-8");
        assert!(crate::broker::handler::tests::has_line_at(
            &logs,
            "ERROR",
            &["worker 7", "before shutdown was signaled"],
        ));
        assert_eq!(
            crate::broker::handler::tests::count_lines_at(
                &logs,
                "ERROR",
                "before shutdown was signaled",
            ),
            1,
        );
        assert!(trigger.take_signal_ends().is_none());
    }

    #[test]
    fn unarmed_exit_guard_leaves_the_signal_ends_open() {
        let logs = crate::broker::handler::tests::capture_logs();
        let (main_end, mut peer) = std::os::unix::net::UnixStream::pair().expect("socketpair");
        peer.set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set read timeout");
        let trigger = ShutdownHandle {
            signal_ends: std::sync::Arc::new(std::sync::Mutex::new(Some(vec![main_end]))),
        };
        let post_go = worker::PostGoFlag::unarmed();
        let guard = WorkerExitGuard {
            worker_id: 7,
            post_go,
            trigger: trigger.clone(),
        };
        drop(guard);

        let mut byte = [0u8; 1];
        assert!(matches!(
            peer.read(&mut byte),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                )
        ));
        let logs = String::from_utf8(logs.lock().expect("log lock").clone()).expect("log UTF-8");
        assert_eq!(
            crate::broker::handler::tests::count_lines_at(
                &logs,
                "ERROR",
                "before shutdown was signaled",
            ),
            0,
        );
        trigger.shutdown();
        assert_eq!(peer.read(&mut byte).expect("shutdown signal peer read"), 0);
    }

    #[test]
    fn second_armed_exit_guard_over_a_consumed_trigger_is_silent() {
        let logs = crate::broker::handler::tests::capture_logs();
        let (main_end, mut peer) = std::os::unix::net::UnixStream::pair().expect("socketpair");
        peer.set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set read timeout");
        let trigger = ShutdownHandle {
            signal_ends: std::sync::Arc::new(std::sync::Mutex::new(Some(vec![main_end]))),
        };
        let first_post_go = worker::PostGoFlag::unarmed();
        first_post_go.arm();
        let second_post_go = worker::PostGoFlag::unarmed();
        second_post_go.arm();
        drop(WorkerExitGuard {
            worker_id: 7,
            post_go: first_post_go,
            trigger: trigger.clone(),
        });
        drop(WorkerExitGuard {
            worker_id: 8,
            post_go: second_post_go,
            trigger: trigger.clone(),
        });

        let mut byte = [0u8; 1];
        assert_eq!(peer.read(&mut byte).expect("signal peer read"), 0);
        let logs = String::from_utf8(logs.lock().expect("log lock").clone()).expect("log UTF-8");
        assert_eq!(
            crate::broker::handler::tests::count_lines_at(
                &logs,
                "ERROR",
                "before shutdown was signaled",
            ),
            1,
        );
        assert!(!crate::broker::handler::tests::has_line_at(
            &logs,
            "ERROR",
            &["worker 8"]
        ));
    }

    #[test]
    fn armed_exit_guard_after_an_explicit_shutdown_is_silent() {
        let logs = crate::broker::handler::tests::capture_logs();
        let (main_end, mut peer) = std::os::unix::net::UnixStream::pair().expect("socketpair");
        peer.set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set read timeout");
        let trigger = ShutdownHandle {
            signal_ends: std::sync::Arc::new(std::sync::Mutex::new(Some(vec![main_end]))),
        };
        let post_go = worker::PostGoFlag::unarmed();
        post_go.arm();
        trigger.shutdown();
        drop(WorkerExitGuard {
            worker_id: 9,
            post_go,
            trigger: trigger.clone(),
        });

        let mut byte = [0u8; 1];
        assert_eq!(peer.read(&mut byte).expect("shutdown signal peer read"), 0);
        let logs = String::from_utf8(logs.lock().expect("log lock").clone()).expect("log UTF-8");
        assert_eq!(
            crate::broker::handler::tests::count_lines_at(
                &logs,
                "ERROR",
                "before shutdown was signaled",
            ),
            0,
        );
    }

    /// AC-1 across every end — one socketpair exists per worker, so a guard
    /// that took the `Vec` but closed only its first element would leave
    /// every other sibling parked in `cancelable_accept`. Each retained peer
    /// must reach EOF, not just the first.
    #[test]
    fn armed_exit_guard_closes_every_signal_end() {
        const WORKERS: usize = 3;

        let mut main_ends = Vec::with_capacity(WORKERS);
        let mut peers = Vec::with_capacity(WORKERS);
        for _ in 0..WORKERS {
            let (main_end, peer) = std::os::unix::net::UnixStream::pair().expect("socketpair");
            peer.set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set read timeout");
            main_ends.push(main_end);
            peers.push(peer);
        }
        let trigger = ShutdownHandle {
            signal_ends: std::sync::Arc::new(std::sync::Mutex::new(Some(main_ends))),
        };
        let post_go = worker::PostGoFlag::unarmed();
        post_go.arm();
        drop(WorkerExitGuard {
            worker_id: 4,
            post_go,
            trigger,
        });

        let mut byte = [0u8; 1];
        for (i, peer) in peers.iter_mut().enumerate() {
            assert_eq!(
                peer.read(&mut byte).expect("signal peer read"),
                0,
                "sibling {i}'s signal end never reached EOF"
            );
        }
    }

    /// `shutdown`'s observable behaviour must survive the extraction of
    /// `take_signal_ends`: the first call closes EVERY main-side end and
    /// emits exactly one INFO line, a second call emits none, and neither
    /// call may ever emit the guard's ERROR line.
    #[test]
    fn shutdown_takes_every_end_and_logs_its_info_line_once() {
        const WORKERS: usize = 2;

        let logs = crate::broker::handler::tests::capture_logs();
        let mut main_ends = Vec::with_capacity(WORKERS);
        let mut peers = Vec::with_capacity(WORKERS);
        for _ in 0..WORKERS {
            let (main_end, peer) = std::os::unix::net::UnixStream::pair().expect("socketpair");
            peer.set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set read timeout");
            main_ends.push(main_end);
            peers.push(peer);
        }
        let trigger = ShutdownHandle {
            signal_ends: std::sync::Arc::new(std::sync::Mutex::new(Some(main_ends))),
        };

        trigger.shutdown();
        let mut byte = [0u8; 1];
        for (i, peer) in peers.iter_mut().enumerate() {
            assert_eq!(
                peer.read(&mut byte).expect("signal peer read"),
                0,
                "worker {i}'s signal end never reached EOF"
            );
        }
        trigger.shutdown();

        let logs = String::from_utf8(logs.lock().expect("log lock").clone()).expect("log UTF-8");
        assert_eq!(
            crate::broker::handler::tests::count_lines_at(
                &logs,
                "INFO",
                "shutdown signaled to workers",
            ),
            1,
        );
        assert_eq!(
            crate::broker::handler::tests::count_lines_at(
                &logs,
                "ERROR",
                "before shutdown was signaled",
            ),
            0,
        );
    }

    /// AC-6 + AC-7 — a callback panic stops a sibling before `join`, and
    /// `join` returns the worker error without an explicit shutdown.
    #[allow(clippy::too_many_lines)] // bounded multi-worker lifecycle harness covers discovery, trigger, and observation
    #[test]
    fn callback_panic_shuts_down_siblings_and_surfaces_as_join_error() {
        const PANIC_TEMPERATURE: i16 = -1;
        const SETUP_BUDGET: Duration = Duration::from_secs(30);
        const OBSERVE_BUDGET: Duration = Duration::from_secs(20);

        let port = find_free_port();
        let addr_str = format!("127.0.0.1:{port}");
        let addr: std::net::SocketAddr = addr_str.parse().expect("parse addr");
        let config = BrokerConfig::new(addr_str)
            .num_workers(2)
            .drain_timeout_secs(1);
        let (cb_tx, cb_rx) = std::sync::mpsc::channel::<(i16, String)>();
        let cb: EventCallback = std::sync::Arc::new(move |event: Event| {
            let Event::SensorV1 { temperature, .. } = event;
            assert!(temperature != PANIC_TEMPERATURE, "test callback panic");
            let _ = cb_tx.send((
                temperature,
                std::thread::current()
                    .name()
                    .unwrap_or("unnamed")
                    .to_string(),
            ));
        });

        let (tx, rx) = std::sync::mpsc::channel::<Result<(Result<(), Error>, Duration), String>>();
        let _h = std::thread::Builder::new()
            .name("t3-ac11-callback-panic-siblings".into())
            .spawn(move || {
                let outcome = (|| -> Result<(Result<(), Error>, Duration), String> {
                    let handle = MqttBroker::start_with_callback(config, Some(cb))
                        .map_err(|e| format!("startup failed before the callback ran: {e}"))?;

                    let setup_deadline = Instant::now() + SETUP_BUDGET;
                    let remaining = || setup_deadline.saturating_duration_since(Instant::now());
                    let mut victim = None;
                    let mut survivor = None;

                    for i in 0_i16..24 {
                        if survivor.is_some() || Instant::now() >= setup_deadline {
                            break;
                        }
                        let mut client = std::net::TcpStream::connect_timeout(
                            &addr,
                            remaining().min(Duration::from_secs(2)),
                        )
                        .map_err(|e| format!("setup budget exhausted at client {i}: {e}"))?;
                        client
                            .set_read_timeout(Some(remaining()))
                            .map_err(|e| format!("setup budget exhausted at client {i}: {e}"))?;
                        client
                            .set_write_timeout(Some(remaining()))
                            .map_err(|e| format!("setup budget exhausted at client {i}: {e}"))?;
                        client
                            .write_all(&V3_CONNECT_TEST)
                            .map_err(|e| format!("setup budget exhausted at client {i}: {e}"))?;
                        client
                            .set_read_timeout(Some(remaining()))
                            .map_err(|e| format!("setup budget exhausted at client {i}: {e}"))?;
                        let mut connack = [0u8; 4];
                        client
                            .read_exact(&mut connack)
                            .map_err(|e| format!("setup budget exhausted at client {i}: {e}"))?;
                        assert_eq!(connack, [0x20, 0x02, 0x00, 0x00]);

                        let mut publish: [u8; 9] =
                            [0x30, 0x07, 0x00, 0x01, b't', 0x09, 0xC4, 0x03, 0xF5];
                        publish[5..7].copy_from_slice(&i.to_be_bytes());
                        client
                            .set_write_timeout(Some(remaining()))
                            .map_err(|e| format!("setup budget exhausted at client {i}: {e}"))?;
                        client
                            .write_all(&publish)
                            .map_err(|e| format!("setup budget exhausted at client {i}: {e}"))?;
                        let (temperature, worker_name) = cb_rx
                            .recv_timeout(setup_deadline.saturating_duration_since(Instant::now()))
                            .map_err(|e| format!("setup budget exhausted at client {i}: {e}"))?;
                        assert_eq!(temperature, i);

                        let already_held = victim
                            .as_ref()
                            .is_some_and(|(_, name)| name == &worker_name)
                            || survivor
                                .as_ref()
                                .is_some_and(|(_, name)| name == &worker_name);
                        if victim.is_none() {
                            victim = Some((client, worker_name));
                        } else if !already_held && survivor.is_none() {
                            survivor = Some((client, worker_name));
                        }
                    }

                    let (mut victim, _victim_name) = victim
                        .ok_or_else(|| "only 0 distinct workers after discovery".to_string())?;
                    let Some((mut survivor, _survivor_name)) = survivor else {
                        return Err("only 1 distinct worker after discovery".to_string());
                    };

                    let t0 = Instant::now();
                    let observe_deadline = t0 + OBSERVE_BUDGET;
                    let mut panic_publish: [u8; 9] =
                        [0x30, 0x07, 0x00, 0x01, b't', 0x09, 0xC4, 0x03, 0xF5];
                    panic_publish[5..7].copy_from_slice(&PANIC_TEMPERATURE.to_be_bytes());
                    victim
                        .set_write_timeout(Some(
                            observe_deadline.saturating_duration_since(Instant::now()),
                        ))
                        .map_err(|e| format!("observation budget exhausted: {e}"))?;
                    victim
                        .write_all(&panic_publish)
                        .map_err(|e| format!("observation budget exhausted: {e}"))?;

                    let remaining = observe_deadline.saturating_duration_since(Instant::now());
                    if remaining == Duration::ZERO {
                        return Err(
                            "survivor did not close before observation deadline".to_string()
                        );
                    }
                    survivor
                        .set_read_timeout(Some(remaining))
                        .map_err(|e| format!("observation budget exhausted: {e}"))?;
                    let mut byte = [0u8; 1];
                    match survivor.read(&mut byte) {
                        Ok(0) => {}
                        Ok(n) => {
                            return Err(format!("survivor read {n} bytes instead of closing"));
                        }
                        Err(e)
                            if matches!(
                                e.kind(),
                                std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::BrokenPipe
                            ) => {}
                        Err(e) => {
                            return Err(format!("survivor closed with unexpected error: {e}"));
                        }
                    }

                    let join_result = handle.join();
                    let elapsed = t0.elapsed();
                    Ok((join_result, elapsed))
                })();
                let _ = match outcome {
                    Ok(value) => tx.send(Ok(value)),
                    Err(error) => tx.send(Err(error)),
                };
            })
            .expect("spawn harness");

        let (join_result, elapsed) = match rx.recv_timeout(Duration::from_secs(90)) {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => panic!("harness setup failed: {error}"),
            Err(error) => panic!("harness watchdog failed: {error:?}"),
        };
        assert!(matches!(join_result, Err(Error::Worker(_))));
        assert!(elapsed <= Duration::from_secs(25));
    }

    /// The lifecycle types are part of the crate-root surface an embedder
    /// writes against (`uring_mqtt::BrokerHandle`), and the thread-safety
    /// bounds are load-bearing: `ShutdownHandle` is cloned into a signal
    /// handler (`Send + 'static`) and triggered from any thread, and
    /// `BrokerHandle` moves to whichever thread joins it. Dropping either
    /// re-export or either auto-trait breaks embedders at compile time —
    /// nothing else in the suite names these paths.
    #[test]
    fn lifecycle_types_are_re_exported_and_thread_safe() {
        const fn assert_send<T: Send>() {}
        const fn assert_send_sync_clone<T: Send + Sync + Clone>() {}

        assert_send::<crate::BrokerHandle>();
        assert_send_sync_clone::<crate::ShutdownHandle>();

        let start: fn(crate::BrokerConfig) -> Result<crate::BrokerHandle, Error> =
            crate::MqttBroker::start;
        let _ = start;
    }

    /// AC-2 upper half — with NO active connections the drain completes on
    /// the first `recv` (every done-sender is already gone), so `join` must
    /// return promptly rather than sitting out `drain_timeout_secs`. The 30s
    /// deadline against a 5s bound fails any implementation that always waits
    /// for the timer.
    #[test]
    fn shutdown_with_no_connections_returns_well_before_the_deadline() {
        let (tx, rx) = std::sync::mpsc::channel::<(Result<(), Error>, Duration)>();
        let _h = std::thread::Builder::new()
            .name("t3-ac2-empty-drain".into())
            .spawn(move || {
                let outcome = (|| -> Result<(), Error> {
                    let port = find_free_port();
                    let addr_str = format!("127.0.0.1:{port}");
                    let config = BrokerConfig::new(addr_str)
                        .drain_timeout_secs(30)
                        .num_workers(1);

                    let handle = MqttBroker::start(config)?;

                    let t0 = Instant::now();
                    handle.shutdown();
                    let join_result = handle.join();
                    let dt = t0.elapsed();

                    let _ = tx.send((join_result, dt));
                    Ok(())
                })();
                if outcome.is_err() {
                    // Sentinel: an Err result AND a duration past every bound.
                    let _ = tx.send((
                        Err(Error::Worker("harness setup failed".into())),
                        Duration::from_secs(3600),
                    ));
                }
            })
            .expect("spawn harness");
        let Ok((join_result, dt)) = rx.recv_timeout(Duration::from_secs(15)) else {
            panic!("harness did not report within 15s")
        };
        match join_result {
            Ok(()) => {}
            Err(e) => panic!("join returned Err: {e}"),
        }
        assert!(
            dt <= Duration::from_secs(5),
            "empty drain took {dt:?}; the worker waited out the 30s deadline \
             instead of returning on the immediately-closed done-channel"
        );
    }

    /// AC-1 + AC-2 across the worker fan-out — one signal must reach EVERY
    /// worker. Three SO_REUSEPORT co-bound workers, one held client pinning a
    /// single worker's drain to the full 1s deadline: `join` returns bounded,
    /// and afterwards no listener survives (a fresh connect is refused within
    /// the teardown window). A signal delivered to only the first worker
    /// leaves a live listener and fails the refusal probe.
    #[test]
    fn every_worker_shuts_down_in_a_multi_worker_server() {
        let (tx, rx) = std::sync::mpsc::channel::<(Result<(), Error>, Duration, bool)>();
        let _h = std::thread::Builder::new()
            .name("t3-ac1-multi-worker-shutdown".into())
            .spawn(move || {
                let outcome = (|| -> Result<(), Error> {
                    let port = find_free_port();
                    let addr_str = format!("127.0.0.1:{port}");
                    let addr: std::net::SocketAddr = addr_str.parse().expect("parse addr");
                    let config = BrokerConfig::new(addr_str)
                        .drain_timeout_secs(1)
                        .num_workers(3);

                    let handle = MqttBroker::start(config)?;

                    let mut client =
                        std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2))
                            .map_err(Error::Io)?;
                    client
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .map_err(Error::Io)?;
                    client.write_all(&V3_CONNECT_TEST).map_err(Error::Io)?;
                    let mut connack = [0u8; 4];
                    client.read_exact(&mut connack).map_err(Error::Io)?;
                    assert_eq!(connack, [0x20, 0x02, 0x00, 0x00]);

                    let t0 = Instant::now();
                    handle.shutdown();
                    let join_result = handle.join();
                    let dt = t0.elapsed();
                    drop(client);

                    // No listener may survive on any worker: monoio defers the
                    // fd close, so allow a bounded teardown window.
                    let mut all_gone = false;
                    for _ in 0..20 {
                        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50))
                            .is_err()
                        {
                            all_gone = true;
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(50));
                    }

                    let _ = tx.send((join_result, dt, all_gone));
                    Ok(())
                })();
                if outcome.is_err() {
                    let _ = tx.send((
                        Err(Error::Worker("harness setup failed".into())),
                        Duration::ZERO,
                        false,
                    ));
                }
            })
            .expect("spawn harness");
        let Ok((join_result, dt, all_gone)) = rx.recv_timeout(Duration::from_secs(15)) else {
            panic!("harness did not report within 15s")
        };
        match join_result {
            Ok(()) => {}
            Err(e) => panic!("join returned Err: {e}"),
        }
        assert!(
            dt >= Duration::from_millis(900),
            "join returned too fast ({dt:?}); the held client pins one worker's \
             drain to the full 1s deadline"
        );
        assert!(
            dt <= Duration::from_secs(4),
            "join returned too slow ({dt:?}); upper bound is 4s with 3s slack"
        );
        assert!(
            all_gone,
            "a listener survived shutdown — the signal did not reach every worker"
        );
    }

    /// AC-7 strengthened — the trigger clones rendezvous on a barrier so the
    /// two `shutdown()` calls genuinely overlap (spawning two threads alone
    /// lets the first finish before the second starts), and a clone that
    /// OUTLIVES the broker stays a permanent no-op instead of panicking on a
    /// server whose workers are already gone.
    #[test]
    fn barrier_synced_and_post_join_shutdown_calls_are_noops() {
        let (tx, rx) = std::sync::mpsc::channel::<Result<(), Error>>();
        let _h = std::thread::Builder::new()
            .name("t3-ac7-barrier-shutdown".into())
            .spawn(move || {
                let outcome = (|| -> Result<(), Error> {
                    let port = find_free_port();
                    let addr_str = format!("127.0.0.1:{port}");
                    let config = BrokerConfig::new(addr_str).num_workers(1);

                    let handle = MqttBroker::start(config)?;

                    let trig1 = handle.shutdown_handle();
                    let trig2 = handle.shutdown_handle();
                    // Taken before shutdown, used after the broker is gone.
                    let survivor = handle.shutdown_handle();

                    // Rendezvous: both triggers enter `shutdown()` together.
                    let barrier = Arc::new(std::sync::Barrier::new(3));
                    let b1 = barrier.clone();
                    let b2 = barrier.clone();
                    let t1 = std::thread::spawn(move || {
                        b1.wait();
                        trig1.shutdown();
                    });
                    let t2 = std::thread::spawn(move || {
                        b2.wait();
                        trig2.shutdown();
                    });
                    barrier.wait();
                    t1.join().expect("thread 1 join");
                    t2.join().expect("thread 2 join");

                    let join_result = handle.join();

                    // The stale clone must be inert now that every worker and
                    // the owning handle are gone.
                    survivor.shutdown();
                    survivor.shutdown();

                    join_result
                })();
                let _ = tx.send(outcome);
            })
            .expect("spawn harness");
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok(())) => {}
            other => panic!("expected Ok(()) from harness, got {other:?}"),
        }
    }

    /// AC-8 — a healthy ingest server's `join` stays blocked until something
    /// signals shutdown. Pins the contract: no future refactor may move
    /// supervision into `join`.
    #[test]
    fn join_stays_blocked_until_shutdown_is_signaled() {
        let (tx, rx) = std::sync::mpsc::channel::<(bool, bool)>();
        let _h = std::thread::Builder::new()
            .name("t4-ac8-join-stays-blocked".into())
            .spawn(move || {
                let outcome = (|| -> Result<(), Error> {
                    let port = find_free_port();
                    let addr_str = format!("127.0.0.1:{port}");
                    let config = BrokerConfig::new(addr_str)
                        .num_workers(2)
                        .drain_timeout_secs(1);

                    let handle = MqttBroker::start(config)?;
                    let trigger = handle.shutdown_handle();

                    // Inner thread: rendezvous THEN `join` — so the 3 s
                    // blocked-ness window is not spent waiting for the
                    // thread to be scheduled.
                    let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
                    let (jtx, jrx) = std::sync::mpsc::channel::<Result<(), Error>>();
                    let inner = std::thread::Builder::new()
                        .name("t4-ac8-inner-join".into())
                        .spawn(move || {
                            let _ = entered_tx.send(());
                            let r = handle.join();
                            let _ = jtx.send(r);
                        })
                        .expect("spawn inner");

                    entered_rx
                        .recv_timeout(Duration::from_secs(10))
                        .map_err(|e| Error::Worker(format!("inner thread never entered: {e}")))?;

                    let stayed_blocked = jrx.recv_timeout(Duration::from_secs(3)).is_err();

                    trigger.shutdown();
                    let returned_after_signal = match jrx.recv_timeout(Duration::from_secs(15)) {
                        Ok(Ok(())) => true,
                        Ok(Err(e)) => {
                            return Err(Error::Worker(format!(
                                "join returned Err after shutdown: {e}"
                            )))
                        }
                        Err(e) => {
                            return Err(Error::Worker(format!(
                                "join did not return after shutdown: {e}"
                            )))
                        }
                    };

                    inner.join().expect("inner thread join");
                    let _ = tx.send((stayed_blocked, returned_after_signal));
                    Ok(())
                })();
                if outcome.is_err() {
                    let _ = tx.send((false, false));
                }
            })
            .expect("spawn harness");
        let Ok((stayed_blocked, returned_after_signal)) = rx.recv_timeout(Duration::from_secs(45))
        else {
            panic!("harness did not report within 45s")
        };
        assert!(
            stayed_blocked,
            "join returned within 3 s on a healthy server with no shutdown signaled"
        );
        assert!(
            returned_after_signal,
            "join did not return within 15 s of trigger.shutdown()"
        );
    }

    /// AC-10, AC-11 — `is_running` is `true` on a healthy server, goes
    /// `false` within 10 s of `shutdown()`. A non-consuming, non-blocking
    /// signal for an embedder that holds the handle and never joins.
    #[test]
    fn is_running_goes_false_once_a_worker_has_exited() {
        let (tx, rx) = std::sync::mpsc::channel::<(bool, bool)>();
        let _h = std::thread::Builder::new()
            .name("t4-ac10-is-running".into())
            .spawn(move || {
                let outcome = (|| -> Result<(), Error> {
                    let port = find_free_port();
                    let addr_str = format!("127.0.0.1:{port}");
                    let config = BrokerConfig::new(addr_str)
                        .num_workers(2)
                        .drain_timeout_secs(1);

                    let handle = MqttBroker::start(config)?;
                    let true_while_healthy = handle.is_running();

                    handle.shutdown();

                    // Poll up to 40 × 250 ms (10 s ceiling).
                    let mut false_after_exit = false;
                    for _ in 0..40 {
                        if !handle.is_running() {
                            false_after_exit = true;
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(250));
                    }

                    // The handle is intentionally not joined here: the point
                    // is that this signal needs no `join`. The harness
                    // thread's `Drop` tears the server down.
                    let _ = tx.send((true_while_healthy, false_after_exit));
                    Ok(())
                })();
                if outcome.is_err() {
                    let _ = tx.send((false, false));
                }
            })
            .expect("spawn harness");
        let Ok((true_while_healthy, false_after_exit)) = rx.recv_timeout(Duration::from_secs(45))
        else {
            panic!("harness did not report within 45s")
        };
        assert!(
            true_while_healthy,
            "is_running was false on a freshly started server"
        );
        assert!(
            false_after_exit,
            "is_running stayed true for more than 10 s after shutdown()"
        );
    }
}
