mod handler;
pub(crate) mod handshake;
pub(crate) mod worker;

use crate::error::Error;
use std::sync::Arc;

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

/// High-performance MQTT broker using Monoio io_uring runtime.
pub struct MqttBroker;

impl MqttBroker {
    /// Run the MQTT broker with the given configuration.
    ///
    /// This function spawns one worker thread per CPU core (or as configured),
    /// each with its own io_uring event loop. Connections are distributed
    /// across workers via SO_REUSEPORT.
    ///
    /// # Arguments
    /// * `config` - Broker configuration
    ///
    /// # Returns
    /// Returns when all workers have exited (typically never in normal operation)
    pub fn run(config: BrokerConfig) -> Result<(), Error> {
        Self::run_with_callback(config, None)
    }

    /// Run the MQTT broker with an event callback.
    ///
    /// # Arguments
    /// * `config` - Broker configuration
    /// * `callback` - Optional callback invoked for each event (called from worker thread)
    pub fn run_with_callback(
        config: BrokerConfig,
        callback: Option<EventCallback>,
    ) -> Result<(), Error> {
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

        // Startup barrier: every worker's fallible setup (runtime build, bind,
        // state/channel/processor spawn) completes before we fan out go/abort.
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let mut go_txs: Vec<std::sync::mpsc::Sender<bool>> = Vec::with_capacity(num_workers);
        let mut handles: Vec<std::thread::JoinHandle<Result<(), Error>>> =
            Vec::with_capacity(num_workers);

        for worker_id in 0..num_workers {
            let config = config.clone();
            let callback = callback.clone();
            let (go_tx, go_rx) = std::sync::mpsc::channel();
            go_txs.push(go_tx);
            let ready_tx = ready_tx.clone();

            match std::thread::Builder::new()
                .name(format!("mqtt-worker-{worker_id}"))
                .spawn(move || -> Result<(), Error> {
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
                        worker_id, config, callback, ready_tx, go_rx,
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

        let mut worker_err: Option<Error> = None;
        for handle in handles {
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

/// Collect per-worker startup reports and fan out a single go/abort decision to
/// every go-sender. Returns `Ok(())` once every worker has reported success
/// (decision = `true`); returns the first `Err` reported, or a generic
/// premature-disconnect error if the ready channel closes with workers missing
/// (decision = `false` in both error cases). Send errors on the go side are
/// ignored — the worker may have already exited.
pub(crate) fn supervise_startup(
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
    use std::time::Duration;

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
            .num_workers(4)
            .backlog(512);

        assert_eq!(config.max_connections_per_worker, 500);
        assert_eq!(config.connection_timeout_secs, 5);
        assert_eq!(config.idle_timeout_secs, 120);
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
}
