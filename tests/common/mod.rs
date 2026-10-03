//! Shared harness for the real-client integration tests: an in-process
//! ingest server on its own thread, a driver thread forwarding a rumqttc
//! connection's events, and the session-window guard both protocol versions
//! share.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use uring_mqtt::{BrokerConfig, MqttBroker, Publish, PublishCallback, QoS};

/// Harness thread name — visible in `top -H` and panic backtraces.
const HARNESS_THREAD_NAME: &str = "itest-mqtt-harness";
/// How often the bounded join re-checks `JoinHandle::is_finished`. Short
/// relative to `DRIVER_JOIN_BOUND` so a healthy exit is not padded by a
/// whole interval, long enough not to spin.
const DRIVER_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Outer watchdog for the harness thread. Bounds both the wait for the
/// test's release signal and the blocking `BrokerHandle::shutdown()` +
/// `join()` teardown the harness runs after it.
const HARNESS_TIMEOUT: Duration = Duration::from_secs(45);

/// Ports already handed out in this test binary. The server binds with
/// `SO_REUSEPORT`, so two servers given the same port would both bind and
/// split each other's clients.
static ISSUED_PORTS: Mutex<Vec<u16>> = Mutex::new(Vec::new());

/// Hand out a TCP port that nothing else in this test binary has been given.
/// The hold is per-binary, not per-process, because each cargo test binary has
/// its own static.
fn find_free_port() -> u16 {
    loop {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
        let port = listener.local_addr().expect("local_addr").port();
        drop(listener);
        let mut guard = ISSUED_PORTS.lock().unwrap_or_else(PoisonError::into_inner);
        if !guard.contains(&port) {
            guard.push(port);
            return port;
        }
    }
}

/// Coarse classification of a rumqttc event for the session-window guard.
///
/// `pub` because it is the return type of a `ClientEvent` method, and that
/// trait is the bound on `next_event`, `await_outgoing_disconnect` and
/// `await_terminal_error`. Narrowing either one below those helpers makes
/// rustc report "more private than the item" (`private_bounds` /
/// `private_interfaces`). No test file needs to NAME them — both impls live
/// here — but they are reachable through the helpers' signatures, so this is
/// interface, not a dead export.
pub enum EventClass {
    /// Skip silently — outgoing traffic or an unsolicited PINGRESP.
    Skip,
    /// The client wrote one of its own PUBLISH packets, carrying the packet
    /// id it assigned (0 for QoS 0). Skipped like any other outgoing event,
    /// but the id is recorded in send order first, so a later acknowledgement
    /// can be tied to the publish that earned it.
    OutgoingPublish(u16),
    /// The first CONNACK opens the session window; any later one is a failure.
    ConnAck,
    /// A peer-initiated DISCONNECT — a failure unless teardown is in flight.
    Disconnect,
    /// Anything else the test wants to observe.
    Other,
}

/// What a rumqttc event contributes to the test's session-window accounting.
/// `class` picks the next-incoming-event wait's verdict; `is_outgoing_disconnect`
/// lets the outgoing-DISCONNECT wait distinguish the one event it returns
/// from every other event rumqttc can yield.
/// `pub` for the same reason as [`EventClass`]: it is the bound three public
/// helpers name in their signatures.
pub trait ClientEvent: std::fmt::Debug + Send + 'static {
    fn class(&self) -> EventClass;
    fn is_outgoing_disconnect(&self) -> bool;
}

impl ClientEvent for rumqttc::Event {
    fn class(&self) -> EventClass {
        match self {
            // rumqttc queues this only after the PUBLISH is written
            // (0.24.0 `state.rs:364-366`), so it always precedes any
            // acknowledgement of that same publish on this channel.
            rumqttc::Event::Outgoing(rumqttc::Outgoing::Publish(pkid)) => {
                EventClass::OutgoingPublish(*pkid)
            }
            rumqttc::Event::Outgoing(_) | rumqttc::Event::Incoming(rumqttc::Packet::PingResp) => {
                EventClass::Skip
            }
            rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_)) => EventClass::ConnAck,
            rumqttc::Event::Incoming(rumqttc::Packet::Disconnect) => EventClass::Disconnect,
            rumqttc::Event::Incoming(_) => EventClass::Other,
        }
    }

    fn is_outgoing_disconnect(&self) -> bool {
        matches!(
            self,
            rumqttc::Event::Outgoing(rumqttc::Outgoing::Disconnect)
        )
    }
}

/// rumqttc 0.24 v5 event: the `Incoming` and `Outgoing` variants wrap the
/// same shape as v3, but `Incoming` carries `rumqttc::v5::mqttbytes::v5::Packet`
/// (so the same match arms work after the type substitution).
impl ClientEvent for rumqttc::v5::Event {
    fn class(&self) -> EventClass {
        match self {
            // Same accounting as v3: the id is recorded in send order before
            // any acknowledgement of the same publish can be returned.
            rumqttc::v5::Event::Outgoing(rumqttc::Outgoing::Publish(pkid)) => {
                EventClass::OutgoingPublish(*pkid)
            }
            rumqttc::v5::Event::Outgoing(_)
            | rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::PingResp(_)) => {
                EventClass::Skip
            }
            rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::ConnAck(_)) => {
                EventClass::ConnAck
            }
            rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::Disconnect(_)) => {
                EventClass::Disconnect
            }
            rumqttc::v5::Event::Incoming(_) => EventClass::Other,
        }
    }

    fn is_outgoing_disconnect(&self) -> bool {
        matches!(
            self,
            rumqttc::v5::Event::Outgoing(rumqttc::Outgoing::Disconnect)
        )
    }
}

/// What one connection's event stream has told `next_event` so far: whether
/// the session window is open, and the packet id of every PUBLISH the client
/// has written, in send order.
#[derive(Debug, Default)]
pub struct Session {
    open: bool,
    sent_publish_pkids: Vec<u16>,
}

impl Session {
    /// A session whose window has not opened yet — the state every live
    /// test starts in, before its first `ConnAck`.
    pub fn new() -> Self {
        Self::default()
    }

    /// A session whose window is already open, for the guard tests that feed
    /// the channel themselves instead of connecting.
    pub fn opened() -> Self {
        Self {
            open: true,
            sent_publish_pkids: Vec::new(),
        }
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    /// The packet id of the `index`-th PUBLISH the client wrote, or `None`
    /// if `next_event` has not observed that many writes yet.
    pub fn sent_publish_pkid(&self, index: usize) -> Option<u16> {
        self.sent_publish_pkids.get(index).copied()
    }
}

/// Distinguish the kinds of failure the test's waits can produce, so the
/// panic messages name the actual cause.
#[allow(clippy::result_large_err)]
pub enum RumqttcOutcome<CE> {
    TimedOut(Duration),
    UnexpectedConnAck,
    UnexpectedDisconnect,
    ConnectionError(CE),
    ClosedAfterDisconnect(CE),
    DriverChannelClosed,
    DisconnectNeverSent,
}

impl<CE: std::fmt::Display> RumqttcOutcome<CE> {
    pub fn describe(&self) -> String {
        match self {
            Self::TimedOut(d) => format!("timed out waiting {d:?}"),
            Self::UnexpectedConnAck => "received an unexpected second ConnAck".to_string(),
            Self::UnexpectedDisconnect => {
                "received an incoming Disconnect before the session ended".to_string()
            }
            Self::ConnectionError(e) => format!("rumqttc connection error: {e}"),
            Self::ClosedAfterDisconnect(e) => {
                format!("rumqttc reported terminal error after teardown flag was set: {e}")
            }
            Self::DriverChannelClosed => "event forwarding channel closed unexpectedly".to_string(),
            Self::DisconnectNeverSent => {
                "connection ended without ever emitting the outgoing Disconnect".to_string()
            }
        }
    }
}

/// One next-incoming-event wait that the brief mandates: drains the channel
/// under a bound, fails on any second `ConnAck`, any incoming `Disconnect`,
/// or any `Err` while the session window is open, and returns the next
/// INCOMING event otherwise. `Outgoing` events are observations of the
/// client's own requests and carry no information the test wants to assert
/// on, so they are skipped silently. So is an incoming `PingResp`: rumqttc
/// sends PINGREQ on its own `KEEP_ALIVE` timer regardless of other traffic
/// and forwards the response as an ordinary incoming event, so it can land
/// ahead of an acknowledgment the test is waiting for. Skipping it keeps
/// that healthy connection from failing the wait; the skip runs under the
/// SAME deadline, which `remaining` recomputes every iteration. After the
/// teardown flag flips, an `Err`
/// is the expected end of a disconnected connection and the helper returns
/// it instead of panicking — that is why the window has an end.
///
/// The session window opens the instant a `ConnAck` is observed: the FIRST
/// `ConnAck` is what step 3 waits for, and any SUBSEQUENT `ConnAck` is the
/// "client silently reconnected" failure mode the brief calls out.
///
/// An outgoing PUBLISH is skipped like any other outgoing event, but its
/// packet id is appended to `session` first. Because the client emits that
/// event only after the PUBLISH is written, the id of a publish is always
/// recorded before any acknowledgement of it can be returned, which is what
/// lets a caller tie an acknowledgement to the publish that earned it.
#[allow(clippy::result_large_err)]
pub fn next_event<E: ClientEvent, CE>(
    rx: &Receiver<Result<E, CE>>,
    bound: Duration,
    teardown: &AtomicBool,
    session: &mut Session,
) -> Result<E, RumqttcOutcome<CE>> {
    let started = Instant::now();
    loop {
        let remaining = bound.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(RumqttcOutcome::TimedOut(bound));
        }
        match rx.recv_timeout(remaining) {
            Ok(Ok(event)) => match event.class() {
                EventClass::Skip => {}
                EventClass::OutgoingPublish(pkid) => session.sent_publish_pkids.push(pkid),
                EventClass::ConnAck => {
                    if session.open {
                        return Err(RumqttcOutcome::UnexpectedConnAck);
                    }
                    session.open = true;
                    return Ok(event);
                }
                EventClass::Disconnect => {
                    if session.open && !teardown.load(Ordering::SeqCst) {
                        return Err(RumqttcOutcome::UnexpectedDisconnect);
                    }
                    return Ok(event);
                }
                EventClass::Other => return Ok(event),
            },
            Ok(Err(err)) => {
                if teardown.load(Ordering::SeqCst) {
                    return Err(RumqttcOutcome::ClosedAfterDisconnect(err));
                }
                return Err(RumqttcOutcome::ConnectionError(err));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                return Err(RumqttcOutcome::TimedOut(bound));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(RumqttcOutcome::DriverChannelClosed);
            }
        }
    }
}

/// Wait for the OUTGOING `Disconnect` event, skipping everything ahead of it
/// in the channel. rumqttc queues that event only after `Disconnect.write`
/// AND the socket flush have both succeeded (0.24.0 `state.rs:464-470`,
/// `eventloop.rs:229-238`), so observing it proves the DISCONNECT actually
/// reached the wire — `client.disconnect()` returning `Ok` proves only that
/// a request was enqueued. `Err` items are skipped here rather than
/// classified: the driver thread already judged each one against the
/// teardown flag at the instant it observed it, which is the only point
/// where that judgement is race-free.
#[allow(clippy::result_large_err)]
pub fn await_outgoing_disconnect<E: ClientEvent, CE>(
    rx: &Receiver<Result<E, CE>>,
    bound: Duration,
) -> Result<(), RumqttcOutcome<CE>> {
    let started = Instant::now();
    loop {
        let remaining = bound.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(RumqttcOutcome::TimedOut(bound));
        }
        match rx.recv_timeout(remaining) {
            Ok(Ok(event)) if event.is_outgoing_disconnect() => return Ok(()),
            Ok(_) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                return Err(RumqttcOutcome::TimedOut(bound));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(RumqttcOutcome::DisconnectNeverSent);
            }
        }
    }
}

#[allow(clippy::result_large_err)]
pub fn assert_outcome<E, CE: std::fmt::Display>(
    outcome: Result<E, RumqttcOutcome<CE>>,
    label: &str,
) -> E {
    match outcome {
        Ok(event) => event,
        Err(err) => panic!("{label}: {}", err.describe()),
    }
}

/// Driver thread: forward every event the connection yields to the main
/// test thread, never swallowing an Err. `recv()` blocks until the
/// eventloop produces the next item, so the loop needs no polling: it
/// ends on the connection's own terminal item or on a failed send once
/// the test thread has dropped the receiver.
///
/// It also CLASSIFIES its terminal error itself, against a shared clone
/// of the teardown flag, and reports the verdict through its join value.
/// Classifying at consumption time instead would be racy: an error that
/// arrives inside the session window can sit queued behind an
/// acknowledgment the main thread is still consuming, and by the time the
/// main thread reaches it the flag has flipped — the session-window
/// violation would then be read as an ordinary post-disconnect close. The
/// driver reads the flag at the instant the error is observed, which is
/// the only race-free point.
pub fn spawn_driver<E, CE, F>(
    name: &str,
    mut recv: F,
    tx: Sender<Result<E, CE>>,
    teardown: Arc<AtomicBool>,
) -> JoinHandle<Result<(), String>>
where
    E: Send + 'static,
    CE: std::fmt::Display + Send + 'static,
    F: FnMut() -> Option<Result<E, CE>> + Send + 'static,
{
    thread::Builder::new()
        .name(name.to_string())
        .spawn(move || loop {
            match recv() {
                Some(Ok(event)) => {
                    if tx.send(Ok(event)).is_err() {
                        return Ok(());
                    }
                }
                Some(Err(err)) => {
                    let in_session = !teardown.load(Ordering::SeqCst);
                    let described = err.to_string();
                    let _ = tx.send(Err(err));
                    return if in_session {
                        Err(format!(
                            "connection failed while the session was open: {described}"
                        ))
                    } else {
                        Ok(())
                    };
                }
                None => return Ok(()),
            }
        })
        .expect("spawn driver")
}

/// Bounded join: a leak here would otherwise only surface as a global
/// suite stall, with no clue which thread hung. The driver runs a
/// blocking recv() that does not observe the request-channel closure
/// (rumqttc's EventLoop holds its own Sender clone, 0.24.0
/// `eventloop.rs:80`), so it will only exit when the broker closes the
/// TCP connection — after reading a DISCONNECT the test sent, or at the
/// shutdown force-close for a test whose client stays connected. The
/// bounded poll catches any case where the broker never closes.
///
/// The driver's verdict on its own terminal error. This is what keeps a
/// failure that happened INSIDE the session window from being excused by
/// the now-true teardown flag.
pub fn join_driver(driver: JoinHandle<Result<(), String>>, bound: Duration) {
    let started = Instant::now();
    while !driver.is_finished() {
        assert!(
            started.elapsed() <= bound,
            "connection driver did not exit within {bound:?} of the connection being expected to end",
        );
        thread::sleep(DRIVER_POLL_INTERVAL);
    }
    if let Err(e) = driver.join().expect("connection driver panicked") {
        panic!("connection driver: {e}");
    }
}

/// One item the `PublishCallback` forwarded to the test thread: the topic,
/// payload, QoS and RETAIN flag the server observed on a single PUBLISH.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeenPublish {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
}

/// In-process MQTT broker running on its own thread. `start` blocks until
/// the broker is listening; `next_publish` yields each `PublishCallback`
/// delivery under a bound; `shutdown_and_join` sends the shutdown signal and
/// waits for the broker's blocking teardown to return, reporting its join
/// result and any callback deliveries the test did not claim.
pub struct ServerHarness {
    port: u16,
    publishes: Receiver<SeenPublish>,
    release_tx: Sender<()>,
    teardown_rx: Receiver<(Result<(), String>, Duration)>,
}

impl ServerHarness {
    /// A host without io_uring fails here instead of silently testing the
    /// legacy driver — `monoio`'s `FusionDriver` would otherwise fall back to
    /// it (`monoio-0.2.4/src/builder.rs:171`) and the suite would not
    /// exercise the io_uring path it exists to exercise.
    pub fn start(drain_timeout_secs: u64) -> Self {
        assert!(
            monoio::utils::detect_uring(),
            "io_uring is unavailable on this host: monoio's FusionDriver would fall back to the legacy driver (monoio-0.2.4/src/builder.rs:171), so this suite would not exercise the io_uring path",
        );

        let port = find_free_port();
        let (startup_tx, startup_rx) = std::sync::mpsc::channel::<Result<(), String>>();
        let (publish_tx, publish_rx) = std::sync::mpsc::channel::<SeenPublish>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let (teardown_tx, teardown_rx) =
            std::sync::mpsc::channel::<(Result<(), String>, Duration)>();

        thread::Builder::new()
            .name(HARNESS_THREAD_NAME.to_string())
            .spawn(move || {
                let report: (Result<(), String>, Duration) = (|| {
                    let config = BrokerConfig::new(format!("127.0.0.1:{port}"))
                        .num_workers(1)
                        .drain_timeout_secs(drain_timeout_secs);
                    let callback: PublishCallback = Arc::new(move |publish: &Publish| {
                        let _ = publish_tx.send(SeenPublish {
                            topic: publish.topic().to_string(),
                            payload: publish.payload().to_vec(),
                            qos: publish.qos(),
                            retain: publish.retain(),
                        });
                    });
                    let handle = match MqttBroker::start_with_callback(config, Some(callback)) {
                        Ok(handle) => handle,
                        Err(e) => {
                            let _ = startup_tx.send(Err(e.to_string()));
                            return (Err(e.to_string()), Duration::ZERO);
                        }
                    };
                    let _ = startup_tx.send(Ok(()));
                    // Block until the test thread signals it is done with the
                    // broker. Without this, `handle` would drop here, the
                    // broker would shut down, and the rumqttc client would
                    // hit "Connection refused" before its first packet left.
                    // Bounded so a test that hangs cannot leak the harness.
                    let release = release_rx.recv_timeout(HARNESS_TIMEOUT);
                    // `join_span`'s origin is HERE, on the thread that calls
                    // `shutdown()`, not on the test thread that sent the
                    // release. Everything the release signal spends crossing
                    // threads is therefore outside the span, so a harness
                    // thread descheduled past a drain deadline can no longer
                    // let a server that force-closes at once satisfy a floor
                    // measured against that deadline.
                    let shutdown_at = Instant::now();
                    handle.shutdown();
                    let join = handle.join().map_err(|e| e.to_string());
                    let join_span = shutdown_at.elapsed();
                    // A watchdog or lost-sender teardown is reported instead
                    // of the join result: the test asked for neither, so its
                    // timing report describes a shutdown it never requested.
                    let trigger: Result<(), String> = match release {
                        Ok(()) => Ok(()),
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(format!(
                            "begin_shutdown() was never called: the {HARNESS_TIMEOUT:?} watchdog started teardown"
                        )),
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(
                            "begin_shutdown() was never called: the release sender was dropped, which started teardown"
                                .to_string(),
                        ),
                    };
                    (trigger.and(join), join_span)
                })();
                let _ = teardown_tx.send(report);
            })
            .expect("spawn harness");

        match startup_rx.recv_timeout(HARNESS_TIMEOUT) {
            Ok(Ok(())) => ServerHarness {
                port,
                publishes: publish_rx,
                release_tx,
                teardown_rx,
            },
            Ok(Err(e)) => panic!("harness failed to start the server: {e}"),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!("harness did not report startup within {HARNESS_TIMEOUT:?}")
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("harness channel closed before reporting startup")
            }
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn next_publish(&self, bound: Duration) -> SeenPublish {
        match self.publishes.recv_timeout(bound) {
            Ok(seen) => seen,
            Err(e) => panic!("no callback delivery within {bound:?}: {e:?}"),
        }
    }

    pub fn begin_shutdown(&self) {
        let _ = self.release_tx.send(());
    }

    /// Call after `begin_shutdown`: every callback ran on a worker thread
    /// that has exited, so no callback can still be running and `unclaimed`
    /// holds every delivery the callback made and the test did not take. It
    /// does NOT account for a publish the server accepted whose event the
    /// worker runtime never processed — the event processor is detached
    /// (`src/broker/worker.rs:314`), so such an event reaches no callback and
    /// leaves no record here. A test that asserts "and no more" needs a
    /// barrier publish ahead of teardown, not an empty `unclaimed` alone.
    pub fn finish(self) -> Teardown {
        match self.teardown_rx.recv_timeout(HARNESS_TIMEOUT) {
            Ok((join, join_span)) => Teardown {
                join,
                join_span,
                unclaimed: self.publishes.try_iter().collect(),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!("harness did not finish server teardown within {HARNESS_TIMEOUT:?}")
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("harness channel closed before reporting teardown")
            }
        }
    }

    pub fn shutdown_and_join(self) -> Teardown {
        self.begin_shutdown();
        self.finish()
    }
}

/// What `ServerHarness::finish` reports: the join result, how long the
/// blocking teardown took, and any callback deliveries the test did not
/// consume.
pub struct Teardown {
    /// `BrokerHandle::join`'s result, or the reason the harness started
    /// teardown without the test asking for it.
    pub join: Result<(), String>,
    /// How long `BrokerHandle::shutdown()` plus `join()` took, measured on
    /// the harness thread around both calls. The origin is the harness's own
    /// `shutdown()` call, so no cross-thread release latency is counted; the
    /// span still ends at or after the worker's real drain, because the
    /// worker arms its drain timer only after observing this signal
    /// (`src/broker/worker.rs:438-443`).
    pub join_span: Duration,
    pub unclaimed: Vec<SeenPublish>,
}

/// Classify a rumqttc v3 terminal error as end-of-stream. True only for the
/// `io::ErrorKind` values the broker force-close and a peer reset can leave on
/// the socket; false for `NetworkTimeout` / `FlushTimeout` / every other
/// `ConnectionError`, so a timeout is not read as a close.
pub fn is_end_of_stream(err: &rumqttc::ConnectionError) -> bool {
    let (rumqttc::ConnectionError::Io(io)
    | rumqttc::ConnectionError::MqttState(rumqttc::StateError::Io(io))) = err
    else {
        return false;
    };
    matches!(
        io.kind(),
        std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::BrokenPipe
    )
}

/// Wait for the connection's terminal `Err` to surface on the channel,
/// skipping any `Ok` items that arrive ahead of it (PINGREQs, PINGRESPs,
/// outgoing-PUBLISH observations). Returns the error so the caller can
/// classify it with `is_end_of_stream`; never accepts a channel timeout as
/// the close.
#[allow(clippy::result_large_err)]
pub fn await_terminal_error<E: ClientEvent, CE>(
    rx: &Receiver<Result<E, CE>>,
    bound: Duration,
) -> Result<CE, RumqttcOutcome<CE>> {
    let started = Instant::now();
    loop {
        let remaining = bound.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(RumqttcOutcome::TimedOut(bound));
        }
        match rx.recv_timeout(remaining) {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Ok(e),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                return Err(RumqttcOutcome::TimedOut(bound));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(RumqttcOutcome::DriverChannelClosed);
            }
        }
    }
}

/// TCP probe timeout — short, just long enough that a probe can race the
/// listener-close window without timing out on a normal kernel handoff.
const REFUSE_PROBE_TIMEOUT: Duration = Duration::from_millis(200);
/// Poll interval for `wait_until_refusing`: short relative to the bound so
/// the post-shutdown window reports a refusal within a few cycles at most,
/// long enough not to spin.
const REFUSE_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Wait until `127.0.0.1:<port>` refuses new TCP connections. A probe that
/// succeeds is dropped at once: the server reads EOF in the handshake and
/// ends that session, so it cannot hold the drain open. The bound is a hard
/// upper limit — panics past it.
///
/// Only `ConnectionRefused` ends the wait. Any other probe error keeps the
/// loop running but is retained, so the panic can say whether the port kept
/// accepting or whether every probe failed for an unrelated reason such as
/// `PermissionDenied` or a routing error.
pub fn wait_until_refusing(port: u16, bound: Duration) {
    let addr = std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, port));
    let started = Instant::now();
    let mut last_err: Option<std::io::Error> = None;
    loop {
        match std::net::TcpStream::connect_timeout(&addr, REFUSE_PROBE_TIMEOUT) {
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => return,
            Ok(probe) => drop(probe),
            Err(e) => last_err = Some(e),
        }
        assert!(
            started.elapsed() <= bound,
            "{addr} did not refuse connections within {bound:?} after shutdown; last probe error: {}",
            last_err.as_ref().map_or_else(
                || "none — every probe connected".to_string(),
                |e| format!("{e} ({:?})", e.kind())
            ),
        );
        thread::sleep(REFUSE_POLL_INTERVAL);
    }
}
