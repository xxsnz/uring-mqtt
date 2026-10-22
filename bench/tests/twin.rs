//! Public-seam tests for the tokio twin (task 3 of the benchmark-harness
//! feature): `twin::start` runs the crate's own `serve_session` and event
//! channel on per-worker tokio `current_thread` runtimes with the uring
//! server's accept, drain, processor and lifecycle shape (AC-9, AC-10,
//! AC-11, AC-12, AC-13, AC-14, AC-53, AC-59). Raw-byte packet fixtures
//! mirror `tests/broker_handle_api.rs:29-38`; the drain/callback test
//! mirrors `tests/broker_handle_api.rs:703-790`.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use std::num::NonZeroU16;

use bench::twin::{self, TokioHandshake, TokioTimer};
use rmqtt_codec::v3::Packet as PacketV3;
use uring_mqtt::runtime_seam::{
    HandshakeIo, MqttPacket, PacketIo, ProtocolVersion, TimedOut, Timer,
};
use uring_mqtt::{BrokerConfig, MaxInboundPacketSize, Publish, PublishCallback};

/// Outer watchdog for every harness thread below — a safety net against a
/// hang, not the acceptance bound itself (each test asserts its own,
/// tighter bound from inside the thread).
const HARNESS_TIMEOUT: Duration = Duration::from_secs(30);

/// v3 CONNECT: ka=60, client id "twin" — same fixture shape as
/// `tests/broker_handle_api.rs:29-38`, with a different client id.
const V3_CONNECT: [u8; 18] = [
    0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x04, b't', b'w',
    b'i', b'n',
];
/// The same v3 CONNECT with CleanSession cleared: a named client asking for
/// a persistent session, which the broker refuses without a reply
/// (`src/broker/handler.rs:258-262`).
const V3_CONNECT_PERSISTENT: [u8; 18] = [
    0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x00, 0x00, 0x3C, 0x00, 0x04, b't', b'w',
    b'i', b'n',
];
/// Successful v3 CONNACK.
const CONNACK_OK: [u8; 4] = [0x20, 0x02, 0x00, 0x00];
/// v3 PINGREQ / PINGRESP / DISCONNECT.
const PINGREQ: [u8; 2] = [0xC0, 0x00];
const PINGRESP: [u8; 2] = [0xD0, 0x00];
const DISCONNECT: [u8; 2] = [0xE0, 0x00];
/// v3 PUBACK for packet id 7.
const PUBACK_7: [u8; 4] = [0x40, 0x02, 0x00, 0x07];
/// v3 PUBACK for packet id 8.
const PUBACK_8: [u8; 4] = [0x40, 0x02, 0x00, 0x08];
/// v3 QoS 1 PUBLISH, topic "t", packet id 7, payload `[1, 2, 3, 4]`.
const V3_PUBLISH_QOS1_ID7: [u8; 11] = [
    0x32, 0x09, 0x00, 0x01, b't', 0x00, 0x07, 1, 2, 3, 4,
];
/// v3 QoS 0 PUBLISH, topic "t", payload `[9, 9]`.
const V3_PUBLISH_QOS0: [u8; 7] = [0x30, 0x05, 0x00, 0x01, b't', 9, 9];

/// Connect retry budget while the twin may still be starting.
const CONNECT_ATTEMPTS: usize = 40;
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// Read/write deadline on a connected client socket.
const IO_TIMEOUT: Duration = Duration::from_secs(10);

/// AC-9/AC-10 callback-visibility bound.
const CALLBACK_VISIBLE_BOUND: Duration = Duration::from_secs(2);
const CALLBACK_POLL_INTERVAL: Duration = Duration::from_millis(20);
/// AC-10 bound on a PUBACK read that must succeed with no further client
/// input: far above loopback latency, far below `IO_TIMEOUT`.
const PARTIAL_ACK_READ_BOUND: Duration = Duration::from_secs(2);
/// Adapter-test window in which a reply the adapter wrote at once would
/// reach a loopback peer; a queued one must not arrive inside it.
const UNFLUSHED_READ_WINDOW: Duration = Duration::from_millis(200);
/// PUBACKs that fill the codecs' 8 KiB write backpressure boundary.
const PUBACKS_AT_THE_WRITE_BOUNDARY: usize = 8 * 1024 / PUBACK_7.len();
/// AC-16/AC-18 bound on a whole adapter scenario (bind, connect, accept,
/// send, flush, reads): above every bound inside it, so a stalled setup
/// fails the test instead of hanging it.
const ADAPTER_SCENARIO_BOUND: Duration = Duration::from_secs(30);

/// AC-11 handshake-timeout bounds.
const HANDSHAKE_TIMEOUT_SECS: u64 = 1;
const HANDSHAKE_CLOSE_MIN: Duration = Duration::from_millis(900);
const HANDSHAKE_CLOSE_MAX: Duration = Duration::from_secs(3);
const HANDSHAKE_READ_TIMEOUT: Duration = Duration::from_secs(4);

/// AC-12 drain/cohort constants.
const COHORT_DRAIN_TIMEOUT_SECS: u64 = 5;
const COHORT_CALLBACK_DELAY: Duration = Duration::from_millis(50);
const COHORT_PUBLISH_COUNT: u8 = 10;
const COHORT_JOIN_BOUND: Duration = Duration::from_secs(5);
/// How long the first delivered publish holds the event processor, so the
/// rest of the cohort is provably still queued when the test samples it.
/// Bounded, and only the first delivery ever waits: a twin that never
/// signals shutdown fails this test's assertions instead of hanging it.
const COHORT_GATE_BOUND: Duration = Duration::from_secs(5);
const COHORT_GATE_POLL_INTERVAL: Duration = Duration::from_millis(10);
/// Deliveries the gate lets through before shutdown: the one holding it.
const COHORT_GATED_DELIVERIES: usize = 1;

/// AC-53 per-round bound (connect, handshake, disconnect, EOF).
const LIMIT_ONE_ROUND_BOUND: Duration = Duration::from_secs(2);

/// AC-59 panicking-callback join bound.
const PANIC_JOIN_BOUND: Duration = Duration::from_secs(10);

/// Session-error and refused-connect join bound (connect, one rejected
/// packet, EOF, join).
const SESSION_ERROR_JOIN_BOUND: Duration = Duration::from_secs(10);

/// AC-14 timer bound — generous relative to the 50 ms deadline under test.
const TIMER_TEST_DEADLINE: Duration = Duration::from_millis(50);

fn loopback_addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}")
        .parse()
        .expect("parse loopback addr")
}

/// Connect to `addr`, retrying while the twin may still be starting. The
/// socket is returned with its read/write timeouts set and no MQTT byte
/// written.
fn connect_raw(addr: SocketAddr) -> std::io::Result<TcpStream> {
    let mut connected = None;
    for _ in 0..CONNECT_ATTEMPTS {
        match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
            Ok(s) => {
                connected = Some(s);
                break;
            }
            Err(_) => std::thread::sleep(CONNECT_RETRY_INTERVAL),
        }
    }
    let Some(client) = connected else {
        return Err(std::io::Error::other(
            "no connection within the retry window",
        ));
    };
    client.set_read_timeout(Some(IO_TIMEOUT))?;
    client.set_write_timeout(Some(IO_TIMEOUT))?;
    Ok(client)
}

/// Connect to `addr` and complete the MQTT v3 handshake. The connected
/// socket is returned with its read/write timeouts set.
fn connect_v3(addr: SocketAddr) -> std::io::Result<TcpStream> {
    let mut client = connect_raw(addr)?;
    client.write_all(&V3_CONNECT)?;
    let mut connack = [0u8; 4];
    client.read_exact(&mut connack)?;
    if connack != CONNACK_OK {
        return Err(std::io::Error::other(format!(
            "unexpected CONNACK {connack:?}"
        )));
    }
    Ok(client)
}

/// Collecting `PublishCallback`: every delivered payload is appended whole
/// to the shared `Vec<u8>`.
fn collecting_callback() -> (PublishCallback, Arc<Mutex<Vec<u8>>>) {
    let sink: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let callback_sink = sink.clone();
    let callback: PublishCallback = Arc::new(move |publish: &Publish| {
        callback_sink
            .lock()
            .expect("sink poisoned")
            .extend_from_slice(publish.payload());
    });
    (callback, sink)
}

/// Thread names currently live in this process, read from
/// `/proc/self/task/*/comm` (Linux only, matching the host this test
/// requires for real TCP/thread behaviour).
fn live_thread_names() -> Vec<String> {
    let mut names = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc/self/task") else {
        return names;
    };
    for entry in entries.flatten() {
        if let Ok(comm) = std::fs::read_to_string(entry.path().join("comm")) {
            names.push(comm.trim().to_string());
        }
    }
    names
}

/// Held by every test that runs a twin, for as long as its twin exists.
/// `twin_start_fails_on_zero_workers_or_an_unbindable_address` asserts on
/// process-global thread names, so a sibling test's live `twin-worker-{id}`
/// threads would be read as leftovers from its own failed start. Cargo runs
/// the tests in this binary on parallel threads, so the exclusion has to be
/// in the test file.
static TWIN_EXCLUSIVE: Mutex<()> = Mutex::new(());

/// Take the twin-exclusive lock. Poison is ignored: a test that panics while
/// holding it must fail on its own assertion, not cascade into every sibling.
fn twin_exclusive() -> MutexGuard<'static, ()> {
    TWIN_EXCLUSIVE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// AC-9 — a QoS 1 PUBLISH reaches the callback with its exact payload and
/// gets a PUBACK carrying its packet id.
#[test]
fn twin_delivers_qos1_publish_and_acks_its_packet_id() {
    let _exclusive = twin_exclusive();
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    std::thread::Builder::new()
        .name("twin-qos1-ack".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), String> {
                let port = bench::free_port();
                let config = BrokerConfig::new(format!("127.0.0.1:{port}")).num_workers(1);
                let (callback, sink) = collecting_callback();
                let handle = twin::start(config, callback)?;

                let mut client =
                    connect_v3(loopback_addr(port)).map_err(|e| format!("connect: {e}"))?;
                client
                    .write_all(&V3_PUBLISH_QOS1_ID7)
                    .map_err(|e| format!("write publish: {e}"))?;

                let mut puback = [0u8; 4];
                client
                    .read_exact(&mut puback)
                    .map_err(|e| format!("read puback: {e}"))?;
                if puback != PUBACK_7 {
                    return Err(format!("unexpected PUBACK {puback:?}"));
                }

                let deadline = Instant::now() + CALLBACK_VISIBLE_BOUND;
                loop {
                    if sink.lock().expect("sink poisoned").as_slice() == [1, 2, 3, 4] {
                        break;
                    }
                    if Instant::now() >= deadline {
                        return Err("callback did not see the payload in time".into());
                    }
                    std::thread::sleep(CALLBACK_POLL_INTERVAL);
                }

                handle.shutdown_handle().shutdown();
                handle.join().map_err(|e| format!("join: {e}"))?;
                Ok(())
            })();
            let _ = tx.send(outcome);
        })
        .expect("spawn harness");

    let Ok(result) = rx.recv_timeout(HARNESS_TIMEOUT) else {
        panic!("harness did not report within {HARNESS_TIMEOUT:?}")
    };
    result.expect("twin must deliver the QoS 1 publish and ack its packet id");
}

/// AC-10 — a QoS 0 PUBLISH reaches the callback with its exact payload.
#[test]
fn twin_delivers_qos0_publish() {
    let _exclusive = twin_exclusive();
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    std::thread::Builder::new()
        .name("twin-qos0-deliver".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), String> {
                let port = bench::free_port();
                let config = BrokerConfig::new(format!("127.0.0.1:{port}")).num_workers(1);
                let (callback, sink) = collecting_callback();
                let handle = twin::start(config, callback)?;

                let mut client =
                    connect_v3(loopback_addr(port)).map_err(|e| format!("connect: {e}"))?;
                client
                    .write_all(&V3_PUBLISH_QOS0)
                    .map_err(|e| format!("write publish: {e}"))?;

                let deadline = Instant::now() + CALLBACK_VISIBLE_BOUND;
                loop {
                    if sink.lock().expect("sink poisoned").as_slice() == [9, 9] {
                        break;
                    }
                    if Instant::now() >= deadline {
                        return Err("callback did not see the payload in time".into());
                    }
                    std::thread::sleep(CALLBACK_POLL_INTERVAL);
                }

                handle.shutdown_handle().shutdown();
                handle.join().map_err(|e| format!("join: {e}"))?;
                Ok(())
            })();
            let _ = tx.send(outcome);
        })
        .expect("spawn harness");

    let Ok(result) = rx.recv_timeout(HARNESS_TIMEOUT) else {
        panic!("harness did not report within {HARNESS_TIMEOUT:?}")
    };
    result.expect("twin must deliver the QoS 0 publish");
}

/// AC-11 — a connected client that never sends CONNECT is closed once
/// `connection_timeout_secs` elapses, no sooner and not much later.
#[test]
fn twin_closes_silent_client_after_handshake_timeout() {
    let _exclusive = twin_exclusive();
    let (tx, rx) = mpsc::channel::<Result<Duration, String>>();
    std::thread::Builder::new()
        .name("twin-handshake-timeout".into())
        .spawn(move || {
            let outcome = (|| -> Result<Duration, String> {
                let port = bench::free_port();
                let config = BrokerConfig::new(format!("127.0.0.1:{port}"))
                    .num_workers(1)
                    .connection_timeout_secs(HANDSHAKE_TIMEOUT_SECS);
                let (callback, _sink) = collecting_callback();
                let handle = twin::start(config, callback)?;

                let mut client = TcpStream::connect_timeout(&loopback_addr(port), CONNECT_TIMEOUT)
                    .map_err(|e| format!("connect: {e}"))?;
                client
                    .set_read_timeout(Some(HANDSHAKE_READ_TIMEOUT))
                    .map_err(|e| format!("set_read_timeout: {e}"))?;

                let start = Instant::now();
                let mut buf = [0u8; 1];
                let n = client
                    .read(&mut buf)
                    .map_err(|e| format!("read: {e}"))?;
                let elapsed = start.elapsed();
                if n != 0 {
                    return Err(format!("expected EOF, read {n} bytes"));
                }

                handle.shutdown_handle().shutdown();
                handle.join().map_err(|e| format!("join: {e}"))?;
                Ok(elapsed)
            })();
            let _ = tx.send(outcome);
        })
        .expect("spawn harness");

    let Ok(result) = rx.recv_timeout(HARNESS_TIMEOUT) else {
        panic!("harness did not report within {HARNESS_TIMEOUT:?}")
    };
    let elapsed = result.expect("silent client must be closed after the handshake timeout");
    assert!(
        elapsed >= HANDSHAKE_CLOSE_MIN,
        "closed too early: {elapsed:?}"
    );
    assert!(
        elapsed <= HANDSHAKE_CLOSE_MAX,
        "closed too late: {elapsed:?}"
    );
}

/// AC-12 — `join` returns only after every publish admitted before
/// `shutdown` reaches the callback. Mirrors
/// `tests/broker_handle_api.rs:703-790`: the callback's only wait on the
/// test is `COHORT_GATE_BOUND`-bounded, so the single worker thread cannot
/// deadlock.
#[test]
fn twin_join_waits_until_every_admitted_publish_reaches_the_callback() {
    let _exclusive = twin_exclusive();
    let (tx, rx) = mpsc::channel::<Result<(usize, Vec<u8>), String>>();
    std::thread::Builder::new()
        .name("twin-join-drain".into())
        .spawn(move || {
            let outcome = (|| -> Result<(usize, Vec<u8>), String> {
                let port = bench::free_port();
                let config = BrokerConfig::new(format!("127.0.0.1:{port}"))
                    .num_workers(1)
                    .drain_timeout_secs(COHORT_DRAIN_TIMEOUT_SECS);

                let delivered: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
                let sink = delivered.clone();
                // Opened by this test once it has sampled `delivered` and
                // requested shutdown.
                let gate = Arc::new(AtomicBool::new(false));
                let callback_gate = gate.clone();
                let callback: PublishCallback = Arc::new(move |publish: &Publish| {
                    let payload = publish.payload();
                    let delivered_so_far = {
                        let mut sink = sink.lock().expect("sink poisoned");
                        sink.push(payload[0]);
                        sink.len()
                    };
                    if delivered_so_far == 1 {
                        // Hold the processor on the first delivery, so the
                        // sample below cannot race the whole cohort to the
                        // callback. The session has already replied
                        // PINGRESP by now: it admits the batch and writes
                        // the reply before it yields to the processor.
                        let waited_since = Instant::now();
                        while !callback_gate.load(Ordering::Acquire)
                            && waited_since.elapsed() < COHORT_GATE_BOUND
                        {
                            std::thread::sleep(COHORT_GATE_POLL_INTERVAL);
                        }
                    }
                    std::thread::sleep(COHORT_CALLBACK_DELAY);
                });

                let handle = twin::start(config, callback)?;

                let mut client =
                    connect_v3(loopback_addr(port)).map_err(|e| format!("connect: {e}"))?;

                let mut batch = Vec::new();
                for i in 0..COHORT_PUBLISH_COUNT {
                    batch.extend_from_slice(&[0x30, 0x04, 0x00, 0x01, b't', i]);
                }
                batch.extend_from_slice(&PINGREQ);
                client
                    .write_all(&batch)
                    .map_err(|e| format!("write batch: {e}"))?;

                let mut pingresp = [0u8; 2];
                client
                    .read_exact(&mut pingresp)
                    .map_err(|e| format!("read pingresp: {e}"))?;
                if pingresp != PINGRESP {
                    return Err(format!("expected PINGRESP, got {pingresp:?}"));
                }

                let n_at_shutdown = delivered.lock().expect("sink poisoned").len();

                handle.shutdown_handle().shutdown();
                gate.store(true, Ordering::Release);
                handle.join().map_err(|e| format!("join: {e}"))?;

                let final_vec = delivered.lock().expect("sink poisoned").clone();
                Ok((n_at_shutdown, final_vec))
            })();
            let _ = tx.send(outcome);
        })
        .expect("spawn harness");

    let bound = COHORT_JOIN_BOUND + HARNESS_TIMEOUT;
    let Ok(result) = rx.recv_timeout(bound) else {
        panic!("harness did not report within {bound:?}")
    };
    let (n_at_shutdown, final_vec) = result.expect("join must drain every admitted publish");
    assert!(
        n_at_shutdown <= COHORT_GATED_DELIVERIES,
        "only the gated first delivery may be done at shutdown, saw {n_at_shutdown}"
    );
    assert_eq!(final_vec, (0..COHORT_PUBLISH_COUNT).collect::<Vec<u8>>());
}

/// AC-13 — `start` returns `Err` for zero workers, an unparseable address,
/// or an address it cannot bind, joining every worker thread it started
/// before returning.
#[test]
fn twin_start_fails_on_zero_workers_or_an_unbindable_address() {
    let _exclusive = twin_exclusive();
    let port = bench::free_port();

    let zero_workers_config =
        BrokerConfig::new(format!("127.0.0.1:{port}")).num_workers(0);
    let (zero_workers_callback, _sink) = collecting_callback();
    let zero_workers_result = twin::start(zero_workers_config, zero_workers_callback);
    assert!(
        zero_workers_result.is_err(),
        "num_workers(0) must fail to start"
    );

    let bad_addr_config = BrokerConfig::new("not-an-address").num_workers(1);
    let (bad_addr_callback, _sink) = collecting_callback();
    let bad_addr_result = twin::start(bad_addr_config, bad_addr_callback);
    assert!(
        bad_addr_result.is_err(),
        "an unparseable bind_addr must fail to start"
    );

    let held_port = bench::free_port();
    let _held_listener = TcpListener::bind(format!("127.0.0.1:{held_port}"))
        .expect("bind the port under test with no SO_REUSEPORT");
    let unbindable_config =
        BrokerConfig::new(format!("127.0.0.1:{held_port}")).num_workers(2);
    let (unbindable_callback, _sink) = collecting_callback();
    let Err(unbindable_err) = twin::start(unbindable_config, unbindable_callback) else {
        panic!("a held, non-SO_REUSEPORT port must fail to start");
    };
    assert!(
        unbindable_err.contains("bind"),
        "the startup error must keep the failing setup step, got {unbindable_err:?}"
    );

    let leftover: Vec<String> = live_thread_names()
        .into_iter()
        .filter(|name| name.starts_with("twin-worker-"))
        .collect();
    assert!(
        leftover.is_empty(),
        "no twin worker thread must remain after a failed start, saw {leftover:?}"
    );
}

/// AC-53 — with `max_connections_per_worker(1)`, a new connection is
/// accepted once the previous one has closed.
#[test]
fn twin_accepts_a_new_connection_after_the_previous_closes_at_limit_one() {
    let _exclusive = twin_exclusive();
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    std::thread::Builder::new()
        .name("twin-limit-one".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), String> {
                let port = bench::free_port();
                let config = BrokerConfig::new(format!("127.0.0.1:{port}"))
                    .num_workers(1)
                    .max_connections_per_worker(1);
                let (callback, _sink) = collecting_callback();
                let handle = twin::start(config, callback)?;

                for round in 0..3 {
                    let mut client = connect_v3(loopback_addr(port))
                        .map_err(|e| format!("round {round} connect: {e}"))?;
                    client
                        .write_all(&DISCONNECT)
                        .map_err(|e| format!("round {round} write disconnect: {e}"))?;
                    let mut buf = [0u8; 1];
                    loop {
                        match client.read(&mut buf) {
                            Ok(0) => break,
                            Ok(_) => continue,
                            Err(e) => {
                                return Err(format!("round {round} read until EOF: {e}"))
                            }
                        }
                    }
                }

                handle.shutdown_handle().shutdown();
                handle.join().map_err(|e| format!("join: {e}"))?;
                Ok(())
            })();
            let _ = tx.send(outcome);
        })
        .expect("spawn harness");

    let bound = 3 * LIMIT_ONE_ROUND_BOUND + HARNESS_TIMEOUT;
    let Ok(result) = rx.recv_timeout(bound) else {
        panic!("harness did not report within {bound:?}")
    };
    result.expect("each of the three connections must get its own CONNACK");
}

/// AC-59 — a panicking publish callback makes `join` return `Err` after
/// every twin worker thread has exited; no explicit `shutdown` is needed.
#[test]
fn twin_join_reports_a_panicking_callback() {
    let _exclusive = twin_exclusive();
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    std::thread::Builder::new()
        .name("twin-panic-callback".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), String> {
                let port = bench::free_port();
                let config = BrokerConfig::new(format!("127.0.0.1:{port}")).num_workers(2);
                let callback: PublishCallback = Arc::new(|_: &Publish| panic!("boom"));
                let handle = twin::start(config, callback)?;

                let mut client =
                    connect_v3(loopback_addr(port)).map_err(|e| format!("connect: {e}"))?;
                client
                    .write_all(&V3_PUBLISH_QOS0)
                    .map_err(|e| format!("write publish: {e}"))?;

                match handle.join() {
                    Err(_) => Ok(()),
                    Ok(dropped) => Err(format!(
                        "expected join to report the panic, got Ok({dropped})"
                    )),
                }
            })();
            let _ = tx.send(outcome);
        })
        .expect("spawn harness");

    let bound = PANIC_JOIN_BOUND + HARNESS_TIMEOUT;
    let Ok(result) = rx.recv_timeout(bound) else {
        panic!("harness did not report within {bound:?}")
    };
    result.expect("join must report a panicking callback as an error");
}

/// AC-59 — the panic is still reported when shutdown was requested first:
/// the callback requests shutdown and then panics, so the worker's biased
/// select takes the shutdown branch with the processor's `JoinError` still
/// unobserved. A worker that tested `is_finished()` there would discard it
/// and report `Ok`.
#[test]
fn twin_join_reports_a_callback_that_panics_after_shutdown_was_requested() {
    let _exclusive = twin_exclusive();
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    std::thread::Builder::new()
        .name("twin-panic-after-shutdown".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), String> {
                let port = bench::free_port();
                let config = BrokerConfig::new(format!("127.0.0.1:{port}")).num_workers(1);
                // Published right after `start`, which is before any client
                // can connect and so before any callback can run.
                let shutdown: Arc<OnceLock<twin::TwinShutdown>> = Arc::new(OnceLock::new());
                let callback_shutdown = shutdown.clone();
                let callback: PublishCallback = Arc::new(move |_: &Publish| {
                    callback_shutdown
                        .get()
                        .expect("shutdown handle published before the first publish")
                        .shutdown();
                    panic!("boom");
                });
                let handle = twin::start(config, callback)?;
                let _ = shutdown.set(handle.shutdown_handle());

                let mut client =
                    connect_v3(loopback_addr(port)).map_err(|e| format!("connect: {e}"))?;
                client
                    .write_all(&V3_PUBLISH_QOS0)
                    .map_err(|e| format!("write publish: {e}"))?;

                match handle.join() {
                    Err(_) => Ok(()),
                    Ok(dropped) => Err(format!(
                        "expected join to report the panic, got Ok({dropped})"
                    )),
                }
            })();
            let _ = tx.send(outcome);
        })
        .expect("spawn harness");

    let bound = PANIC_JOIN_BOUND + HARNESS_TIMEOUT;
    let Ok(result) = rx.recv_timeout(bound) else {
        panic!("harness did not report within {bound:?}")
    };
    result.expect("join must report a callback that panics once shutdown is pending");
}

/// Review pass 3 — a non-routine session error is server-side evidence an
/// invalid run needs, so `join` reports it. A first packet that is not
/// CONNECT makes `serve_session` return a decode error, which
/// `Error::is_routine_disconnect` classifies as not routine. A routine
/// disconnect stays silent: every other test here closes its client and
/// still asserts `join` returns `Ok`.
#[test]
fn twin_join_reports_a_non_routine_session_error() {
    let _exclusive = twin_exclusive();
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    std::thread::Builder::new()
        .name("twin-session-error".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), String> {
                let port = bench::free_port();
                let config = BrokerConfig::new(format!("127.0.0.1:{port}")).num_workers(1);
                let (callback, _sink) = collecting_callback();
                let handle = twin::start(config, callback)?;

                let mut client =
                    connect_raw(loopback_addr(port)).map_err(|e| format!("connect: {e}"))?;
                client
                    .write_all(&PINGREQ)
                    .map_err(|e| format!("write a first packet that is not CONNECT: {e}"))?;
                // Read to EOF so the rejected session has ended before
                // shutdown; the twin writes nothing back.
                let mut buf = [0u8; 1];
                loop {
                    match client.read(&mut buf) {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(e) => return Err(format!("read until EOF: {e}")),
                    }
                }

                handle.shutdown_handle().shutdown();
                match handle.join() {
                    Err(e) if e.contains("session") => Ok(()),
                    Err(e) => Err(format!("unexpected worker error: {e}")),
                    Ok(dropped) => Err(format!(
                        "expected join to report the session error, got Ok({dropped})"
                    )),
                }
            })();
            let _ = tx.send(outcome);
        })
        .expect("spawn harness");

    let bound = SESSION_ERROR_JOIN_BOUND + HARNESS_TIMEOUT;
    let Ok(result) = rx.recv_timeout(bound) else {
        panic!("harness did not report within {bound:?}")
    };
    result.expect("join must report a non-routine session error");
}

/// AC-29 — a session cut off mid-packet is the generator's own "stalled"
/// evidence (`bench/src/gen.rs:921-934`), not a server-side failure
/// (decision Q8), so `join` stays quiet about it exactly as it does for a
/// routine disconnect.
#[test]
fn twin_join_stays_quiet_after_a_session_cut_off_mid_packet() {
    let _exclusive = twin_exclusive();
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    std::thread::Builder::new()
        .name("twin-truncated-packet".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), String> {
                let port = bench::free_port();
                let config = BrokerConfig::new(format!("127.0.0.1:{port}")).num_workers(1);
                let (callback, _sink) = collecting_callback();
                let handle = twin::start(config, callback)?;

                let mut client =
                    connect_raw(loopback_addr(port)).map_err(|e| format!("connect: {e}"))?;
                client
                    .write_all(&V3_CONNECT)
                    .map_err(|e| format!("write CONNECT: {e}"))?;
                let mut connack = [0u8; 4];
                client
                    .read_exact(&mut connack)
                    .map_err(|e| format!("read CONNACK: {e}"))?;
                client
                    .write_all(&[0x30, 0x0A, 0x00, 0x01, b't'])
                    .map_err(|e| format!("write a PUBLISH missing 7 body bytes: {e}"))?;
                client
                    .shutdown(std::net::Shutdown::Write)
                    .map_err(|e| format!("shutdown write half: {e}"))?;
                // Read to EOF so the stalled session has ended before
                // shutdown; the twin writes nothing back.
                let mut buf = [0u8; 1];
                loop {
                    match client.read(&mut buf) {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(e) => return Err(format!("read until EOF: {e}")),
                    }
                }

                handle.shutdown_handle().shutdown();
                handle.join().map_err(|e| format!("join: {e}"))?;
                Ok(())
            })();
            let _ = tx.send(outcome);
        })
        .expect("spawn harness");

    let bound = SESSION_ERROR_JOIN_BOUND + HARNESS_TIMEOUT;
    let Ok(result) = rx.recv_timeout(bound) else {
        panic!("harness did not report within {bound:?}")
    };
    result.expect("join must stay quiet after a session cut off mid-packet");
}

/// Review pass 4 — a refused connect is server-side evidence too: the
/// shipped worker counts it (`note_refused`, `src/broker/worker.rs:579`) and
/// decisions Q15 and R2-Q2 make it invalidate the run, so `join` reports it.
/// A named v3 CONNECT with CleanSession cleared is refused and closed
/// without a reply, which `serve_session` returns as
/// `Ok(SessionOutcome::Refused)`.
#[test]
fn twin_join_reports_a_refused_connect() {
    let _exclusive = twin_exclusive();
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    std::thread::Builder::new()
        .name("twin-refused-connect".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), String> {
                let port = bench::free_port();
                let config = BrokerConfig::new(format!("127.0.0.1:{port}")).num_workers(1);
                let (callback, _sink) = collecting_callback();
                let handle = twin::start(config, callback)?;

                let mut client =
                    connect_raw(loopback_addr(port)).map_err(|e| format!("connect: {e}"))?;
                client
                    .write_all(&V3_CONNECT_PERSISTENT)
                    .map_err(|e| format!("write a persistent-session CONNECT: {e}"))?;
                // Read to EOF so the refused session has ended before
                // shutdown; the twin writes nothing back.
                let mut buf = [0u8; 1];
                loop {
                    match client.read(&mut buf) {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(e) => return Err(format!("read until EOF: {e}")),
                    }
                }

                handle.shutdown_handle().shutdown();
                match handle.join() {
                    Err(e) if e.contains("connect refused") => Ok(()),
                    Err(e) => Err(format!("unexpected worker error: {e}")),
                    Ok(dropped) => Err(format!(
                        "expected join to report the refused connect, got Ok({dropped})"
                    )),
                }
            })();
            let _ = tx.send(outcome);
        })
        .expect("spawn harness");

    let bound = SESSION_ERROR_JOIN_BOUND + HARNESS_TIMEOUT;
    let Ok(result) = rx.recv_timeout(bound) else {
        panic!("harness did not report within {bound:?}")
    };
    result.expect("join must report a refused connect");
}

/// AC-17 — with `max_connections_per_worker(1)`, a second accepted
/// connection is dropped at the worker's connection limit, and `join`
/// reports the cause by name.
#[test]
fn twin_join_reports_a_connection_dropped_at_its_connection_limit() {
    let _exclusive = twin_exclusive();
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    std::thread::Builder::new()
        .name("twin-limit-join".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), String> {
                let port = bench::free_port();
                let config = BrokerConfig::new(format!("127.0.0.1:{port}"))
                    .num_workers(1)
                    .max_connections_per_worker(1);
                let (callback, _sink) = collecting_callback();
                let handle = twin::start(config, callback)?;

                let mut client_a =
                    connect_v3(loopback_addr(port)).map_err(|e| format!("connect a: {e}"))?;
                let mut client_b =
                    connect_raw(loopback_addr(port)).map_err(|e| format!("connect b: {e}"))?;

                let mut buf = [0u8; 1];
                match client_b.read(&mut buf) {
                    Ok(0) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
                    Ok(n) => {
                        return Err(format!(
                            "expected client b dropped without CONNACK, read {n} byte(s)"
                        ))
                    }
                    Err(e) => return Err(format!("unexpected read error on client b: {e}")),
                }

                client_a
                    .write_all(&DISCONNECT)
                    .map_err(|e| format!("write disconnect: {e}"))?;
                loop {
                    match client_a.read(&mut buf) {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(e) => return Err(format!("read until EOF: {e}")),
                    }
                }

                handle.shutdown_handle().shutdown();
                match handle.join() {
                    Err(e) if e.contains("connection limit 1 reached") => Ok(()),
                    Err(e) => Err(format!("unexpected worker error: {e}")),
                    Ok(dropped) => Err(format!(
                        "expected join to report the limit drop, got Ok({dropped})"
                    )),
                }
            })();
            let _ = tx.send(outcome);
        })
        .expect("spawn harness");

    let bound = SESSION_ERROR_JOIN_BOUND + HARNESS_TIMEOUT;
    let Ok(result) = rx.recv_timeout(bound) else {
        panic!("harness did not report within {bound:?}")
    };
    result.expect("join must report the connection dropped at its connection limit");
}

/// AC-14 — `TokioTimer::timeout` races a future against a deadline: `Err`
/// for one still pending, `Ok` for one that finishes first.
#[test]
fn tokio_timer_times_out_a_pending_future() {
    let runtime = tokio::runtime::Runtime::new().expect("build tokio runtime");

    let timed_out = runtime.block_on(async {
        TokioTimer::timeout(TIMER_TEST_DEADLINE, std::future::pending::<()>()).await
    });
    assert_eq!(timed_out, Err(TimedOut));

    let completed = runtime.block_on(async {
        TokioTimer::timeout(TIMER_TEST_DEADLINE, async { 5 }).await
    });
    assert_eq!(completed, Ok(5));
}

/// AC-16 — the tokio adapter's `send` only queues: two PUBACKs sent below
/// the write boundary do not reach the peer until `flush`, which then
/// delivers both in send order.
#[test]
fn tokio_packets_send_queues_replies_until_flush() {
    use tokio::io::AsyncReadExt as _;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let scenario = async {
        let listener = tokio::net::TcpListener::bind(loopback_addr(0))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let mut client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect");
        let (server, _) = listener.accept().await.expect("accept");
        let mut packets = TokioHandshake::new(server, MaxInboundPacketSize::DEFAULT)
            .into_packets(ProtocolVersion::MQTT3, MaxInboundPacketSize::DEFAULT);

        for packet_id in [7, 8] {
            let packet_id = NonZeroU16::new(packet_id).expect("non-zero");
            PacketIo::send(
                &mut packets,
                MqttPacket::V3(PacketV3::PublishAck { packet_id }),
            )
            .await
            .expect("send");
        }
        let mut acks = [0u8; 8];
        let early = tokio::time::timeout(UNFLUSHED_READ_WINDOW, client.read(&mut acks)).await;
        assert!(early.is_err(), "bytes reached the peer before flush: {early:?}");

        PacketIo::flush(&mut packets).await.expect("flush");
        tokio::time::timeout(IO_TIMEOUT, client.read_exact(&mut acks))
            .await
            .expect("read timeout")
            .expect("read PUBACKs");
        assert_eq!(acks[..4], PUBACK_7);
        assert_eq!(acks[4..], PUBACK_8);
    };
    runtime.block_on(async {
        tokio::time::timeout(ADAPTER_SCENARIO_BOUND, scenario)
            .await
            .expect("adapter scenario timeout");
    });
}

/// AC-16, AC-18 — `send` holds replies up to the 8 KiB write boundary:
/// with 2 048 PUBACKs encoded, `write_buffer` holds all 8 192 bytes and the
/// peer reads nothing; `flush` then delivers them and empties the buffer.
#[test]
fn tokio_packets_send_holds_replies_up_to_the_write_boundary() {
    use tokio::io::AsyncReadExt as _;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let scenario = async {
        let listener = tokio::net::TcpListener::bind(loopback_addr(0))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let mut client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect");
        let (server, _) = listener.accept().await.expect("accept");
        let mut packets = TokioHandshake::new(server, MaxInboundPacketSize::DEFAULT)
            .into_packets(ProtocolVersion::MQTT3, MaxInboundPacketSize::DEFAULT);
        let packet_id = NonZeroU16::new(7).expect("non-zero");

        for _ in 0..PUBACKS_AT_THE_WRITE_BOUNDARY {
            PacketIo::send(
                &mut packets,
                MqttPacket::V3(PacketV3::PublishAck { packet_id }),
            )
            .await
            .expect("send");
        }
        let mut queued = vec![0u8; PUBACKS_AT_THE_WRITE_BOUNDARY * PUBACK_7.len()];
        assert_eq!(packets.write_buffer().len(), queued.len());
        let early = tokio::time::timeout(UNFLUSHED_READ_WINDOW, client.read(&mut queued)).await;
        assert!(early.is_err(), "bytes reached the peer before flush: {early:?}");

        PacketIo::flush(&mut packets).await.expect("flush");
        assert!(packets.write_buffer().is_empty(), "flush left bytes queued");
        tokio::time::timeout(IO_TIMEOUT, client.read_exact(&mut queued))
            .await
            .expect("read timeout")
            .expect("read the PUBACKs");
        assert!(
            queued.chunks(PUBACK_7.len()).all(|ack| ack == PUBACK_7),
            "flushed bytes are not PUBACKs"
        );
    };
    runtime.block_on(async {
        tokio::time::timeout(ADAPTER_SCENARIO_BOUND, scenario)
            .await
            .expect("adapter scenario timeout");
    });
}

/// AC-25 — `PacketCodec::decode_eof` mirrors `MqttDecoder`'s truncation
/// report: a header announcing 10 remaining bytes with none supplied before
/// EOF yields `UnexpectedEof` from `next_packet`, not a silent `None`.
#[test]
fn tokio_packets_report_eof_after_a_header_only_packet_as_an_error() {
    use tokio::io::AsyncWriteExt as _;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let scenario = async {
        let listener = tokio::net::TcpListener::bind(loopback_addr(0))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let mut client = tokio::net::TcpStream::connect(addr).await.expect("connect");
        let (server, _) = listener.accept().await.expect("accept");
        let mut packets = TokioHandshake::new(server, MaxInboundPacketSize::DEFAULT)
            .into_packets(ProtocolVersion::MQTT3, MaxInboundPacketSize::DEFAULT);

        client
            .write_all(&[0x30, 0x0A])
            .await
            .expect("write header-only packet");
        client.shutdown().await.expect("shutdown client write");

        let result = tokio::time::timeout(IO_TIMEOUT, PacketIo::next_packet(&mut packets))
            .await
            .expect("next_packet timeout");
        match result {
            Some(Err(e)) => assert_eq!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof,
                "expected UnexpectedEof, got {e:?}"
            ),
            other => panic!("expected Some(Err(UnexpectedEof)), got {other:?}"),
        }
    };
    runtime.block_on(async {
        tokio::time::timeout(ADAPTER_SCENARIO_BOUND, scenario)
            .await
            .expect("adapter scenario timeout");
    });
}

/// AC-25 — the twin's other truncation shape: body bytes of an unfinished
/// packet buffered at EOF. tokio-util's default `decode_eof` reports that as
/// an untyped `Other` error, which the `TruncatedPacket` evidence policy in
/// `join` cannot recognise, so the kind is pinned here as well.
#[test]
fn tokio_packets_report_eof_inside_a_packet_body_as_an_error() {
    use tokio::io::AsyncWriteExt as _;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let scenario = async {
        let listener = tokio::net::TcpListener::bind(loopback_addr(0))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let mut client = tokio::net::TcpStream::connect(addr).await.expect("connect");
        let (server, _) = listener.accept().await.expect("accept");
        let mut packets = TokioHandshake::new(server, MaxInboundPacketSize::DEFAULT)
            .into_packets(ProtocolVersion::MQTT3, MaxInboundPacketSize::DEFAULT);

        client
            .write_all(&[0x30, 0x0A, 0x00, 0x01, b't'])
            .await
            .expect("write a packet missing its last body bytes");
        client.shutdown().await.expect("shutdown client write");

        let result = tokio::time::timeout(IO_TIMEOUT, PacketIo::next_packet(&mut packets))
            .await
            .expect("next_packet timeout");
        match result {
            Some(Err(e)) => assert_eq!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof,
                "expected UnexpectedEof, got {e:?}"
            ),
            other => panic!("expected Some(Err(UnexpectedEof)), got {other:?}"),
        }
    };
    runtime.block_on(async {
        tokio::time::timeout(ADAPTER_SCENARIO_BOUND, scenario)
            .await
            .expect("adapter scenario timeout");
    });
}

/// AC-10 — a PUBACK owed before a partial packet reaches the client with
/// no further input: the PUBLISH and all but the last byte of a second
/// one go out in one write, and the first PUBACK must arrive before the
/// client sends that last byte.
#[test]
fn twin_acks_a_publish_while_the_next_packet_is_still_partial() {
    let _exclusive = twin_exclusive();
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    std::thread::Builder::new()
        .name("twin-partial-ack".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), String> {
                let port = bench::free_port();
                let config = BrokerConfig::new(format!("127.0.0.1:{port}")).num_workers(1);
                let (callback, _sink) = collecting_callback();
                let handle = twin::start(config, callback)?;

                let mut client =
                    connect_v3(loopback_addr(port)).map_err(|e| format!("connect: {e}"))?;
                client
                    .set_read_timeout(Some(PARTIAL_ACK_READ_BOUND))
                    .map_err(|e| format!("set read timeout: {e}"))?;
                let split = V3_PUBLISH_QOS1_ID7.len() - 1;
                let mut first = V3_PUBLISH_QOS1_ID7.to_vec();
                first.extend_from_slice(&V3_PUBLISH_QOS1_ID7[..split]);
                client
                    .write_all(&first)
                    .map_err(|e| format!("write publish + partial: {e}"))?;

                let mut puback = [0u8; 4];
                client
                    .read_exact(&mut puback)
                    .map_err(|e| format!("read first puback: {e}"))?;
                if puback != PUBACK_7 {
                    return Err(format!("unexpected first PUBACK {puback:?}"));
                }
                client
                    .write_all(&V3_PUBLISH_QOS1_ID7[split..])
                    .map_err(|e| format!("write last byte: {e}"))?;
                client
                    .read_exact(&mut puback)
                    .map_err(|e| format!("read second puback: {e}"))?;
                if puback != PUBACK_7 {
                    return Err(format!("unexpected second PUBACK {puback:?}"));
                }

                handle.shutdown_handle().shutdown();
                handle.join().map_err(|e| format!("join: {e}"))?;
                Ok(())
            })();
            let _ = tx.send(outcome);
        })
        .expect("spawn harness");

    let Ok(result) = rx.recv_timeout(HARNESS_TIMEOUT) else {
        panic!("harness did not report within {HARNESS_TIMEOUT:?}")
    };
    result.expect("twin must ack a publish while the next packet is still partial");
}
