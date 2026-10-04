//! Public-seam tests for the embedder workflows this feature exists for: an
//! embedder that holds a `BrokerHandle` and never calls `join` notices a dead
//! ingest server through `BrokerHandle::is_running` (AC-10, AC-11), and an
//! embedder that calls `run_with_callback` at the default worker count gets
//! `Err(Error::Worker(_))` back instead of a hang when a callback panics
//! (AC-6, AC-7). An embedder also builds a `Publish` with the public
//! constructor and runs its own callback against it, with no server at all
//! (AC-22). The in-crate tests cover the same criteria from inside
//! `src/broker/mod.rs`; these prove the workflows are reachable through the
//! published API only.

use std::io::{Read, Write};
use std::time::{Duration, Instant};
use uring_mqtt::{
    BrokerConfig, Error, MaxInboundPacketSize, MqttBroker, Publish, PublishCallback, QoS,
};

/// Workers started by the retained-handle test — more than one, so the
/// signal is not trivially satisfied by a single dying thread.
const WORKERS: usize = 2;
/// Per-worker drain deadline; short so the whole test stays bounded.
const DRAIN_TIMEOUT_SECS: u64 = 1;
/// `is_running` poll cadence and ceiling: 40 × 250 ms = 10 s.
const POLL_INTERVAL: Duration = Duration::from_millis(250);
const POLL_ATTEMPTS: usize = 40;
/// Outer watchdog: the harness thread must report within this budget,
/// including its `BrokerHandle::drop` teardown.
const HARNESS_TIMEOUT: Duration = Duration::from_secs(45);

/// v3 CONNECT: ka=60, client id "test" — same fixture as the in-crate suite.
const V3_CONNECT: [u8; 18] = [
    0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x04, b't', b'e',
    b's', b't',
];
/// Successful v3 CONNACK.
const CONNACK_OK: [u8; 4] = [0x20, 0x02, 0x00, 0x00];
/// v3 PUBLISH, topic "t", 4-byte sensor payload. Bytes 5-6 are the
/// big-endian temperature the callback keys on.
const V3_PUBLISH: [u8; 9] = [0x30, 0x07, 0x00, 0x01, b't', 0x09, 0xC4, 0x03, 0xF5];
/// Temperature whose delivery makes the test callback panic.
const PANIC_TEMPERATURE: i16 = -1;
/// Client connect retry budget: 40 × 250 ms = 10 s. Generous because the
/// `run_with_callback` test races the server's own startup.
const CONNECT_ATTEMPTS: usize = 40;
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// Read/write deadline on a connected client socket.
const IO_TIMEOUT: Duration = Duration::from_secs(10);
/// `run_with_callback` must return within this bound after the triggering
/// PUBLISH; generously above the 1 s drain, far below any hang.
const RUN_RETURN_BOUND: Duration = Duration::from_secs(30);
/// Watchdog for the same test: exceeded only by an actual hang.
const RUN_WATCHDOG: Duration = Duration::from_secs(75);

fn find_free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    l.local_addr().expect("local_addr").port()
}

/// Connect to `addr` (retrying while the server may still be starting),
/// complete the MQTT v3 handshake, and publish one sensor reading carrying
/// `temperature`. The connected socket is returned so the caller can keep
/// the connection open. Which worker serves it is the kernel's
/// `SO_REUSEPORT` choice — every assertion here holds for any of them.
fn connect_and_publish(
    addr: std::net::SocketAddr,
    temperature: i16,
) -> std::io::Result<std::net::TcpStream> {
    let mut connected = None;
    for _ in 0..CONNECT_ATTEMPTS {
        match std::net::TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
            Ok(s) => {
                connected = Some(s);
                break;
            }
            Err(_) => std::thread::sleep(CONNECT_RETRY_INTERVAL),
        }
    }
    let Some(mut client) = connected else {
        return Err(std::io::Error::other(
            "no connection within the retry window",
        ));
    };
    client.set_read_timeout(Some(IO_TIMEOUT))?;
    client.set_write_timeout(Some(IO_TIMEOUT))?;
    client.write_all(&V3_CONNECT)?;
    let mut connack = [0u8; 4];
    client.read_exact(&mut connack)?;
    if connack != CONNACK_OK {
        return Err(std::io::Error::other(format!(
            "unexpected CONNACK {connack:?}"
        )));
    }
    let mut publish = V3_PUBLISH;
    publish[5..7].copy_from_slice(&temperature.to_be_bytes());
    client.write_all(&publish)?;
    Ok(client)
}

/// A callback that panics on `PANIC_TEMPERATURE` and ignores every other
/// reading. `PublishCallback` is re-exported at the crate root, so the
/// embedder uses the named alias rather than spelling the trait-object type.
fn panicking_callback() -> PublishCallback {
    std::sync::Arc::new(|event: &Publish| {
        let payload = event.payload();
        assert!(
            payload.len() >= 2,
            "test publishes must encode the marker in payload[0..2]"
        );
        let marker = i16::from_be_bytes([payload[0], payload[1]]);
        assert!(
            marker != PANIC_TEMPERATURE,
            "integration test callback panic"
        );
    })
}

/// AC-10, AC-11 — a handle-holding embedder sees `true` on a healthy server
/// and `false` once a worker has exited, without consuming the handle and
/// without blocking in `join`.
#[test]
fn is_running_reports_worker_death_to_a_non_joining_embedder() {
    let (tx, rx) = std::sync::mpsc::channel::<(bool, bool)>();
    let _h = std::thread::Builder::new()
        .name("api-ac10-is-running".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), Error> {
                let port = find_free_port();
                let config = BrokerConfig::new(format!("127.0.0.1:{port}"))
                    .num_workers(WORKERS)
                    .drain_timeout_secs(DRAIN_TIMEOUT_SECS);

                let handle = MqttBroker::start(config)?;
                let true_while_healthy = handle.is_running();

                handle.shutdown();

                let mut false_after_exit = false;
                for _ in 0..POLL_ATTEMPTS {
                    if !handle.is_running() {
                        false_after_exit = true;
                        break;
                    }
                    std::thread::sleep(POLL_INTERVAL);
                }

                // Teardown before reporting, so the outer watchdog also
                // covers the blocking `Drop`. The handle is never joined:
                // the point is that this signal needs no `join`.
                drop(handle);
                let _ = tx.send((true_while_healthy, false_after_exit));
                Ok(())
            })();
            if outcome.is_err() {
                let _ = tx.send((false, false));
            }
        })
        .expect("spawn harness");
    let Ok((true_while_healthy, false_after_exit)) = rx.recv_timeout(HARNESS_TIMEOUT) else {
        panic!("harness did not report within {HARNESS_TIMEOUT:?}")
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

/// AC-6 + AC-7 at the entry point the epic reported as hanging: with the
/// DEFAULT worker count (CPU count) and no explicit shutdown,
/// `run_with_callback` used to block forever on a sibling parked in
/// `cancelable_accept` once a callback panicked. It must now return
/// `Err(Error::Worker(_))` within a bounded window.
#[test]
fn run_with_callback_returns_worker_error_when_a_callback_panics() {
    let port = find_free_port();
    let addr_str = format!("127.0.0.1:{port}");
    let addr: std::net::SocketAddr = addr_str.parse().expect("parse addr");
    // No `num_workers` call: the default is what the reported defect used.
    let config = BrokerConfig::new(addr_str).drain_timeout_secs(DRAIN_TIMEOUT_SECS);

    let (tx, rx) = std::sync::mpsc::channel::<Result<(), Error>>();
    let _h = std::thread::Builder::new()
        .name("api-run-callback-panic".into())
        .spawn(move || {
            let _ = tx.send(MqttBroker::run_with_callback(
                config,
                Some(panicking_callback()),
            ));
        })
        .expect("spawn server");

    let t0 = Instant::now();
    let client = connect_and_publish(addr, PANIC_TEMPERATURE).expect("panic-triggering client");
    let result = match rx.recv_timeout(RUN_WATCHDOG) {
        Ok(result) => result,
        Err(e) => panic!("run_with_callback did not return within {RUN_WATCHDOG:?}: {e:?}"),
    };
    let elapsed = t0.elapsed();
    drop(client);

    assert!(
        matches!(result, Err(Error::Worker(_))),
        "expected Err(Error::Worker(_)), got {result:?}"
    );
    assert!(
        elapsed <= RUN_RETURN_BOUND,
        "run_with_callback returned only after {elapsed:?}"
    );
}

/// AC-22 — the embedder workflow `Publish::new` exists for (Q9): a callback is
/// unit-tested through the published API alone, with no io_uring server. Every
/// QoS variant round-trips through the accessors, and the constructed value
/// drives a real `PublishCallback`.
#[test]
fn publish_constructed_through_the_public_api_drives_a_callback() {
    /// Topic and payload the constructor must hand back unchanged.
    const TOPIC: &str = "sensors/rack-1/temp";
    const PAYLOAD: [u8; 4] = [0x09, 0xC4, 0x03, 0xF5];

    /// What the callback recorded for one publish: topic, payload length,
    /// QoS and RETAIN.
    type SeenPublish = (String, usize, QoS, bool);
    type SeenSink = std::sync::Arc<std::sync::Mutex<Vec<SeenPublish>>>;

    let seen: SeenSink = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = seen.clone();
    let callback: PublishCallback = std::sync::Arc::new(move |event: &Publish| {
        sink.lock().expect("sink poisoned").push((
            event.topic().to_string(),
            event.payload().len(),
            event.qos(),
            event.retain(),
        ));
    });

    for (qos, retain) in [
        (QoS::AtMostOnce, false),
        (QoS::AtLeastOnce, true),
        (QoS::ExactlyOnce, false),
    ] {
        let publish = Publish::new(TOPIC, &PAYLOAD, qos, retain);
        assert_eq!(publish.topic(), TOPIC, "topic round-trip for {qos:?}");
        assert_eq!(
            publish.payload(),
            &PAYLOAD,
            "payload round-trip for {qos:?}"
        );
        assert_eq!(publish.qos(), qos, "qos round-trip");
        assert_eq!(publish.retain(), retain, "retain round-trip for {qos:?}");
        callback(&publish);
    }

    let seen = seen.lock().expect("sink poisoned").clone();
    assert_eq!(
        seen,
        vec![
            (TOPIC.to_string(), PAYLOAD.len(), QoS::AtMostOnce, false),
            (TOPIC.to_string(), PAYLOAD.len(), QoS::AtLeastOnce, true),
            (TOPIC.to_string(), PAYLOAD.len(), QoS::ExactlyOnce, false),
        ],
        "callback saw every constructed publish unchanged"
    );
}

/// AC-9, AC-10 — the drop total is part of the published surface: an embedder
/// holding a `BrokerHandle` calls `dropped_publishes` and gets 0 while its
/// callback keeps up with real traffic. The in-crate tests force overflow
/// through a test-only hook; from outside the crate only the public getter and
/// its value on a server that drops nothing are observable, which is exactly
/// what an embedder's monitoring loop depends on.
#[test]
fn dropped_publishes_reads_zero_through_the_published_api_while_the_callback_keeps_up() {
    /// Temperature of the one reading this test publishes — any value the
    /// panicking callback does not key on.
    const QUIET_TEMPERATURE: i16 = 2500;

    let (tx, rx) = std::sync::mpsc::channel::<(bool, u64, u64)>();
    let _h = std::thread::Builder::new()
        .name("api-dropped-publishes".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), Error> {
                let port = find_free_port();
                let addr: std::net::SocketAddr = format!("127.0.0.1:{port}")
                    .parse()
                    .expect("parse loopback addr");
                let config = BrokerConfig::new(format!("127.0.0.1:{port}"))
                    .num_workers(WORKERS)
                    .drain_timeout_secs(DRAIN_TIMEOUT_SECS);

                let (seen_tx, seen_rx) = std::sync::mpsc::channel::<usize>();
                let callback: PublishCallback = std::sync::Arc::new(move |publish: &Publish| {
                    let _ = seen_tx.send(publish.payload().len());
                });
                let handle = MqttBroker::start_with_callback(config, Some(callback))?;
                let before = handle.dropped_publishes();

                let client = connect_and_publish(addr, QUIET_TEMPERATURE)
                    .map_err(|_| Error::Worker("publish did not reach the server".into()))?;
                // The callback ran, so the publish was not discarded — the
                // 0 below is a drop total on a server that did ingest, not on
                // an idle one.
                let delivered = seen_rx.recv_timeout(HARNESS_TIMEOUT).is_ok();
                let after = handle.dropped_publishes();

                drop(client);
                handle.shutdown();
                drop(handle);
                let _ = tx.send((delivered, before, after));
                Ok(())
            })();
            if outcome.is_err() {
                let _ = tx.send((false, u64::MAX, u64::MAX));
            }
        })
        .expect("spawn harness");

    let Ok((delivered, before, after)) = rx.recv_timeout(HARNESS_TIMEOUT) else {
        panic!("harness did not report within {HARNESS_TIMEOUT:?}")
    };
    assert!(
        delivered,
        "the callback never ran, so the drop total below would prove nothing"
    );
    assert_eq!(before, 0, "a freshly started server reported a drop");
    assert_eq!(
        after, 0,
        "a publish the callback received was counted as dropped"
    );
}

/// AC-11 by the route the fail-fast policy was chosen for: a worker dies on
/// its own, with no `shutdown()` and no `join()` anywhere, and the embedder
/// holding the handle still sees `is_running` go false. The in-crate test
/// only exercises the `shutdown()` route to worker exit.
#[test]
fn is_running_goes_false_after_a_callback_panic_without_shutdown() {
    let (tx, rx) = std::sync::mpsc::channel::<(bool, bool)>();
    let _h = std::thread::Builder::new()
        .name("api-panic-is-running".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), Error> {
                let port = find_free_port();
                let addr_str = format!("127.0.0.1:{port}");
                let addr: std::net::SocketAddr = addr_str.parse().expect("parse addr");
                let config = BrokerConfig::new(addr_str)
                    .num_workers(WORKERS)
                    .drain_timeout_secs(DRAIN_TIMEOUT_SECS);

                let handle = MqttBroker::start_with_callback(config, Some(panicking_callback()))?;
                let true_while_healthy = handle.is_running();

                let client = connect_and_publish(addr, PANIC_TEMPERATURE).map_err(Error::Io)?;

                let mut false_after_panic = false;
                for _ in 0..POLL_ATTEMPTS {
                    if !handle.is_running() {
                        false_after_panic = true;
                        break;
                    }
                    std::thread::sleep(POLL_INTERVAL);
                }

                // Teardown before reporting, so the outer watchdog also
                // covers the blocking `Drop`. `join` is never called: the
                // panic alone had to produce the signal.
                drop(client);
                drop(handle);
                let _ = tx.send((true_while_healthy, false_after_panic));
                Ok(())
            })();
            if outcome.is_err() {
                let _ = tx.send((false, false));
            }
        })
        .expect("spawn harness");
    let Ok((true_while_healthy, false_after_panic)) = rx.recv_timeout(HARNESS_TIMEOUT) else {
        panic!("harness did not report within {HARNESS_TIMEOUT:?}")
    };
    assert!(
        true_while_healthy,
        "is_running was false on a freshly started server"
    );
    assert!(
        false_after_panic,
        "is_running stayed true for more than 10 s after a callback panic"
    );
}

/// AC-8, AC-9 — a configured, non-default bound reaches enforcement through
/// the worker path: the server hands a PUBLISH exactly at the bound to the
/// callback and closes the connection for one PUBLISH one byte over it, with
/// no partial delivery of the over-bound payload.
#[test]
fn configured_inbound_bound_delivers_at_the_bound_and_closes_one_byte_over() {
    /// Non-default bound this test configures the server with.
    const CONFIGURED_BOUND: u32 = 64;
    /// Temperature of the baseline reading — any value the panicking
    /// callback does not key on.
    const QUIET_TEMPERATURE_FOR_BOUND: i16 = 2500;

    let (tx, rx) = std::sync::mpsc::channel::<(bool, Vec<usize>)>();
    let _h = std::thread::Builder::new()
        .name("api-inbound-bound".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), Error> {
                let port = find_free_port();
                let addr: std::net::SocketAddr = format!("127.0.0.1:{port}")
                    .parse()
                    .expect("parse loopback addr");
                let config = BrokerConfig::new(format!("127.0.0.1:{port}"))
                    .num_workers(1)
                    .drain_timeout_secs(DRAIN_TIMEOUT_SECS)
                    .max_inbound_packet_size(
                        MaxInboundPacketSize::new(CONFIGURED_BOUND).expect("in range"),
                    );

                let (seen_tx, seen_rx) = std::sync::mpsc::channel::<usize>();
                let callback: PublishCallback = std::sync::Arc::new(move |publish: &Publish| {
                    let _ = seen_tx.send(publish.payload().len());
                });
                let handle = MqttBroker::start_with_callback(config, Some(callback))?;

                let mut client =
                    connect_and_publish(addr, QUIET_TEMPERATURE_FOR_BOUND).map_err(|_| {
                        Error::Worker("baseline publish did not reach the server".into())
                    })?;
                let baseline = seen_rx
                    .recv_timeout(HARNESS_TIMEOUT)
                    .map_err(|_| Error::Worker("baseline publish not delivered".into()))?;
                assert_eq!(
                    baseline, 4,
                    "baseline publish must be the 4-byte sensor payload"
                );

                let mut at_bound = vec![0x30, 0x3E, 0x00, 0x01, b't'];
                at_bound.extend(std::iter::repeat_n(0u8, 59));
                client
                    .write_all(&at_bound)
                    .map_err(|_| Error::Worker("at-bound publish write failed".into()))?;
                let at_bound_len = seen_rx
                    .recv_timeout(HARNESS_TIMEOUT)
                    .map_err(|_| Error::Worker("at-bound publish not delivered".into()))?;
                assert_eq!(at_bound_len, 59, "at-bound publish must reach the callback");

                let mut over_bound = vec![0x30, 0x3F, 0x00, 0x01, b't'];
                over_bound.extend(std::iter::repeat_n(0u8, 60));
                client
                    .write_all(&over_bound)
                    .map_err(|_| Error::Worker("over-bound publish write failed".into()))?;

                let mut probe = [0u8; 1];
                let closed = match client.read(&mut probe) {
                    Ok(0) => true,
                    Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => true,
                    Ok(_) | Err(_) => false,
                };

                drop(client);
                handle.shutdown();
                drop(handle);
                let late: Vec<usize> = seen_rx.try_iter().collect();
                let _ = tx.send((closed, late));
                Ok(())
            })();
            if outcome.is_err() {
                let _ = tx.send((false, Vec::new()));
            }
        })
        .expect("spawn harness");

    let Ok((closed, late)) = rx.recv_timeout(HARNESS_TIMEOUT) else {
        panic!("harness did not report within {HARNESS_TIMEOUT:?}")
    };
    assert!(
        closed,
        "the over-bound PUBLISH did not close the connection"
    );
    assert!(
        late.is_empty(),
        "the over-bound payload reached the callback: {late:?}"
    );
}

/// AC-19 — a configured, non-default bound reaches the version-detection
/// phase too: a header-only first packet over the configured bound (but far
/// under the default) closes within the read bound. This proves
/// `VersionDecoder` receives the configured bound, not the default.
#[test]
fn configured_inbound_bound_closes_a_header_only_first_packet_at_once() {
    /// Non-default bound this test configures the server with — same value
    /// as `configured_inbound_bound_delivers_at_the_bound_and_closes_one_byte_over`.
    const CONFIGURED_BOUND: u32 = 64;
    /// Handshake budget generous enough that a timeout close cannot pass as
    /// the bound close.
    const HEADER_ONLY_HANDSHAKE_SECS: u64 = 10;
    /// Read-timeout bound on the probe read after the header-only write.
    const HEADER_ONLY_CLOSE_BOUND: Duration = Duration::from_secs(1);

    let (tx, rx) = std::sync::mpsc::channel::<bool>();
    let _h = std::thread::Builder::new()
        .name("api-header-only-bound".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), Error> {
                let port = find_free_port();
                let addr: std::net::SocketAddr = format!("127.0.0.1:{port}")
                    .parse()
                    .expect("parse loopback addr");
                let config = BrokerConfig::new(format!("127.0.0.1:{port}"))
                    .num_workers(1)
                    .drain_timeout_secs(DRAIN_TIMEOUT_SECS)
                    .connection_timeout_secs(HEADER_ONLY_HANDSHAKE_SECS)
                    .max_inbound_packet_size(
                        MaxInboundPacketSize::new(CONFIGURED_BOUND).expect("in range"),
                    );

                let handle = MqttBroker::start(config)?;

                let mut connected = None;
                for _ in 0..CONNECT_ATTEMPTS {
                    match std::net::TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
                        Ok(s) => {
                            connected = Some(s);
                            break;
                        }
                        Err(_) => std::thread::sleep(CONNECT_RETRY_INTERVAL),
                    }
                }
                let Some(mut client) = connected else {
                    return Err(Error::Worker(
                        "no connection within the retry window".into(),
                    ));
                };
                client
                    .set_read_timeout(Some(HEADER_ONLY_CLOSE_BOUND))
                    .map_err(Error::Io)?;
                client.write_all(&[0x10, 0x3F]).map_err(Error::Io)?;

                let mut probe = [0u8; 1];
                let closed = match client.read(&mut probe) {
                    Ok(0) => true,
                    Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => true,
                    Ok(_) | Err(_) => false,
                };

                drop(client);
                handle.shutdown();
                drop(handle);
                let _ = tx.send(closed);
                Ok(())
            })();
            if outcome.is_err() {
                let _ = tx.send(false);
            }
        })
        .expect("spawn harness");

    let Ok(closed) = rx.recv_timeout(HARNESS_TIMEOUT) else {
        panic!("harness did not report within {HARNESS_TIMEOUT:?}")
    };
    assert!(
        closed,
        "the header-only over-the-configured-bound first packet did not close the connection"
    );
}

/// AC-14 — a configured, non-default inbound bound reaches the v5 CONNACK's
/// Maximum Packet Size property through the public API, not just the
/// in-crate seam.
#[test]
fn v5_connack_advertises_the_configured_inbound_bound() {
    /// Non-default bound this test configures the server with.
    const ADVERTISED_BOUND: u32 = 4096;
    /// v5 CONNECT: ka=60, client id "test".
    const V5_CONNECT: [u8; 19] = [
        0x10, 0x11, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00, 0x04,
        b't', b'e', b's', b't',
    ];
    /// Accept CONNACK advertising a 4096-byte Maximum Packet Size.
    const V5_CONNACK_ADVERTISING_4096: [u8; 15] = [
        0x20, 0x0D, 0x00, 0x00, 0x0A, 0x11, 0x00, 0x00, 0x00, 0x00, 0x27, 0x00, 0x00, 0x10, 0x00,
    ];

    let (tx, rx) = std::sync::mpsc::channel::<Result<[u8; 15], String>>();
    let _h = std::thread::Builder::new()
        .name("api-v5-connack-bound".into())
        .spawn(move || {
            let outcome = (|| -> Result<(), Error> {
                let port = find_free_port();
                let addr: std::net::SocketAddr = format!("127.0.0.1:{port}")
                    .parse()
                    .expect("parse loopback addr");
                let config = BrokerConfig::new(format!("127.0.0.1:{port}"))
                    .num_workers(1)
                    .drain_timeout_secs(DRAIN_TIMEOUT_SECS)
                    .max_inbound_packet_size(
                        MaxInboundPacketSize::new(ADVERTISED_BOUND).expect("in range"),
                    );

                let handle = MqttBroker::start(config)?;

                let mut connected = None;
                for _ in 0..CONNECT_ATTEMPTS {
                    match std::net::TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
                        Ok(s) => {
                            connected = Some(s);
                            break;
                        }
                        Err(_) => std::thread::sleep(CONNECT_RETRY_INTERVAL),
                    }
                }
                let Some(mut client) = connected else {
                    return Err(Error::Worker(
                        "no connection within the retry window".into(),
                    ));
                };
                client
                    .set_read_timeout(Some(IO_TIMEOUT))
                    .map_err(Error::Io)?;
                client
                    .set_write_timeout(Some(IO_TIMEOUT))
                    .map_err(Error::Io)?;
                client.write_all(&V5_CONNECT).map_err(Error::Io)?;

                let mut connack = [0u8; 15];
                let read_result = client
                    .read_exact(&mut connack)
                    .map_err(|e| e.to_string())
                    .map(|()| connack);
                let _ = tx.send(read_result);

                drop(client);
                handle.shutdown();
                drop(handle);
                Ok(())
            })();
            if let Err(e) = outcome {
                let _ = tx.send(Err(e.to_string()));
            }
        })
        .expect("spawn harness");

    let Ok(read_result) = rx.recv_timeout(HARNESS_TIMEOUT) else {
        panic!("harness did not report within {HARNESS_TIMEOUT:?}")
    };
    let connack = read_result.expect("CONNACK read");
    assert_eq!(
        connack, V5_CONNACK_ADVERTISING_4096,
        "accept CONNACK must advertise the configured 4096-byte inbound bound"
    );
}
