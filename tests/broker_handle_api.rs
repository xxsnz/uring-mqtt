//! Public-seam tests for the embedder workflows this feature exists for: an
//! embedder that holds a `BrokerHandle` and never calls `join` notices a dead
//! ingest server through `BrokerHandle::is_running` (AC-10, AC-11), and an
//! embedder that calls `run_with_callback` at the default worker count gets
//! `Err(Error::Worker(_))` back instead of a hang when a callback panics
//! (AC-6, AC-7). The in-crate tests cover the same criteria from inside
//! `src/broker/mod.rs`; these prove the workflows are reachable through the
//! published API only.

use std::io::{Read, Write};
use std::time::{Duration, Instant};
use uring_mqtt::{BrokerConfig, Error, Event, MqttBroker};

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
    0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x00, 0x00, 0x3C, 0x00, 0x04, b't', b'e',
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
/// reading. `EventCallback` is not re-exported at the crate root, so an
/// embedder spells the trait-object type out — as this does.
fn panicking_callback() -> std::sync::Arc<dyn Fn(Event) + Send + Sync> {
    std::sync::Arc::new(|event| {
        let Event::SensorV1 { temperature, .. } = event;
        assert!(
            temperature != PANIC_TEMPERATURE,
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
