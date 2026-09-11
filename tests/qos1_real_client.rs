//! AC-31 — the epic's Done-when, that no other task in F2.1 covers: a real
//! third-party MQTT client library (`rumqttc`) drives the broker over a TCP
//! socket, publishes at QoS 1 and gets a PUBACK back, then sends a wildcard
//! SUBSCRIBE that the broker refuses with a SUBACK carrying one failure
//! code, then publishes at QoS 1 again to prove the session survived. The
//! fixtures used in the in-crate tests are hand-written byte literals; this
//! test confirms a real decoder, not `rmqtt-codec`, agrees with them.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use uring_mqtt::{BrokerConfig, MqttBroker};

/// One item the driver's forwarding channel carries: a rumqttc event wrapped
/// in `Result`, the way `Connection::recv` yields it.
type EventMsg = Result<rumqttc::Event, rumqttc::ConnectionError>;
/// Receiving end of the driver's forwarding channel. Aliased so clippy's
/// `type_complexity` does not flag the signature of `next_event`.
type EventReceiver = Receiver<EventMsg>;

/// Client identifier the rumqttc connection presents on CONNECT.
const CLIENT_ID: &str = "uring-mqtt-itest";
/// `set_keep_alive` argument — short enough that the broker's idle timeout
/// (`idle_timeout_secs`, default 300 s) never fires in this test, long
/// enough to let the broker's PINGRESP loop run if it wants to.
const KEEP_ALIVE: Duration = Duration::from_secs(5);
/// `Client::new` capacity argument — the bound on rumqttc's own request
/// queue, which the test fills with two publishes, one subscribe and one
/// disconnect. The channel that ferries events back to the test thread is a
/// separate, unbounded `std::sync::mpsc`.
const REQUEST_CHANNEL_CAP: usize = 16;
/// Per-worker drain deadline — same value the sibling test
/// `tests/broker_handle_api.rs` uses; short so teardown is bounded.
const DRAIN_TIMEOUT_SECS: u64 = 1;
/// Outer watchdog for the harness thread. The harness's `BrokerHandle::drop`
/// blocking teardown runs under this bound.
const HARNESS_TIMEOUT: Duration = Duration::from_secs(45);
/// Window for the very first `ConnAck` to land after the harness reports
/// startup complete.
const CONNACK_BOUND: Duration = Duration::from_secs(5);
/// Window for a QoS 1 `PubAck` to land after a `publish` call returns `Ok`.
const ACK_BOUND: Duration = Duration::from_secs(5);
/// Window for a `SubAck` to land after a `subscribe` call returns `Ok`.
const SUBACK_BOUND: Duration = Duration::from_secs(5);
/// Window for the driver thread to observe the disconnect take effect
/// (terminal iterator item) after `client.disconnect()` is called.
const DISCONNECT_OBSERVE_BOUND: Duration = Duration::from_secs(5);
/// Upper bound on joining the driver thread — exceeded only by a leak.
const DRIVER_JOIN_BOUND: Duration = Duration::from_secs(10);
/// How often the bounded join re-checks `JoinHandle::is_finished`. Short
/// relative to `DRIVER_JOIN_BOUND` so a healthy exit is not padded by a
/// whole interval, long enough not to spin.
const DRIVER_POLL_INTERVAL: Duration = Duration::from_millis(50);
/// Harness thread name — visible in `top -H` and panic backtraces.
const HARNESS_THREAD_NAME: &str = "itest-qos1-harness";
/// Driver thread name — same purpose.
const DRIVER_THREAD_NAME: &str = "itest-qos1-driver";
/// SensorV1 payload bytes the same fixtures the in-crate tests use:
/// temperature 25.00 °C (0x09C4 = 2500) and pressure 1013 hPa (0x03F5).
const SENSOR_PAYLOAD: [u8; 4] = [0x09, 0xC4, 0x03, 0xF5];

fn find_free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    l.local_addr().expect("local_addr").port()
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
#[allow(clippy::result_large_err)]
fn next_event(
    rx: &EventReceiver,
    bound: Duration,
    teardown: &AtomicBool,
    session_open: &AtomicBool,
) -> Result<rumqttc::Event, RumqttcOutcome> {
    let started = Instant::now();
    loop {
        let remaining = bound.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(RumqttcOutcome::TimedOut(bound));
        }
        match rx.recv_timeout(remaining) {
            Ok(Ok(event)) => match event {
                rumqttc::Event::Outgoing(_)
                | rumqttc::Event::Incoming(rumqttc::Packet::PingResp) => {}
                rumqttc::Event::Incoming(packet @ rumqttc::Packet::ConnAck(_)) => {
                    if session_open.load(Ordering::SeqCst) {
                        return Err(RumqttcOutcome::UnexpectedConnAck);
                    }
                    session_open.store(true, Ordering::SeqCst);
                    return Ok(rumqttc::Event::Incoming(packet));
                }
                rumqttc::Event::Incoming(rumqttc::Packet::Disconnect) => {
                    if session_open.load(Ordering::SeqCst) && !teardown.load(Ordering::SeqCst) {
                        return Err(RumqttcOutcome::UnexpectedDisconnect);
                    }
                    return Ok(rumqttc::Event::Incoming(rumqttc::Packet::Disconnect));
                }
                incoming @ rumqttc::Event::Incoming(_) => return Ok(incoming),
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
fn await_outgoing_disconnect(rx: &EventReceiver, bound: Duration) -> Result<(), RumqttcOutcome> {
    let started = Instant::now();
    loop {
        let remaining = bound.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(RumqttcOutcome::TimedOut(bound));
        }
        match rx.recv_timeout(remaining) {
            Ok(Ok(rumqttc::Event::Outgoing(rumqttc::Outgoing::Disconnect))) => return Ok(()),
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

/// Distinguish the kinds of failure the test's waits can produce, so the
/// panic messages name the actual cause.
#[allow(clippy::result_large_err)]
enum RumqttcOutcome {
    TimedOut(Duration),
    UnexpectedConnAck,
    UnexpectedDisconnect,
    ConnectionError(rumqttc::ConnectionError),
    ClosedAfterDisconnect(rumqttc::ConnectionError),
    DriverChannelClosed,
    DisconnectNeverSent,
}

impl RumqttcOutcome {
    fn describe(&self) -> String {
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

fn assert_outcome(outcome: Result<rumqttc::Event, RumqttcOutcome>, label: &str) -> rumqttc::Event {
    match outcome {
        Ok(event) => event,
        Err(err) => panic!("{label}: {}", err.describe()),
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn qos1_publish_acked_and_session_survives_refused_subscribe() {
    let (harness_tx, harness_rx) = std::sync::mpsc::channel::<Result<u16, String>>();
    let (teardown_tx, teardown_rx) = std::sync::mpsc::channel::<Result<(), String>>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let port = find_free_port();
    let harness_port = port;
    let _harness = thread::Builder::new()
        .name(HARNESS_THREAD_NAME.to_string())
        .spawn(move || {
            let outcome: Result<(), String> = (|| -> Result<(), String> {
                let addr = format!("127.0.0.1:{harness_port}");
                let config = BrokerConfig::new(addr)
                    .num_workers(1)
                    .drain_timeout_secs(DRAIN_TIMEOUT_SECS);
                let handle = MqttBroker::start(config).map_err(|e| e.to_string())?;
                let _ = harness_tx.send(Ok(harness_port));
                // Block until the test thread signals it is done with the
                // broker. Without this, `handle` would drop here, the
                // broker would shut down, and the rumqttc client would hit
                // "Connection refused" before its first packet left.
                // Bounded so a test that hangs cannot leak the harness.
                match release_rx.recv_timeout(HARNESS_TIMEOUT) {
                    Ok(())
                    | Err(
                        std::sync::mpsc::RecvTimeoutError::Timeout
                        | std::sync::mpsc::RecvTimeoutError::Disconnected,
                    ) => {}
                }
                // Teardown BEFORE reporting, the same way the sibling
                // harnesses in `tests/broker_handle_api.rs` do it: the
                // shutdown drain and the blocking worker joins run here,
                // inside the window the test's completion wait bounds, so a
                // stalled `BrokerHandle::drop` fails the test instead of
                // outliving it unobserved.
                drop(handle);
                Ok(())
            })();
            if let Err(ref e) = outcome {
                let _ = harness_tx.send(Err(e.clone()));
            }
            let _ = teardown_tx.send(outcome);
        })
        .expect("spawn harness");

    // Wait for the harness to confirm the broker is listening.
    let port = match harness_rx.recv_timeout(HARNESS_TIMEOUT) {
        Ok(Ok(port)) => port,
        Ok(Err(e)) => panic!("harness failed to start broker: {e}"),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("harness did not report within {HARNESS_TIMEOUT:?}")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("harness channel closed before reporting startup")
        }
    };

    // Build the rumqttc blocking client. `set_keep_alive` is short enough
    // that the broker's idle timer is never at risk; long enough that the
    // broker's idle loop (if it pings) does not interfere with our timing.
    let mut opts = rumqttc::MqttOptions::new(CLIENT_ID, "127.0.0.1", port);
    opts.set_keep_alive(KEEP_ALIVE);
    let (client, mut connection) = rumqttc::Client::new(opts, REQUEST_CHANNEL_CAP);

    let (event_tx, event_rx): (Sender<EventMsg>, EventReceiver) = std::sync::mpsc::channel();
    let teardown = Arc::new(AtomicBool::new(false));
    let session_open = Arc::new(AtomicBool::new(false));

    // Driver thread: forward every event the connection yields to the main
    // test thread, never swallowing an Err. `recv()` blocks until the
    // eventloop produces the next item, so the loop needs no polling: it
    // ends on the connection's own terminal item or on a failed send once
    // the test thread has dropped the receiver.
    //
    // It also CLASSIFIES its terminal error itself, against a shared clone
    // of the teardown flag, and reports the verdict through its join value.
    // Classifying at consumption time instead would be racy: an error that
    // arrives inside the session window can sit queued behind an
    // acknowledgment the main thread is still consuming, and by the time the
    // main thread reaches it the flag has flipped — the session-window
    // violation would then be read as an ordinary post-disconnect close. The
    // driver reads the flag at the instant the error is observed, which is
    // the only race-free point.
    let driver_teardown = Arc::clone(&teardown);
    let driver = thread::Builder::new()
        .name(DRIVER_THREAD_NAME.to_string())
        .spawn(move || -> Result<(), String> {
            loop {
                match connection.recv() {
                    Ok(Ok(event)) => {
                        if event_tx.send(Ok(event)).is_err() {
                            return Ok(());
                        }
                    }
                    Ok(Err(err)) => {
                        let in_session = !driver_teardown.load(Ordering::SeqCst);
                        let described = err.to_string();
                        let _ = event_tx.send(Err(err));
                        return if in_session {
                            Err(format!(
                                "connection failed while the session was open: {described}"
                            ))
                        } else {
                            Ok(())
                        };
                    }
                    Err(rumqttc::RecvError) => return Ok(()),
                }
            }
        })
        .expect("spawn driver");

    // Step 3 — wait for the first ConnAck.
    let connack = assert_outcome(
        next_event(&event_rx, CONNACK_BOUND, &teardown, &session_open),
        "first ConnAck",
    );
    assert!(
        matches!(
            connack,
            rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))
        ),
        "first event was {connack:?}",
    );

    // Step 4 — first QoS 1 publish.
    client
        .publish("t", rumqttc::QoS::AtLeastOnce, false, SENSOR_PAYLOAD)
        .expect("first publish call");
    let puback_1 = assert_outcome(
        next_event(&event_rx, ACK_BOUND, &teardown, &session_open),
        "first PubAck",
    );
    let first_pkid = match puback_1 {
        rumqttc::Event::Incoming(rumqttc::Packet::PubAck(ack)) => ack.pkid,
        other => panic!("expected PubAck, got {other:?}"),
    };

    // Step 5 — wildcard SUBSCRIBE that the broker must refuse with one
    // failure code (AC-26: every shape — wildcard, +, $share/ — gets the
    // same refusal path).
    client
        .subscribe("t/#", rumqttc::QoS::AtMostOnce)
        .expect("subscribe call");
    let suback = assert_outcome(
        next_event(&event_rx, SUBACK_BOUND, &teardown, &session_open),
        "SubAck",
    );
    let return_codes = match suback {
        rumqttc::Event::Incoming(rumqttc::Packet::SubAck(ack)) => ack.return_codes,
        other => panic!("expected SubAck, got {other:?}"),
    };
    assert_eq!(
        return_codes.len(),
        1,
        "broker returned {} reason codes for a single-filter SUBSCRIBE",
        return_codes.len(),
    );
    assert!(
        return_codes
            .iter()
            .all(|c| matches!(c, rumqttc::SubscribeReasonCode::Failure)),
        "expected every SubAck reason code to be Failure, got {return_codes:?}",
    );

    // Step 6 — second QoS 1 publish, correlated by packet id so the
    // first publish's PubAck cannot be counted twice.
    client
        .publish("t", rumqttc::QoS::AtLeastOnce, false, SENSOR_PAYLOAD)
        .expect("second publish call");
    let puback_2 = assert_outcome(
        next_event(&event_rx, ACK_BOUND, &teardown, &session_open),
        "second PubAck",
    );
    let second_pkid = match puback_2 {
        rumqttc::Event::Incoming(rumqttc::Packet::PubAck(ack)) => ack.pkid,
        other => panic!("expected PubAck, got {other:?}"),
    };
    assert_ne!(
        first_pkid, second_pkid,
        "first and second PubAck carried the same packet id {first_pkid}",
    );

    // Step 6 — teardown. Order matters: flip the flag FIRST so any Err the
    // driver observes from here on is the expected terminal one, not a
    // session-window violation. `disconnect()` only enqueues a request, so
    // its `Ok` says nothing about the wire; the outgoing-Disconnect wait
    // below is what proves the packet was written and flushed.
    teardown.store(true, Ordering::SeqCst);
    client.disconnect().expect("client.disconnect");
    drop(client);

    // Bounded join: a leak here would otherwise only surface as a global
    // suite stall, with no clue which thread hung. The driver runs a
    // blocking recv() that does not observe the request-channel closure
    // (rumqttc's EventLoop holds its own Sender clone, 0.24.0
    // `eventloop.rs:80`), so it will only exit when the broker closes the
    // TCP connection — which it does after reading the DISCONNECT we sent.
    // The bounded poll catches any case where the broker never closes.
    let driver_started = Instant::now();
    while !driver.is_finished() {
        assert!(
            driver_started.elapsed() <= DRIVER_JOIN_BOUND,
            "connection driver did not exit within {DRIVER_JOIN_BOUND:?} \
             after client.disconnect()",
        );
        thread::sleep(DRIVER_POLL_INTERVAL);
    }
    // The driver's verdict on its own terminal error. This is what keeps a
    // failure that happened INSIDE the session window from being excused by
    // the now-true teardown flag.
    if let Err(e) = driver.join().expect("connection driver panicked") {
        panic!("connection driver: {e}");
    }

    // Require the outgoing Disconnect the driver forwarded. Discarding this
    // wait would let a session that died before the DISCONNECT was written
    // count as a clean close; rumqttc emits the event only after the write
    // and flush both succeed, so this is the assertion that the requested
    // disconnect really happened.
    if let Err(err) = await_outgoing_disconnect(&event_rx, DISCONNECT_OBSERVE_BOUND) {
        panic!("outgoing Disconnect: {}", err.describe());
    }

    // Drop the receiving end so the driver sees a clean channel close when
    // its last send fails.
    drop(event_rx);

    // Tell the harness thread it may now drop the BrokerHandle, which
    // triggers the broker's shutdown drain and worker join. Done last so
    // every assertion above ran against a live broker.
    let _ = release_tx.send(());

    // Require the harness's post-teardown report before returning: the
    // startup message was sent while the broker was still running, so
    // without this wait the test could pass with the drain still in
    // flight and a stalled `BrokerHandle::drop` invisible.
    match teardown_rx.recv_timeout(HARNESS_TIMEOUT) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("harness reported a failure: {e}"),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("harness did not finish broker teardown within {HARNESS_TIMEOUT:?}")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("harness channel closed before reporting teardown")
        }
    }
}
