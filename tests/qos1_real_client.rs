//! AC-31 — the epic's Done-when, that no other task in F2.1 covers: a real
//! third-party MQTT client library (`rumqttc`) drives the broker over a TCP
//! socket, publishes at QoS 1 and gets a PUBACK back, then sends a wildcard
//! SUBSCRIBE that the broker refuses with a SUBACK carrying one failure
//! code, then publishes at QoS 1 again to prove the session survived, and
//! finally publishes at QoS 2 and completes PUBREC → PUBREL → PUBCOMP. The
//! fixtures used in the in-crate tests are hand-written byte literals; this
//! test confirms a real decoder, not `rmqtt-codec`, agrees with them.

#[allow(dead_code)] // each test binary uses a different subset of the shared harness
mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;

/// One item the driver's forwarding channel carries: a rumqttc event wrapped
/// in `Result`, the way `Connection::recv` yields it.
type EventMsg = Result<rumqttc::Event, rumqttc::ConnectionError>;
/// Receiving end of the driver's forwarding channel. Aliased so clippy's
/// `type_complexity` does not flag the channel's `let` annotation.
type EventReceiver = Receiver<EventMsg>;

/// Client identifier the rumqttc connection presents on CONNECT.
const CLIENT_ID: &str = "uring-mqtt-itest";
/// `set_keep_alive` argument. The broker negotiates a v3 idle deadline of
/// 1.5x this value (7.5 s, capped by `idle_timeout_secs`). The broker sends
/// no pings of its own: rumqttc 0.24 schedules a PINGREQ every interval
/// regardless of traffic, which normally keeps the connection inside that
/// deadline, and the broker answers each with a PINGRESP that `next_event`
/// skips.
const KEEP_ALIVE: Duration = Duration::from_secs(5);
/// `Client::new` capacity argument — the bound on rumqttc's own request
/// queue, which the test fills with three publishes, one subscribe and one
/// disconnect. The channel that ferries events back to the test thread is a
/// separate, unbounded `std::sync::mpsc`.
const REQUEST_CHANNEL_CAP: usize = 16;
/// Per-worker drain deadline — same value the sibling test
/// `tests/broker_handle_api.rs` uses; short so teardown is bounded.
const DRAIN_TIMEOUT_SECS: u64 = 1;
/// Window for the very first `ConnAck` to land after the harness reports
/// startup complete.
const CONNACK_BOUND: Duration = Duration::from_secs(5);
/// Window for a QoS 1 `PubAck`, or a QoS 2 `PubRec` / `PubComp`, to land
/// after the triggering event.
const ACK_BOUND: Duration = Duration::from_secs(5);
/// Window for a `SubAck` to land after a `subscribe` call returns `Ok`.
const SUBACK_BOUND: Duration = Duration::from_secs(5);
/// Window for the driver thread to observe the disconnect take effect
/// (terminal iterator item) after `client.disconnect()` is called.
const DISCONNECT_OBSERVE_BOUND: Duration = Duration::from_secs(5);
/// Upper bound on joining the driver thread — exceeded only by a leak.
const DRIVER_JOIN_BOUND: Duration = Duration::from_secs(10);
/// Driver thread name — visible in `top -H` and panic backtraces.
const DRIVER_THREAD_NAME: &str = "itest-mqtt-driver";
/// SensorV1 payload bytes the same fixtures the in-crate tests use:
/// temperature 25.00 °C (0x09C4 = 2500) and pressure 1013 hPa (0x03F5).
const SENSOR_PAYLOAD: [u8; 4] = [0x09, 0xC4, 0x03, 0xF5];
/// Wait bound for the `next_event` guard tests, which feed the channel
/// themselves: every message is already queued, so the bound is only the
/// margin that keeps a scheduling hiccup from timing the wait out.
const GUARD_BOUND: Duration = Duration::from_secs(1);

#[test]
#[allow(clippy::too_many_lines)]
fn qos1_and_qos2_publishes_complete_and_session_survives_refused_subscribe() {
    let harness = common::ServerHarness::start(DRAIN_TIMEOUT_SECS);

    // Build the rumqttc blocking client. rumqttc 0.24, not the broker, sends
    // PINGREQ every `KEEP_ALIVE`, which normally keeps the session inside its
    // negotiated 7.5 s idle deadline; `next_event` skips the broker's PINGRESP
    // replies, so they do not interfere with our timing.
    let mut opts = rumqttc::MqttOptions::new(CLIENT_ID, "127.0.0.1", harness.port());
    opts.set_keep_alive(KEEP_ALIVE);
    let (client, mut connection) = rumqttc::Client::new(opts, REQUEST_CHANNEL_CAP);

    let (event_tx, event_rx): (Sender<EventMsg>, EventReceiver) = std::sync::mpsc::channel();
    let teardown = Arc::new(AtomicBool::new(false));
    let mut session = common::Session::new();

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
    let driver = common::spawn_driver(
        DRIVER_THREAD_NAME,
        move || connection.recv().ok(),
        event_tx,
        Arc::clone(&teardown),
    );

    // Step 3 — wait for the first ConnAck.
    let connack = common::assert_outcome(
        common::next_event(&event_rx, CONNACK_BOUND, &teardown, &mut session),
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
    let puback_1 = common::assert_outcome(
        common::next_event(&event_rx, ACK_BOUND, &teardown, &mut session),
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
    let suback = common::assert_outcome(
        common::next_event(&event_rx, SUBACK_BOUND, &teardown, &mut session),
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
    let puback_2 = common::assert_outcome(
        common::next_event(&event_rx, ACK_BOUND, &teardown, &mut session),
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

    // Step 7 — QoS 2 publish: rumqttc writes PUBREL on the PUBREC by itself
    // (rumqttc 0.24 `state.rs:259-279`), so the next incoming packets are
    // PubRec then PubComp, both carrying the publish's packet id.
    client
        .publish("t", rumqttc::QoS::ExactlyOnce, false, SENSOR_PAYLOAD)
        .expect("QoS 2 publish call");
    let pubrec = common::assert_outcome(
        common::next_event(&event_rx, ACK_BOUND, &teardown, &mut session),
        "PubRec",
    );
    let rec_pkid = match pubrec {
        rumqttc::Event::Incoming(rumqttc::Packet::PubRec(rec)) => rec.pkid,
        other => panic!("expected PubRec, got {other:?}"),
    };
    let pubcomp = common::assert_outcome(
        common::next_event(&event_rx, ACK_BOUND, &teardown, &mut session),
        "PubComp",
    );
    let comp_pkid = match pubcomp {
        rumqttc::Event::Incoming(rumqttc::Packet::PubComp(comp)) => comp.pkid,
        other => panic!("expected PubComp, got {other:?}"),
    };
    assert_eq!(
        rec_pkid, comp_pkid,
        "PubComp must carry the PubRec's packet id"
    );

    // Step 8 — teardown. Order matters: flip the flag FIRST so any Err the
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
    common::join_driver(driver, DRIVER_JOIN_BOUND);

    // Require the outgoing Disconnect the driver forwarded. Discarding this
    // wait would let a session that died before the DISCONNECT was written
    // count as a clean close; rumqttc emits the event only after the write
    // and flush both succeed, so this is the assertion that the requested
    // disconnect really happened.
    if let Err(err) = common::await_outgoing_disconnect(&event_rx, DISCONNECT_OBSERVE_BOUND) {
        panic!("outgoing Disconnect: {}", err.describe());
    }

    // Nothing reads the channel after this point, and the driver was
    // joined above, so dropping the receiver only releases the channel.
    drop(event_rx);

    // Release the harness thread, which then calls BrokerHandle::shutdown()
    // and join() to run the broker's shutdown drain and worker join to
    // completion. Done last so every assertion above ran against a live
    // broker.
    let report = harness.shutdown_and_join();
    assert_eq!(report.join, Ok(()), "server join after shutdown");
}

/// AC-12 — the session window is a local `Session` now, so the guard that
/// reads it must answer exactly as the shared atomic did: the first `ConnAck`
/// opens the window and is returned to step 3, and any later `ConnAck` is
/// the silent client reconnect the helper exists to catch. The live test
/// never reaches the second branch, so only this case pins it.
#[test]
fn next_event_opens_the_window_on_the_first_connack_and_refuses_a_second() {
    let (tx, rx) = std::sync::mpsc::channel::<EventMsg>();
    let teardown = AtomicBool::new(false);
    let mut session = common::Session::new();

    tx.send(Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(
        rumqttc::ConnAck::new(rumqttc::ConnectReturnCode::Success, false),
    ))))
    .expect("queue the first ConnAck");
    assert!(
        matches!(
            common::next_event(&rx, GUARD_BOUND, &teardown, &mut session),
            Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_)))
        ),
        "the first ConnAck is returned to the caller"
    );
    assert!(
        session.is_open(),
        "the first ConnAck opens the session window"
    );

    tx.send(Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(
        rumqttc::ConnAck::new(rumqttc::ConnectReturnCode::Success, false),
    ))))
    .expect("queue a second ConnAck");
    assert!(
        matches!(
            common::next_event(&rx, GUARD_BOUND, &teardown, &mut session),
            Err(common::RumqttcOutcome::UnexpectedConnAck)
        ),
        "a second ConnAck inside an open window is a failure"
    );
}

/// AC-12 — the same `Session` decides the incoming-`Disconnect` guard: inside
/// an open window with the teardown flag still clear it is a failure, and
/// after the flag flips it is the expected end of the session and is returned.
#[test]
fn next_event_refuses_an_in_session_disconnect_and_accepts_it_after_teardown() {
    let (tx, rx) = std::sync::mpsc::channel::<EventMsg>();
    let teardown = AtomicBool::new(false);
    let mut session = common::Session::opened();

    tx.send(Ok(rumqttc::Event::Incoming(rumqttc::Packet::Disconnect)))
        .expect("queue the in-session Disconnect");
    assert!(
        matches!(
            common::next_event(&rx, GUARD_BOUND, &teardown, &mut session),
            Err(common::RumqttcOutcome::UnexpectedDisconnect)
        ),
        "a Disconnect before teardown ends the session early"
    );

    teardown.store(true, Ordering::SeqCst);
    tx.send(Ok(rumqttc::Event::Incoming(rumqttc::Packet::Disconnect)))
        .expect("queue the teardown Disconnect");
    assert!(
        matches!(
            common::next_event(&rx, GUARD_BOUND, &teardown, &mut session),
            Ok(rumqttc::Event::Incoming(rumqttc::Packet::Disconnect))
        ),
        "after the teardown flag is set the Disconnect is the expected end"
    );
    assert!(
        session.is_open(),
        "neither Disconnect branch closes the window"
    );
}

/// AC-13 — `Outgoing` events and the broker's PINGRESP reply are skipped
/// under the same deadline, so the acknowledgment queued behind them is
/// still the event the wait returns, and the skip leaves the window as it
/// found it.
#[test]
fn next_event_skips_outgoing_and_pingresp_before_the_awaited_incoming() {
    let (tx, rx) = std::sync::mpsc::channel::<EventMsg>();
    let teardown = AtomicBool::new(false);
    let mut session = common::Session::opened();

    tx.send(Ok(rumqttc::Event::Outgoing(rumqttc::Outgoing::PingReq)))
        .expect("queue the client's own PINGREQ observation");
    tx.send(Ok(rumqttc::Event::Incoming(rumqttc::Packet::PingResp)))
        .expect("queue the broker's PINGRESP");
    tx.send(Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(
        rumqttc::PubAck::new(1),
    ))))
    .expect("queue the awaited PUBACK");

    assert!(
        matches!(
            common::next_event(&rx, GUARD_BOUND, &teardown, &mut session),
            Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_)))
        ),
        "the PUBACK behind the skipped events is what the wait returns"
    );
    assert!(
        session.is_open(),
        "skipped events leave the session window open"
    );
}

/// AC-3 — an outgoing PUBLISH is skipped like any other outgoing event, but
/// its packet id is recorded in send order first. That record is what ties
/// an acknowledgement to the publish that earned it, so a broker answering
/// the wrong flow cannot pass.
#[test]
fn next_event_records_outgoing_publish_packet_ids_in_send_order() {
    let (tx, rx) = std::sync::mpsc::channel::<EventMsg>();
    let teardown = AtomicBool::new(false);
    let mut session = common::Session::opened();

    // A QoS 0 publish (pkid 0), then two acknowledged ones, then the PUBACK
    // the caller is waiting for.
    for pkid in [0, 7, 9] {
        tx.send(Ok(rumqttc::Event::Outgoing(rumqttc::Outgoing::Publish(
            pkid,
        ))))
        .expect("queue the client's own publish observation");
    }
    tx.send(Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(
        rumqttc::PubAck::new(7),
    ))))
    .expect("queue the awaited PUBACK");

    assert!(
        matches!(
            common::next_event(&rx, GUARD_BOUND, &teardown, &mut session),
            Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_)))
        ),
        "the outgoing publishes are skipped, not returned"
    );
    assert_eq!(session.sent_publish_pkid(0), Some(0));
    assert_eq!(session.sent_publish_pkid(1), Some(7));
    assert_eq!(session.sent_publish_pkid(2), Some(9));
    assert_eq!(
        session.sent_publish_pkid(3),
        None,
        "only the observed publishes are recorded"
    );
}
