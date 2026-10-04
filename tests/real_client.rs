//! Real-client proof of the ingest profile (F4.1): rumqttc 0.24 drives an
//! in-process server over loopback TCP, and every test checks the wire outcome
//! and, where a PUBLISH is involved, the exact Publish the callback received.

#[allow(dead_code)] // each test binary uses a different subset of the shared harness
mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;

/// Per-worker drain deadline — short so teardown is bounded.
const DRAIN_TIMEOUT_SECS: u64 = 1;
/// `Client::new` capacity argument — the bound on rumqttc's own request
/// queue, which the tests use for three publishes and a disconnect.
const REQUEST_CHANNEL_CAP: usize = 16;
/// `set_keep_alive` argument. rumqttc pings on this timer; no test here
/// outlives it before its last ack.
const KEEP_ALIVE: Duration = Duration::from_secs(5);
/// Window for the very first `ConnAck` to land after the harness reports
/// startup complete.
const CONNACK_BOUND: Duration = Duration::from_secs(5);
/// Window for a QoS 1 `PubAck`, or a QoS 2 `PubRec` / `PubComp`, to land
/// after the triggering event.
const ACK_BOUND: Duration = Duration::from_secs(5);
/// Window for a `PublishCallback` delivery to land after the triggering
/// event.
const CALLBACK_BOUND: Duration = Duration::from_secs(5);
/// Window for the driver thread to observe the disconnect take effect
/// (terminal iterator item) after `client.disconnect()` is called.
const DISCONNECT_OBSERVE_BOUND: Duration = Duration::from_secs(5);
/// Upper bound on joining the driver thread — exceeded only by a leak.
const DRIVER_JOIN_BOUND: Duration = Duration::from_secs(10);
/// Driver thread name — visible in `top -H` and panic backtraces.
const DRIVER_THREAD_NAME: &str = "itest-real-client-driver";
/// Client identifier the rumqttc v3 connection presents on CONNECT.
const V3_CLIENT_ID: &str = "uring-mqtt-itest-v3";
/// Topics the three v3 publishes are sent to, one per QoS level.
const V3_TOPICS: [&str; 3] = ["itest/v3/qos0", "itest/v3/qos1", "itest/v3/qos2"];
/// Raw payload bytes for the QoS 0 publish: includes 0x00 and 0xFF so a
/// dropped or NUL-terminated copy would change the bytes. Distinct from the
/// QoS 1/2 payloads, never SensorV1.
const QOS0_PAYLOAD: [u8; 4] = [0x00, 0x01, 0xFE, 0xFF];
/// Raw payload bytes for the QoS 1 publish.
const QOS1_PAYLOAD: [u8; 3] = [0x10, 0x00, 0x20];
/// Raw payload bytes for the QoS 2 publish.
const QOS2_PAYLOAD: [u8; 5] = [0xC0, 0xFF, 0xEE, 0x00, 0x7F];
/// Topic of the barrier publish each exactness test sends once it has taken
/// every callback it expects. An empty `unclaimed` after teardown cannot
/// establish "and no more" on its own: the server's event processor is
/// detached (`src/broker/worker.rs:314`), so an event still queued when the
/// worker runtime ends reaches no callback and leaves no record anywhere the
/// test can see. The barrier closes that gap for every event the connection
/// produced before it, because it travels the same first-in-first-out
/// per-worker event channel (`src/broker/worker.rs:307-316`): any extra
/// callback queued ahead of the barrier is delivered first and fails the
/// barrier assertion.
const BARRIER_TOPIC: &str = "itest/barrier";
/// Raw payload bytes for the barrier publish. Distinct from every other
/// payload here, never SensorV1.
const BARRIER_PAYLOAD: [u8; 2] = [0x5A, 0xA5];

/// One item the driver's forwarding channel carries: a rumqttc event wrapped
/// in `Result`, the way `Connection::recv` yields it.
type V3EventMsg = Result<rumqttc::Event, rumqttc::ConnectionError>;
/// Receiving end of the driver's forwarding channel. Aliased so clippy's
/// `type_complexity` does not flag the channel's `let` annotation.
type V3EventReceiver = Receiver<V3EventMsg>;

/// Build the expected callback value from the topic, payload, QoS and
/// RETAIN flag the test sent — expected values are always the local
/// constants, never a server read-back.
fn seen(topic: &str, payload: &[u8], qos: uring_mqtt::QoS, retain: bool) -> common::SeenPublish {
    common::SeenPublish {
        topic: topic.to_string(),
        payload: payload.to_vec(),
        qos,
        retain,
    }
}

/// Take the next callback delivery and require it to be the barrier publish.
/// A delivery the test did not expect is queued ahead of the barrier, so it
/// surfaces here instead of being lost at worker teardown — see
/// `BARRIER_TOPIC`.
fn assert_barrier_is_next(harness: &common::ServerHarness) {
    assert_eq!(
        harness.next_publish(CALLBACK_BOUND),
        seen(
            BARRIER_TOPIC,
            &BARRIER_PAYLOAD,
            uring_mqtt::QoS::AtMostOnce,
            false
        ),
        "a callback delivery arrived ahead of the barrier publish: the server delivered more publishes than the test sent",
    );
}

/// AC-1, AC-3: a rumqttc v3 client's three publishes reach the
/// `PublishCallback` with exact topic, payload, QoS and RETAIN, in send
/// order and no more, and the QoS 1/2 acknowledgement flows complete.
#[test]
#[allow(clippy::too_many_lines)]
fn v3_client_publishes_at_every_qos_reach_the_callback_exactly() {
    let harness = common::ServerHarness::start(DRAIN_TIMEOUT_SECS);

    let mut opts = rumqttc::MqttOptions::new(V3_CLIENT_ID, "127.0.0.1", harness.port());
    opts.set_keep_alive(KEEP_ALIVE);
    let (client, mut connection) = rumqttc::Client::new(opts, REQUEST_CHANNEL_CAP);

    let (event_tx, event_rx): (Sender<V3EventMsg>, V3EventReceiver) = std::sync::mpsc::channel();
    let teardown = Arc::new(AtomicBool::new(false));
    let mut session = common::Session::new();

    let driver = common::spawn_driver(
        DRIVER_THREAD_NAME,
        move || connection.recv().ok(),
        event_tx,
        Arc::clone(&teardown),
    );

    // First event must be the CONNACK.
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

    // Submit all three publishes back to back, without waiting for any
    // callback in between, so the server can queue them together and the
    // order check below means something.
    client
        .publish(
            V3_TOPICS[0],
            rumqttc::QoS::AtMostOnce,
            false,
            QOS0_PAYLOAD.to_vec(),
        )
        .expect("QoS 0 publish call");
    // RETAIN true on the QoS 1 publish, so both RETAIN values cross the
    // wire.
    client
        .publish(
            V3_TOPICS[1],
            rumqttc::QoS::AtLeastOnce,
            true,
            QOS1_PAYLOAD.to_vec(),
        )
        .expect("QoS 1 publish call");
    client
        .publish(
            V3_TOPICS[2],
            rumqttc::QoS::ExactlyOnce,
            false,
            QOS2_PAYLOAD.to_vec(),
        )
        .expect("QoS 2 publish call");

    // Acknowledgements, in the order the server answers one connection.
    let puback = common::assert_outcome(
        common::next_event(&event_rx, ACK_BOUND, &teardown, &mut session),
        "PubAck",
    );
    let puback_pkid = match puback {
        rumqttc::Event::Incoming(rumqttc::Packet::PubAck(ack)) => {
            assert_ne!(ack.pkid, 0, "PubAck carried pkid 0");
            ack.pkid
        }
        other => panic!("expected PubAck, got {other:?}"),
    };
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
        "PubComp pkid {comp_pkid} differs from PubRec pkid {rec_pkid}",
    );

    // Tie each acknowledgement to the publish that earned it. `next_event`
    // recorded the packet id of every PUBLISH the client wrote, in send
    // order and always before any acknowledgement of it, so index 1 is the
    // QoS 1 publish and index 2 the QoS 2 one. Without this, a PUBACK for
    // the QoS 2 publish and a PUBREC/PUBCOMP for the QoS 1 publish would
    // satisfy every assertion above.
    let qos0_pkid = session
        .sent_publish_pkid(0)
        .expect("the QoS 0 publish was never written");
    let qos1_pkid = session
        .sent_publish_pkid(1)
        .expect("the QoS 1 publish was never written");
    let qos2_pkid = session
        .sent_publish_pkid(2)
        .expect("the QoS 2 publish was never written");
    assert_eq!(qos0_pkid, 0, "a QoS 0 publish must carry pkid 0");
    assert_ne!(
        qos1_pkid, qos2_pkid,
        "the QoS 1 and QoS 2 publishes went out under the same pkid {qos1_pkid}",
    );
    assert_eq!(
        puback_pkid, qos1_pkid,
        "PubAck pkid {puback_pkid} is not the QoS 1 publish's pkid {qos1_pkid}",
    );
    assert_eq!(
        rec_pkid, qos2_pkid,
        "PubRec pkid {rec_pkid} is not the QoS 2 publish's pkid {qos2_pkid}",
    );

    // Callbacks, in send order. Expected values are the consts, never a
    // server read-back.
    let got: Vec<common::SeenPublish> = (0..3)
        .map(|_| harness.next_publish(CALLBACK_BOUND))
        .collect();
    assert_eq!(
        got,
        vec![
            seen(
                V3_TOPICS[0],
                &QOS0_PAYLOAD,
                uring_mqtt::QoS::AtMostOnce,
                false
            ),
            seen(
                V3_TOPICS[1],
                &QOS1_PAYLOAD,
                uring_mqtt::QoS::AtLeastOnce,
                true
            ),
            seen(
                V3_TOPICS[2],
                &QOS2_PAYLOAD,
                uring_mqtt::QoS::ExactlyOnce,
                false
            ),
        ],
        "callback deliveries differ from what was sent, or arrived out of send order",
    );

    // Barrier: anything the server queued behind the three publishes above
    // is delivered before this one.
    client
        .publish(
            BARRIER_TOPIC,
            rumqttc::QoS::AtMostOnce,
            false,
            BARRIER_PAYLOAD.to_vec(),
        )
        .expect("barrier publish call");
    assert_barrier_is_next(&harness);

    // Teardown: flip the flag first so any Err the driver observes from
    // here on is the expected terminal one, not a session-window
    // violation.
    teardown.store(true, Ordering::SeqCst);
    client.disconnect().expect("client.disconnect");
    drop(client);

    common::join_driver(driver, DRIVER_JOIN_BOUND);

    if let Err(err) = common::await_outgoing_disconnect(&event_rx, DISCONNECT_OBSERVE_BOUND) {
        panic!("outgoing Disconnect: {}", err.describe());
    }

    drop(event_rx);

    let report = harness.shutdown_and_join();
    assert_eq!(report.join, Ok(()), "server join after shutdown");
    assert_eq!(
        report.unclaimed,
        Vec::new(),
        "the callback received a publish after the barrier",
    );
}

/// Above monoio-codec's 8 KiB initial `Framed` read buffer
/// (`monoio-codec-0.3.4/src/framed.rs:18`), so the frame needs more than one
/// read. Below rumqttc's default 10 KiB outgoing limit
/// (`rumqttc-0.24.0/src/lib.rs:503`).
const LARGE_PAYLOAD_LEN: usize = 9 * 1024;
/// A prime period, so a dropped, duplicated or shifted chunk at any
/// power-of-two boundary changes the bytes.
const LARGE_PAYLOAD_PATTERN_PERIOD: usize = 251;
/// Client identifier the rumqttc v3 connection presents on CONNECT for the
/// large-payload test.
const V3_LARGE_CLIENT_ID: &str = "uring-mqtt-itest-large";
/// Topic the 9 KiB QoS 1 publish is sent to.
const V3_LARGE_TOPIC: &str = "itest/v3/large";

/// AC-13, AC-14: a 9 KiB QoS 1 PUBLISH, which the server must assemble
/// across more than one read, arrives byte-for-byte at the callback.
#[test]
fn v3_client_publish_larger_than_the_initial_read_buffer_reaches_the_callback_intact() {
    let harness = common::ServerHarness::start(DRAIN_TIMEOUT_SECS);

    let mut opts = rumqttc::MqttOptions::new(V3_LARGE_CLIENT_ID, "127.0.0.1", harness.port());
    opts.set_keep_alive(KEEP_ALIVE);
    let (client, mut connection) = rumqttc::Client::new(opts, REQUEST_CHANNEL_CAP);

    let (event_tx, event_rx): (Sender<V3EventMsg>, V3EventReceiver) = std::sync::mpsc::channel();
    let teardown = Arc::new(AtomicBool::new(false));
    let mut session = common::Session::new();

    let driver = common::spawn_driver(
        DRIVER_THREAD_NAME,
        move || connection.recv().ok(),
        event_tx,
        Arc::clone(&teardown),
    );

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

    let large_payload: Vec<u8> = (0..LARGE_PAYLOAD_LEN)
        .map(|i| u8::try_from(i % LARGE_PAYLOAD_PATTERN_PERIOD).expect("pattern index fits in u8"))
        .collect();
    client
        .publish(
            V3_LARGE_TOPIC,
            rumqttc::QoS::AtLeastOnce,
            false,
            large_payload.clone(),
        )
        .expect("large publish call");

    let puback = common::assert_outcome(
        common::next_event(&event_rx, ACK_BOUND, &teardown, &mut session),
        "large PubAck",
    );
    let large_puback_pkid = match puback {
        rumqttc::Event::Incoming(rumqttc::Packet::PubAck(ack)) => ack.pkid,
        other => panic!("expected PubAck, got {other:?}"),
    };
    // The PUBACK must carry the pkid of the one publish this test wrote.
    let large_pkid = session
        .sent_publish_pkid(0)
        .expect("the large publish was never written");
    assert_ne!(large_pkid, 0, "a QoS 1 publish must carry a nonzero pkid");
    assert_eq!(
        large_puback_pkid, large_pkid,
        "PubAck pkid {large_puback_pkid} is not the large publish's pkid {large_pkid}",
    );

    assert_eq!(
        harness.next_publish(CALLBACK_BOUND),
        seen(
            V3_LARGE_TOPIC,
            &large_payload,
            uring_mqtt::QoS::AtLeastOnce,
            false
        ),
        "the large publish reached the callback altered",
    );

    // Barrier: anything the server queued behind the large publish is
    // delivered before this one.
    client
        .publish(
            BARRIER_TOPIC,
            rumqttc::QoS::AtMostOnce,
            false,
            BARRIER_PAYLOAD.to_vec(),
        )
        .expect("barrier publish call");
    assert_barrier_is_next(&harness);

    teardown.store(true, Ordering::SeqCst);
    client.disconnect().expect("client.disconnect");
    drop(client);

    common::join_driver(driver, DRIVER_JOIN_BOUND);

    if let Err(err) = common::await_outgoing_disconnect(&event_rx, DISCONNECT_OBSERVE_BOUND) {
        panic!("outgoing Disconnect: {}", err.describe());
    }

    drop(event_rx);

    let report = harness.shutdown_and_join();
    assert_eq!(report.join, Ok(()), "server join after shutdown");
    assert_eq!(
        report.unclaimed,
        Vec::new(),
        "the callback received a publish after the barrier",
    );
}

/// Client identifier the rumqttc v5 connection presents on CONNECT.
const V5_CLIENT_ID: &str = "uring-mqtt-itest-v5";
/// Topics the three v5 publishes are sent to, one per QoS level.
const V5_TOPICS: [&str; 3] = ["itest/v5/qos0", "itest/v5/qos1", "itest/v5/qos2"];
/// Single-filter wildcard SUBSCRIBE the v5 subscribe-refusal test sends.
const V5_SUBSCRIBE_FILTER: &str = "itest/v5/#";
/// Wait bound for the channel-fed `next_event` guard test: every message is
/// already queued, so the bound is only the margin that keeps a scheduling
/// hiccup from timing the wait out.
const GUARD_BOUND: Duration = Duration::from_secs(1);

/// One item the driver's forwarding channel carries for a v5 connection:
/// a `rumqttc::v5::Event` wrapped in `Result`, the way `Connection::recv`
/// yields it.
type V5EventMsg = Result<rumqttc::v5::Event, rumqttc::v5::ConnectionError>;
/// Receiving end of the driver's forwarding channel for v5.
type V5EventReceiver = Receiver<V5EventMsg>;

/// AC-2, AC-4, AC-5: a rumqttc v5 client's three publishes reach the
/// `PublishCallback` with exact topic, payload, QoS and RETAIN, in send
/// order and no more; the CONNACK carries Success and Session Present 0;
/// the QoS 1/2 acknowledgement flows complete with reason Success on
/// every PUBACK, PUBREC and PUBCOMP.
#[test]
#[allow(clippy::too_many_lines)]
fn v5_client_publishes_at_every_qos_reach_the_callback_exactly() {
    let harness = common::ServerHarness::start(DRAIN_TIMEOUT_SECS);

    // rumqttc v5 forbids keep-alive < 5 s (0.24.0 `src/v5/mod.rs:224`) and
    // defaults to 60 s, so no `set_keep_alive` call here.
    let opts = rumqttc::v5::MqttOptions::new(V5_CLIENT_ID, "127.0.0.1", harness.port());
    let (client, mut connection) = rumqttc::v5::Client::new(opts, REQUEST_CHANNEL_CAP);

    let (event_tx, event_rx): (Sender<V5EventMsg>, V5EventReceiver) = std::sync::mpsc::channel();
    let teardown = Arc::new(AtomicBool::new(false));
    let mut session = common::Session::new();

    let driver = common::spawn_driver(
        DRIVER_THREAD_NAME,
        move || connection.recv().ok(),
        event_tx,
        Arc::clone(&teardown),
    );

    // First event must be the CONNACK carrying Success and Session Present 0.
    let connack = common::assert_outcome(
        common::next_event(&event_rx, CONNACK_BOUND, &teardown, &mut session),
        "first ConnAck",
    );
    let ack = match connack {
        rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::ConnAck(ack)) => ack,
        other => panic!("first event was {other:?}"),
    };
    assert_eq!(
        ack.code,
        rumqttc::v5::mqttbytes::v5::ConnectReturnCode::Success,
        "CONNACK reason {:?} is not Success",
        ack.code,
    );
    assert!(
        !ack.session_present,
        "Session Present was set on a clean-only connection",
    );

    // All three publishes back to back, no wait between them.
    client
        .publish(
            V5_TOPICS[0],
            rumqttc::v5::mqttbytes::QoS::AtMostOnce,
            false,
            QOS0_PAYLOAD.to_vec(),
        )
        .expect("QoS 0 publish call");
    // RETAIN true on the QoS 1 publish, so both RETAIN values cross the wire.
    client
        .publish(
            V5_TOPICS[1],
            rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
            true,
            QOS1_PAYLOAD.to_vec(),
        )
        .expect("QoS 1 publish call");
    client
        .publish(
            V5_TOPICS[2],
            rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
            false,
            QOS2_PAYLOAD.to_vec(),
        )
        .expect("QoS 2 publish call");

    // Acknowledgements, in the order the server answers one connection.
    let puback = common::assert_outcome(
        common::next_event(&event_rx, ACK_BOUND, &teardown, &mut session),
        "PubAck",
    );
    let puback_pkid = match puback {
        rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::PubAck(ack)) => {
            assert_eq!(
                ack.reason,
                rumqttc::v5::mqttbytes::v5::PubAckReason::Success,
                "PubAck reason {:?} is not Success",
                ack.reason,
            );
            assert_ne!(ack.pkid, 0, "PubAck carried pkid 0");
            ack.pkid
        }
        other => panic!("expected PubAck, got {other:?}"),
    };
    let pubrec = common::assert_outcome(
        common::next_event(&event_rx, ACK_BOUND, &teardown, &mut session),
        "PubRec",
    );
    let rec_pkid = match pubrec {
        rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::PubRec(rec)) => {
            assert_eq!(
                rec.reason,
                rumqttc::v5::mqttbytes::v5::PubRecReason::Success,
                "PubRec reason {:?} is not Success",
                rec.reason,
            );
            rec.pkid
        }
        other => panic!("expected PubRec, got {other:?}"),
    };
    let pubcomp = common::assert_outcome(
        common::next_event(&event_rx, ACK_BOUND, &teardown, &mut session),
        "PubComp",
    );
    let comp_pkid = match pubcomp {
        rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::PubComp(comp)) => {
            assert_eq!(
                comp.reason,
                rumqttc::v5::mqttbytes::v5::PubCompReason::Success,
                "PubComp reason {:?} is not Success",
                comp.reason,
            );
            comp.pkid
        }
        other => panic!("expected PubComp, got {other:?}"),
    };
    assert_eq!(
        rec_pkid, comp_pkid,
        "PubComp pkid {comp_pkid} differs from PubRec pkid {rec_pkid}",
    );

    // Tie each acknowledgement to the publish that earned it. Same
    // accounting as the v3 test: index 1 is the QoS 1 publish and index 2
    // the QoS 2 one, and rumqttc only requires that the ack id be
    // outstanding — never that the publish it answers had the matching
    // QoS — so this correlation is what stops a swapped-flow server.
    let qos0_pkid = session
        .sent_publish_pkid(0)
        .expect("the QoS 0 publish was never written");
    let qos1_pkid = session
        .sent_publish_pkid(1)
        .expect("the QoS 1 publish was never written");
    let qos2_pkid = session
        .sent_publish_pkid(2)
        .expect("the QoS 2 publish was never written");
    assert_eq!(qos0_pkid, 0, "a QoS 0 publish must carry pkid 0");
    assert_ne!(
        qos1_pkid, qos2_pkid,
        "the QoS 1 and QoS 2 publishes went out under the same pkid {qos1_pkid}",
    );
    assert_eq!(
        puback_pkid, qos1_pkid,
        "PubAck pkid {puback_pkid} is not the QoS 1 publish's pkid {qos1_pkid}",
    );
    assert_eq!(
        rec_pkid, qos2_pkid,
        "PubRec pkid {rec_pkid} is not the QoS 2 publish's pkid {qos2_pkid}",
    );

    // Callbacks, in send order. Expected values are the consts, never a
    // server read-back.
    let got: Vec<common::SeenPublish> = (0..3)
        .map(|_| harness.next_publish(CALLBACK_BOUND))
        .collect();
    assert_eq!(
        got,
        vec![
            seen(
                V5_TOPICS[0],
                &QOS0_PAYLOAD,
                uring_mqtt::QoS::AtMostOnce,
                false
            ),
            seen(
                V5_TOPICS[1],
                &QOS1_PAYLOAD,
                uring_mqtt::QoS::AtLeastOnce,
                true
            ),
            seen(
                V5_TOPICS[2],
                &QOS2_PAYLOAD,
                uring_mqtt::QoS::ExactlyOnce,
                false
            ),
        ],
        "callback deliveries differ from what was sent, or arrived out of send order",
    );

    // Barrier: anything the server queued behind the three publishes above
    // is delivered before this one.
    client
        .publish(
            BARRIER_TOPIC,
            rumqttc::v5::mqttbytes::QoS::AtMostOnce,
            false,
            BARRIER_PAYLOAD.to_vec(),
        )
        .expect("barrier publish call");
    assert_barrier_is_next(&harness);

    teardown.store(true, Ordering::SeqCst);
    client.disconnect().expect("client.disconnect");
    drop(client);

    common::join_driver(driver, DRIVER_JOIN_BOUND);

    if let Err(err) = common::await_outgoing_disconnect(&event_rx, DISCONNECT_OBSERVE_BOUND) {
        panic!("outgoing Disconnect: {}", err.describe());
    }

    drop(event_rx);

    let report = harness.shutdown_and_join();
    assert_eq!(report.join, Ok(()), "server join after shutdown");
    assert_eq!(
        report.unclaimed,
        Vec::new(),
        "the callback received a publish after the barrier",
    );
}

/// Regression guard for the v5 `ClientEvent` impl: the live v5 tests run far
/// inside their 60 s keep-alive (rumqttc `src/v5/mod.rs:119-142`), so no
/// PINGRESP reaches them, and a second `ConnAck` only lands if the client
/// silently reconnects. This guard pins both, plus the outgoing-Disconnect
/// classifier and the v5 write-PUBLISH packet-id recording.
#[test]
fn next_event_classifies_v5_events_like_v3_events() {
    let (tx, rx) = std::sync::mpsc::channel::<V5EventMsg>();
    let teardown = AtomicBool::new(false);
    let mut session = common::Session::new();

    // First CONNACK opens the window.
    let first_connack = rumqttc::v5::mqttbytes::v5::ConnAck {
        session_present: false,
        code: rumqttc::v5::mqttbytes::v5::ConnectReturnCode::Success,
        properties: None,
    };
    tx.send(Ok(rumqttc::v5::Event::Incoming(
        rumqttc::v5::mqttbytes::v5::Packet::ConnAck(first_connack.clone()),
    )))
    .expect("queue the first ConnAck");
    let returned_connack = match common::next_event(&rx, GUARD_BOUND, &teardown, &mut session) {
        Ok(event) => event,
        Err(other) => panic!("first ConnAck: {}", other.describe()),
    };
    assert!(
        matches!(
            returned_connack,
            rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::ConnAck(_))
        ),
        "first ConnAck was returned as {returned_connack:?}",
    );
    assert!(
        session.is_open(),
        "the first v5 ConnAck opens the session window",
    );

    // PINGREQ skip, write-PUBLISH id recording, PINGRESP skip, awaited PubAck.
    tx.send(Ok(rumqttc::v5::Event::Outgoing(rumqttc::Outgoing::PingReq)))
        .expect("queue the client's own PINGREQ observation");
    tx.send(Ok(rumqttc::v5::Event::Outgoing(
        rumqttc::Outgoing::Publish(3),
    )))
    .expect("queue the client's own PUBLISH observation with pkid 3");
    tx.send(Ok(rumqttc::v5::Event::Incoming(
        rumqttc::v5::mqttbytes::v5::Packet::PingResp(rumqttc::v5::mqttbytes::v5::PingResp),
    )))
    .expect("queue the broker's PINGRESP");
    tx.send(Ok(rumqttc::v5::Event::Incoming(
        rumqttc::v5::mqttbytes::v5::Packet::PubAck(rumqttc::v5::mqttbytes::v5::PubAck::new(
            1, None,
        )),
    )))
    .expect("queue the awaited PubAck");
    let returned_puback = match common::next_event(&rx, GUARD_BOUND, &teardown, &mut session) {
        Ok(event) => event,
        Err(other) => panic!("v5 PubAck: {}", other.describe()),
    };
    assert!(
        matches!(
            returned_puback,
            rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::PubAck(_))
        ),
        "the PubAck behind the skipped v5 events was returned, got {returned_puback:?}",
    );
    assert_eq!(
        session.sent_publish_pkid(0),
        Some(3),
        "the v5 write-PUBLISH event was not recorded under its pkid",
    );

    // Second ConnAck is the silent-reconnect violation.
    tx.send(Ok(rumqttc::v5::Event::Incoming(
        rumqttc::v5::mqttbytes::v5::Packet::ConnAck(first_connack),
    )))
    .expect("queue a second ConnAck");
    match common::next_event(&rx, GUARD_BOUND, &teardown, &mut session) {
        Err(common::RumqttcOutcome::UnexpectedConnAck) => {}
        Err(other) => panic!(
            "expected UnexpectedConnAck on a second v5 ConnAck, got: {}",
            other.describe()
        ),
        Ok(event) => panic!("expected UnexpectedConnAck on a second v5 ConnAck, got {event:?}"),
    }

    // Outgoing Disconnect is what the await_outgoing_disconnect helper
    // observes.
    tx.send(Ok(rumqttc::v5::Event::Outgoing(
        rumqttc::Outgoing::Disconnect,
    )))
    .expect("queue the outgoing Disconnect");
    if let Err(err) = common::await_outgoing_disconnect(&rx, GUARD_BOUND) {
        panic!("outgoing v5 Disconnect: {}", err.describe());
    }
}

/// AC-6: a rumqttc v5 client's single-filter SUBSCRIBE is answered with
/// SUBACK reason 0x83 (`SubscribeAckReason::ImplementationSpecificError`),
/// observed as `StateError::SubFail { reason: ImplementationSpecific }`.
/// Session continuation after the refusal is not observable with rumqttc
/// and is covered over a socket by `src/broker/handler.rs:1890`
/// (`v5_wildcard_subscribe_is_refused_and_session_continues`).
#[test]
fn v5_client_subscribe_is_refused_with_implementation_specific_error() {
    let harness = common::ServerHarness::start(DRAIN_TIMEOUT_SECS);

    // rumqttc v5 forbids keep-alive < 5 s (0.24.0 `src/v5/mod.rs:224`) and
    // defaults to 60 s, so no `set_keep_alive` call here.
    let opts = rumqttc::v5::MqttOptions::new(V5_CLIENT_ID, "127.0.0.1", harness.port());
    let (client, mut connection) = rumqttc::v5::Client::new(opts, REQUEST_CHANNEL_CAP);

    let (event_tx, event_rx): (Sender<V5EventMsg>, V5EventReceiver) = std::sync::mpsc::channel();
    let teardown = Arc::new(AtomicBool::new(false));
    let mut session = common::Session::new();

    let driver = common::spawn_driver(
        DRIVER_THREAD_NAME,
        move || connection.recv().ok(),
        event_tx,
        Arc::clone(&teardown),
    );

    let connack = common::assert_outcome(
        common::next_event(&event_rx, CONNACK_BOUND, &teardown, &mut session),
        "first ConnAck",
    );
    assert!(
        matches!(
            connack,
            rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::ConnAck(_))
        ),
        "first event was {connack:?}",
    );

    // rumqttc 0.24 ends its own connection on a failure SUBACK
    // (`src/v5/state.rs:254-264` + `eventloop.rs` `poll` calls `clean()`),
    // so the refusal arrives as the connection's terminal error. The
    // teardown flag is set first so `next_event` classifies the terminal
    // error as `ClosedAfterDisconnect` rather than `ConnectionError`.
    teardown.store(true, Ordering::SeqCst);
    client
        .subscribe(V5_SUBSCRIBE_FILTER, rumqttc::v5::mqttbytes::QoS::AtMostOnce)
        .expect("subscribe call");

    match common::next_event(&event_rx, ACK_BOUND, &teardown, &mut session) {
        Err(common::RumqttcOutcome::ClosedAfterDisconnect(
            rumqttc::v5::ConnectionError::MqttState(rumqttc::v5::StateError::SubFail { reason }),
        )) => {
            assert_eq!(
                reason,
                rumqttc::v5::mqttbytes::v5::SubscribeReasonCode::ImplementationSpecific,
                "SUBACK reason was {reason:?}, not ImplementationSpecific (0x83)",
            );
        }
        Ok(event) => panic!("expected the SUBACK refusal, got {event:?}"),
        Err(other) => panic!("expected the SUBACK refusal, got: {}", other.describe()),
    }

    common::join_driver(driver, DRIVER_JOIN_BOUND);
    drop(client);
    drop(event_rx);

    let report = harness.shutdown_and_join();
    assert_eq!(report.join, Ok(()), "server join after shutdown");
    assert_eq!(
        report.unclaimed,
        Vec::new(),
        "the callback received a publish although none was sent",
    );
}

/// Keep-alive the silent test declares on CONNECT. Server deadline:
/// `min(1.5 × keep_alive, idle_timeout_secs)` (`src/broker/handshake.rs:105-110`),
/// i.e. 1.5 s here.
const SILENT_KEEP_ALIVE: Duration = Duration::from_secs(1);
/// How long the silent test stops polling its eventloop before checking the
/// connection's state. Equal to the 1.5 s server deadline plus 2 s slack, so
/// any in-budget close has landed but the 300 s idle default cannot have.
const SILENCE: Duration = Duration::from_millis(3500);
/// Window for the silent client's first post-silence poll to surface the close.
const END_OF_STREAM_BOUND: Duration = Duration::from_secs(2);
/// Keep-alive the pinging test declares on CONNECT. Server deadline: 3 s;
/// rumqttc pings every `keep_alive` while the eventloop is polled
/// (`src/eventloop.rs:241-255`).
const PINGING_KEEP_ALIVE: Duration = Duration::from_secs(2);
/// Time the pinging client spends alive before sending the after-pings QoS 1
/// publish. More than two 3 s deadlines, so a server that ignores the
/// keep-alive or closes on a PINGREQ cannot survive the window.
const ALIVE_WINDOW: Duration = Duration::from_secs(7);
/// Client identifier the silent v3 connection presents on CONNECT.
const V3_SILENT_CLIENT_ID: &str = "uring-mqtt-itest-silent";
/// Client identifier the pinging v3 connection presents on CONNECT.
const V3_PINGING_CLIENT_ID: &str = "uring-mqtt-itest-pinging";
/// Topic the pinging client publishes to after the alive window.
const V3_PINGING_TOPIC: &str = "itest/v3/after-pings";

/// AC-7: a rumqttc v3 client that declares keep-alive 1 s and then sends
/// nothing for 3.5 s is closed by the server within 2 s of resuming the
/// poll. The 300 s `idle_timeout_secs` default
/// (`src/broker/mod.rs:45`) makes the declared value the cause.
#[test]
fn silent_client_is_closed_by_its_declared_keep_alive() {
    let harness = common::ServerHarness::start(DRAIN_TIMEOUT_SECS);

    let mut opts = rumqttc::MqttOptions::new(V3_SILENT_CLIENT_ID, "127.0.0.1", harness.port());
    opts.set_keep_alive(SILENT_KEEP_ALIVE);
    let (client, mut connection) = rumqttc::Client::new(opts, REQUEST_CHANNEL_CAP);

    // No driver thread: this test polls the eventloop itself, because
    // rumqttc writes PINGREQ only while polled (0.24.0
    // `src/eventloop.rs:241-255`), and a polled driver would defeat the
    // silence we are trying to observe.
    match connection.recv_timeout(CONNACK_BOUND) {
        Ok(Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_)))) => {}
        other => panic!("expected the first ConnAck, got {other:?}"),
    }

    std::thread::sleep(SILENCE);

    let resumed = std::time::Instant::now();
    let end: rumqttc::ConnectionError = loop {
        let remaining = END_OF_STREAM_BOUND.saturating_sub(resumed.elapsed());
        assert!(
            !remaining.is_zero(),
            "connection still open {END_OF_STREAM_BOUND:?} after a {SILENCE:?} silence",
        );
        // A PINGREQ rumqttc writes on resuming cannot rescue a closed
        // connection: an answering server surfaces as the `Incoming` panic
        // arm below.
        match connection.recv_timeout(remaining) {
            Ok(Ok(rumqttc::Event::Outgoing(_))) => {}
            Ok(Ok(incoming)) => panic!(
                "server answered {incoming:?} after the client's silence: the connection was not closed",
            ),
            Ok(Err(err)) => break err,
            Err(e) => panic!("connection still open after the client's silence: {e:?}"),
        }
    };
    assert!(
        common::is_end_of_stream(&end),
        "expected end-of-stream, got {end:?}",
    );

    drop(client);
    drop(connection);

    let report = harness.shutdown_and_join();
    assert_eq!(report.join, Ok(()), "server join after shutdown");
}

/// AC-8: a rumqttc v3 client whose declared keep-alive is 2 s and whose
/// driver keeps the eventloop polled outlives more than two 3 s server
/// deadlines. The after-pings QoS 1 publish therefore receives its PUBACK
/// on the same connection — proving the server answered the PINGREQs
/// instead of closing on them.
#[test]
fn pinging_client_outlives_several_keep_alive_deadlines() {
    let harness = common::ServerHarness::start(DRAIN_TIMEOUT_SECS);

    let mut opts = rumqttc::MqttOptions::new(V3_PINGING_CLIENT_ID, "127.0.0.1", harness.port());
    opts.set_keep_alive(PINGING_KEEP_ALIVE);
    let (client, mut connection) = rumqttc::Client::new(opts, REQUEST_CHANNEL_CAP);

    let (event_tx, event_rx): (Sender<V3EventMsg>, V3EventReceiver) = std::sync::mpsc::channel();
    let teardown = Arc::new(AtomicBool::new(false));
    let mut session = common::Session::new();

    let driver = common::spawn_driver(
        DRIVER_THREAD_NAME,
        move || connection.recv().ok(),
        event_tx,
        Arc::clone(&teardown),
    );

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

    // The driver keeps polling throughout the window, so rumqttc's own
    // PINGREQs and our PINGRESPs flow. `next_event` skips them; a close
    // inside the window surfaces as `ConnectionError`, a silent reconnect
    // as `UnexpectedConnAck`.
    std::thread::sleep(ALIVE_WINDOW);

    client
        .publish(
            V3_PINGING_TOPIC,
            rumqttc::QoS::AtLeastOnce,
            false,
            QOS1_PAYLOAD.to_vec(),
        )
        .expect("after-pings publish call");

    let puback = common::assert_outcome(
        common::next_event(&event_rx, ACK_BOUND, &teardown, &mut session),
        "after-pings PubAck",
    );
    assert!(
        matches!(puback, rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_))),
        "expected PubAck after the alive window, got {puback:?}",
    );

    let after_pings_pkid = session
        .sent_publish_pkid(0)
        .expect("the after-pings publish was never written");
    assert_ne!(
        after_pings_pkid, 0,
        "a QoS 1 publish must carry a nonzero pkid",
    );

    assert_eq!(
        harness.next_publish(CALLBACK_BOUND),
        seen(
            V3_PINGING_TOPIC,
            &QOS1_PAYLOAD,
            uring_mqtt::QoS::AtLeastOnce,
            false
        ),
        "the after-pings publish reached the callback altered",
    );

    // Barrier: anything the server queued behind the after-pings publish
    // is delivered before this one.
    client
        .publish(
            BARRIER_TOPIC,
            rumqttc::QoS::AtMostOnce,
            false,
            BARRIER_PAYLOAD.to_vec(),
        )
        .expect("barrier publish call");
    assert_barrier_is_next(&harness);

    teardown.store(true, Ordering::SeqCst);
    client.disconnect().expect("client.disconnect");
    drop(client);

    common::join_driver(driver, DRIVER_JOIN_BOUND);

    if let Err(err) = common::await_outgoing_disconnect(&event_rx, DISCONNECT_OBSERVE_BOUND) {
        panic!("outgoing Disconnect: {}", err.describe());
    }

    drop(event_rx);

    let report = harness.shutdown_and_join();
    assert_eq!(report.join, Ok(()), "server join after shutdown");
    assert_eq!(
        report.unclaimed,
        Vec::new(),
        "the callback received a publish after the barrier",
    );
}

/// Drain deadline the staying-client test gives the worker on start. The
/// window has to hold a full QoS 1 round-trip late inside the drain
/// (`LATE_DRAIN_PROBE_AT`) and still leave room for the round-trip to
/// finish, so it is longer than the drain the other tests use.
const FORCE_CLOSE_DRAIN_SECS: u64 = 5;
/// How long the staying-client test waits, after the port has started
/// refusing, before it sends the QoS 1 publish that proves the connection is
/// still served. Late enough that a server which force-closes at or near the
/// listener close cannot answer it, early enough that the acknowledgement and
/// the callback delivery both land before the `FORCE_CLOSE_DRAIN_SECS`
/// deadline even when the refusal is observed a little late.
const LATE_DRAIN_PROBE_AT: Duration = Duration::from_secs(3);
/// Drain deadline AC-9 names, and the one the short staying-client test
/// gives the worker on start. Most other tests configure a one-second
/// drain too (`DRAIN_TIMEOUT_SECS`), but only this test keeps a live
/// client connected across one on purpose.
const SHORT_DRAIN_SECS: u64 = 1;
/// How long the short staying-client test holds its own thread still
/// right after `begin_shutdown()`: twice `SHORT_DRAIN_SECS`, so on an
/// undelayed harness the force-close at the deadline lands while the test
/// thread is asleep — schedule stress for any step that needs the drain open.
const SHORT_DRAIN_STALL: Duration = Duration::from_secs(2 * SHORT_DRAIN_SECS);
/// Upper bound AC-9 sets on `common::Teardown::join_span` — the harness's own
/// `BrokerHandle::shutdown()` plus `join()` — in every shutdown test. 10 s
/// absorbs the longer staying test's 5 s drain and the worker thread
/// teardown, with room for CI jitter.
const JOIN_BOUND: Duration = Duration::from_secs(10);
/// Window for the staying client to see its connection end after the
/// worker has joined. The force-close happens at worker-thread teardown,
/// so the bound is only the IPC propagation the listener
/// close + TCP RST take to reach the client.
const CLOSE_OBSERVE_BOUND: Duration = Duration::from_secs(5);
/// Drain deadline the leaving-client test gives the worker on start. Long
/// enough that no wait inside the drain can race the force-close — the
/// drain's graceful wait would otherwise wake on its own deadline instead
/// of on the last client leaving, masking AC-11 (`src/broker/worker.rs:1442`).
const EARLY_DRAIN_SECS: u64 = 30;
/// Upper bound on `wait_until_refusing` in the leaving-client test. The
/// listener is dropped inside `BrokerHandle::shutdown`, so the bound is
/// only the IPC propagation the release + listener-close take.
const REFUSING_BOUND: Duration = Duration::from_secs(5);
/// Client identifier the rumqttc v3 connection presents on CONNECT for the
/// staying-client drain test.
const V3_STAYING_CLIENT_ID: &str = "uring-mqtt-itest-staying";
/// Client identifier the rumqttc v3 connection presents on CONNECT for the
/// short staying-client drain test.
const V3_SHORT_DRAIN_CLIENT_ID: &str = "uring-mqtt-itest-staying-short";
/// Client identifier the rumqttc v3 connection presents on CONNECT for the
/// leaving-client drain test.
const V3_LEAVING_CLIENT_ID: &str = "uring-mqtt-itest-leaving";
/// Topic the leaving-client test publishes to during the drain, proving the
/// worker still serves a connected client after the listener is gone.
const V3_DRAIN_TOPIC: &str = "itest/v3/during-drain";
/// Topic the staying-client test publishes to late inside the drain, proving
/// the worker still serves the connection shortly before the force-close.
const V3_STAYING_DRAIN_TOPIC: &str = "itest/v3/staying-drain";

/// AC-9, AC-10 with a drain long enough to probe inside: a rumqttc v3 client
/// that stays connected after `BrokerHandle::shutdown` holds the drain open
/// until the configured deadline; the client is still served
/// `LATE_DRAIN_PROBE_AT` after the port stopped accepting, `join` returns no
/// earlier than the full `FORCE_CLOSE_DRAIN_SECS` deadline and no later than
/// `JOIN_BOUND` after `BrokerHandle::shutdown()` was called, and the client
/// then sees its connection end with end-of-stream, never by a timeout.
///
/// The two clocks have different origins on purpose, and each claims only
/// what its origin supports.
/// * The `join` window is `report.join_span`, measured on the harness thread
///   around its own `shutdown()` + `join()` calls. No cross-thread release
///   latency is inside it, so a harness thread descheduled for a whole drain
///   deadline can no longer let an immediate force-close satisfy the floor.
///   The span still starts before the worker arms its drain timer
///   (`src/broker/worker.rs:438-443`), so it can only be longer than the real
///   drain: the full deadline is a floor no correct server can fail — unlike
///   a floor measured from an observed drain start, which lags the timer by
///   up to one probe timeout plus one poll interval and would reject a
///   correct server whenever that lag exceeded the slack allowed for it.
///   This assertion is what pins the drain to its configured length.
/// * The late QoS 1 round-trip is timed from the instant the port refuses new
///   connections. That instant follows `drop(listener)`
///   (`src/broker/worker.rs:438`), which is on the shutdown path but BEFORE
///   `monoio::time::timeout` arms the drain timer two statements later, and
///   the refusal can also be observed late. So the probe is at least
///   `LATE_DRAIN_PROBE_AT` after the worker stopped accepting, which is NOT
///   the same as `LATE_DRAIN_PROBE_AT` into the timed drain: a worker
///   descheduled between those two statements shifts the probe earlier into
///   the drain by that delay. The probe is therefore a clock-free liveness
///   fact — the worker still serves a connected client seconds after it
///   stopped accepting, so a force-close at the listener close is ruled out —
///   and not a measurement of the probe's position inside the drain. Pinning
///   the deadline itself is the `joined_after` assertion's job.
///
/// AC-10 is observed strictly AFTER the teardown report, so the end-of-stream
/// the client sees is bounded from `join` returning, not from the force-close
/// racing it.
///
/// This is the only staying-client test that sends a publish inside the
/// drain. The one-second test sets its teardown flag before shutdown and
/// cannot tell a close at the listener close from one at the deadline, so a
/// server that closes the client at once yet joins at the deadline is caught
/// here, by the late round-trip, and nowhere else.
#[test]
#[allow(clippy::too_many_lines)]
fn shutdown_force_closes_a_staying_client_only_after_the_drain_deadline() {
    let harness = common::ServerHarness::start(FORCE_CLOSE_DRAIN_SECS);
    let port = harness.port();

    let mut opts = rumqttc::MqttOptions::new(V3_STAYING_CLIENT_ID, "127.0.0.1", harness.port());
    opts.set_keep_alive(KEEP_ALIVE);
    let (client, mut connection) = rumqttc::Client::new(opts, REQUEST_CHANNEL_CAP);

    let (event_tx, event_rx): (Sender<V3EventMsg>, V3EventReceiver) = std::sync::mpsc::channel();
    let teardown = Arc::new(AtomicBool::new(false));
    let mut session = common::Session::new();

    let driver = common::spawn_driver(
        DRIVER_THREAD_NAME,
        move || connection.recv().ok(),
        event_tx,
        Arc::clone(&teardown),
    );

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

    // AC-9's window is timed by the harness thread itself, from its own
    // `shutdown()` call, which is at or before the worker's drain timer.
    harness.begin_shutdown();
    // Once the port refuses, the listener is gone, so the worker is on its
    // shutdown path. The drain timer arms shortly after that, at an instant
    // this test cannot observe.
    common::wait_until_refusing(port, REFUSING_BOUND);

    // The sleep starts at the observed refusal, so the publish below lands at
    // least `LATE_DRAIN_PROBE_AT` after the worker stopped accepting — inside
    // the drain, though not at a pinned offset into it. With the teardown flag
    // still clear, any connection error from here to the acknowledgement is an
    // in-session failure: the drain is supposed to keep serving this client
    // until its deadline.
    std::thread::sleep(LATE_DRAIN_PROBE_AT);
    client
        .publish(
            V3_STAYING_DRAIN_TOPIC,
            rumqttc::QoS::AtLeastOnce,
            false,
            QOS1_PAYLOAD.to_vec(),
        )
        .expect("late in-drain publish call");
    let puback = common::assert_outcome(
        common::next_event(&event_rx, ACK_BOUND, &teardown, &mut session),
        "late in-drain PubAck",
    );
    assert!(
        matches!(puback, rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_))),
        "expected PubAck {LATE_DRAIN_PROBE_AT:?} after the port stopped accepting, got {puback:?}",
    );
    assert_eq!(
        harness.next_publish(CALLBACK_BOUND),
        seen(
            V3_STAYING_DRAIN_TOPIC,
            &QOS1_PAYLOAD,
            uring_mqtt::QoS::AtLeastOnce,
            false
        ),
        "the late in-drain publish reached the callback altered",
    );

    // Flip the flag before the force-close so it surfaces as
    // `ClosedAfterDisconnect` (which `is_end_of_stream` classifies
    // correctly) rather than a session-window violation.
    teardown.store(true, Ordering::SeqCst);

    // AC-9 first: the teardown report is what AC-10's observation has to
    // follow, so take it before looking at the client's socket at all.
    let report = harness.finish();
    assert_eq!(report.join, Ok(()), "server join after shutdown");
    let joined_after = report.join_span;
    let floor = Duration::from_secs(FORCE_CLOSE_DRAIN_SECS);
    assert!(
        joined_after >= floor,
        "join returned {joined_after:?} after BrokerHandle::shutdown(), below the {floor:?} the {FORCE_CLOSE_DRAIN_SECS}s deadline sets, although a client stayed connected",
    );
    assert!(joined_after <= JOIN_BOUND, "join took {joined_after:?}");

    // AC-10: now that `join` has returned, the client's connection must end
    // within `CLOSE_OBSERVE_BOUND`, and end by a close rather than a timeout.
    let end = match common::await_terminal_error(&event_rx, CLOSE_OBSERVE_BOUND) {
        Ok(e) => e,
        Err(o) => panic!(
            "the staying client never saw its connection end after join: {}",
            o.describe()
        ),
    };
    assert!(
        common::is_end_of_stream(&end),
        "expected end-of-stream after the force-close, got {end:?}",
    );

    common::join_driver(driver, DRIVER_JOIN_BOUND);
    drop(client);

    assert_eq!(
        report.unclaimed,
        Vec::new(),
        "the callback received publishes beyond the one sent",
    );
}

/// AC-9, AC-10 at the configured one-second deadline: a rumqttc v3 client
/// that stays connected after `BrokerHandle::shutdown` holds the drain open,
/// `join` returns no earlier than `SHORT_DRAIN_SECS` and no later than
/// `JOIN_BOUND` after `BrokerHandle::shutdown()` was called, and the client
/// then sees its connection end with end-of-stream.
///
/// This is the only real-client test that deliberately keeps a live
/// client connected across a configured one-second drain. The raw-socket
/// in-crate tests `shutdown_then_join_returns_within_drain_deadline` and
/// `worker_force_closes_all_idle_connections_at_drain_deadline` pin the
/// same deadline without a protocol client; this test pins it for a
/// rumqttc session that completed CONNECT and CONNACK.
///
/// Nothing here needs the drain to still be open. A one-second drain cannot
/// hold an in-drain round-trip: the refusal is observed up to one probe
/// timeout plus one poll interval after the listener closes, and a test
/// thread descheduled past the deadline would then publish into a
/// connection a correct server has already force-closed. So the teardown
/// flag is set BEFORE shutdown starts, every close is classified as
/// teardown, and the test then sleeps `SHORT_DRAIN_STALL` on purpose. That
/// stall is schedule stress, not a guarantee: unless the harness or worker
/// thread is itself delayed by about a second, the force-close lands while
/// this thread sleeps, so a later step that needs the open drain fails on
/// almost every run instead of as a rare CI flake.
///
/// The cost: this test cannot tell a force-close at the deadline from a
/// close at the listener close followed by a join at the deadline. The
/// in-drain liveness probe that rules that out belongs to
/// `shutdown_force_closes_a_staying_client_only_after_the_drain_deadline`,
/// whose five-second drain leaves room for it.
#[test]
fn shutdown_force_closes_a_staying_client_at_the_short_drain_deadline() {
    let harness = common::ServerHarness::start(SHORT_DRAIN_SECS);

    let mut opts = rumqttc::MqttOptions::new(V3_SHORT_DRAIN_CLIENT_ID, "127.0.0.1", harness.port());
    opts.set_keep_alive(KEEP_ALIVE);
    let (client, mut connection) = rumqttc::Client::new(opts, REQUEST_CHANNEL_CAP);

    let (event_tx, event_rx): (Sender<V3EventMsg>, V3EventReceiver) = std::sync::mpsc::channel();
    let teardown = Arc::new(AtomicBool::new(false));
    let mut session = common::Session::new();

    let driver = common::spawn_driver(
        DRIVER_THREAD_NAME,
        move || connection.recv().ok(),
        event_tx,
        Arc::clone(&teardown),
    );

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

    // Set before shutdown starts, so the force-close at the deadline is
    // classified as teardown however late this thread runs again.
    teardown.store(true, Ordering::SeqCst);
    // AC-9's window is timed by the harness thread itself, from its own
    // `shutdown()` call, so the stall below is outside it.
    harness.begin_shutdown();
    std::thread::sleep(SHORT_DRAIN_STALL);

    let report = harness.finish();
    assert_eq!(report.join, Ok(()), "server join after shutdown");
    let joined_after = report.join_span;
    let floor = Duration::from_secs(SHORT_DRAIN_SECS);
    assert!(
        joined_after >= floor,
        "join returned {joined_after:?} after BrokerHandle::shutdown(), below the {floor:?} the {SHORT_DRAIN_SECS}s deadline sets, although a client stayed connected",
    );
    assert!(joined_after <= JOIN_BOUND, "join took {joined_after:?}");

    // AC-10: bounded only after the teardown report.
    let end = match common::await_terminal_error(&event_rx, CLOSE_OBSERVE_BOUND) {
        Ok(e) => e,
        Err(o) => panic!(
            "the staying client never saw its connection end after join: {}",
            o.describe()
        ),
    };
    assert!(
        common::is_end_of_stream(&end),
        "expected end-of-stream after the force-close, got {end:?}",
    );

    common::join_driver(driver, DRIVER_JOIN_BOUND);
    drop(client);

    assert_eq!(
        report.unclaimed,
        Vec::new(),
        "the callback received a publish although none was sent",
    );
}

/// AC-11, AC-12: a rumqttc v3 client that is still connected when
/// `BrokerHandle::shutdown` stops accepting connections is served
/// throughout the drain (its QoS 1 publish gets a PUBACK and reaches the
/// callback), and `join` returns within `JOIN_BOUND` once the client
/// disconnects — well before the 30 s drain deadline.
#[test]
#[allow(clippy::too_many_lines)]
fn drain_serves_a_connected_client_and_ends_when_it_leaves() {
    let harness = common::ServerHarness::start(EARLY_DRAIN_SECS);
    let port = harness.port();

    let mut opts = rumqttc::MqttOptions::new(V3_LEAVING_CLIENT_ID, "127.0.0.1", harness.port());
    opts.set_keep_alive(KEEP_ALIVE);
    let (client, mut connection) = rumqttc::Client::new(opts, REQUEST_CHANNEL_CAP);

    let (event_tx, event_rx): (Sender<V3EventMsg>, V3EventReceiver) = std::sync::mpsc::channel();
    let teardown = Arc::new(AtomicBool::new(false));
    let mut session = common::Session::new();

    let driver = common::spawn_driver(
        DRIVER_THREAD_NAME,
        move || connection.recv().ok(),
        event_tx,
        Arc::clone(&teardown),
    );

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

    // The listener is gone, so the worker is draining while this client is
    // still connected.
    harness.begin_shutdown();
    common::wait_until_refusing(port, REFUSING_BOUND);

    // With the teardown flag still clear, any connection error here is an
    // in-session failure — the drain is supposed to keep serving this
    // client.
    client
        .publish(
            V3_DRAIN_TOPIC,
            rumqttc::QoS::AtLeastOnce,
            false,
            QOS1_PAYLOAD.to_vec(),
        )
        .expect("in-drain publish call");
    let puback = common::assert_outcome(
        common::next_event(&event_rx, ACK_BOUND, &teardown, &mut session),
        "in-drain PubAck",
    );
    assert!(
        matches!(puback, rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_))),
        "expected PubAck during the drain, got {puback:?}",
    );
    assert_eq!(
        harness.next_publish(CALLBACK_BOUND),
        seen(
            V3_DRAIN_TOPIC,
            &QOS1_PAYLOAD,
            uring_mqtt::QoS::AtLeastOnce,
            false
        ),
        "the in-drain publish reached the callback altered",
    );

    teardown.store(true, Ordering::SeqCst);
    client.disconnect().expect("client.disconnect");
    drop(client);

    common::join_driver(driver, DRIVER_JOIN_BOUND);

    if let Err(err) = common::await_outgoing_disconnect(&event_rx, DISCONNECT_OBSERVE_BOUND) {
        panic!("outgoing Disconnect: {}", err.describe());
    }

    drop(event_rx);

    let report = harness.finish();
    assert_eq!(report.join, Ok(()), "server join after shutdown");
    let joined_after = report.join_span;
    assert!(
        joined_after <= JOIN_BOUND,
        "join took {joined_after:?}: the drain waited for its {EARLY_DRAIN_SECS}s deadline instead of ending when the last client left",
    );
    assert_eq!(
        report.unclaimed,
        Vec::new(),
        "the callback received publishes beyond the one sent",
    );
}

/// Wait bound the `await_terminal_error` guard test lets expire on purpose.
/// Nothing is queued behind it, so the test waits it out in full and it stays
/// short.
const GUARD_TIMEOUT_BOUND: Duration = Duration::from_millis(200);
/// Packet id the `await_terminal_error` guard test puts on the outgoing
/// PUBLISH observation it queues ahead of the terminal error.
const GUARD_SKIPPED_PKID: u16 = 5;
/// Bound the `wait_until_refusing` guard test gives the helper while a
/// listener is still bound. Longer than the helper's own 50 ms poll interval
/// (`tests/common/mod.rs` `REFUSE_POLL_INTERVAL`), so the helper probes more
/// than once before the bound expires.
const REFUSING_GUARD_BOUND: Duration = Duration::from_millis(100);
/// I/O error kinds `is_end_of_stream` must accept: what a broker force-close
/// and a peer reset leave on the socket.
const CLOSE_IO_KINDS: [std::io::ErrorKind; 3] = [
    std::io::ErrorKind::ConnectionAborted,
    std::io::ErrorKind::ConnectionReset,
    std::io::ErrorKind::BrokenPipe,
];
/// I/O error kinds `is_end_of_stream` must reject: a socket that timed out or
/// would block is still open.
const OPEN_IO_KINDS: [std::io::ErrorKind; 2] =
    [std::io::ErrorKind::TimedOut, std::io::ErrorKind::WouldBlock];

/// Regression guard for `common::is_end_of_stream`, the one check that keeps
/// AC-7 and AC-10 from reading a timeout as proof that the server closed the
/// connection. The live tests only ever reach its `true` branch, so nothing
/// else fails if it starts classifying every error as end-of-stream.
#[test]
fn is_end_of_stream_accepts_only_the_close_io_kinds() {
    for kind in CLOSE_IO_KINDS {
        assert!(
            common::is_end_of_stream(&rumqttc::ConnectionError::Io(std::io::Error::from(kind))),
            "ConnectionError::Io({kind:?}) is an end-of-stream",
        );
        assert!(
            common::is_end_of_stream(&rumqttc::ConnectionError::MqttState(
                rumqttc::StateError::Io(std::io::Error::from(kind))
            )),
            "ConnectionError::MqttState(StateError::Io({kind:?})) is an end-of-stream",
        );
    }

    for kind in OPEN_IO_KINDS {
        assert!(
            !common::is_end_of_stream(&rumqttc::ConnectionError::Io(std::io::Error::from(kind))),
            "ConnectionError::Io({kind:?}) is not an end-of-stream",
        );
        assert!(
            !common::is_end_of_stream(&rumqttc::ConnectionError::MqttState(
                rumqttc::StateError::Io(std::io::Error::from(kind))
            )),
            "ConnectionError::MqttState(StateError::Io({kind:?})) is not an end-of-stream",
        );
    }

    // The two timeout flavours rumqttc reports without an I/O error, and a
    // state error that carries none either: the PLAN's anti-pattern list
    // forbids reading any of them as a close.
    assert!(
        !common::is_end_of_stream(&rumqttc::ConnectionError::NetworkTimeout),
        "ConnectionError::NetworkTimeout is not an end-of-stream",
    );
    assert!(
        !common::is_end_of_stream(&rumqttc::ConnectionError::FlushTimeout),
        "ConnectionError::FlushTimeout is not an end-of-stream",
    );
    assert!(
        !common::is_end_of_stream(&rumqttc::ConnectionError::MqttState(
            rumqttc::StateError::AwaitPingResp
        )),
        "ConnectionError::MqttState(StateError::AwaitPingResp) is not an end-of-stream",
    );
}

/// Regression guard for `common::await_terminal_error`. AC-10 is proof of a
/// force-close only because this helper never reports an expired bound or a
/// closed channel as the connection's terminal error, and never returns an
/// `Ok` event in place of one. The live test reaches its `Ok` path alone.
#[test]
fn await_terminal_error_skips_ok_events_and_rejects_a_timeout() {
    let (tx, rx) = std::sync::mpsc::channel::<V3EventMsg>();

    // Events queued ahead of the terminal error are skipped, not returned.
    tx.send(Ok(rumqttc::Event::Outgoing(rumqttc::Outgoing::PingReq)))
        .expect("queue the client's own PINGREQ observation");
    tx.send(Ok(rumqttc::Event::Outgoing(rumqttc::Outgoing::Publish(
        GUARD_SKIPPED_PKID,
    ))))
    .expect("queue the client's own PUBLISH observation");
    tx.send(Err(rumqttc::ConnectionError::Io(std::io::Error::from(
        std::io::ErrorKind::ConnectionReset,
    ))))
    .expect("queue the terminal error");
    let returned = match common::await_terminal_error(&rx, GUARD_BOUND) {
        Ok(err) => err,
        Err(other) => panic!(
            "the terminal error behind two queued events: {}",
            other.describe()
        ),
    };
    assert!(
        common::is_end_of_stream(&returned),
        "the queued ConnectionReset came back as {returned:?}",
    );

    // An idle channel whose sender is still alive is a timeout, never a close.
    match common::await_terminal_error(&rx, GUARD_TIMEOUT_BOUND) {
        Err(common::RumqttcOutcome::TimedOut(bound)) => assert_eq!(
            bound, GUARD_TIMEOUT_BOUND,
            "TimedOut reports the bound it was given",
        ),
        Err(other) => panic!(
            "expected TimedOut on an idle channel, got: {}",
            other.describe()
        ),
        Ok(err) => panic!("expected TimedOut on an idle channel, got the error {err:?}"),
    }

    // A driver end that dropped is DriverChannelClosed, not a close either.
    drop(tx);
    match common::await_terminal_error(&rx, GUARD_BOUND) {
        Err(common::RumqttcOutcome::DriverChannelClosed) => {}
        Err(other) => panic!(
            "expected DriverChannelClosed once the sender dropped, got: {}",
            other.describe()
        ),
        Ok(err) => {
            panic!("expected DriverChannelClosed once the sender dropped, got the error {err:?}")
        }
    }
}

/// Regression guard for `common::wait_until_refusing`: AC-12's in-drain
/// round-trip means something only because the helper's bound is a hard
/// failure. If the helper returned while the listener was still up, the
/// leaving-client test would publish before the drain had begun and would
/// prove nothing about the drain.
#[test]
#[should_panic(expected = "did not refuse connections within")]
fn wait_until_refusing_panics_while_the_port_still_accepts() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind the probe target");
    let port = listener.local_addr().expect("local_addr").port();

    // The listener stays bound for the whole call, so the kernel accepts
    // every probe from its backlog and the helper must reach its bound.
    common::wait_until_refusing(port, REFUSING_GUARD_BOUND);
}
