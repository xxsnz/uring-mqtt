//! Public-seam tests for the load generator (tasks 5 and 6 of the
//! benchmark-harness feature): `gen::run_generator` drives closed-loop
//! QoS 1/QoS 0 throughput and connections/s against an in-process
//! `MqttBroker`, paces open-loop latency slots at an exact total rate,
//! reports counts that reconcile with the server's `Recorder`, and always
//! returns by its hard deadline even when a test-local peer withholds
//! CONNACK or PUBACK, stops draining, or closes the socket
//! (AC-19 through AC-26). Raw-byte fixtures and the free-port registry
//! mirror `tests/broker_handle_api.rs:29-38` and `bench/tests/twin.rs`.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::num::NonZeroU16;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use rmqtt_codec::v3::{Codec, Packet};
use tokio_util::codec::Decoder;

use bench::gen::{self, GenReport, Mode, PacketIds};
use bench::payload::{monotonic_now_ns, Stamp, TOPIC};
use bench::record::{Recorder, Window};
use bench::wire;
use uring_mqtt::{BrokerConfig, BrokerHandle, MqttBroker};

const NS_PER_SEC: u64 = 1_000_000_000;
/// Origin lead, matching the runner's `GEN_LEAD_SECS` shape: the origin is
/// always at least this far ahead of the clock the test reads it from.
const LEAD_SECS: u64 = 1;
const WARMUP_SECS: u64 = 1;
const WINDOW_SECS: u64 = 2;
const CONNECTIONS: u32 = 4;
const QOS1_WINDOW: u32 = 32;
const DRAIN_GRACE: Duration = Duration::from_secs(2);

/// Watchdog for the broker-backed harness threads below — a safety net
/// against a hang, not the acceptance bound itself.
const HARNESS_TIMEOUT: Duration = Duration::from_secs(30);
/// Watchdog for the hard-deadline tests, generous above their own bound.
const DEADLINE_WATCHDOG: Duration = Duration::from_secs(60);

const CONNACK_OK: [u8; 4] = [0x20, 0x02, 0x00, 0x00];

/// Next whole second at or after `ns`.
fn next_whole_second_at_or_after(ns: u64) -> u64 {
    ns.div_ceil(NS_PER_SEC) * NS_PER_SEC
}

/// Origin `GEN_LEAD_SECS`-equivalent seconds ahead of the clock this test
/// reads it from: the next whole second at or after `now + lead_secs`.
fn origin_with_lead(lead_secs: u64) -> u64 {
    next_whole_second_at_or_after(monotonic_now_ns() + lead_secs * NS_PER_SEC)
}

/// AC-22's slot count for a window ending `span_ns` after the origin: the
/// global slot indices `j` with `⌊j × 10⁹ / rate⌋` below the slot grid end,
/// `SLOT_GRID_END_LEAD_NS` before the window end.
fn grid_slots(rate: u64, span_ns: u64) -> u64 {
    let grid_ns = u128::from(span_ns - gen::SLOT_GRID_END_LEAD_NS);
    u64::try_from((grid_ns * u128::from(rate)).div_ceil(u128::from(NS_PER_SEC)))
        .expect("slot count fits in u64")
}

/// Run `body` on its own thread with a watchdog, so a hang in the function
/// under test fails the test instead of the whole suite.
fn run_bounded<T: Send + 'static>(
    name: &'static str,
    timeout: Duration,
    body: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> T {
    let (tx, rx) = mpsc::channel::<Result<T, String>>();
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            let _ = tx.send(body());
        })
        .expect("spawn harness");
    let Ok(result) = rx.recv_timeout(timeout) else {
        panic!("harness did not report within {timeout:?}")
    };
    result.expect("harness body failed")
}

/// Start an in-process uring `MqttBroker` on a fresh port with a `Recorder`
/// armed for `origin_ns`/`window`, mirroring the task brief's fixed server
/// setup (`MqttBroker::start_with_callback(... .num_workers(1), Some(recorder.callback()))`).
fn start_broker_and_recorder(
    connections: u32,
    origin_ns: u64,
    window: Window,
) -> (BrokerHandle, Recorder, SocketAddr) {
    let port = bench::free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse addr");
    let recorder = Recorder::new(connections);
    recorder.arm(origin_ns, window).expect("arm recorder");
    let config = BrokerConfig::new(addr.to_string()).num_workers(1);
    let handle = MqttBroker::start_with_callback(config, Some(recorder.callback()))
        .expect("start broker");
    (handle, recorder, addr)
}

/// A test-local TCP peer that answers CONNACK once, then never reads again
/// (the kernel's TCP receive buffer fills and the writer stalls). Stands in
/// for a broker that accepts the session but stops draining — the process
/// boundary stands in for a real backpressured peer, not a mock of a module
/// this project owns.
fn silent_after_connack_peer() -> SocketAddr {
    let port = bench::free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse addr");
    let listener = TcpListener::bind(addr).expect("bind silent peer");
    let addr = listener.local_addr().expect("local_addr");
    std::thread::Builder::new()
        .name("gen-silent-after-connack".into())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                std::thread::spawn(move || {
                    // Read enough bytes to consume the CONNECT, then reply
                    // CONNACK and stop reading. The kernel's receive buffer
                    // fills and the client's write_all blocks until the
                    // hard deadline.
                    let mut buf = [0u8; 256];
                    let Ok(n) = stream.read(&mut buf) else { return };
                    if n == 0 {
                        return;
                    }
                    if stream.write_all(&CONNACK_OK).is_err() {
                        return;
                    }
                    // Deliberately do not read again. Keep this thread alive
                    // long enough for the client's write_all to time out;
                    // the harness drops the listener when the test ends.
                    std::thread::sleep(Duration::from_secs(30));
                });
            }
        })
        .expect("spawn silent peer");
    addr
}

/// A test-local TCP peer that answers CONNACK once, then closes the socket.
/// Stands in for a broker that accepts the session and then drops it, so the
/// generator's publish writes fail promptly instead of blocking. A process
/// boundary stand-in, not a mock of a module this project owns.
fn closes_after_connack_peer() -> SocketAddr {
    let port = bench::free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse addr");
    let listener = TcpListener::bind(addr).expect("bind closing peer");
    let addr = listener.local_addr().expect("local_addr");
    std::thread::Builder::new()
        .name("gen-closes-after-connack".into())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                std::thread::spawn(move || {
                    let mut buf = [0u8; 256];
                    let Ok(n) = stream.read(&mut buf) else { return };
                    if n == 0 {
                        return;
                    }
                    if stream.write_all(&CONNACK_OK).is_err() {
                        return;
                    }
                    // Close both directions. Data the client writes after this
                    // draws a RST, so its next write_all fails.
                    drop(stream);
                });
            }
        })
        .expect("spawn closing peer");
    addr
}

/// A test-local TCP peer that accepts every connection and never writes a
/// byte back, standing in for a broker withholding CONNACK. A process
/// boundary stand-in, not a mock of a module this project owns.
fn silent_connack_peer() -> SocketAddr {
    let port = bench::free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse addr");
    let listener = TcpListener::bind(addr).expect("bind silent peer");
    let addr = listener.local_addr().expect("local_addr");
    std::thread::Builder::new()
        .name("gen-silent-connack".into())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                std::thread::spawn(move || {
                    let mut buf = [0u8; 256];
                    loop {
                        match stream.read(&mut buf) {
                            Ok(0) | Err(_) => break,
                            Ok(_) => continue,
                        }
                    }
                });
            }
        })
        .expect("spawn silent peer");
    addr
}

/// A test-local TCP peer that answers CONNACK, then reads and discards every
/// further byte without ever writing a PUBACK, standing in for a broker
/// withholding acknowledgement.
fn silent_puback_peer() -> SocketAddr {
    let port = bench::free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse addr");
    let listener = TcpListener::bind(addr).expect("bind silent peer");
    let addr = listener.local_addr().expect("local_addr");
    std::thread::Builder::new()
        .name("gen-silent-puback".into())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                std::thread::spawn(move || {
                    // 64 KiB read buffer: at the latency test's 100 000
                    // msg/s × ~95 B/msg the peer must drain ~9.5 MB/s of
                    // kernel TCP receive buffer; a 256-byte syscall budget
                    // stalls the writer under WSL2 syscall overhead and
                    // makes the offered-equals-65535 assertion fail.
                    let mut buf = vec![0u8; 65_536].into_boxed_slice();
                    let Ok(n) = stream.read(&mut buf) else { return };
                    if n == 0 {
                        return;
                    }
                    if stream.write_all(&CONNACK_OK).is_err() {
                        return;
                    }
                    loop {
                        match stream.read(&mut buf) {
                            Ok(0) | Err(_) => break,
                            Ok(_) => continue,
                        }
                    }
                });
            }
        })
        .expect("spawn silent peer");
    addr
}

/// The slot grid ends `SLOT_GRID_END_LEAD_NS` before the window end, and
/// never before the window start: a window shorter than that lead leaves
/// no slot the generator may claim, instead of a grid end below its own
/// start whose slot count underflows the window.
#[test]
fn slot_grid_ends_before_the_window_and_never_before_its_start() {
    let origin_ns = 10 * NS_PER_SEC;
    let long = Window {
        start_ns: origin_ns + NS_PER_SEC,
        end_ns: origin_ns + 3 * NS_PER_SEC,
    };
    assert_eq!(
        gen::slot_grid_end_ns(long),
        long.end_ns - gen::SLOT_GRID_END_LEAD_NS
    );

    // A window shorter than the lead: the grid end clamps to the start.
    let short = Window {
        start_ns: origin_ns + NS_PER_SEC,
        end_ns: origin_ns + NS_PER_SEC + gen::SLOT_GRID_END_LEAD_NS / 2,
    };
    assert_eq!(gen::slot_grid_end_ns(short), short.start_ns);
    // Both counts come off the same grid end, so the window holds no
    // slot and `scheduled` stops at the warm-up slots.
    let (scheduled, window_scheduled) = gen::slot_counts(1000, origin_ns, short);
    assert_eq!(
        window_scheduled, 0,
        "a clamped grid schedules no window slot"
    );
    assert_eq!(scheduled, 1000, "the warm-up second still holds its slots");
}

/// AC-22's schedule, pinned with literals. Every other slot assertion in
/// this file goes through `grid_slots`, which reads
/// `gen::SLOT_GRID_END_LEAD_NS` and repeats `slot_counts`' own ceiling
/// arithmetic, so changing the constant and the formula together would
/// keep them green while changing the specified schedule. These numbers
/// are independent of both: a 10 ms lead, and the exact counts a 1000
/// slots/s run schedules over a 1 s warm-up plus a 2 s window.
#[test]
fn slot_schedule_matches_its_specified_lead_and_counts() {
    assert_eq!(
        gen::SLOT_GRID_END_LEAD_NS, 10_000_000,
        "AC-22 pins a 10 ms grid-end lead"
    );
    let origin_ns = 10_000_000_000;
    let window = Window {
        start_ns: 11_000_000_000,
        end_ns: 13_000_000_000,
    };
    assert_eq!(gen::slot_grid_end_ns(window), 12_990_000_000);
    // 2.990 s of grid at 1000 slots/s = 2990 global slots, of which the
    // 1 s warm-up holds the first 1000.
    assert_eq!(gen::slot_counts(1000, origin_ns, window), (2990, 1990));
    assert_eq!(gen::scheduled_slots(1000, origin_ns, window), 2990);
}

/// Regression guard: `wire::encode_publish` produces bytes `rmqtt-codec`'s
/// v3 `Codec` decodes back to the stamp it carries.
#[test]
fn publish_encoding_decodes_with_rmqtt_codec() {
    let stamp = Stamp {
        conn_id: 3,
        seq: 9,
        scheduled_ns: 123_456_789,
    };
    let payload = stamp.encode();
    let packet_id = NonZeroU16::new(9).expect("9 is non-zero");

    let mut dst = Vec::new();
    wire::encode_publish(&mut dst, 1, Some(packet_id), &payload);

    let mut buf = BytesMut::from(dst.as_slice());
    let mut codec = Codec::default();
    let (packet, _consumed) = codec
        .decode(&mut buf)
        .expect("decode must not error")
        .expect("one full packet must be framed");
    let Packet::Publish(publish) = packet else {
        panic!("expected a Publish packet, got {packet:?}")
    };
    assert_eq!(publish.topic.as_ref() as &str, TOPIC);
    assert_eq!(publish.packet_id, Some(packet_id));
    assert_eq!(publish.payload.as_ref(), &payload[..]);
}

/// Regression guard: `PacketIds` never hands out an id still in flight, runs
/// out once every id is allocated, and reuses a released id on the next
/// allocation.
#[test]
fn packet_ids_never_hand_out_an_id_still_in_flight() {
    let mut ids = PacketIds::new();
    let mut seen = std::collections::HashSet::new();
    for _ in 0..65_535u32 {
        let id = ids.allocate().expect("allocate before exhaustion");
        assert!(
            seen.insert(id),
            "allocate must never hand out an id still in flight"
        );
    }
    assert_eq!(
        ids.allocate(),
        None,
        "every id is in flight, so allocate must return None"
    );

    assert!(
        ids.release(5),
        "id 5 was allocated and must report as released"
    );
    let reallocated = ids.allocate().expect("allocate after a release");
    assert_eq!(
        reallocated.get(),
        5,
        "the next allocate must hand out the id just released"
    );
}

/// AC-19, AC-20 — a QoS 1 throughput run offers at least one publish, the
/// server delivers exactly what was offered with no duplicates or foreign
/// payloads, and every offered publish is acked.
#[test]
fn qos1_throughput_offers_exactly_what_the_server_delivers() {
    let (report, delivery) = run_bounded("gen-qos1-throughput", HARNESS_TIMEOUT, move || {
        let origin_ns = origin_with_lead(LEAD_SECS);
        let window = Window {
            start_ns: origin_ns + WARMUP_SECS * NS_PER_SEC,
            end_ns: origin_ns + (WARMUP_SECS + WINDOW_SECS) * NS_PER_SEC,
        };
        let (handle, recorder, addr) = start_broker_and_recorder(CONNECTIONS, origin_ns, window);

        let config = gen::GenConfig {
            addr,
            mode: Mode::Throughput,
            qos: 1,
            connections: CONNECTIONS,
            qos1_window: QOS1_WINDOW,
            origin_ns,
            window,
            drain_grace: DRAIN_GRACE,
        };
        let report: GenReport =
            gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))?;

        handle.shutdown();
        handle.join().map_err(|e| format!("join broker: {e}"))?;
        let delivery = recorder.report();
        Ok((report, delivery))
    });

    assert!(
        report.offered > 0,
        "the generator must offer at least one publish"
    );
    assert_eq!(
        delivery.delivered, report.offered,
        "the server must deliver exactly what was offered"
    );
    assert_eq!(
        report.acked,
        Some(report.offered),
        "every offered QoS 1 publish must be acked"
    );
    assert_eq!(delivery.duplicates, 0);
    assert_eq!(delivery.foreign, 0);
    assert_eq!(report.protocol_errors, 0);
    assert_eq!(report.stalled, 0);
}

/// AC-19 — a QoS 0 throughput run offers at least one publish and the
/// server delivers exactly what was offered; QoS 0 never acks.
#[test]
fn qos0_throughput_offers_exactly_what_the_server_delivers() {
    let (report, delivery) = run_bounded("gen-qos0-throughput", HARNESS_TIMEOUT, move || {
        let origin_ns = origin_with_lead(LEAD_SECS);
        let window = Window {
            start_ns: origin_ns + WARMUP_SECS * NS_PER_SEC,
            end_ns: origin_ns + (WARMUP_SECS + WINDOW_SECS) * NS_PER_SEC,
        };
        let (handle, recorder, addr) = start_broker_and_recorder(CONNECTIONS, origin_ns, window);

        let config = gen::GenConfig {
            addr,
            mode: Mode::Throughput,
            qos: 0,
            connections: CONNECTIONS,
            qos1_window: QOS1_WINDOW,
            origin_ns,
            window,
            drain_grace: DRAIN_GRACE,
        };
        let report: GenReport =
            gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))?;

        handle.shutdown();
        handle.join().map_err(|e| format!("join broker: {e}"))?;
        let delivery = recorder.report();
        Ok((report, delivery))
    });

    assert!(
        report.offered > 0,
        "the generator must offer at least one publish"
    );
    assert_eq!(
        delivery.delivered, report.offered,
        "the server must deliver exactly what was offered"
    );
    assert_eq!(report.acked, None, "QoS 0 must never report an ack count");
}

/// AC-21 — a connections-mode run against a healthy server completes at
/// least one cycle with no connect or protocol errors.
#[test]
fn connections_mode_completes_cycles_without_errors() {
    let report = run_bounded("gen-connections-mode", HARNESS_TIMEOUT, move || {
        let origin_ns = origin_with_lead(LEAD_SECS);
        let window = Window {
            start_ns: origin_ns + WARMUP_SECS * NS_PER_SEC,
            end_ns: origin_ns + (WARMUP_SECS + WINDOW_SECS) * NS_PER_SEC,
        };
        let (handle, _recorder, addr) =
            start_broker_and_recorder(CONNECTIONS, origin_ns, window);

        let config = gen::GenConfig {
            addr,
            mode: Mode::Connections,
            qos: 0,
            connections: CONNECTIONS,
            qos1_window: QOS1_WINDOW,
            origin_ns,
            window,
            drain_grace: DRAIN_GRACE,
        };
        let report: GenReport =
            gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))?;

        handle.shutdown();
        handle.join().map_err(|e| format!("join broker: {e}"))?;
        Ok(report)
    });

    assert!(
        report.cycles_in_window.unwrap_or(0) > 0,
        "connections mode must complete at least one cycle in the window"
    );
    assert_eq!(report.connect_errors, 0);
    assert_eq!(report.protocol_errors, 0);
}

/// AC-24 — when the peer accepts but never writes CONNACK, the generator
/// still returns by its hard deadline and counts every connection as a
/// connect error.
#[test]
fn generator_returns_by_its_deadline_when_connack_is_withheld() {
    let (report, elapsed, bound) =
        run_bounded("gen-deadline-connack", DEADLINE_WATCHDOG, move || {
            let addr = silent_connack_peer();
            let before_ns = monotonic_now_ns();
            let wall_start = Instant::now();
            let origin_ns = origin_with_lead(LEAD_SECS);
            let window = Window {
                start_ns: origin_ns,
                end_ns: origin_ns + NS_PER_SEC,
            };
            let config = gen::GenConfig {
                addr,
                mode: Mode::Throughput,
                qos: 1,
                connections: 2,
                qos1_window: QOS1_WINDOW,
                origin_ns,
                window,
                drain_grace: Duration::from_secs(1),
            };
            let report: GenReport =
                gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))?;
            let elapsed = wall_start.elapsed();
            let bound = Duration::from_nanos(origin_ns.saturating_sub(before_ns))
                + Duration::from_secs(1 + 1 + gen::IO_TIMEOUT_SECS + 2);
            Ok((report, elapsed, bound))
        });

    assert!(
        elapsed <= bound,
        "generator exceeded its hard deadline: {elapsed:?} > {bound:?}"
    );
    assert_eq!(report.connect_errors, 2);
}

/// AC-24 — when the peer acks CONNACK but withholds every PUBACK, the
/// generator still returns by the same hard deadline, having offered exactly
/// one QoS 1 window and acked none of it.
#[test]
fn generator_returns_by_its_deadline_when_puback_is_withheld() {
    let (report, elapsed, bound) =
        run_bounded("gen-deadline-puback", DEADLINE_WATCHDOG, move || {
            let addr = silent_puback_peer();
            let before_ns = monotonic_now_ns();
            let wall_start = Instant::now();
            let origin_ns = origin_with_lead(LEAD_SECS);
            let window = Window {
                start_ns: origin_ns,
                end_ns: origin_ns + NS_PER_SEC,
            };
            let config = gen::GenConfig {
                addr,
                mode: Mode::Throughput,
                qos: 1,
                connections: 1,
                qos1_window: QOS1_WINDOW,
                origin_ns,
                window,
                drain_grace: Duration::from_secs(1),
            };
            let report: GenReport =
                gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))?;
            let elapsed = wall_start.elapsed();
            let bound = Duration::from_nanos(origin_ns.saturating_sub(before_ns))
                + Duration::from_secs(1 + 1 + gen::IO_TIMEOUT_SECS + 2);
            Ok((report, elapsed, bound))
        });

    assert!(
        elapsed <= bound,
        "generator exceeded its hard deadline: {elapsed:?} > {bound:?}"
    );
    assert_eq!(
        report.offered, u64::from(QOS1_WINDOW),
        "one withheld-ack connection offers exactly one QoS 1 window"
    );
    assert_eq!(report.acked, Some(0));
    assert!(report.stalled >= 1);
}

/// AC-22, AC-25 — a latency-mode run against a healthy server offers every
/// scheduled slot at an exact total rate, no slots are missed, the recorder
/// sees them all, and only window-due slots land in `lateness`.
#[test]
fn latency_mode_sends_every_scheduled_slot_at_a_modest_rate() {
    const RATE: u64 = 2_000;
    let (report, delivery) = run_bounded("gen-latency-modest", HARNESS_TIMEOUT, move || {
        let origin_ns = origin_with_lead(LEAD_SECS);
        let window = Window {
            start_ns: origin_ns + WARMUP_SECS * NS_PER_SEC,
            end_ns: origin_ns + (WARMUP_SECS + WINDOW_SECS) * NS_PER_SEC,
        };
        let (handle, recorder, addr) = start_broker_and_recorder(CONNECTIONS, origin_ns, window);

        let config = gen::GenConfig {
            addr,
            mode: Mode::Latency { rate: RATE },
            qos: 1,
            connections: CONNECTIONS,
            qos1_window: QOS1_WINDOW,
            origin_ns,
            window,
            drain_grace: DRAIN_GRACE,
        };
        let report: GenReport =
            gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))?;

        handle.shutdown();
        handle.join().map_err(|e| format!("join broker: {e}"))?;
        let delivery = recorder.report();
        Ok((report, delivery))
    });

    let scheduled = grid_slots(RATE, (WARMUP_SECS + WINDOW_SECS) * NS_PER_SEC);
    let window_scheduled = scheduled - RATE * WARMUP_SECS;
    assert_eq!(report.scheduled, Some(scheduled));
    assert_eq!(report.window_scheduled, Some(window_scheduled));
    assert_eq!(report.offered, scheduled);
    assert_eq!(report.missed_slots, 0);
    assert_eq!(report.connect_errors, 0);
    assert_eq!(report.stalled, 0);
    assert_eq!(report.lateness_over_range, 0);

    assert_eq!(
        delivery.delivered, report.offered,
        "the server must deliver exactly what was offered"
    );
    let recorder_latency_sum: u64 = delivery.latency.iter().map(|(_, c)| *c).sum();
    assert_eq!(
        recorder_latency_sum, window_scheduled,
        "recorder latency must cover every window-due slot"
    );
    let report_latency_sum: u64 = report.lateness.iter().map(|(_, c)| *c).sum();
    assert_eq!(
        report_latency_sum, window_scheduled,
        "report lateness must cover every window-due slot"
    );
}

/// AC-22 — a non-divisible rate schedules the exact `ceil(rate × grid span)`
/// count and offers every scheduled slot at that exact count.
#[test]
fn latency_mode_schedules_the_exact_count_for_a_non_divisible_rate() {
    const RATE: u64 = 7;
    let report = run_bounded("gen-latency-non-divisible", HARNESS_TIMEOUT, move || {
        let origin_ns = origin_with_lead(LEAD_SECS);
        let window = Window {
            start_ns: origin_ns,
            end_ns: origin_ns + WINDOW_SECS * NS_PER_SEC,
        };
        let (handle, _recorder, addr) = start_broker_and_recorder(3, origin_ns, window);

        let config = gen::GenConfig {
            addr,
            mode: Mode::Latency { rate: RATE },
            qos: 1,
            connections: 3,
            qos1_window: QOS1_WINDOW,
            origin_ns,
            window,
            drain_grace: DRAIN_GRACE,
        };
        let report: GenReport =
            gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))?;
        handle.shutdown();
        handle.join().map_err(|e| format!("join broker: {e}"))?;
        Ok(report)
    });

    assert_eq!(
        report.scheduled,
        Some(14),
        "ceil(7 × 1.99 s) = 14 slots must be scheduled"
    );
    assert_eq!(report.offered, 14);
    assert_eq!(report.missed_slots, 0);
}

/// AC-26 — under TCP backpressure, latency mode begins no write at or after
/// the window end and returns within its hard bound.
#[test]
fn latency_mode_begins_no_write_after_the_window_under_backpressure() {
    const RATE: u64 = 200_000;
    let (report, window_end_ns, elapsed, bound) =
        run_bounded("gen-latency-backpressure", DEADLINE_WATCHDOG, move || {
            let addr = silent_after_connack_peer();
            let before_ns = monotonic_now_ns();
            let wall_start = Instant::now();
            let origin_ns = origin_with_lead(LEAD_SECS);
            let window = Window {
                start_ns: origin_ns,
                end_ns: origin_ns + NS_PER_SEC,
            };
            let config = gen::GenConfig {
                addr,
                mode: Mode::Latency { rate: RATE },
                qos: 0,
                connections: 1,
                qos1_window: QOS1_WINDOW,
                origin_ns,
                window,
                drain_grace: Duration::from_secs(1),
            };
            let report: GenReport =
                gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))?;
            let elapsed = wall_start.elapsed();
            let bound = Duration::from_nanos(origin_ns.saturating_sub(before_ns))
                + Duration::from_secs(1 + 1 + gen::IO_TIMEOUT_SECS + 2);
            Ok((report, window.end_ns, elapsed, bound))
        });

    assert!(
        elapsed <= bound,
        "generator exceeded its hard deadline under backpressure: {elapsed:?} > {bound:?}"
    );
    let last = report
        .last_write_start_ns
        .expect("at least one write must have started before the window end");
    assert!(
        last < window_end_ns,
        "last_write_start_ns must precede window.end_ns: {last} >= {window_end_ns}"
    );
    assert!(
        report.scheduled.unwrap_or(0) > 0,
        "scheduled slots must exist for the backpressure window"
    );
}

/// AC-26 — a backpressured run whose rate leaves billions of unsent slots
/// still returns within its hard bound, and accounts for every scheduled slot
/// exactly once. A scan of the remaining slots would hold the connection task
/// far past the deadline at this rate.
#[test]
fn latency_mode_accounts_for_billions_of_remaining_slots_within_the_deadline() {
    const RATE: u64 = 2_000_000_000;
    let (report, elapsed, bound) =
        run_bounded("gen-latency-huge-rate", DEADLINE_WATCHDOG, move || {
            let addr = silent_after_connack_peer();
            let before_ns = monotonic_now_ns();
            let wall_start = Instant::now();
            let origin_ns = origin_with_lead(LEAD_SECS);
            let window = Window {
                start_ns: origin_ns,
                end_ns: origin_ns + NS_PER_SEC,
            };
            let config = gen::GenConfig {
                addr,
                mode: Mode::Latency { rate: RATE },
                qos: 0,
                connections: 1,
                qos1_window: QOS1_WINDOW,
                origin_ns,
                window,
                drain_grace: Duration::from_secs(1),
            };
            let report: GenReport =
                gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))?;
            let elapsed = wall_start.elapsed();
            let bound = Duration::from_nanos(origin_ns.saturating_sub(before_ns))
                + Duration::from_secs(1 + 1 + gen::IO_TIMEOUT_SECS + 2);
            Ok((report, elapsed, bound))
        });

    assert!(
        elapsed <= bound,
        "generator exceeded its hard deadline at a rate it cannot drive: {elapsed:?} > {bound:?}"
    );
    assert_eq!(
        report.scheduled,
        Some(grid_slots(RATE, NS_PER_SEC)),
        "a one-second window at RATE slots/s schedules exactly its slot grid"
    );
    assert_eq!(
        report.offered + report.stalled + report.missed_slots,
        grid_slots(RATE, NS_PER_SEC),
        "every scheduled slot must be offered, stalled or missed exactly once"
    );
    assert!(
        report.missed_slots > 1_000_000_000,
        "the unsent slots of this rate must be counted, not scanned: {}",
        report.missed_slots
    );
}

/// AC-24 — a peer that closes after CONNACK ends the pacer at the first failed
/// publish write: the failed batch is the only `stalled` one, the slots it
/// never reached are counted, and the run returns inside its hard bound.
/// Retrying every later due batch would repeat the failed write and its
/// diagnostic once per batch, millions of times at this rate.
#[test]
fn latency_mode_stops_pacing_after_the_first_failed_publish_write() {
    const RATE: u64 = 2_000_000_000;
    let (report, elapsed, bound) =
        run_bounded("gen-latency-write-failure", DEADLINE_WATCHDOG, move || {
            let addr = closes_after_connack_peer();
            let before_ns = monotonic_now_ns();
            let wall_start = Instant::now();
            let origin_ns = origin_with_lead(LEAD_SECS);
            let window = Window {
                start_ns: origin_ns,
                end_ns: origin_ns + NS_PER_SEC,
            };
            let config = gen::GenConfig {
                addr,
                mode: Mode::Latency { rate: RATE },
                qos: 0,
                connections: 1,
                qos1_window: QOS1_WINDOW,
                origin_ns,
                window,
                drain_grace: Duration::from_secs(1),
            };
            let report: GenReport =
                gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))?;
            let elapsed = wall_start.elapsed();
            let bound = Duration::from_nanos(origin_ns.saturating_sub(before_ns))
                + Duration::from_secs(1 + 1 + gen::IO_TIMEOUT_SECS + 2);
            Ok((report, elapsed, bound))
        });

    assert!(
        elapsed <= bound,
        "generator exceeded its hard deadline against a peer that closed: {elapsed:?} > {bound:?}"
    );
    assert_eq!(
        report.scheduled,
        Some(grid_slots(RATE, NS_PER_SEC)),
        "a one-second window at RATE slots/s schedules exactly its slot grid"
    );
    assert_eq!(
        report.offered + report.stalled + report.missed_slots,
        grid_slots(RATE, NS_PER_SEC),
        "every scheduled slot must be offered, stalled or missed exactly once"
    );
    let batch = u64::try_from(gen::BATCH).expect("BATCH fits in u64");
    assert!(
        report.stalled <= batch,
        "only the batch of the first failed write may count as stalled: {}",
        report.stalled
    );
}

/// AC-23 — a QoS 1 slot whose packet id cannot be allocated counts in
/// `missed_slots`; offered saturates at the `PacketIds` ceiling.
#[test]
fn latency_slot_without_a_free_packet_id_is_counted_missed() {
    const RATE: u64 = 100_000;
    let report = run_bounded("gen-latency-no-packet-id", DEADLINE_WATCHDOG, move || {
        let addr = silent_puback_peer();
        let origin_ns = origin_with_lead(LEAD_SECS);
        let window = Window {
            start_ns: origin_ns,
            end_ns: origin_ns + NS_PER_SEC,
        };
        let config = gen::GenConfig {
            addr,
            mode: Mode::Latency { rate: RATE },
            qos: 1,
            connections: 1,
            qos1_window: QOS1_WINDOW,
            origin_ns,
            window,
            drain_grace: Duration::from_secs(1),
        };
        let report: GenReport =
            gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))?;
        Ok(report)
    });

    assert_eq!(
        report.offered, 65_535,
        "PacketIds has 65 535 ids, so offered must saturate at that count"
    );
    assert!(
        report.missed_slots >= 1,
        "at least one scheduled slot must count as missed when packet ids are exhausted"
    );
}

/// AC-23 + AC-24 — once every packet id of a QoS 1 connection is in flight, a
/// rate that leaves billions of unsendable due slots must still return inside
/// the hard bound. Retrying one slot at a time would scan `PacketIds` per slot
/// with no `.await` in between and hold the connection task far past it.
#[test]
fn latency_mode_bounds_packet_id_exhaustion_at_a_rate_it_cannot_drive() {
    const RATE: u64 = 2_000_000_000;
    let (report, elapsed, bound) =
        run_bounded("gen-latency-no-packet-id-huge-rate", DEADLINE_WATCHDOG, move || {
            let addr = silent_puback_peer();
            let before_ns = monotonic_now_ns();
            let wall_start = Instant::now();
            let origin_ns = origin_with_lead(LEAD_SECS);
            let window = Window {
                start_ns: origin_ns,
                end_ns: origin_ns + NS_PER_SEC,
            };
            let config = gen::GenConfig {
                addr,
                mode: Mode::Latency { rate: RATE },
                qos: 1,
                connections: 1,
                qos1_window: QOS1_WINDOW,
                origin_ns,
                window,
                drain_grace: Duration::from_secs(1),
            };
            let report: GenReport =
                gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))?;
            let elapsed = wall_start.elapsed();
            let bound = Duration::from_nanos(origin_ns.saturating_sub(before_ns))
                + Duration::from_secs(1 + 1 + gen::IO_TIMEOUT_SECS + 2);
            Ok((report, elapsed, bound))
        });

    assert!(
        elapsed <= bound,
        "generator exceeded its hard deadline with every packet id in flight: \
         {elapsed:?} > {bound:?}"
    );
    assert_eq!(
        report.scheduled,
        Some(grid_slots(RATE, NS_PER_SEC)),
        "a one-second window at RATE slots/s schedules exactly its slot grid"
    );
    assert!(
        report.missed_slots > 1_000_000_000,
        "the unsendable slots of this rate must be counted, not scanned: {}",
        report.missed_slots
    );
    assert!(
        report.offered <= 65_535,
        "a QoS 1 connection cannot offer more slots than it has packet ids: {}",
        report.offered
    );
}

/// A QoS 1 window wider than the packet-id space holds permits no packet can
/// use, so `run_generator` refuses it before opening any connection.
#[test]
fn generator_refuses_a_qos1_window_past_the_packet_id_space() {
    let origin_ns = origin_with_lead(LEAD_SECS);
    let config = gen::GenConfig {
        addr: SocketAddr::from(([127, 0, 0, 1], 1)),
        mode: Mode::Throughput,
        qos: 1,
        connections: 1,
        qos1_window: u32::from(u16::MAX) + 1,
        origin_ns,
        window: Window {
            start_ns: origin_ns,
            end_ns: origin_ns + WINDOW_SECS * NS_PER_SEC,
        },
        drain_grace: DRAIN_GRACE,
    };
    let err = gen::run_generator(&config).expect_err("an oversized window must be refused");
    assert!(err.contains("qos1_window"), "unexpected refusal: {err}");
}

/// A drain grace whose nanosecond count overflows a `u64` is refused
/// before any connection opens. `run_generator` adds the grace to
/// `window.end_ns` in nanoseconds, so without the bound the conversion
/// panics and the runner sees a signal instead of a `GenReport`.
#[test]
fn generator_refuses_a_drain_grace_past_the_nanosecond_range() {
    let origin_ns = origin_with_lead(LEAD_SECS);
    let config = gen::GenConfig {
        addr: SocketAddr::from(([127, 0, 0, 1], 1)),
        mode: Mode::Throughput,
        qos: 0,
        connections: 1,
        qos1_window: QOS1_WINDOW,
        origin_ns,
        window: Window {
            start_ns: origin_ns,
            end_ns: origin_ns + WINDOW_SECS * NS_PER_SEC,
        },
        // u64::MAX nanoseconds is about 18_446_744_073 seconds.
        drain_grace: Duration::from_secs(18_446_744_074),
    };
    let err = gen::run_generator(&config).expect_err("an oversized grace must be refused");
    assert!(err.contains("drain grace"), "unexpected refusal: {err}");
}

/// A window starting before the origin has no slot schedule: the start
/// offset saturates to zero and the origin sleep would run long past
/// `window.end + drain_grace`. `run_generator` refuses it instead of
/// sleeping and reporting on a window that had already passed.
#[test]
fn generator_refuses_a_window_starting_before_its_origin() {
    let origin_ns = origin_with_lead(LEAD_SECS) + 3600 * NS_PER_SEC;
    let config = gen::GenConfig {
        addr: SocketAddr::from(([127, 0, 0, 1], 1)),
        mode: Mode::Latency { rate: 1000 },
        qos: 0,
        connections: 1,
        qos1_window: QOS1_WINDOW,
        origin_ns,
        window: Window {
            start_ns: origin_ns - 2 * NS_PER_SEC,
            end_ns: origin_ns - NS_PER_SEC,
        },
        drain_grace: DRAIN_GRACE,
    };
    let err = gen::run_generator(&config).expect_err("a pre-origin window must be refused");
    assert!(err.contains("origin"), "unexpected refusal: {err}");
}
