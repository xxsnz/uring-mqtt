//! CLI contract tests for `bench sub` (task 7 of the benchmark-harness
//! feature, AC-16, AC-17, AC-18, AC-51, AC-58): the mosquitto-side
//! subscriber endpoint completes the ARM handshake over stdin/stdout,
//! records every stamped PUBLISH, acks QoS 1, and on SIGTERM writes a
//! `bench::record::DeliveryReport` with `dropped: null`. Each fake broker
//! below is a process-boundary stand-in for mosquitto (not a mock of a
//! module this project owns).
//!
//! Raw MQTT v3.1.1 byte layout mirrors `bench/tests/twin.rs` /
//! `tests/broker_handle_api.rs:31-39`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;

use bench::payload::{monotonic_now_ns, Stamp, PAYLOAD_BYTES, TOPIC};
use bench::record::DeliveryReport;

const READY_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_TIMEOUT: Duration = Duration::from_secs(15);
/// How long the drain test's sender waits before its last line — long enough
/// that a collector which stops at an empty channel misses it.
const DRAIN_LAST_LINE_DELAY: Duration = Duration::from_millis(200);
const WAIT_POLL: Duration = Duration::from_millis(50);
const ONE_SEC_NS: u64 = 1_000_000_000;
/// Matches `bench::record::WINDOW_MAX_SPAN_NS` exactly, so the window the
/// test arms is the widest an `ARM` line may ask for.
const WINDOW_SPAN_NS: u64 = 3_600_000_000_000;
const FLOOD_PACKETS: usize = 200_000;

fn report_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("bench-cli-sub-{tag}-{}.json", std::process::id()))
}

/// Owns the spawned sub process. `Drop` kills and reaps a child that is
/// still running, so a failed assertion or a timeout cannot leave a sub
/// holding its port.
struct SubChild(Child);

impl std::ops::Deref for SubChild {
    type Target = Child;

    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for SubChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for SubChild {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

/// Spawn `bench sub` with piped stdio. Stderr is drained on a background
/// thread so a child that writes diagnostics cannot block on a full pipe.
fn spawn_sub(
    qos: u8,
    addr: &str,
    report: &std::path::Path,
) -> (SubChild, Receiver<String>, ChildStdin) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_bench"))
        .arg("sub")
        .arg("--addr")
        .arg(addr)
        .arg("--qos")
        .arg(qos.to_string())
        .arg("--connections")
        .arg("1")
        .arg("--report")
        .arg(report)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bench sub");

    let stdout = child.stdout.take().expect("child stdout piped");
    let stderr = child.stderr.take().expect("child stderr piped");
    let stdin = child.stdin.take().expect("child stdin piped");

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            match line {
                Ok(line) => {
                    if tx.send(line).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    std::thread::spawn(move || {
        let mut sink = Vec::new();
        let _ = BufReader::new(stderr).read_to_end(&mut sink);
    });

    (SubChild(child), rx, stdin)
}

fn recv_line(rx: &Receiver<String>, timeout: Duration, expect: &str) {
    let line = rx
        .recv_timeout(timeout)
        .unwrap_or_else(|e| panic!("expected {expect:?} within {timeout:?}: {e}"));
    assert_eq!(line, expect, "expected {expect:?}, got {line:?}");
}

/// Every stdout line `bench sub` printed, read until its forwarding
/// thread ends because the child closed stdout, so a line still in
/// transit is never missed. A stdout still open after `READY_TIMEOUT`
/// with no line fails the test instead of hanging it.
fn drain_stdout(rx: &Receiver<String>) -> Vec<String> {
    let mut seen = Vec::new();
    loop {
        match rx.recv_timeout(READY_TIMEOUT) {
            Ok(line) => seen.push(line),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return seen,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!("stdout forwarding did not end within {READY_TIMEOUT:?}")
            }
        }
    }
}

fn wait_exit_success(child: &mut Child, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            assert!(status.success(), "expected exit 0, got {status:?}");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "sub did not exit within {timeout:?} after SIGTERM"
        );
        std::thread::sleep(WAIT_POLL);
    }
}

fn wait_exit_code(child: &mut Child, timeout: Duration, expected: i32) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            assert_eq!(
                status.code(),
                Some(expected),
                "expected exit {expected}, got {status:?}"
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "sub did not exit within {timeout:?}"
        );
        std::thread::sleep(WAIT_POLL);
    }
}

fn send_sigterm(child: &Child) {
    let pid = i32::try_from(child.id()).expect("child pid fits in i32");
    kill(Pid::from_raw(pid), Signal::SIGTERM).expect("send SIGTERM");
}

fn read_report(report: &std::path::Path) -> DeliveryReport {
    let data = std::fs::read_to_string(report).expect("read report file");
    serde_json::from_str(&data).expect("parse report json")
}

fn read_json(report: &std::path::Path) -> String {
    std::fs::read_to_string(report).expect("read report file")
}

/// MQTT v3 CONNACK carrying "connection accepted" (return code 0).
const CONNACK_OK: [u8; 4] = [0x20, 0x02, 0x00, 0x00];
/// MQTT v3 CONNACK carrying "connection accepted" with Session Present set.
const CONNACK_SESSION_PRESENT: [u8; 4] = [0x20, 0x02, 0x01, 0x00];
/// MQTT v3 CONNACK whose Remaining Length (3) is one past the two bytes MQTT 3.1.1 defines.
const CONNACK_LONG_BODY: [u8; 5] = [0x20, 0x03, 0x00, 0x00, 0xFF];

/// MQTT v3 SUBACK granting QoS 1 for a single subscription (packet id 1).
const SUBACK_QOS1: [u8; 5] = [0x90, 0x03, 0x00, 0x01, 0x01];
/// MQTT v3 SUBACK refusing the subscription (return code 0x80).
const SUBACK_REFUSED: [u8; 5] = [0x90, 0x03, 0x00, 0x01, 0x80];

/// CONNACK immediately followed by a whole SUBACK, sent before any
/// SUBSCRIBE: a complete unsolicited frame.
const CONNACK_THEN_SUBACK: [u8; 9] = [0x20, 0x02, 0x00, 0x00, 0x90, 0x03, 0x00, 0x01, 0x01];
/// CONNACK immediately followed by a bare SUBACK fixed header whose body is
/// withheld — the shape a codec poll would still swallow.
const CONNACK_THEN_SUBACK_HEADER: [u8; 6] = [0x20, 0x02, 0x00, 0x00, 0x90, 0x03];
/// CONNACK immediately followed by a single unsolicited byte.
const CONNACK_THEN_ONE_BYTE: [u8; 5] = [0x20, 0x02, 0x00, 0x00, 0x90];

/// Fixed-header first bytes of the two requests the fake broker reads.
const CONNECT_FIRST_BYTE: u8 = 0x10;
const SUBSCRIBE_FIRST_BYTE: u8 = 0x82;
/// Remaining Length varint: payload bits per byte and the continuation flag.
const VARINT_PAYLOAD_MASK: u8 = 0x7F;
const VARINT_CONTINUATION_BIT: u8 = 0x80;
const VARINT_MULTIPLIER_STEP: usize = 128;
/// Multiplier of the fourth (last legal) Remaining Length byte.
const VARINT_MAX_MULTIPLIER: usize = 128 * 128 * 128;

/// MQTT v3 PUBLISH packet for `payload` on `TOPIC` at the given QoS and
/// packet id. `qos == 0` ignores `packet_id`.
fn encode_publish(qos: u8, packet_id: u16, payload: &[u8; PAYLOAD_BYTES]) -> Vec<u8> {
    let topic_len = u16::try_from(TOPIC.len()).expect("topic len fits u16");
    let mut body = Vec::new();
    body.extend_from_slice(&topic_len.to_be_bytes());
    body.extend_from_slice(TOPIC.as_bytes());
    if qos > 0 {
        body.extend_from_slice(&packet_id.to_be_bytes());
    }
    body.extend_from_slice(payload);
    let fixed = 0x30 | (qos << 1);
    let mut packet = vec![fixed];
    let remaining_len = body.len();
    let len_byte = u8::try_from(remaining_len).expect("PUBLISH body fits one byte");
    packet.push(len_byte);
    packet.extend_from_slice(&body);
    packet
}

/// A fake MQTT v3 broker that accepts a connection, replies CONNACK /
/// SUBACK, then drives an explicit scenario. The scenario runs on a
/// dedicated thread so the listener can outlive the brief's sub-handshake
/// (`bench sub` holds the TCP read side open until SIGTERM).
struct FakeBroker {
    addr: SocketAddr,
}

/// Spawn the broker. `scenario` runs on a dedicated thread that owns the
/// accepted stream; it sees the accepted socket post-CONNECT/keep-alive.
fn spawn_fake_broker(
    name: &'static str,
    scenario: impl FnOnce(std::net::TcpStream) + Send + 'static,
) -> FakeBroker {
    let port = bench::free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse addr");
    let listener = TcpListener::bind(addr).expect("bind fake broker");
    let actual = listener.local_addr().expect("local_addr");
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            let Ok((stream, _)) = listener.accept() else { return };
            scenario(stream);
        })
        .expect("spawn fake broker");
    FakeBroker { addr: actual }
}

impl FakeBroker {
    fn addr(&self) -> String {
        format!("{}", self.addr)
    }
}

/// Drive the standard SUBSCRIBE handshake: read the SUBSCRIBE (whatever the
/// sub sends), send the configured SUBACK, then hand the stream to
/// `after_suback` for the test's data scenario.
fn handshake_and(
    stream: &mut std::net::TcpStream,
    suback: &[u8],
    after_suback: impl FnOnce(&mut std::net::TcpStream),
) {
    // Read the whole CONNECT before replying: TCP may deliver the frame in
    // any number of segments, and a CONNACK sent after a short read would
    // leave the CONNECT body in the stream, so the SUBSCRIBE read below
    // would start inside it and never recognise the packet.
    let Some(connect) = read_frame(stream) else { return };
    if connect.first() != Some(&CONNECT_FIRST_BYTE) {
        return;
    }
    if stream.write_all(&CONNACK_OK).is_err() {
        return;
    }
    let Some(subscribe) = read_frame(stream) else { return };
    if subscribe.first() != Some(&SUBSCRIBE_FIRST_BYTE) {
        return;
    }
    if stream.write_all(suback).is_err() {
        return;
    }
    after_suback(stream);
}

/// Read one whole MQTT frame: the fixed-header byte, the Remaining Length
/// varint, then exactly that many body bytes. Returns the fixed-header byte
/// followed by the body, or `None` on a closed stream, a read error or an
/// illegal Remaining Length.
fn read_frame(stream: &mut std::net::TcpStream) -> Option<Vec<u8>> {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first).ok()?;
    let mut remaining = 0usize;
    let mut multiplier = 1usize;
    loop {
        let mut len_byte = [0u8; 1];
        stream.read_exact(&mut len_byte).ok()?;
        remaining += usize::from(len_byte[0] & VARINT_PAYLOAD_MASK) * multiplier;
        if len_byte[0] & VARINT_CONTINUATION_BIT == 0 {
            break;
        }
        if multiplier >= VARINT_MAX_MULTIPLIER {
            return None;
        }
        multiplier *= VARINT_MULTIPLIER_STEP;
    }
    let mut frame = vec![0u8; remaining + 1];
    frame[0] = first[0];
    stream.read_exact(&mut frame[1..]).ok()?;
    Some(frame)
}

/// Read one PUBACK and return its packet id. Reads a 4-byte packet header:
/// 0x40, 0x02, id_hi, id_lo.
fn read_puback(stream: &mut std::net::TcpStream) -> u16 {
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).expect("read PUBACK");
    assert_eq!(buf[0], 0x40, "expected PUBACK fixed header");
    assert_eq!(buf[1], 0x02, "expected PUBACK remaining length 2");
    u16::from_be_bytes([buf[2], buf[3]])
}

/// AC-16 + AC-17: the broker grants a QoS 1 subscription, three stamped
/// PUBLISHes arrive, every PUBACK id matches, SIGTERM turns the run into
/// `delivered 3` with `dropped: null`.
#[test]
fn sub_records_and_acks_every_delivered_publish_then_reports_on_sigterm() {
    // The deliveries wait for the test's ARMED signal. A PUBLISH that reaches
    // the sub before its ARM line counts as foreign, not as a delivery, so an
    // unsynchronised broker could fail the `delivered 3` assertion while the
    // sub behaved correctly (`PROGRESS.md:157` orders the scenario the same
    // way: wait `ARMED`, then send).
    let (armed_tx, armed_rx) = mpsc::channel::<()>();
    let broker = spawn_fake_broker("sub-test-fake", move |mut stream| {
        handshake_and(&mut stream, &SUBACK_QOS1, move |stream| {
            if armed_rx.recv_timeout(READY_TIMEOUT).is_err() {
                return;
            }
            // Send three QoS 1 PUBLISHes with consecutive packet ids and
            // distinct stamps, then read each PUBACK.
            for (i, expected_id) in (1u16..=3u16).enumerate() {
                let seq = u64::try_from(i).expect("seq fits u64");
                let stamp = Stamp {
                    conn_id: 0,
                    seq,
                    scheduled_ns: monotonic_now_ns(),
                };
                let packet = encode_publish(1, expected_id, &stamp.encode());
                if stream.write_all(&packet).is_err() {
                    return;
                }
                let id = read_puback(stream);
                assert_eq!(id, expected_id, "PUBACK id out of order");
            }
            // Hold the connection open until SIGTERM closes the sub.
            std::thread::sleep(Duration::from_secs(30));
        });
    });

    let port = broker.addr();
    let report = report_path("records-acks");
    let _ = std::fs::remove_file(&report);

    let (mut child, rx, mut stdin) = spawn_sub(1, &port, &report);
    recv_line(&rx, READY_TIMEOUT, "READY");

    let origin_ns = (monotonic_now_ns() / ONE_SEC_NS) * ONE_SEC_NS;
    let end_ns = origin_ns + WINDOW_SPAN_NS;
    writeln!(stdin, "ARM {origin_ns} {origin_ns} {end_ns}").expect("write ARM line");
    stdin.flush().expect("flush ARM line");
    recv_line(&rx, READY_TIMEOUT, "ARMED");
    armed_tx.send(()).expect("release the fake broker's deliveries");

    // Give the fake broker a beat to deliver every stamped PUBLISH and read
    // every PUBACK.
    std::thread::sleep(Duration::from_millis(200));

    send_sigterm(&child);
    wait_exit_success(&mut child, EXIT_TIMEOUT);

    let delivery = read_report(&report);
    assert_eq!(delivery.delivered, 3, "three stamps delivered exactly once");
    assert_eq!(delivery.duplicates, 0, "no duplicate stamps");
    assert_eq!(delivery.foreign, 0, "every stamp decoded on TOPIC");

    let raw = read_json(&report);
    assert!(raw.contains("\"dropped\": null"), "raw JSON must show dropped: null, got {raw:?}");

    let _ = std::fs::remove_file(&report);
}

/// AC-51: SIGTERM before the ARM handshake writes the report and exits 0.
#[test]
fn sub_reports_and_exits_on_sigterm_before_arm() {
    let broker = spawn_fake_broker("sub-test-stay-armed", |mut stream| {
        handshake_and(&mut stream, &SUBACK_QOS1, |_stream| {
            std::thread::sleep(Duration::from_secs(30));
        });
    });

    let port = broker.addr();
    let report = report_path("before-arm");
    let _ = std::fs::remove_file(&report);

    // The stdin pipe stays open through SIGTERM and writes nothing, so the
    // signal arrives while the reader thread is blocked in `read_line`.
    let (mut child, rx, _stdin) = spawn_sub(1, &port, &report);
    recv_line(&rx, READY_TIMEOUT, "READY");

    send_sigterm(&child);
    wait_exit_success(&mut child, EXIT_TIMEOUT);

    let delivery = read_report(&report);
    assert_eq!(delivery.delivered, 0, "no client ever delivered a stamp");
    assert_eq!(delivery.foreign, 0, "unarmed recorder stays silent");

    let _ = std::fs::remove_file(&report);
}

/// AC-51: SIGTERM while the broker withholds its CONNACK — before `READY`
/// and before any ARM line — still writes the report and exits 0. The signal
/// handler must therefore be installed before the network handshake, because
/// the default action would kill the child with no report at all.
#[test]
fn sub_reports_and_exits_on_sigterm_during_setup() {
    // The accepted connection is the sync point: `bench sub` installs its
    // signal handler before it connects, so an accepted socket proves the
    // handler is in place and the SIGTERM below cannot hit the default action.
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let broker = spawn_fake_broker("sub-test-silent-setup", move |_stream| {
        let _ = accepted_tx.send(());
        // Answer nothing: the sub waits in its CONNACK read until the signal.
        std::thread::sleep(Duration::from_secs(30));
    });

    let port = broker.addr();
    let report = report_path("during-setup");
    let _ = std::fs::remove_file(&report);

    let (mut child, rx, _stdin) = spawn_sub(1, &port, &report);
    accepted_rx
        .recv_timeout(READY_TIMEOUT)
        .expect("fake broker accepts the sub's connection");

    send_sigterm(&child);
    wait_exit_success(&mut child, EXIT_TIMEOUT);

    let seen = drain_stdout(&rx);
    assert!(
        !seen.iter().any(|line| line == "READY"),
        "the handshake never completed, so READY must not appear, saw {seen:?}"
    );
    let delivery = read_report(&report);
    assert_eq!(delivery.delivered, 0, "no stamp can arrive before SUBACK");

    let _ = std::fs::remove_file(&report);
}

/// AC-58: SIGTERM while the broker has stopped draining makes `bench sub`
/// write its report and exit 0 within 15 s. The sub still records at
/// least one stamp: even with PUBACK writes blocked, every PUBLISH that
/// the sub already decoded gets recorded before the write stall.
#[test]
fn sub_reports_on_sigterm_while_the_broker_stops_reading() {
    let report = report_path("stuck-broker");
    let _ = std::fs::remove_file(&report);

    let broker = spawn_fake_broker("sub-test-flooder", |mut stream| {
        handshake_and(&mut stream, &SUBACK_QOS1, |stream| {
            // Slam 200 000 QoS 1 PUBLISHes into the kernel socket buffer
            // and never read a PUBACK. The sub's first PUBACK write stalls
            // after the kernel buffer fills; `bench sub` then waits only
            // for the shutdown watch.
            let mut buf = Vec::with_capacity(64 * FLOOD_PACKETS);
            for i in 0..FLOOD_PACKETS {
                let seq = u64::try_from(i).expect("seq fits u64");
                let stamp = Stamp {
                    conn_id: 0,
                    seq,
                    scheduled_ns: monotonic_now_ns(),
                };
                let packet_id = u16::try_from((i % 65_535) + 1).expect("id fits u16");
                encode_publish_with(&mut buf, 1, packet_id, &stamp.encode());
            }
            if stream.write_all(&buf).is_err() {
                return;
            }
            // Hold the connection open without draining the PUBACK writes.
            std::thread::sleep(Duration::from_secs(30));
        });
    });

    let port = broker.addr();
    let (mut child, rx, mut stdin) = spawn_sub(1, &port, &report);
    recv_line(&rx, READY_TIMEOUT, "READY");

    let origin_ns = (monotonic_now_ns() / ONE_SEC_NS) * ONE_SEC_NS;
    let end_ns = origin_ns + WINDOW_SPAN_NS;
    writeln!(stdin, "ARM {origin_ns} {origin_ns} {end_ns}").expect("write ARM line");
    stdin.flush().expect("flush ARM line");
    recv_line(&rx, READY_TIMEOUT, "ARMED");

    // Let the flooder deliver enough to confirm the sub is alive and
    // receiving real stamps.
    std::thread::sleep(Duration::from_secs(1));

    send_sigterm(&child);
    wait_exit_success(&mut child, EXIT_TIMEOUT);

    let delivery = read_report(&report);
    assert!(
        delivery.delivered >= 1,
        "at least one stamp must have been recorded before the PUBACK stall, got {}",
        delivery.delivered
    );

    let _ = std::fs::remove_file(&report);
}

/// Append a single MQTT v3 PUBLISH packet to `dst`. Mirrors `encode_publish`
/// but writes into an existing buffer to avoid 200 000 allocations during
/// the flood.
fn encode_publish_with(dst: &mut Vec<u8>, qos: u8, packet_id: u16, payload: &[u8; PAYLOAD_BYTES]) {
    let topic_len = u16::try_from(TOPIC.len()).expect("topic len fits u16");
    let body_len = 2 + TOPIC.len() + if qos > 0 { 2 } else { 0 } + PAYLOAD_BYTES;
    let fixed = 0x30 | (qos << 1);
    dst.push(fixed);
    dst.push(u8::try_from(body_len).expect("PUBLISH body fits one byte"));
    dst.extend_from_slice(&topic_len.to_be_bytes());
    dst.extend_from_slice(TOPIC.as_bytes());
    if qos > 0 {
        dst.extend_from_slice(&packet_id.to_be_bytes());
    }
    dst.extend_from_slice(payload);
}

/// R2-Q2: the subscription asks for QoS 1, so a QoS 2 delivery is a broker
/// protocol violation this endpoint cannot acknowledge. It must not be counted
/// as a delivery, and the run must end non-zero with its report written.
#[test]
fn sub_exits_nonzero_when_the_broker_delivers_qos_2() {
    // The violating frame waits for ARMED as well: a QoS 2 PUBLISH read
    // before the ARM line ends the run at once, and the sub's stdout would
    // close before the `ARMED` assertion below could see it.
    let (armed_tx, armed_rx) = mpsc::channel::<()>();
    let broker = spawn_fake_broker("sub-test-qos2", move |mut stream| {
        handshake_and(&mut stream, &SUBACK_QOS1, move |stream| {
            if armed_rx.recv_timeout(READY_TIMEOUT).is_err() {
                return;
            }
            let stamp = Stamp {
                conn_id: 0,
                seq: 0,
                scheduled_ns: monotonic_now_ns(),
            };
            let _ = stream.write_all(&encode_publish(2, 1, &stamp.encode()));
            std::thread::sleep(Duration::from_secs(30));
        });
    });

    let port = broker.addr();
    let report = report_path("qos2");
    let _ = std::fs::remove_file(&report);

    let (mut child, rx, mut stdin) = spawn_sub(1, &port, &report);
    recv_line(&rx, READY_TIMEOUT, "READY");

    let origin_ns = (monotonic_now_ns() / ONE_SEC_NS) * ONE_SEC_NS;
    let end_ns = origin_ns + WINDOW_SPAN_NS;
    writeln!(stdin, "ARM {origin_ns} {origin_ns} {end_ns}").expect("write ARM line");
    stdin.flush().expect("flush ARM line");
    recv_line(&rx, READY_TIMEOUT, "ARMED");
    armed_tx.send(()).expect("release the QoS 2 delivery");

    wait_exit_code(&mut child, EXIT_TIMEOUT, 1);

    let delivery = read_report(&report);
    assert_eq!(delivery.delivered, 0, "a QoS 2 frame is never a delivery");
    assert_eq!(delivery.foreign, 0, "the frame is refused before recording");

    let _ = std::fs::remove_file(&report);
}

/// AC-8: a CONNACK whose body is not exactly two bytes must end the run with
/// no READY printed and no report written.
#[test]
fn sub_exits_nonzero_when_the_connack_body_is_not_two_bytes() {
    let broker = spawn_fake_broker("sub-test-long-connack", |mut stream| {
        let Some(connect) = read_frame(&mut stream) else { return };
        if connect.first() != Some(&CONNECT_FIRST_BYTE) {
            return;
        }
        if stream.write_all(&CONNACK_LONG_BODY).is_err() {
            return;
        }
        let Some(subscribe) = read_frame(&mut stream) else { return };
        if subscribe.first() == Some(&SUBSCRIBE_FIRST_BYTE) {
            let _ = stream.write_all(&SUBACK_QOS1);
        }
        std::thread::sleep(Duration::from_secs(5));
    });

    let port = broker.addr();
    let report = report_path("long-connack");
    let _ = std::fs::remove_file(&report);

    let (mut child, rx, _stdin) = spawn_sub(1, &port, &report);

    wait_exit_code(&mut child, EXIT_TIMEOUT, 1);

    let seen = drain_stdout(&rx);
    assert!(
        !seen.iter().any(|line| line == "READY"),
        "a long CONNACK body must never print READY, saw {seen:?}"
    );
    assert!(
        !report.exists(),
        "a long CONNACK body must write no report"
    );

    let _ = std::fs::remove_file(&report);
}

/// AC-18: a SUBACK that refuses the subscription must end the run with no
/// `READY` printed.
#[test]
fn sub_exits_nonzero_when_the_broker_refuses_the_subscription() {
    let broker = spawn_fake_broker("sub-test-refused", |mut stream| {
        handshake_and(&mut stream, &SUBACK_REFUSED, |_stream| {
            // Hand back to the broker by holding the stream briefly; the
            // sub closes the socket on its own way out.
            std::thread::sleep(Duration::from_secs(5));
        });
    });

    let port = broker.addr();
    let report = report_path("refused");
    let _ = std::fs::remove_file(&report);

    let (mut child, rx, _stdin) = spawn_sub(1, &port, &report);

    wait_exit_code(&mut child, EXIT_TIMEOUT, 1);

    let seen = drain_stdout(&rx);
    assert!(
        !seen.iter().any(|line| line == "READY"),
        "a refused subscription must never print READY, saw {seen:?}"
    );
    assert!(
        !report.exists(),
        "a refused subscription must write no report"
    );

    let _ = std::fs::remove_file(&report);
}

/// AC-10: a CONNACK claiming a stored session on a clean-session CONNECT
/// must end the run with no `READY` printed and no report written.
#[test]
fn sub_exits_nonzero_when_the_connack_claims_a_session() {
    let broker = spawn_fake_broker("sub-test-session-present", |mut stream| {
        let Some(connect) = read_frame(&mut stream) else { return };
        if connect.first() != Some(&CONNECT_FIRST_BYTE) {
            return;
        }
        if stream.write_all(&CONNACK_SESSION_PRESENT).is_err() {
            return;
        }
        let Some(subscribe) = read_frame(&mut stream) else { return };
        if subscribe.first() == Some(&SUBSCRIBE_FIRST_BYTE) {
            let _ = stream.write_all(&SUBACK_QOS1);
        }
        std::thread::sleep(Duration::from_secs(5));
    });

    let port = broker.addr();
    let report = report_path("session-present");
    let _ = std::fs::remove_file(&report);

    let (mut child, rx, _stdin) = spawn_sub(1, &port, &report);

    wait_exit_code(&mut child, EXIT_TIMEOUT, 1);

    let seen = drain_stdout(&rx);
    assert!(
        !seen.iter().any(|line| line == "READY"),
        "a session-claiming CONNACK must never print READY, saw {seen:?}"
    );
    assert!(
        !report.exists(),
        "a session-claiming CONNACK must write no report"
    );

    let _ = std::fs::remove_file(&report);
}

/// AC-8: broker bytes that reach `bench sub` after its CONNACK but before
/// any SUBSCRIBE must be refused without writing SUBSCRIBE, whether they
/// form a whole frame, a bare fixed header, or a single byte.
#[test]
fn sub_fails_before_subscribing_when_bytes_arrive_ahead_of_its_subscribe() {
    for (reply, tag) in [
        (&CONNACK_THEN_SUBACK[..], "early-suback"),
        (&CONNACK_THEN_SUBACK_HEADER[..], "early-header"),
        (&CONNACK_THEN_ONE_BYTE[..], "early-byte"),
    ] {
        let (seen_tx, seen_rx) = std::sync::mpsc::channel::<bool>();
        let reply = reply.to_vec();
        let broker = spawn_fake_broker("sub-test-early-bytes", move |mut stream| {
            let Some(connect) = read_frame(&mut stream) else { return };
            if connect.first() != Some(&CONNECT_FIRST_BYTE) {
                return;
            }
            if stream.write_all(&reply).is_err() {
                return;
            }
            let subscribed = read_frame(&mut stream)
                .is_some_and(|frame| frame.first() == Some(&SUBSCRIBE_FIRST_BYTE));
            let _ = seen_tx.send(subscribed);
            if subscribed {
                let _ = stream.write_all(&SUBACK_QOS1);
                std::thread::sleep(Duration::from_secs(5));
            }
        });

        let port = broker.addr();
        let report = report_path(tag);
        let _ = std::fs::remove_file(&report);

        let (mut child, rx, _stdin) = spawn_sub(1, &port, &report);

        wait_exit_code(&mut child, EXIT_TIMEOUT, 1);

        let seen = drain_stdout(&rx);

        assert_eq!(
            seen_rx.recv_timeout(EXIT_TIMEOUT),
            Ok(false),
            "bench sub must refuse before writing SUBSCRIBE ({tag})"
        );
        assert!(
            !seen.iter().any(|line| line == "READY"),
            "early bytes before SUBSCRIBE must never print READY ({tag}), saw {seen:?}"
        );
        assert!(
            !report.exists(),
            "early bytes before SUBSCRIBE must write no report ({tag})"
        );

        let _ = std::fs::remove_file(&report);
    }
}

/// `drain_stdout` returns every line its sender wrote until it disconnects,
/// even when the last line lands after a quiet beat — a helper that stops at
/// an empty channel or collects synchronously would miss the delayed line.
#[test]
fn drain_stdout_returns_every_line_until_the_sender_disconnects() {
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::Builder::new()
        .name("cli-sub-drain-test".into())
        .spawn(move || {
            let _ = tx.send("first".to_string());
            std::thread::sleep(DRAIN_LAST_LINE_DELAY);
            let _ = tx.send("READY".to_string());
        })
        .expect("spawn drain sender");

    let seen = drain_stdout(&rx);
    assert_eq!(
        seen,
        vec!["first".to_string(), "READY".to_string()],
        "drain_stdout must wait for the sender's last line and drop"
    );
}
