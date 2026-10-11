//! CLI contract tests for `bench server` (task 4 of the benchmark-harness
//! feature, AC-15, AC-16, AC-51): the subcommand completes the ARM
//! handshake over stdin/stdout and, on SIGTERM, writes a
//! `bench::record::DeliveryReport` with the drop total before exiting 0.
//! Raw MQTT v3.1.1 byte layout mirrors `bench/tests/twin.rs:26-50` /
//! `tests/broker_handle_api.rs:31-39`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;

use bench::payload::{monotonic_now_ns, Stamp, PAYLOAD_BYTES, TOPIC};
use bench::record::{DeliveryReport, CONNECTIONS_MAX};

const READY_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_TIMEOUT: Duration = Duration::from_secs(15);
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const WAIT_POLL: Duration = Duration::from_millis(50);
const ONE_SEC_NS: u64 = 1_000_000_000;
/// Matches `bench::record::WINDOW_MAX_SPAN_NS` exactly, so the window the
/// test arms is the widest an `ARM` line may ask for.
const WINDOW_SPAN_NS: u64 = 3_600_000_000_000;

fn report_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("bench-cli-server-{tag}-{}.json", std::process::id()))
}

/// Owns the spawned server process. Dropped on normal return and on unwind
/// alike, so a failed assertion or a timeout cannot leave an orphaned
/// endpoint holding its port: the server waits after stdin EOF, so the guard
/// kills and reaps a child that is still running.
struct ServerChild(Child);

impl std::ops::Deref for ServerChild {
    type Target = Child;

    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for ServerChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for ServerChild {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

/// Spawn `bench server` with piped stdio. Stderr is drained on a background
/// thread so a child that writes diagnostics cannot block on a full pipe.
fn spawn_server(
    imp: &str,
    addr: &str,
    report: &std::path::Path,
) -> (ServerChild, Receiver<String>, ChildStdin) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_bench"))
        .arg("server")
        .arg("--impl")
        .arg(imp)
        .arg("--workers")
        .arg("1")
        .arg("--addr")
        .arg(addr)
        .arg("--connections")
        .arg("1")
        .arg("--report")
        .arg(report)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bench server");

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

    (ServerChild(child), rx, stdin)
}

fn recv_line(rx: &Receiver<String>, timeout: Duration, expect: &str) {
    let line = rx
        .recv_timeout(timeout)
        .unwrap_or_else(|e| panic!("expected {expect:?} within {timeout:?}: {e}"));
    assert_eq!(line, expect);
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
            "server did not exit within {timeout:?} after SIGTERM"
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

fn encode_connect(client_id: &str) -> Vec<u8> {
    let id_bytes = client_id.as_bytes();
    let mut remaining = Vec::new();
    remaining.extend_from_slice(&4u16.to_be_bytes());
    remaining.extend_from_slice(b"MQTT");
    remaining.push(0x04); // protocol level
    remaining.push(0x02); // connect flags: CleanSession
    remaining.extend_from_slice(&0u16.to_be_bytes()); // keep-alive 0
    let id_len = u16::try_from(id_bytes.len()).expect("client id len fits u16");
    remaining.extend_from_slice(&id_len.to_be_bytes());
    remaining.extend_from_slice(id_bytes);
    let mut packet = vec![0x10];
    packet.push(u8::try_from(remaining.len()).expect("CONNECT remaining length fits one byte"));
    packet.extend_from_slice(&remaining);
    packet
}

fn encode_publish_qos1(payload: &[u8; PAYLOAD_BYTES], packet_id: u16) -> Vec<u8> {
    let mut remaining = Vec::new();
    let topic_len = u16::try_from(TOPIC.len()).expect("topic len fits u16");
    remaining.extend_from_slice(&topic_len.to_be_bytes());
    remaining.extend_from_slice(TOPIC.as_bytes());
    remaining.extend_from_slice(&packet_id.to_be_bytes());
    remaining.extend_from_slice(payload);
    let mut packet = vec![0x32]; // PUBLISH, QoS 1
    packet.push(u8::try_from(remaining.len()).expect("PUBLISH remaining length fits one byte"));
    packet.extend_from_slice(&remaining);
    packet
}

fn read_connack(stream: &mut TcpStream) {
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).expect("read CONNACK");
    assert_eq!(buf, [0x20, 0x02, 0x00, 0x00], "expected a successful CONNACK");
}

fn read_puback(stream: &mut TcpStream) -> u16 {
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).expect("read PUBACK");
    assert_eq!(buf[0], 0x40, "expected a PUBACK fixed header");
    assert_eq!(buf[1], 0x02, "expected PUBACK remaining length 2");
    u16::from_be_bytes([buf[2], buf[3]])
}

/// Drive the whole handshake for one server implementation: READY, ARM,
/// four raw-client QoS 1 PUBLISHes with one repeated `seq` (ids 1..=4,
/// stamps conn 0 seq 0, 1, 2, 1), SIGTERM, then the written report.
fn run_distinct_stamps_scenario(imp: &str) {
    let port = bench::free_port();
    let addr = format!("127.0.0.1:{port}");
    let report = report_path(imp);
    let _ = std::fs::remove_file(&report);

    let (mut child, rx, mut stdin) = spawn_server(imp, &addr, &report);
    recv_line(&rx, READY_TIMEOUT, "READY");

    let origin_ns = (monotonic_now_ns() / ONE_SEC_NS) * ONE_SEC_NS;
    let end_ns = origin_ns + WINDOW_SPAN_NS;
    writeln!(stdin, "ARM {origin_ns} {origin_ns} {end_ns}").expect("write ARM line");
    stdin.flush().expect("flush ARM line");
    recv_line(&rx, READY_TIMEOUT, "ARMED");

    let mut stream = TcpStream::connect(&addr).expect("connect raw client");
    stream.set_read_timeout(Some(IO_TIMEOUT)).expect("set read timeout");
    stream.set_write_timeout(Some(IO_TIMEOUT)).expect("set write timeout");
    stream
        .write_all(&encode_connect("bench-cli"))
        .expect("write CONNECT");
    read_connack(&mut stream);

    let seqs: [u64; 4] = [0, 1, 2, 1];
    for (i, &seq) in seqs.iter().enumerate() {
        let packet_id = u16::try_from(i + 1).expect("packet id fits u16");
        let stamp = Stamp {
            conn_id: 0,
            seq,
            scheduled_ns: monotonic_now_ns(),
        };
        let packet = encode_publish_qos1(&stamp.encode(), packet_id);
        stream.write_all(&packet).expect("write PUBLISH");
    }
    for i in 0..seqs.len() {
        let expected_id = u16::try_from(i + 1).expect("packet id fits u16");
        let acked_id = read_puback(&mut stream);
        assert_eq!(acked_id, expected_id, "PUBACK packet id out of order");
    }
    drop(stream);

    send_sigterm(&child);
    wait_exit_success(&mut child, EXIT_TIMEOUT);

    let delivery = read_report(&report);
    assert_eq!(delivery.delivered, 3, "distinct stamps conn 0 seq 0,1,2");
    assert_eq!(delivery.duplicates, 1, "seq 1 was published twice");
    assert_eq!(delivery.dropped, Some(0), "an armed run must drop nothing");
    assert_eq!(delivery.foreign, 0, "every PUBLISH was armed and on TOPIC");
    let latency_total: u64 = delivery.latency.iter().map(|&(_, count)| count).sum();
    assert_eq!(latency_total, 3, "latency counts must sum to the delivered total");

    let _ = std::fs::remove_file(&report);
}

#[test]
fn uring_server_reports_distinct_stamps_and_zero_drops_on_sigterm() {
    run_distinct_stamps_scenario("uring");
}

#[test]
fn tokio_server_reports_distinct_stamps_and_zero_drops_on_sigterm() {
    run_distinct_stamps_scenario("tokio");
}

#[test]
fn server_reports_and_exits_on_sigterm_before_arm() {
    let port = bench::free_port();
    let addr = format!("127.0.0.1:{port}");
    let report = report_path("before-arm");
    let _ = std::fs::remove_file(&report);

    // The stdin pipe stays open through SIGTERM and writes nothing, so the
    // signal arrives while the reader thread is blocked in `read_line`.
    let (mut child, rx, _stdin) = spawn_server("uring", &addr, &report);
    recv_line(&rx, READY_TIMEOUT, "READY");

    send_sigterm(&child);
    wait_exit_success(&mut child, EXIT_TIMEOUT);

    let delivery = read_report(&report);
    assert_eq!(delivery.delivered, 0, "no client ever connected");
    assert_eq!(delivery.dropped, Some(0));

    let _ = std::fs::remove_file(&report);
}

/// A connection count past `bench::record::CONNECTIONS_MAX` asks the recorder
/// to hold more latency histograms than its memory budget allows, so it must
/// be refused before the endpoint and the report.
#[test]
fn server_exits_nonzero_on_a_connection_count_past_the_maximum() {
    let port = bench::free_port();
    let addr = format!("127.0.0.1:{port}");
    let report = report_path("too-many-connections");
    let _ = std::fs::remove_file(&report);

    let output = Command::new(env!("CARGO_BIN_EXE_bench"))
        .arg("server")
        .arg("--impl")
        .arg("uring")
        .arg("--workers")
        .arg("1")
        .arg("--addr")
        .arg(&addr)
        .arg("--connections")
        .arg((CONNECTIONS_MAX + 1).to_string())
        .arg("--report")
        .arg(&report)
        .stdin(Stdio::null())
        .output()
        .expect("run bench server");

    assert_eq!(
        output.status.code(),
        Some(1),
        "a connection count past the maximum must exit 1"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&format!("connections must be between 1 and {CONNECTIONS_MAX}")),
        "expected the connection-count diagnostic, got {stderr:?}"
    );
    assert!(
        !String::from_utf8_lossy(&output.stdout).contains("READY"),
        "a refused connection count must never print READY"
    );
    assert!(
        !report.exists(),
        "a refused connection count must write no report"
    );
}

/// AC-15 — a protocol violation costs the run its validity, not its drop
/// accounting: the twin's `join` reports the violation, and the report still
/// carries the server's drop total.
#[test]
fn tokio_server_reports_the_drop_total_when_a_session_violates() {
    let port = bench::free_port();
    let addr = format!("127.0.0.1:{port}");
    let report = report_path("tokio-violation");
    let _ = std::fs::remove_file(&report);

    let (mut child, rx, _stdin) = spawn_server("tokio", &addr, &report);
    recv_line(&rx, READY_TIMEOUT, "READY");

    let mut stream = TcpStream::connect(&addr).expect("connect raw client");
    stream.set_read_timeout(Some(IO_TIMEOUT)).expect("set read timeout");
    stream.set_write_timeout(Some(IO_TIMEOUT)).expect("set write timeout");
    stream
        .write_all(&encode_connect("bench-cli-violation"))
        .expect("write CONNECT");
    read_connack(&mut stream);
    // A second CONNECT on the same session is a protocol violation
    // (`src/broker/handler.rs:4169`); the server closes the connection.
    stream
        .write_all(&encode_connect("bench-cli-violation"))
        .expect("write duplicate CONNECT");
    let mut tail = [0u8; 1];
    let _ = stream.read(&mut tail);
    drop(stream);

    send_sigterm(&child);
    let deadline = Instant::now() + EXIT_TIMEOUT;
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "server did not exit within {EXIT_TIMEOUT:?} after SIGTERM"
        );
        std::thread::sleep(WAIT_POLL);
    };
    assert_eq!(status.code(), Some(1), "a protocol violation must exit 1");

    let delivery = read_report(&report);
    assert_eq!(
        delivery.dropped,
        Some(0),
        "a failed join must still carry the drop total"
    );
    assert_eq!(delivery.delivered, 0, "the session published nothing");

    let _ = std::fs::remove_file(&report);
}

#[test]
fn server_exits_nonzero_on_unparseable_address() {
    let report = report_path("bad-addr");
    let _ = std::fs::remove_file(&report);

    let (mut child, rx, stdin) = spawn_server("uring", "not-an-address", &report);
    drop(stdin);

    let deadline = Instant::now() + EXIT_TIMEOUT;
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "server did not exit within {EXIT_TIMEOUT:?} on an unparseable address"
        );
        std::thread::sleep(WAIT_POLL);
    };

    assert_eq!(status.code(), Some(1), "an unparseable address must exit 1");
    let seen: Vec<String> = rx.try_iter().collect();
    assert!(
        !seen.iter().any(|line| line == "READY"),
        "an unparseable address must never print READY, saw {seen:?}"
    );

    let _ = std::fs::remove_file(&report);
}
