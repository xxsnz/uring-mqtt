//! Public-seam tests for the load generator (tasks 5 and 6 of the
//! benchmark-harness feature): `gen::run_generator` drives closed-loop
//! QoS 1/QoS 0 throughput and connections/s against an in-process
//! `MqttBroker`, paces open-loop latency slots at an exact total rate,
//! reports counts that reconcile with the server's `Recorder`, and always
//! returns by its hard deadline even when a test-local peer withholds
//! CONNACK or PUBACK, stops draining, or closes the socket
//! (AC-19 through AC-26). Raw-byte fixtures and the free-port registry
//! mirror `tests/broker_handle_api.rs:29-38` and `bench/tests/twin.rs`.

use std::io::{BufReader, PipeWriter, Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::num::NonZeroU16;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use nix::fcntl::{fcntl, FcntlArg};
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

/// Time the acking peer keeps its socket open after the generator's EOF —
/// past the generator's whole deadline.
const PEER_HOLD_OPEN: Duration = Duration::from_secs(10);
/// Time, from the first PUBLISH, the acking peer acknowledges every PUBLISH
/// before it closes the connection mid-stream.
const ACK_PHASE: Duration = Duration::from_millis(200);
/// Latency rate for the failed-batch test: the acking peer keeps up with it,
/// so the pacer never runs out of packet ids and is writing when the peer
/// closes.
const FAILED_BATCH_RATE: u64 = 100_000;
/// Latency rate of the diagnostic CLI runs: a few slots, enough for a first PUBLISH.
const DIAGNOSTIC_RATE: u64 = 100;
/// Time `bench gen` gets to exit: its own hard deadline plus
/// `IO_TIMEOUT_SECS`, with room for process start-up.
const GEN_CLI_EXIT_BOUND: Duration = Duration::from_secs(20);
/// Time `bench gen` gets to exit when nobody drains its stderr:
/// `GEN_CLI_EXIT_BOUND` plus the `gen::IO_TIMEOUT_SECS` grace `main` gives
/// its own blocked failure print.
const GEN_CLI_BLOCKED_STDERR_EXIT_BOUND: Duration =
    Duration::from_secs(GEN_CLI_EXIT_BOUND.as_secs() + gen::IO_TIMEOUT_SECS);
/// Chunk the stderr pipe is filled in. A write of at most `PIPE_BUF` bytes
/// into a pipe with that much room left never blocks, so the last chunk
/// fills the pipe exactly instead of waiting for a reader.
const PIPE_FILL_CHUNK: usize = 4096;
const WAIT_POLL: Duration = Duration::from_millis(50);
/// Slack past `window.end_ns + drain_grace` for a teardown that the
/// connection deadline bounds — scheduling and runtime shutdown, less
/// than the `IO_TIMEOUT_SECS` backstop such a teardown must not reach.
const TEARDOWN_SLACK: Duration = Duration::from_secs(2);

const CONNACK_OK: [u8; 4] = [0x20, 0x02, 0x00, 0x00];
const CONNACK_SESSION_PRESENT: [u8; 4] = [0x20, 0x02, 0x01, 0x00];
const CONNACK_RESERVED_FLAG: [u8; 4] = [0x20, 0x02, 0x02, 0x00];
/// Session Present, then two reserved bits — every one must be refused in
/// connections mode.
const NONZERO_ACK_FLAGS: [u8; 3] = [0x01, 0x02, 0x80];
/// A CONNACK whose Remaining Length (3) is one past the two bytes MQTT
/// 3.1.1 defines; the pinned codec ignores the extra byte.
const CONNACK_LONG_BODY: [u8; 5] = [0x20, 0x03, 0x00, 0x00, 0xFF];
/// A valid CONNACK and, in the same write, a PUBACK for packet id 1 — a
/// frame no client is owed before its first PUBLISH.
const CONNACK_THEN_PUBACK: [u8; 8] = [0x20, 0x02, 0x00, 0x00, 0x40, 0x02, 0x00, 0x01];
/// A valid CONNACK and, in the same write, the first byte of another
/// frame — a partial frame must be refused like a whole one.
const CONNACK_THEN_ONE_BYTE: [u8; 5] = [0x20, 0x02, 0x00, 0x00, 0x40];
/// A PINGREQ response — a frame no QoS 0 client is owed, unsolicited mid-session.
const PINGRESP: [u8; 2] = [0xD0, 0x00];

/// A PINGRESP sent twice — a reader that prints every refusal shows two lines.
const PINGRESP_TWICE: &[u8] = b"\xD0\x00\xD0\x00";
/// A QoS 0 PUBLISH, topic `t`, payload `SECRET`, sent twice.
const PUBLISH_TWICE: &[u8] =
    b"\x30\x09\x00\x01tSECRET\x30\x09\x00\x01tSECRET";
/// Payload of `PUBLISH_TWICE`; no diagnostic may contain it.
const PUBLISH_PAYLOAD_MARKER: &str = "SECRET";
/// A SUBACK sent twice.
const SUBACK_TWICE: &[u8] = b"\x90\x03\x00\x01\x00\x90\x03\x00\x01\x00";
/// An UNSUBACK sent twice.
const UNSUBACK_TWICE: &[u8] = b"\xB0\x02\x00\x01\xB0\x02\x00\x01";
/// A PUBACK for packet id 65535 — `PacketIds::allocate` (`bench/src/gen.rs:225-249`)
/// never allocates that id inside this short run, so it is never in flight.
const PUBACK_NOT_IN_FLIGHT: &[u8] = b"\x40\x02\xFF\xFF";

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

/// A test-local peer that answers a complete CONNECT with `reply`, reads
/// once more and closes — a process-boundary stand-in for a broker sending a
/// malformed or session-claiming CONNACK. The CONNECT goes through the same
/// `Codec` as in `acking_peer`, so a fragmented one cannot make the peer
/// reply and close while part of the request is still unread.
fn connack_peer(reply: &[u8]) -> SocketAddr {
    let reply = reply.to_vec();
    let port = bench::free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse addr");
    let listener = TcpListener::bind(addr).expect("bind connack peer");
    let addr = listener.local_addr().expect("local_addr");
    std::thread::Builder::new()
        .name("gen-connack-peer".into())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let reply = reply.clone();
                std::thread::spawn(move || {
                    let mut buf = [0u8; 256];
                    let mut codec = Codec::default();
                    let mut pending = BytesMut::new();
                    loop {
                        let Ok(n) = stream.read(&mut buf) else { return };
                        if n == 0 {
                            return;
                        }
                        pending.extend_from_slice(&buf[..n]);
                        match codec.decode(&mut pending) {
                            Ok(Some((Packet::Connect(_), _))) => break,
                            Ok(_) => continue,
                            Err(_) => return,
                        }
                    }
                    if stream.write_all(&reply).is_err() {
                        return;
                    }
                    let _ = stream.read(&mut buf);
                    drop(stream);
                });
            }
        })
        .expect("spawn connack peer");
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

/// A test-local peer that acknowledges every QoS 1 PUBLISH. With
/// `close_after` it closes that long after the first PUBLISH, while the
/// generator is still writing; without it, it acknowledges until the
/// generator's EOF and then holds its socket open. In the closing form every
/// PUBLISH is acknowledged, so the generator never waits on packet ids, its
/// next write after the close draws a RST and the write after that fails —
/// a failed batch whose earlier publishes were acknowledged. A
/// process-boundary stand-in, not a mock.
fn acking_peer(close_after: Option<Duration>) -> SocketAddr {
    let port = bench::free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse addr");
    let listener = TcpListener::bind(addr).expect("bind acking peer");
    let addr = listener.local_addr().expect("local_addr");
    std::thread::Builder::new()
        .name("gen-acking-peer".into())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                std::thread::spawn(move || {
                    let mut buf = vec![0u8; 65_536].into_boxed_slice();
                    let mut started: Option<Instant> = None;
                    let mut codec = Codec::default();
                    let mut pending = BytesMut::new();
                    while close_after
                        .is_none_or(|phase| started.is_none_or(|t| t.elapsed() < phase))
                    {
                        let Ok(n) = stream.read(&mut buf) else { break };
                        if n == 0 {
                            break;
                        }
                        pending.extend_from_slice(&buf[..n]);
                        let mut acks = Vec::new();
                        while let Ok(Some((packet, _))) = codec.decode(&mut pending) {
                            match packet {
                                Packet::Connect(_) => {
                                    if stream.write_all(&CONNACK_OK).is_err() {
                                        return;
                                    }
                                }
                                Packet::Publish(p) => {
                                    started.get_or_insert_with(Instant::now);
                                    if let Some(id) = p.packet_id {
                                        let id = id.get();
                                        acks.extend_from_slice(&[
                                            0x40,
                                            0x02,
                                            (id >> 8) as u8,
                                            (id & 0xFF) as u8,
                                        ]);
                                    }
                                }
                                _ => {}
                            }
                        }
                        if stream.write_all(&acks).is_err() {
                            break;
                        }
                    }
                    if close_after.is_some() {
                        drop(stream);
                    } else {
                        std::thread::sleep(PEER_HOLD_OPEN);
                        drop(stream);
                    }
                });
            }
        })
        .expect("spawn acking peer");
    addr
}

/// Owns a spawned `bench gen` process. `Drop` kills and reaps a child that
/// is still running, so a failed assertion or a timeout cannot leave a
/// generator holding its port.
struct GenChild(Child);

impl std::ops::Deref for GenChild {
    type Target = Child;

    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for GenChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for GenChild {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

/// Spawn `bench gen` with `args` plus `--report`, wait for it to exit within
/// `GEN_CLI_EXIT_BOUND`, and return its parsed report with its captured
/// stderr. Stderr is drained on a background thread so a child that writes
/// diagnostics cannot block on a full pipe (`spawn_sub`, `cli_sub.rs:66-113`).
fn run_gen_cli(tag: &str, args: &[String]) -> Result<(GenReport, String), String> {
    let report_path =
        std::env::temp_dir().join(format!("bench-gen-{tag}-{}.json", std::process::id()));
    let _ = std::fs::remove_file(&report_path);

    let mut child = GenChild(
        Command::new(env!("CARGO_BIN_EXE_bench"))
            .arg("gen")
            .args(args)
            .arg("--report")
            .arg(&report_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn bench gen: {e}"))?,
    );

    let stderr = child.stderr.take().expect("child stderr piped");
    let stderr_handle = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = BufReader::new(stderr).read_to_end(&mut buf);
        String::from_utf8_lossy(&buf).into_owned()
    });

    let deadline = Instant::now() + GEN_CLI_EXIT_BOUND;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|e| format!("try_wait: {e}"))? {
            break status;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "bench gen did not exit within {GEN_CLI_EXIT_BOUND:?}"
            ));
        }
        std::thread::sleep(WAIT_POLL);
    };
    // Collected before the exit check: a refusal (a late start, a report
    // write failure) explains itself on stderr, and an error that carries
    // only the status discards that explanation.
    let stderr = stderr_handle
        .join()
        .map_err(|_| "stderr thread panicked".to_string())?;
    if !status.success() {
        return Err(format!("bench gen exited {status:?}; stderr:\n{stderr}"));
    }
    let data =
        std::fs::read_to_string(&report_path).map_err(|e| format!("read report: {e}"))?;
    let _ = std::fs::remove_file(&report_path);
    let report: GenReport =
        serde_json::from_str(&data).map_err(|e| format!("parse report: {e}"))?;
    Ok((report, stderr))
}

/// Fill `writer`'s pipe to its capacity, so the next write into that pipe
/// blocks until something drains it. The capacity is read from the kernel
/// (`F_GETPIPE_SZ`): a fixed guess would either leave room for the child's
/// diagnostic or block this thread instead of the child.
fn fill_to_capacity(writer: &mut PipeWriter) -> Result<(), String> {
    let capacity = fcntl(&*writer, FcntlArg::F_GETPIPE_SZ)
        .map_err(|e| format!("read stderr pipe capacity: {e}"))?;
    let capacity =
        usize::try_from(capacity).map_err(|e| format!("stderr pipe capacity: {e}"))?;
    let chunk = [b'x'; PIPE_FILL_CHUNK];
    let mut written = 0;
    while written < capacity {
        let n = (capacity - written).min(PIPE_FILL_CHUNK);
        writer
            .write_all(&chunk[..n])
            .map_err(|e| format!("fill stderr pipe: {e}"))?;
        written += n;
    }
    Ok(())
}

/// Spawn `bench gen` with `args` plus `--report` against a stderr pipe
/// that is already full and that nobody drains, wait for it to exit within
/// `GEN_CLI_BLOCKED_STDERR_EXIT_BOUND`, and return its exit status with
/// whether it left a report behind. `run_gen_cli` drains stderr on a
/// thread, so only this spawn exercises `main`'s bounded failure print and
/// `spawn_progress_watchdog` against a stderr write that blocks.
fn run_gen_cli_with_full_stderr(
    tag: &str,
    args: &[String],
) -> Result<(ExitStatus, bool), String> {
    let report_path =
        std::env::temp_dir().join(format!("bench-gen-{tag}-{}.json", std::process::id()));
    let _ = std::fs::remove_file(&report_path);

    let (stderr_read, mut stderr_write) =
        std::io::pipe().map_err(|e| format!("stderr pipe: {e}"))?;
    fill_to_capacity(&mut stderr_write)?;

    let mut child = GenChild(
        Command::new(env!("CARGO_BIN_EXE_bench"))
            .arg("gen")
            .args(args)
            .arg("--report")
            .arg(&report_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr_write))
            .spawn()
            .map_err(|e| format!("spawn bench gen: {e}"))?,
    );

    let deadline = Instant::now() + GEN_CLI_BLOCKED_STDERR_EXIT_BOUND;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|e| format!("try_wait: {e}"))? {
            break status;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "bench gen did not exit within {GEN_CLI_BLOCKED_STDERR_EXIT_BOUND:?} \
                 while nobody drained its stderr"
            ));
        }
        std::thread::sleep(WAIT_POLL);
    };
    // Held open until the child has exited: closing the read end earlier
    // would fail the child's write with EPIPE instead of blocking it.
    drop(stderr_read);
    let wrote_report = report_path.exists();
    let _ = std::fs::remove_file(&report_path);
    Ok((status, wrote_report))
}

/// A test-local peer that completes the CONNACK handshake, then on the
/// first `Packet::Publish` it decodes writes `frame` once, unsolicited, and
/// never writes a PUBACK. A process-boundary stand-in for a broker sending
/// an unsolicited frame mid-session.
fn frame_after_first_publish_peer(frame: &'static [u8]) -> SocketAddr {
    let port = bench::free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse addr");
    let listener = TcpListener::bind(addr).expect("bind frame-after-publish peer");
    let addr = listener.local_addr().expect("local_addr");
    std::thread::Builder::new()
        .name("gen-frame-after-publish".into())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                std::thread::spawn(move || {
                    let mut buf = vec![0u8; 65_536].into_boxed_slice();
                    let mut codec = Codec::default();
                    let mut pending = BytesMut::new();
                    let mut sent = false;
                    while let Ok(n) = stream.read(&mut buf) {
                        if n == 0 {
                            break;
                        }
                        pending.extend_from_slice(&buf[..n]);
                        while let Ok(Some((packet, _))) = codec.decode(&mut pending) {
                            match packet {
                                Packet::Connect(_) => {
                                    if stream.write_all(&CONNACK_OK).is_err() {
                                        return;
                                    }
                                }
                                Packet::Publish(_) if !sent => {
                                    sent = true;
                                    if stream.write_all(frame).is_err() {
                                        return;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    drop(stream);
                });
            }
        })
        .expect("spawn frame-after-publish peer");
    addr
}

/// The flags of `qos1_throughput_reconciles_a_failed_batch_with_the_acks_it_received`,
/// generalized over `mode` and `qos` for the diagnostic CLI runs below.
fn cli_run_args(addr: SocketAddr, mode: &str, qos: u8) -> Vec<String> {
    let origin_ns = origin_with_lead(LEAD_SECS);
    let window_end_ns = origin_ns + NS_PER_SEC;
    vec![
        "--addr".into(),
        addr.to_string(),
        "--mode".into(),
        mode.into(),
        "--rate".into(),
        DIAGNOSTIC_RATE.to_string(),
        "--qos".into(),
        qos.to_string(),
        "--connections".into(),
        "1".into(),
        "--qos1-window".into(),
        QOS1_WINDOW.to_string(),
        "--origin-ns".into(),
        origin_ns.to_string(),
        "--window-start-ns".into(),
        origin_ns.to_string(),
        "--window-end-ns".into(),
        window_end_ns.to_string(),
        "--drain-grace-secs".into(),
        "1".into(),
    ]
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
        report.offered + report.missed_slots,
        grid_slots(RATE, NS_PER_SEC),
        "every scheduled slot must be offered or missed exactly once"
    );
    assert!(
        report.stalled <= report.offered,
        "a stalled publish is an offered one"
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
        report.offered + report.missed_slots,
        grid_slots(RATE, NS_PER_SEC),
        "every scheduled slot must be offered or missed exactly once"
    );
    let batch = u64::try_from(gen::BATCH).expect("BATCH fits in u64");
    assert!(
        report.stalled <= batch,
        "only the batch of the first failed write may count as stalled: {}",
        report.stalled
    );
    assert!(report.stalled >= 1, "the failed batch must count as stalled");
    assert!(
        report.stalled <= report.offered,
        "a stalled publish is an offered one"
    );
}

/// AC-12, AC-13 — a QoS 1 throughput connection against a peer that
/// acknowledges every PUBLISH and then closes mid-stream prints the publish
/// write diagnostic exactly once and reconciles the failed batch's ids:
/// every id is either acked or counted stalled, and the two sum to offered.
#[test]
fn qos1_throughput_reconciles_a_failed_batch_with_the_acks_it_received() {
    let addr = acking_peer(Some(ACK_PHASE));
    let before_ns = monotonic_now_ns();
    let wall_start = Instant::now();
    let origin_ns = origin_with_lead(LEAD_SECS);
    let window_end_ns = origin_ns + NS_PER_SEC;
    let args: Vec<String> = vec![
        "--addr".into(),
        addr.to_string(),
        "--mode".into(),
        "throughput".into(),
        "--qos".into(),
        "1".into(),
        "--connections".into(),
        "1".into(),
        "--qos1-window".into(),
        "65535".into(),
        "--origin-ns".into(),
        origin_ns.to_string(),
        "--window-start-ns".into(),
        origin_ns.to_string(),
        "--window-end-ns".into(),
        window_end_ns.to_string(),
        "--drain-grace-secs".into(),
        "1".into(),
    ];
    let (report, stderr) =
        run_gen_cli("qos1-throughput-failed-batch", &args).expect("bench gen must exit 0");
    let elapsed = wall_start.elapsed();
    let bound = Duration::from_nanos(origin_ns.saturating_sub(before_ns))
        + Duration::from_secs(1 + 1 + gen::IO_TIMEOUT_SECS + 2);
    assert!(
        elapsed <= bound,
        "bench gen exceeded its hard deadline: {elapsed:?} > {bound:?}"
    );

    let failure_lines: Vec<&str> = stderr
        .lines()
        .filter(|line| line.starts_with("gen: connection ") && line.contains(": publish write "))
        .collect();
    assert_eq!(
        failure_lines.len(),
        1,
        "exactly one connection must print the publish write diagnostic, got {failure_lines:?}"
    );

    let acked = report.acked.expect("QoS 1 reports acked");
    assert!(
        acked >= 1,
        "at least one publish must have been acked before the write failed"
    );
    assert!(
        report.stalled >= 1,
        "the failed batch must leave ids unresolved"
    );
    assert_eq!(
        acked + report.stalled,
        report.offered,
        "acked and stalled must reconcile with offered"
    );
    assert_eq!(report.protocol_errors, 0);
}

/// AC-12, AC-13, AC-15 — the same reconciliation as
/// `qos1_throughput_reconciles_a_failed_batch_with_the_acks_it_received`, in
/// latency mode: every scheduled slot is still offered or missed exactly
/// once.
#[test]
fn qos1_latency_reconciles_a_failed_batch_with_the_acks_it_received() {
    let addr = acking_peer(Some(ACK_PHASE));
    let before_ns = monotonic_now_ns();
    let wall_start = Instant::now();
    let origin_ns = origin_with_lead(LEAD_SECS);
    let window_end_ns = origin_ns + NS_PER_SEC;
    let args: Vec<String> = vec![
        "--addr".into(),
        addr.to_string(),
        "--mode".into(),
        "latency".into(),
        "--rate".into(),
        FAILED_BATCH_RATE.to_string(),
        "--qos".into(),
        "1".into(),
        "--connections".into(),
        "1".into(),
        "--qos1-window".into(),
        "65535".into(),
        "--origin-ns".into(),
        origin_ns.to_string(),
        "--window-start-ns".into(),
        origin_ns.to_string(),
        "--window-end-ns".into(),
        window_end_ns.to_string(),
        "--drain-grace-secs".into(),
        "1".into(),
    ];
    let (report, stderr) =
        run_gen_cli("qos1-latency-failed-batch", &args).expect("bench gen must exit 0");
    let elapsed = wall_start.elapsed();
    let bound = Duration::from_nanos(origin_ns.saturating_sub(before_ns))
        + Duration::from_secs(1 + 1 + gen::IO_TIMEOUT_SECS + 2);
    assert!(
        elapsed <= bound,
        "bench gen exceeded its hard deadline: {elapsed:?} > {bound:?}"
    );

    let failure_lines: Vec<&str> = stderr
        .lines()
        .filter(|line| line.starts_with("gen: connection ") && line.contains(": publish write "))
        .collect();
    assert_eq!(
        failure_lines.len(),
        1,
        "exactly one connection must print the publish write diagnostic, got {failure_lines:?}"
    );

    let acked = report.acked.expect("QoS 1 reports acked");
    assert!(
        acked >= 1,
        "at least one publish must have been acked before the write failed"
    );
    assert!(
        report.stalled >= 1,
        "the failed batch must leave ids unresolved"
    );
    assert_eq!(
        acked + report.stalled,
        report.offered,
        "acked and stalled must reconcile with offered"
    );
    assert_eq!(report.protocol_errors, 0);
    assert_eq!(
        report.offered + report.missed_slots,
        report.scheduled.expect("latency reports scheduled"),
        "every scheduled slot must be offered or missed"
    );
}

/// AC-12, AC-14 — a QoS 0 throughput connection against a peer that closes
/// right after CONNACK counts its one failed batch as stalled in full, and
/// offered accounts for at least that much.
#[test]
fn qos0_throughput_counts_its_failed_batch_as_offered_and_stalled() {
    let (report, elapsed, bound) =
        run_bounded("gen-qos0-throughput-failed-batch", DEADLINE_WATCHDOG, move || {
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
                mode: Mode::Throughput,
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
        "generator exceeded its hard deadline: {elapsed:?} > {bound:?}"
    );
    assert_eq!(
        report.stalled, 64,
        "the failed batch's whole BATCH must count as stalled"
    );
    assert!(
        report.offered >= report.stalled,
        "a stalled publish is an offered one"
    );
}

/// AC-13 — a QoS 1 throughput connection against a peer that acknowledges
/// every PUBLISH and withholds EOF still returns by its hard deadline, with
/// every offered publish acked and nothing stalled.
#[test]
fn qos1_throughput_returns_by_its_deadline_when_the_peer_withholds_eof() {
    let (report, elapsed, bound) =
        run_bounded(
            "gen-qos1-throughput-withholds-eof",
            DEADLINE_WATCHDOG,
            move || {
                let addr = acking_peer(None);
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
            },
        );

    assert!(
        elapsed <= bound,
        "generator exceeded its hard deadline against a peer withholding EOF: {elapsed:?} > {bound:?}"
    );
    assert_eq!(
        report.acked,
        Some(report.offered),
        "every offered QoS 1 publish must be acked"
    );
    assert_eq!(report.stalled, 0, "a clean run must leave nothing stalled");
    assert_eq!(report.protocol_errors, 0);
}

/// AC-1, AC-2 — a clean QoS 0 throughput connection against a peer that
/// reads every PUBLISH and withholds EOF ends at its own deadline, with
/// nothing stalled.
#[test]
fn qos0_throughput_returns_by_its_deadline_when_the_peer_withholds_eof() {
    let (report, elapsed, bound) =
        run_bounded(
            "gen-qos0-throughput-withholds-eof",
            DEADLINE_WATCHDOG,
            move || {
                let addr = acking_peer(None);
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
                    + Duration::from_nanos(NS_PER_SEC)
                    + config.drain_grace
                    + TEARDOWN_SLACK;
                Ok((report, elapsed, bound))
            },
        );

    assert!(
        elapsed <= bound,
        "a QoS 0 teardown must end by its connection deadline, not the backstop: {elapsed:?} > {bound:?}"
    );
    assert_eq!(report.stalled, 0, "a peer withholding EOF must cost no stalled connection");
    assert_eq!(report.protocol_errors, 0);
    assert!(report.offered > 0, "the connection must have published inside the window");
}

/// AC-4 — a QoS 0 latency connection runs the same reader as QoS 0
/// throughput, so a frame the broker sends after CONNACK counts as a
/// protocol error instead of going unread.
#[test]
fn latency_qos0_counts_a_frame_the_broker_sends_after_connack() {
    let (report, _stderr) = run_gen_cli(
        "latency-qos0-unsolicited",
        &cli_run_args(frame_after_first_publish_peer(&PINGRESP), "latency", 0),
    )
    .expect("bench gen must exit 0");
    assert_eq!(report.protocol_errors, 1);
    assert_eq!(report.connect_errors, 0);
    assert!(report.offered >= 1);
}

/// AC-6, AC-7 — every session reader prints exactly one stderr line naming
/// the first frame it refuses, whatever kind that frame is, and only one
/// line even when the same unexpected frame arrives twice.
#[test]
fn readers_name_the_first_packet_they_reject() {
    for (mode, qos, frame, kind, tag) in [
        ("throughput", 0u8, PINGRESP_TWICE, "PINGRESP", "reject-t0"),
        ("throughput", 1u8, PUBLISH_TWICE, "PUBLISH", "reject-t1"),
        ("latency", 0u8, SUBACK_TWICE, "SUBACK", "reject-l0"),
        ("latency", 1u8, UNSUBACK_TWICE, "UNSUBACK", "reject-l1"),
    ] {
        let (report, stderr) = run_gen_cli(
            tag,
            &cli_run_args(frame_after_first_publish_peer(frame), mode, qos),
        )
        .expect("bench gen must exit 0");
        let rejected: Vec<&str> = stderr
            .lines()
            .filter(|l| {
                l.starts_with("gen: connection 0: QoS ") && l.contains(" reader rejected ")
            })
            .collect();
        assert_eq!(
            rejected,
            vec![
                format!("gen: connection 0: QoS {qos} reader rejected an unexpected {kind}")
                    .as_str()
            ],
            "{tag}: exactly one rejection line naming {kind}"
        );
        assert_eq!(
            report.protocol_errors, 2,
            "{tag}: both refused frames must still be counted"
        );
        assert!(
            !stderr.contains(PUBLISH_PAYLOAD_MARKER),
            "{tag}: a diagnostic must never contain a PUBLISH payload"
        );
    }

    for (mode, tag) in [
        ("throughput", "reject-puback-t1"),
        ("latency", "reject-puback-l1"),
    ] {
        let (report, stderr) = run_gen_cli(
            tag,
            &cli_run_args(
                frame_after_first_publish_peer(PUBACK_NOT_IN_FLIGHT),
                mode,
                1,
            ),
        )
        .expect("bench gen must exit 0");
        let rejected: Vec<&str> = stderr
            .lines()
            .filter(|l| {
                l.starts_with("gen: connection 0: QoS ") && l.contains(" reader rejected ")
            })
            .collect();
        assert_eq!(
            rejected,
            vec![
                "gen: connection 0: QoS 1 reader rejected a PUBACK for packet id 65535, which is not in flight"
            ],
            "{tag}: exactly one not-in-flight PUBACK rejection line"
        );
        assert_eq!(
            report.protocol_errors, 1,
            "{tag}: the not-in-flight PUBACK is the only refused frame"
        );
    }
}

/// AC-3, AC-6 — `bench gen` must still answer when nobody drains its
/// stderr. The peer's unsolicited PINGRESP makes the QoS 0 reader print its
/// refusal, that `eprintln!` blocks on the full pipe, and the reader never
/// stops: `join_and_report`'s reader wait fails the run — or
/// `spawn_progress_watchdog` answers for it when the blocked reader holds
/// every worker — `main`'s failure print blocks the same way, and the
/// process still exits 1 with no report. `run_gen_cli` drains stderr
/// continuously, so no other test can observe this.
#[test]
fn gen_exits_without_a_report_when_nobody_drains_its_stderr() {
    let (status, wrote_report) = run_gen_cli_with_full_stderr(
        "blocked-stderr",
        &cli_run_args(frame_after_first_publish_peer(&PINGRESP), "throughput", 0),
    )
    .expect("bench gen must exit with its stderr blocked");
    assert_eq!(
        status.code(),
        Some(1),
        "a run that can build no report must exit 1"
    );
    assert!(
        !wrote_report,
        "a run that failed must leave no report behind"
    );
}

/// AC-5 — a clean QoS 0 latency connection against a peer that reads every
/// PUBLISH and withholds EOF still ends by its connection deadline, with
/// its new reader joined inside `latency_teardown`'s bound.
#[test]
fn latency_qos0_returns_by_its_deadline_when_the_peer_withholds_eof() {
    let (report, elapsed, bound) =
        run_bounded(
            "gen-latency-qos0-withholds-eof",
            DEADLINE_WATCHDOG,
            move || {
                let addr = acking_peer(None);
                let before_ns = monotonic_now_ns();
                let wall_start = Instant::now();
                let origin_ns = origin_with_lead(LEAD_SECS);
                let window = Window {
                    start_ns: origin_ns,
                    end_ns: origin_ns + NS_PER_SEC,
                };
                let config = gen::GenConfig {
                    addr,
                    mode: Mode::Latency { rate: DIAGNOSTIC_RATE },
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
                    + Duration::from_nanos(NS_PER_SEC)
                    + config.drain_grace
                    + TEARDOWN_SLACK;
                Ok((report, elapsed, bound))
            },
        );

    assert!(
        elapsed <= bound,
        "a QoS 0 latency teardown must end by its connection deadline, not the backstop: {elapsed:?} > {bound:?}"
    );
    assert_eq!(report.stalled, 0, "a peer withholding EOF must cost no stalled connection");
    assert_eq!(report.protocol_errors, 0);
    assert!(report.offered > 0, "the connection must have published inside the window");
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

/// AC-8 — a CONNACK claiming a stored session on this cycle's
/// clean-session CONNECT is a protocol error in throughput mode, not an
/// accepted session.
#[test]
fn throughput_refuses_a_connack_that_claims_a_session() {
    let report: GenReport = run_bounded("gen-session-present", DEADLINE_WATCHDOG, move || {
        let addr = connack_peer(&CONNACK_SESSION_PRESENT);
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
        gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))
    });

    assert_eq!(report.protocol_errors, 2);
    assert_eq!(report.connect_errors, 2);
    assert_eq!(report.offered, 0);
}

/// AC-11 — a CONNACK the codec rejects with a non-`Io` decode error is the
/// broker's malformed reply, counted as a protocol error like every other
/// malformed frame.
#[test]
fn throughput_counts_a_connack_with_reserved_flags_as_a_protocol_error() {
    let report: GenReport = run_bounded("gen-reserved-flag", DEADLINE_WATCHDOG, move || {
        let addr = connack_peer(&CONNACK_RESERVED_FLAG);
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
        gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))
    });

    assert_eq!(report.protocol_errors, 2);
    assert_eq!(report.connect_errors, 2);
    assert_eq!(report.offered, 0);
}

/// AC-7 — a CONNACK whose Remaining Length is not 2 is refused as a
/// protocol error in throughput mode, even though the pinned codec accepts
/// the longer body and ignores its tail.
#[test]
fn throughput_refuses_a_connack_whose_body_is_not_two_bytes() {
    let report: GenReport = run_bounded("gen-connack-long-body", DEADLINE_WATCHDOG, move || {
        let addr = connack_peer(&CONNACK_LONG_BODY);
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
        gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))
    });

    assert_eq!(report.protocol_errors, 2);
    assert_eq!(report.connect_errors, 2);
    assert_eq!(report.offered, 0);
}

/// AC-6 — any byte the codec read past the CONNACK, a whole frame or part
/// of one, is the broker's violation and refuses the session as a protocol
/// error in throughput mode.
#[test]
fn throughput_refuses_a_session_whose_connack_arrives_with_more_bytes() {
    for reply in [&CONNACK_THEN_PUBACK[..], &CONNACK_THEN_ONE_BYTE[..]] {
        let report: GenReport =
            run_bounded("gen-connack-read-ahead", DEADLINE_WATCHDOG, move || {
                let addr = connack_peer(reply);
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
                gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))
            });

        assert_eq!(
            report.protocol_errors, 2,
            "reply {reply:02x?} must count 2 protocol errors"
        );
        assert_eq!(
            report.connect_errors, 2,
            "reply {reply:02x?} must count 2 connect errors"
        );
        assert_eq!(report.offered, 0, "reply {reply:02x?} must offer nothing");
    }
}

/// AC-7 — a CONNACK whose Remaining Length is not 2 is refused on the QoS 0
/// latency path too. The refusal happens in `open_session`, before any reader
/// starts.
#[test]
fn latency_qos0_refuses_a_connack_whose_body_is_not_two_bytes() {
    const RATE: u64 = 1;
    let report: GenReport =
        run_bounded("gen-latency-connack-long-body", DEADLINE_WATCHDOG, move || {
            let addr = connack_peer(&CONNACK_LONG_BODY);
            let origin_ns = origin_with_lead(LEAD_SECS);
            let window = Window {
                start_ns: origin_ns,
                end_ns: origin_ns + NS_PER_SEC,
            };
            let config = gen::GenConfig {
                addr,
                mode: Mode::Latency { rate: RATE },
                qos: 0,
                connections: 2,
                qos1_window: QOS1_WINDOW,
                origin_ns,
                window,
                drain_grace: Duration::from_secs(1),
            };
            gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))
        });

    assert_eq!(report.protocol_errors, 2);
    assert_eq!(report.connect_errors, 2);
    assert_eq!(report.offered, 0);
}

/// AC-6 — any byte the codec read past the CONNACK refuses the session on
/// the QoS 0 latency path too.
#[test]
fn latency_qos0_refuses_a_session_whose_connack_arrives_with_more_bytes() {
    const RATE: u64 = 1;
    for reply in [&CONNACK_THEN_PUBACK[..], &CONNACK_THEN_ONE_BYTE[..]] {
        let report: GenReport =
            run_bounded("gen-latency-connack-read-ahead", DEADLINE_WATCHDOG, move || {
                let addr = connack_peer(reply);
                let origin_ns = origin_with_lead(LEAD_SECS);
                let window = Window {
                    start_ns: origin_ns,
                    end_ns: origin_ns + NS_PER_SEC,
                };
                let config = gen::GenConfig {
                    addr,
                    mode: Mode::Latency { rate: RATE },
                    qos: 0,
                    connections: 2,
                    qos1_window: QOS1_WINDOW,
                    origin_ns,
                    window,
                    drain_grace: Duration::from_secs(1),
                };
                gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))
            });

        assert_eq!(
            report.protocol_errors, 2,
            "reply {reply:02x?} must count 2 protocol errors"
        );
        assert_eq!(
            report.connect_errors, 2,
            "reply {reply:02x?} must count 2 connect errors"
        );
        assert_eq!(report.offered, 0, "reply {reply:02x?} must offer nothing");
    }
}

/// AC-7 — connections mode needs no new length check: the exact
/// `20 02 00 00` comparison it already makes counts a longer CONNACK body as
/// a protocol error and completes no cycle.
#[test]
fn connections_mode_refuses_a_connack_whose_body_is_not_two_bytes() {
    let report: GenReport =
        run_bounded("gen-connections-long-body", DEADLINE_WATCHDOG, move || {
            let addr = connack_peer(&CONNACK_LONG_BODY);
            let origin_ns = origin_with_lead(LEAD_SECS);
            let window = Window {
                start_ns: origin_ns,
                end_ns: origin_ns + NS_PER_SEC,
            };
            let config = gen::GenConfig {
                addr,
                mode: Mode::Connections,
                qos: 0,
                connections: 1,
                qos1_window: QOS1_WINDOW,
                origin_ns,
                window,
                drain_grace: Duration::from_secs(1),
            };
            gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))
        });

    assert!(
        report.protocol_errors >= 1,
        "a longer CONNACK body must count a protocol error"
    );
    assert_eq!(
        report.cycles_in_window,
        Some(0),
        "a longer CONNACK body must complete no cycle"
    );
}

/// AC-9 — connections mode must refuse a CONNACK whose acknowledge-flags
/// byte sets Session Present or either reserved bit, counting a protocol
/// error and completing no cycle.
#[test]
fn connections_mode_refuses_a_connack_with_session_present_or_reserved_flags() {
    for &flags in &NONZERO_ACK_FLAGS {
        let report: GenReport =
            run_bounded("gen-connections-refuses", DEADLINE_WATCHDOG, move || {
                let addr = connack_peer(&[0x20, 0x02, flags, 0x00]);
                let origin_ns = origin_with_lead(LEAD_SECS);
                let window = Window {
                    start_ns: origin_ns,
                    end_ns: origin_ns + NS_PER_SEC,
                };
                let config = gen::GenConfig {
                    addr,
                    mode: Mode::Connections,
                    qos: 0,
                    connections: 1,
                    qos1_window: QOS1_WINDOW,
                    origin_ns,
                    window,
                    drain_grace: Duration::from_secs(1),
                };
                gen::run_generator(&config).map_err(|e| format!("run_generator: {e}"))
            });

        assert!(
            report.protocol_errors >= 1,
            "acknowledge-flags byte {flags:#04x} must count a protocol error"
        );
        assert_eq!(
            report.cycles_in_window,
            Some(0),
            "acknowledge-flags byte {flags:#04x} must complete no cycle"
        );
    }
}

/// A test-local peer that decodes a whole CONNECT, the same loop as
/// `connack_peer`, then drops the stream without writing — the client
/// reads EOF, not a reset.
fn closes_after_connect_peer() -> SocketAddr {
    let port = bench::free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse addr");
    let listener = TcpListener::bind(addr).expect("bind closes-after-connect peer");
    let addr = listener.local_addr().expect("local_addr");
    std::thread::Builder::new()
        .name("gen-closes-after-connect".into())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                std::thread::spawn(move || {
                    let mut buf = [0u8; 256];
                    let mut codec = Codec::default();
                    let mut pending = BytesMut::new();
                    loop {
                        let Ok(n) = stream.read(&mut buf) else { return };
                        if n == 0 {
                            return;
                        }
                        pending.extend_from_slice(&buf[..n]);
                        match codec.decode(&mut pending) {
                            Ok(Some((Packet::Connect(_), _))) => break,
                            Ok(_) => continue,
                            Err(_) => return,
                        }
                    }
                    drop(stream);
                });
            }
        })
        .expect("spawn closes-after-connect peer");
    addr
}

/// AC-9 for one publishing `mode`: a failed generator handshake prints
/// `gen: connection {id}: {operation}: {cause}` — a refused connect names the
/// address and the OS cause, a peer that closes before CONNACK names the
/// close, and a peer that withholds CONNACK names the read timeout. Each mode
/// prints from its own `OpenError::Connect` arm, so each one is checked.
fn assert_open_failures_name_their_operation_and_cause(mode: &str) {
    let port = bench::free_port();
    let refused_addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse addr");
    let (report, stderr) = run_gen_cli(
        &format!("open-failure-refused-{mode}"),
        &cli_run_args(refused_addr, mode, 1),
    )
    .expect("bench gen must exit 0");
    let refused_prefix = format!("gen: connection 0: connect {refused_addr}: ");
    let refused_lines: Vec<&str> = stderr
        .lines()
        .filter(|line| line.starts_with(&refused_prefix) && line.contains("(os error 111)"))
        .collect();
    assert_eq!(
        refused_lines.len(),
        1,
        "expected one refused-connect line starting {refused_prefix:?} and containing (os error 111); stderr:\n{stderr}"
    );
    assert_eq!(report.connect_errors, 1);
    assert_eq!(report.protocol_errors, 0);

    let (report, stderr) = run_gen_cli(
        &format!("open-failure-closed-{mode}"),
        &cli_run_args(closes_after_connect_peer(), mode, 1),
    )
    .expect("bench gen must exit 0");
    let closed_lines: Vec<&str> = stderr
        .lines()
        .filter(|line| *line == "gen: connection 0: peer closed before CONNACK")
        .collect();
    assert_eq!(
        closed_lines.len(),
        1,
        "expected one exact peer-closed-before-CONNACK line; stderr:\n{stderr}"
    );
    assert_eq!(report.connect_errors, 1);

    let (report, stderr) = run_gen_cli(
        &format!("open-failure-withheld-{mode}"),
        &cli_run_args(silent_connack_peer(), mode, 1),
    )
    .expect("bench gen must exit 0");
    let withheld_line = format!("gen: connection 0: read CONNACK: timed out after {} s", gen::IO_TIMEOUT_SECS);
    let withheld_lines: Vec<&str> = stderr
        .lines()
        .filter(|line| **line == withheld_line)
        .collect();
    assert_eq!(
        withheld_lines.len(),
        1,
        "expected one exact CONNACK-read-timeout line {withheld_line:?}; stderr:\n{stderr}"
    );
    assert_eq!(report.connect_errors, 1);
}

/// AC-9 — the throughput path's handshake failures name their operation and
/// their cause.
#[test]
fn open_failures_name_their_operation_and_cause() {
    assert_open_failures_name_their_operation_and_cause("throughput");
}

/// AC-9 — the latency path names them too. `latency_open_session` prints from
/// its own `OpenError::Connect` arm, which the throughput cases cannot
/// observe, so this run would stay green if that arm lost its cause.
#[test]
fn latency_open_failures_name_their_operation_and_cause() {
    assert_open_failures_name_their_operation_and_cause("latency");
}
