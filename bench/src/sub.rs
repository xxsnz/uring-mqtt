//! `bench sub` subcommand (task 7 of the benchmark-harness feature): the
//! mosquitto-side subscriber endpoint that arms a [`crate::record::Recorder`]
//! on stdin, records every delivered stamp, acks QoS 1 PUBLISHes, and on
//! SIGTERM writes a [`crate::record::DeliveryReport`] with `dropped: null`
//! (AC-16, AC-17, AC-18, AC-51, AC-58). Signatures copied from
//! `features/benchmark-harness/PLAN.md` `## Interface Contracts`.

use std::io::{self, BufRead, Read, Write};
use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::os::fd::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::time::Duration;

use bytes::BytesMut;
use futures_util::StreamExt;
use nix::errno::Errno;
use nix::sys::socket::{recv, MsgFlags};
use rmqtt_codec::types::QoS;
use rmqtt_codec::v3::{Codec, Packet, SubscribeReturnCode};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{oneshot, watch};
use tokio_util::codec::{Decoder, FramedRead};

use crate::gen::IO_TIMEOUT_SECS;
use crate::payload::monotonic_now_ns;
use crate::record::{parse_arm_line, DeliveryReport, Recorder, CONNECTIONS_MAX};
use crate::wire::{self, DISCONNECT, INBOUND_REMAINING_LENGTH_MAX};

/// Longest stdin line the reader accepts. The handshake is "READY → ARM →
/// ARMED", and only the second message carries data; 128 bytes still leaves
/// generous slack over the three `u64`s in `ARM <origin> <start> <end>`.
/// A runaway line without a newline is reported rather than buffered.
const STDIN_LINE_MAX_BYTES: u64 = 128;

/// MQTT packet id the subscriber always sends in its SUBSCRIBE. A real
/// subscriber may pick any non-zero id, but pinning one keeps the bytes the
/// test-local fake broker reads trivial.
const SUBSCRIBE_PACKET_ID: u16 = 1;

/// Return codes a SUBACK for this one-filter SUBSCRIBE may carry.
const SUBACK_RETURN_CODES: usize = 1;

/// Bytes the one non-blocking `recv` before SUBSCRIBE takes from the socket.
/// Any byte refuses the session; this many hold a whole SUBACK (5 bytes) or
/// any other acknowledgement-sized frame, so the refusal can name it.
const PRE_SUBSCRIBE_PROBE_BYTES: usize = 64;

#[derive(Debug, Clone)]
pub struct SubArgs {
    pub addr: String,
    pub qos: u8,
    pub connections: u32,
    pub report: PathBuf,
}

pub fn run_sub(args: &SubArgs) -> Result<(), String> {
    // The QoS the broker should deliver at; a value outside {0, 1} is a
    // CLI mistake the caller can fix, and the recorder's connection count
    // is the same memory budget `bench server` enforces. Refusing up front
    // keeps the ARM handshake from racing an out-of-memory kill.
    if args.qos > 1 {
        return Err(format!("qos must be 0 or 1, got {}", args.qos));
    }
    if args.connections == 0 || args.connections > CONNECTIONS_MAX {
        return Err(format!(
            "connections must be between 1 and {CONNECTIONS_MAX}, got {}",
            args.connections
        ));
    }

    let addr: SocketAddr = args.addr.parse().map_err(|e| format!("addr: {e}"))?;
    let recorder = Recorder::new(args.connections);
    let qos = args.qos;
    let report_path = args.report.clone();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("build tokio runtime: {e}"))?;

    runtime.block_on(async move { run_sub_session(addr, qos, recorder, report_path).await })
}

/// What made `read_loop` stop. The unified shutdown path below writes the
/// report on every variant — the brief's "report is written on every
/// shutdown path" guarantee is satisfied by reaching the unified path from
/// any arm of the select.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LoopExit {
    /// The watch fired (signal, or a stalled PUBACK write that then waited
    /// for it). The run ends with exit 0 (AC-51, AC-58).
    Shutdown,
    /// A failure the run must not hide: a refused ARM line, an unreadable
    /// stdin line, a broker that closed the connection, an undecodable
    /// frame, a control packet the benchmark protocol does not expect, a
    /// delivery at a QoS the subscription did not ask for, a QoS 1 PUBLISH
    /// with no packet id, or a PUBACK write that failed outright. The report
    /// is still written, and the cause leaves the process with exit 1 so the
    /// runner keeps it as a harness error.
    Failed(String),
}

/// What the stdin reader hands the ARM branch: `Arm(line)` for a readable
/// line, `Closed` for EOF, `ReadFailed(reason)` when the line cannot be
/// read. Mirrors `server::Control` so invalid UTF-8 on the ARM channel ends
/// the run with its cause instead of looking like EOF.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ArmInput {
    Arm(String),
    Closed,
    ReadFailed(String),
}

/// The read and write halves of the subscribed broker connection.
type Subscribed = (
    FramedRead<tokio::io::ReadHalf<TcpStream>, Codec>,
    tokio::io::WriteHalf<TcpStream>,
);

async fn run_sub_session(
    addr: SocketAddr,
    qos: u8,
    recorder: Recorder,
    report_path: PathBuf,
) -> Result<(), String> {
    let io_timeout = Duration::from_secs(IO_TIMEOUT_SECS);

    // ctrlc's `termination` feature installs SA_RESTART. The handler goes in
    // before the network handshake: a broker that accepts TCP and then
    // withholds CONNACK or SUBACK would otherwise leave the default signal
    // action to kill the process with no report, while AC-51 requires a
    // report and exit 0 for every SIGTERM that arrives before ARM.
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    ctrlc::set_handler(move || {
        let _ = shutdown_tx.send(true);
    })
    .map_err(|e| format!("install ctrl-c handler: {e}"))?;

    let (mut read, mut write_half) = tokio::select! {
        biased;
        _ = shutdown_rx.changed() => {
            // Setup was cancelled before READY: no stamp was recorded and
            // there is no subscription to tear down, so the empty report plus
            // exit 0 is the whole shutdown path (AC-51).
            return write_run_report(&report_path, &recorder);
        }
        setup = connect_and_subscribe(addr, qos, io_timeout) => setup?,
    };

    // READY is printed only after SUBACK so a refused subscription never
    // tells the runner "the endpoint is up" (AC-18).
    println!("READY");
    io::stdout().flush().map_err(|e| format!("flush READY: {e}"))?;

    // The std::thread reads blocking stdin because ctrlc installs SA_RESTART
    // and the runtime thread cannot sit in `read_line` without starving the
    // select below. The sender dying makes the receiver return `Err(_)`
    // which the select branch folds into "stay unarmed".
    let (arm_tx, arm_rx) = oneshot::channel::<ArmInput>();
    spawn_stdin_reader(arm_tx);

    let mut arm_rx: Option<oneshot::Receiver<ArmInput>> = Some(arm_rx);
    let exit = read_loop(
        &mut read,
        &mut write_half,
        &recorder,
        &mut shutdown_rx,
        &mut arm_rx,
        qos_into_enum(qos),
        io_timeout,
    )
    .await;

    // The unified shutdown path: every read-loop exit reaches here, so the
    // report is written exactly once on every shutdown path (signal, peer
    // close, write stall) — the brief's "report is written on every
    // shutdown path" guarantee.
    let _ = tokio::time::timeout(io_timeout, write_half.write_all(&DISCONNECT))
        .await
        .map_err(|_| ())
        .and_then(|r| r.map_err(|_| ()));

    write_run_report(&report_path, &recorder)?;

    // The report keeps the accounting; the cause of a broker or ARM failure
    // leaves through the exit code, which the runner records as a harness
    // error instead of reading a clean run out of a broken one.
    match exit {
        LoopExit::Shutdown => Ok(()),
        LoopExit::Failed(cause) => Err(cause),
    }
}

/// Connect, CONNECT/CONNACK, then SUBSCRIBE/SUBACK, each step under
/// `io_timeout`. Every error here happens before `READY`, so the caller ends
/// the run with the cause and writes no report (AC-18).
async fn connect_and_subscribe(
    addr: SocketAddr,
    qos: u8,
    io_timeout: Duration,
) -> Result<Subscribed, String> {
    // A destination that silently drops connection attempts must not outlast
    // the bounded CONNECT/SUBACK steps below, so setup takes the same
    // deadline as every other operation (same shape as `gen::open_session`).
    let stream = tokio::time::timeout(io_timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| format!("connect {addr}: timed out after {IO_TIMEOUT_SECS} s"))?
        .map_err(|e| format!("connect {addr}: {e}"))?;
    stream
        .set_nodelay(true)
        .map_err(|e| format!("set TCP_NODELAY: {e}"))?;
    let socket_fd = stream.as_raw_fd();
    let (read_half, mut write_half) = tokio::io::split(stream);

    write_with_timeout(
        write_half.write_all(&wire::encode_connect("bench-sub")),
        io_timeout,
        "CONNECT",
    )
    .await?;

    let mut read = FramedRead::new(read_half, Codec::new(INBOUND_REMAINING_LENGTH_MAX));
    let (connack_packet, connack_remaining_length) =
        read_packet(&mut read, io_timeout, "CONNACK").await?;
    let Packet::ConnectAck(ack) = connack_packet else {
        return Err(format!("expected CONNACK, got {connack_packet:?}"));
    };
    // The pinned codec checks only that a CONNACK body holds at least
    // two bytes and ignores the rest, so a longer body would pass every
    // check below (`gen::open_session` refuses it the same way).
    if connack_remaining_length != wire::CONNACK_REMAINING_LENGTH {
        return Err(format!(
            "CONNACK remaining length was {connack_remaining_length}, expected {}",
            wire::CONNACK_REMAINING_LENGTH
        ));
    }
    if ack.return_code != rmqtt_codec::v3::ConnectAckReason::ConnectionAccepted {
        return Err(format!(
            "CONNACK refused: {:?}",
            ack.return_code.reason()
        ));
    }
    // `wire::encode_connect` sets CleanSession, so [MQTT-3.2.2-1]
    // requires Session Present 0; a broker claiming a stored session
    // is not answering this CONNECT, and no subscription built on it
    // may lead to READY (AC-18).
    if ack.session_present {
        return Err("CONNACK claims a stored session on a clean-session CONNECT".to_string());
    }
    refuse_unsolicited_bytes(read.read_buffer(), socket_fd)?;

    let packet_id = NonZeroU16::new(SUBSCRIBE_PACKET_ID).expect("non-zero");
    write_with_timeout(
        write_half.write_all(&wire::encode_subscribe(packet_id, qos)),
        io_timeout,
        "SUBSCRIBE",
    )
    .await?;

    let (suback_packet, _) = read_packet(&mut read, io_timeout, "SUBACK").await?;
    let Packet::SubscribeAck {
        packet_id: acked_id,
        status,
    } = &suback_packet
    else {
        return Err(format!("expected SUBACK, got {suback_packet:?}"));
    };
    // A SUBACK that acknowledges another request, or that answers one filter
    // with several return codes, does not say this subscription exists, so it
    // must not lead to READY (AC-18).
    if acked_id.get() != SUBSCRIBE_PACKET_ID {
        return Err(format!(
            "SUBACK for packet id {}, expected {SUBSCRIBE_PACKET_ID}",
            acked_id.get()
        ));
    }
    let [granted] = status.as_slice() else {
        return Err(format!(
            "SUBACK has {} return codes, expected {SUBACK_RETURN_CODES}",
            status.len()
        ));
    };
    let expected = SubscribeReturnCode::Success(qos_into_enum(qos));
    if *granted != expected {
        let byte = subscribe_return_code_byte(*granted);
        return Err(format!("subscription refused: {byte:#04x}"));
    }

    Ok((read, write_half))
}

/// Write the run's report. `bench sub` is the mosquitto-side endpoint; the
/// broker's drop total is meaningless on this side and the JSON serialises it
/// as `null` (AC-17, decision Q7).
fn write_run_report(path: &std::path::Path, recorder: &Recorder) -> Result<(), String> {
    let mut report = recorder.report();
    report.dropped = None;
    write_report(path, &report)
}

#[allow(clippy::too_many_arguments)]
async fn read_loop(
    read: &mut FramedRead<tokio::io::ReadHalf<TcpStream>, Codec>,
    write_half: &mut tokio::io::WriteHalf<TcpStream>,
    recorder: &Recorder,
    shutdown_rx: &mut watch::Receiver<bool>,
    arm_rx: &mut Option<oneshot::Receiver<ArmInput>>,
    expected_qos: QoS,
    io_timeout: Duration,
) -> LoopExit {
    let mut waiting_for_shutdown = false;
    loop {
        if waiting_for_shutdown {
            // After a stalled PUBACK the brief says we wait only for
            // the watch; `.changed().await` resolves with `Err(_)` only when
            // the sender is dropped (ctrlc owns it until exit), so a plain
            // `.await` is the wait. AC-58 makes this a successful run: the
            // stall is already on stderr, and the report still carries every
            // stamp recorded before it.
            let _ = shutdown_rx.changed().await;
            return LoopExit::Shutdown;
        }

        let arm_branch = async {
            match arm_rx.as_mut() {
                Some(rx) => rx.await.unwrap_or(ArmInput::Closed),
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => { return LoopExit::Shutdown; }
            arm = arm_branch => {
                *arm_rx = None;
                match arm {
                    ArmInput::Arm(line) => {
                        // A refused ARM line leaves the recorder unarmed, so
                        // every later delivery would count as foreign. The
                        // run ends with the cause instead, and `ARMED` is
                        // printed only after the recorder is armed (AC-16) —
                        // same contract as `server::run_handshake`.
                        if let Err(e) = parse_arm_line(&line)
                            .and_then(|(origin_ns, window)| recorder.arm(origin_ns, window))
                        {
                            return LoopExit::Failed(e);
                        }
                        println!("ARMED");
                        let _ = io::stdout().flush();
                    }
                    ArmInput::Closed => { /* EOF: stay unarmed and wait for a signal */ }
                    // Not EOF: the ARM line could not be read at all, so the
                    // missing `ARMED` must carry its cause out of the run
                    // instead of waiting for a signal that reports exit 0.
                    ArmInput::ReadFailed(reason) => return LoopExit::Failed(reason),
                }
            }
            packet = read.next() => {
                // A broker that closes the connection or sends an
                // undecodable frame after READY has ended the measured run;
                // both carry their cause out so the record keeps it.
                let Some(frame) = packet else {
                    return LoopExit::Failed("broker closed the connection".to_string());
                };
                let (parsed, _consumed) = match frame {
                    Ok(decoded) => decoded,
                    Err(e) => return LoopExit::Failed(format!("decode broker frame: {e}")),
                };
                let Packet::Publish(publish) = parsed else {
                    // After SUBACK a delivery is the only packet this
                    // endpoint expects: it sends no PINGREQ and no
                    // UNSUBSCRIBE, so no PINGRESP or UNSUBACK can be an
                    // answer to its own traffic. A second CONNACK or SUBACK,
                    // or any other control packet, is broker work inside the
                    // measured stream that a reconciled report would hide, so
                    // the run carries it out instead (R2-Q2) — the same
                    // treatment the generator's reader gives an unexpected
                    // packet (`gen.rs:717`).
                    return LoopExit::Failed(format!(
                        "broker sent an unexpected packet after SUBACK: {parsed:?}"
                    ));
                };
                // The SUBACK granted exactly the requested QoS, so a delivery
                // at any other QoS is a broker protocol violation: a
                // downgraded delivery did less work than the benchmark row
                // configured, and QoS 2 cannot be completed at all because
                // PUBREC/PUBREL are outside the benchmark protocol. Either
                // way, recording it would put a stamp the configured run
                // never asked for into an otherwise clean report (R2-Q2).
                if publish.qos != expected_qos {
                    return LoopExit::Failed(format!(
                        "broker delivered QoS {} PUBLISH on {}, expected QoS {}",
                        u8::from(publish.qos),
                        publish.topic,
                        u8::from(expected_qos),
                    ));
                }
                // The broker may deliver any topic it likes; only `bench/load`
                // is a benchmark delivery, and anything else is foreign
                // (R2-Q2).
                recorder.record_on_topic(
                    &publish.topic,
                    publish.payload.as_ref(),
                    monotonic_now_ns(),
                );
                if publish.qos == QoS::AtLeastOnce {
                    let Some(id) = publish.packet_id else {
                        return LoopExit::Failed(
                            "QoS 1 PUBLISH without packet id".to_string(),
                        );
                    };
                    let puback = wire::encode_puback(id);
                    let timed = tokio::time::timeout(
                        io_timeout,
                        write_half.write_all(&puback),
                    );
                    tokio::select! {
                        _ = shutdown_rx.changed() => { return LoopExit::Shutdown; }
                        res = timed => {
                            match res {
                                Ok(Ok(())) => {}
                                // A write that fails outright is not AC-58's
                                // "broker stopped reading": the connection is
                                // broken, the stamp stays unacked, and a clean
                                // run record would hide it. Same treatment as
                                // a broker that closed the connection.
                                Ok(Err(e)) => {
                                    return LoopExit::Failed(format!("write PUBACK: {e}"));
                                }
                                // The broker has stopped draining. AC-58 makes
                                // this a successful run, so packet reading
                                // ends, the next loop wait is the watch, and
                                // stderr carries the evidence.
                                Err(_) => {
                                    eprintln!(
                                        "write PUBACK: timed out after {} s; waiting for shutdown",
                                        io_timeout.as_secs()
                                    );
                                    waiting_for_shutdown = true;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Write the entire buffer under the deadline. The `what` label appears in
/// the error so a stalled CONNECT or SUBSCRIBE is debuggable from the
/// failure message alone. The PUBACK writes in `read_loop` carry their own
/// deadline because they also race the shutdown watch.
async fn write_with_timeout<F, E>(
    fut: F,
    timeout: Duration,
    what: &'static str,
) -> Result<(), String>
where
    F: std::future::Future<Output = Result<(), E>>,
    E: std::fmt::Display,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(format!("write {what}: {e}")),
        Err(_) => Err(format!(
            "write {what}: timed out after {} s",
            timeout.as_secs()
        )),
    }
}

/// Read exactly one packet, with its Remaining Length, from the framed broker
/// stream, treating a peer close and a decode failure as run-level errors with
/// their own labels. The Remaining Length lets the CONNACK check in
/// `connect_and_subscribe` refuse a frame the codec would otherwise let
/// through, as the parallel read in `gen::open_session` does.
async fn read_packet(
    read: &mut FramedRead<tokio::io::ReadHalf<TcpStream>, Codec>,
    timeout: Duration,
    what: &'static str,
) -> Result<(Packet, u32), String> {
    match tokio::time::timeout(timeout, read.next()).await {
        Ok(Some(Ok((packet, remaining_length)))) => Ok((packet, remaining_length)),
        Ok(Some(Err(e))) => Err(format!("decode {what}: {e}")),
        Ok(None) => Err(format!("peer closed before {what}")),
        Err(_) => Err(format!(
            "read {what}: timed out after {} s",
            timeout.as_secs()
        )),
    }
}

/// Refuses the session when the broker sent anything after its CONNACK
/// before this client wrote SUBSCRIBE. A clean-session client is owed
/// nothing yet, so such bytes cannot answer the request. `buffered` is
/// what the CONNACK read left in the codec buffer; `socket_fd` is then
/// probed once with a non-blocking `recv`, which asks the kernel
/// directly — Tokio's cached readiness can still hide bytes that are
/// already queued. The codec is not polled: it consumes a complete
/// fixed header even while the body is missing. Every byte taken here
/// refuses the session, so taking it loses nothing. A byte that arrives
/// after this probe cannot be told apart from a reply and passes.
fn refuse_unsolicited_bytes(buffered: &[u8], socket_fd: RawFd) -> Result<(), String> {
    let mut early = BytesMut::from(buffered);
    let mut probe = [0u8; PRE_SUBSCRIBE_PROBE_BYTES];
    match recv(socket_fd, &mut probe, MsgFlags::MSG_DONTWAIT) {
        Ok(0) => return Err("peer closed before SUBSCRIBE".to_string()),
        Ok(n) => early.extend_from_slice(&probe[..n]),
        Err(Errno::EAGAIN) => {}
        Err(e) => return Err(format!("read before SUBSCRIBE: {e}")),
    }
    if early.is_empty() {
        return Ok(());
    }
    let unsolicited = early.len();
    Err(match Codec::new(INBOUND_REMAINING_LENGTH_MAX).decode(&mut early) {
        Ok(Some((packet, _))) => {
            format!("broker sent {} before SUBSCRIBE", wire::packet_kind(&packet))
        }
        Ok(None) | Err(_) => format!(
            "CONNACK was followed by {unsolicited} unsolicited bytes before SUBSCRIBE"
        ),
    })
}

/// Map a `qos` byte (0 or 1) to the `rmqtt_codec::types::QoS` variant. The
/// CLI check above keeps this total.
fn qos_into_enum(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        _ => QoS::AtLeastOnce,
    }
}

/// Wire byte for a SUBACK return code, for the diagnostic string the brief
/// specifies (`subscription refused: {code:#04x}`).
fn subscribe_return_code_byte(code: SubscribeReturnCode) -> u8 {
    match code {
        SubscribeReturnCode::Success(qos) => u8::from(qos),
        SubscribeReturnCode::Failure => 0x80,
    }
}

/// One-shot reader for the ARM handshake. Sends `Arm(line)` for a readable
/// line, `Closed` for EOF, and `ReadFailed(reason)` for a read error (invalid
/// UTF-8 included) or a line the byte limit cut short — the same three
/// outcomes as `server::spawn_stdin_reader`. The reader thread dies on the
/// first send, so the select's `arm_rx` becomes a permanent pending after the
/// outcome has been delivered.
fn spawn_stdin_reader(tx: oneshot::Sender<ArmInput>) {
    std::thread::Builder::new()
        .name("bench-sub-stdin".into())
        .spawn(move || {
            let stdin = io::stdin();
            let mut locked = stdin.lock();
            let mut line = String::new();
            let outcome = match (&mut locked)
                .take(STDIN_LINE_MAX_BYTES)
                .read_line(&mut line)
            {
                Ok(0) => ArmInput::Closed,
                // A read error is not EOF: invalid UTF-8 on the ARM channel
                // must end the run with its cause, not leave the loop waiting
                // for a signal that reports a clean run.
                Err(e) => ArmInput::ReadFailed(format!("read stdin line: {e}")),
                Ok(read) => {
                    let truncated = !line.ends_with('\n')
                        && u64::try_from(read).expect("read fits in u64")
                            >= STDIN_LINE_MAX_BYTES;
                    if truncated {
                        ArmInput::ReadFailed(format!(
                            "stdin line is longer than {STDIN_LINE_MAX_BYTES} bytes"
                        ))
                    } else {
                        ArmInput::Arm(line)
                    }
                }
            };
            let _ = tx.send(outcome);
        })
        .expect("spawn stdin reader");
}

fn write_report(path: &std::path::Path, report: &DeliveryReport) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| format!("create report dir: {e}"))?;
        }
    }
    let file = std::fs::File::create(path).map_err(|e| format!("create report: {e}"))?;
    let mut writer = io::BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, report).map_err(|e| format!("write report: {e}"))?;
    writer.flush().map_err(|e| format!("flush report: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{refuse_unsolicited_bytes, Errno};
    use crate::wire::INBOUND_REMAINING_LENGTH_MAX;
    use futures_util::{FutureExt, StreamExt};
    use rmqtt_codec::v3::Codec;
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio_util::codec::FramedRead;

    /// CONNACK with Return Code 0 and Session Present 0.
    const CONNACK_ACCEPTED: [u8; 4] = [0x20, 0x02, 0x00, 0x00];
    /// Whole SUBACK for packet id 1 (5 bytes).
    const BUFFERED_SUBACK: [u8; 5] = [0x90, 0x03, 0x00, 0x01, 0x01];
    /// Time the test's only runtime thread blocks in `std::thread::sleep` so
    /// loopback queues the byte while the reactor cannot see it.
    const LOOPBACK_SETTLE: Duration = Duration::from_millis(50);
    /// One whole frame of every MQTT 3.1.1 control packet type, beside the
    /// name `wire::packet_kind` must give it. A broker sends only some of
    /// them, but the refusal names whatever decodes, and a wrong arm of the
    /// mapping would otherwise name the wrong packet to the operator.
    const EVERY_PACKET_KIND: [(&[u8], &str); 14] = [
        (b"\x10\x0C\x00\x04MQTT\x04\x02\x00\x00\x00\x00", "CONNECT"),
        (b"\x20\x02\x00\x00", "CONNACK"),
        (b"\x30\x03\x00\x01t", "PUBLISH"),
        (b"\x40\x02\x00\x01", "PUBACK"),
        (b"\x50\x02\x00\x01", "PUBREC"),
        (b"\x62\x02\x00\x01", "PUBREL"),
        (b"\x70\x02\x00\x01", "PUBCOMP"),
        (b"\x82\x06\x00\x01\x00\x01t\x00", "SUBSCRIBE"),
        (b"\x90\x03\x00\x01\x00", "SUBACK"),
        (b"\xA2\x05\x00\x01\x00\x01t", "UNSUBSCRIBE"),
        (b"\xB0\x02\x00\x01", "UNSUBACK"),
        (b"\xC0\x00", "PINGREQ"),
        (b"\xD0\x00", "PINGRESP"),
        (b"\xE0\x00", "DISCONNECT"),
    ];

    /// A connected loopback pair: a blocking `std` peer and a client
    /// registered with the test's runtime.
    fn loopback_pair() -> (std::net::TcpStream, tokio::net::TcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let client = std::net::TcpStream::connect(listener.local_addr().expect("local addr"))
            .expect("connect");
        let (peer, _) = listener.accept().expect("accept");
        peer.set_nodelay(true).expect("peer TCP_NODELAY");
        client.set_nonblocking(true).expect("non-blocking client");
        (
            peer,
            tokio::net::TcpStream::from_std(client).expect("register client"),
        )
    }

    #[tokio::test]
    async fn refuse_unsolicited_bytes_names_what_the_codec_buffer_holds() {
        let (_peer, client) = loopback_pair();
        let socket_fd = client.as_raw_fd();
        // The peer writes nothing, so `recv` returns `EAGAIN`.
        assert_eq!(refuse_unsolicited_bytes(&[], socket_fd), Ok(()));
        assert_eq!(
            refuse_unsolicited_bytes(&BUFFERED_SUBACK, socket_fd),
            Err("broker sent SUBACK before SUBSCRIBE".to_string())
        );
        // A fixed header without its body.
        assert_eq!(
            refuse_unsolicited_bytes(&BUFFERED_SUBACK[..2], socket_fd),
            Err("CONNACK was followed by 2 unsolicited bytes before SUBSCRIBE".to_string())
        );
        assert_eq!(
            refuse_unsolicited_bytes(&BUFFERED_SUBACK[..1], socket_fd),
            Err("CONNACK was followed by 1 unsolicited bytes before SUBSCRIBE".to_string())
        );
    }

    #[tokio::test]
    async fn refuse_unsolicited_bytes_finds_a_queued_byte_tokio_has_not_seen() {
        let (mut peer, client) = loopback_pair();
        let socket_fd = client.as_raw_fd();
        let (read_half, _write_half) = tokio::io::split(client);
        let mut read = FramedRead::new(read_half, Codec::new(INBOUND_REMAINING_LENGTH_MAX));
        peer.write_all(&CONNACK_ACCEPTED).expect("peer writes CONNACK");
        read.next().await.expect("a frame").expect("a CONNACK");
        assert_eq!(
            refuse_unsolicited_bytes(read.read_buffer(), socket_fd),
            Ok(()),
            "nothing follows the CONNACK yet"
        );
        peer.write_all(&BUFFERED_SUBACK[..1])
            .expect("peer writes one byte");
        std::thread::sleep(LOOPBACK_SETTLE);
        assert!(
            read.next().now_or_never().is_none(),
            "precondition: Tokio's cached readiness still hides the queued byte"
        );
        assert!(
            read.read_buffer().is_empty(),
            "the byte is only in the kernel queue"
        );
        assert_eq!(
            refuse_unsolicited_bytes(read.read_buffer(), socket_fd),
            Err(
                "CONNACK was followed by 1 unsolicited bytes before SUBSCRIBE".to_string()
            )
        );
    }

    /// AC-8 — the refusal names the packet by its MQTT control packet name
    /// for every type the codec decodes, so no refusal carries wire bytes or
    /// a `{:?}` of a decoded frame.
    #[tokio::test]
    async fn refuse_unsolicited_bytes_names_every_packet_kind_it_can_decode() {
        let (_peer, client) = loopback_pair();
        let socket_fd = client.as_raw_fd();
        // The peer writes nothing, so every case comes from `buffered`.
        for (frame, kind) in EVERY_PACKET_KIND {
            assert_eq!(
                refuse_unsolicited_bytes(frame, socket_fd),
                Err(format!("broker sent {kind} before SUBSCRIBE")),
                "the refusal must name the {kind} the broker sent"
            );
        }
    }

    /// AC-8 — a peer that closes between its CONNACK and this client's
    /// SUBSCRIBE is refused by that cause: the probe reads end of file, which
    /// is neither nothing queued nor a frame.
    #[tokio::test]
    async fn refuse_unsolicited_bytes_reports_a_peer_that_closed_before_subscribe() {
        let (peer, client) = loopback_pair();
        let socket_fd = client.as_raw_fd();
        drop(peer);
        std::thread::sleep(LOOPBACK_SETTLE);
        assert_eq!(
            refuse_unsolicited_bytes(&[], socket_fd),
            Err("peer closed before SUBSCRIBE".to_string())
        );
    }

    /// AC-8 — a probe that fails names the read and its cause instead of
    /// passing the session through. Linux answers a close that leaves unread
    /// bytes with a reset, so the probe fails rather than reading end of file.
    #[tokio::test]
    async fn refuse_unsolicited_bytes_names_a_failed_read_before_subscribe() {
        let (peer, client) = loopback_pair();
        let socket_fd = client.as_raw_fd();
        let (_read_half, mut write_half) = tokio::io::split(client);
        write_half
            .write_all(&CONNACK_ACCEPTED)
            .await
            .expect("client writes bytes the peer never reads");
        std::thread::sleep(LOOPBACK_SETTLE);
        drop(peer);
        std::thread::sleep(LOOPBACK_SETTLE);
        assert_eq!(
            refuse_unsolicited_bytes(&[], socket_fd),
            Err(format!("read before SUBSCRIBE: {}", Errno::ECONNRESET))
        );
    }
}
