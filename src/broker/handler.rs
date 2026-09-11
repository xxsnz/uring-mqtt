use monoio::io::sink::SinkExt;
use monoio::io::stream::Stream;
use monoio::io::{AsyncReadRent, AsyncWriteRent};
use monoio::net::TcpStream;
use monoio_codec::Framed;
use std::num::NonZeroU32;
use std::time::Duration;

use super::worker::EventSender;
use super::Event;
use crate::broker::handshake::{self, ConnectDecision};
use crate::broker::packet::{Disposition, Reply};
use crate::codec::mqtt::{
    ConnectAck, ConnectAckReason, DecodeError, MqttEncoder, MqttPacket, PacketV3, PacketV5,
};
use crate::codec::version::{ProtocolVersion, VersionDecoder};
use crate::connection::ConnectionState;
use crate::error::Error;
use rmqtt_codec::v5::ConnectAckReason as V5ConnectAckReason;

/// What a completed (non-error) client session amounted to.
#[derive(Debug)]
pub(crate) enum SessionOutcome {
    Served,
    Refused,
    /// Closed by the packet loop's protocol-violation policy.
    Violation,
}

const TIMEOUT_CTX_CONNACK_FLUSH: &str = "handshake timeout during CONNACK flush";
pub(super) const TIMEOUT_CTX_PINGRESP_FLUSH: &str = "idle timeout during PINGRESP flush";
pub(super) const TIMEOUT_CTX_PUBACK_FLUSH: &str = "idle timeout during PUBACK flush";
pub(super) const TIMEOUT_CTX_SUBACK_FLUSH: &str = "idle timeout during SUBACK flush";
pub(super) const TIMEOUT_CTX_UNSUBACK_FLUSH: &str = "idle timeout during UNSUBACK flush";
const TIMEOUT_CTX_DISCONNECT_FLUSH: &str = "idle timeout during DISCONNECT flush";

/// Handle a single MQTT client connection.
pub async fn handle_client(
    stream: TcpStream,
    event_tx: EventSender,
    connection_timeout_secs: u64,
    idle_timeout_secs: u64,
) -> Result<SessionOutcome, Error> {
    let peer_addr = stream.peer_addr().map_err(Error::Io)?;
    handle_client_io(
        stream,
        peer_addr,
        event_tx,
        connection_timeout_secs,
        idle_timeout_secs,
    )
    .await
}

/// Log a transport failure on a handshake read: routine hangups at debug,
/// non-routine anomalies at warn. Genuine decoder rejections are handled
/// separately at the call site so their violation wording stays verbatim.
fn log_handshake_transport_error(peer_addr: std::net::SocketAddr, err: &Error) {
    if err.is_routine_disconnect() {
        tracing::debug!("client disconnected during handshake ({peer_addr}): {err}");
    } else {
        tracing::warn!("handshake transport error ({peer_addr}): {err}");
    }
}

/// IO-generic body of `handle_client`. The public entry keeps the
/// monomorphic `TcpStream` signature; tests substitute a `TestIo` wrapper.
#[allow(clippy::too_many_lines)] // per-arm timeout diagnostics push the body to the pedantic 100-line boundary
async fn handle_client_io<IO>(
    stream: IO,
    peer_addr: std::net::SocketAddr,
    event_tx: EventSender,
    connection_timeout_secs: u64,
    idle_timeout_secs: u64,
) -> Result<SessionOutcome, Error>
where
    IO: AsyncReadRent + AsyncWriteRent,
{
    let mut state = ConnectionState::new();

    // Shared handshake budget — version detect + CONNECT decode + the
    // handshake reply writes share one timer; config is clamped to
    // MAX_TIMEOUT_SECS so monoio's timer, which panics converting absurd
    // Durations to millis, never sees a deadline past the supported ceiling
    // (no `Instant + Duration` sum anywhere).
    let handshake_start = std::time::Instant::now();
    let budget = Duration::from_secs(connection_timeout_secs.min(handshake::MAX_TIMEOUT_SECS));
    let remaining = || budget.saturating_sub(handshake_start.elapsed());

    // Phase 1: Detect protocol version.
    let mut framed = Framed::new(stream, VersionDecoder::new());

    let version = match monoio::time::timeout(remaining(), framed.next()).await {
        Ok(Some(Ok(v))) => v,
        Ok(Some(Err(e))) => {
            if e.get_ref()
                .and_then(|s| s.downcast_ref::<DecodeError>())
                .is_some()
            {
                // VersionCodec rejects any first packet that is not CONNECT, so this is
                // the boundary where "first packet was not CONNECT" actually surfaces.
                tracing::warn!(
                    "handshake violation: first packet was not CONNECT ({peer_addr}): {e}"
                );
                return Err(Error::Io(e));
            }
            let err = Error::Io(e);
            log_handshake_transport_error(peer_addr, &err);
            return Err(err);
        }
        Ok(None) => return Err(Error::ClientClosed),
        Err(_) => {
            tracing::debug!("handshake timeout during version detect ({peer_addr})");
            return Err(Error::Timeout);
        }
    };

    tracing::debug!("Detected protocol version: {:?}", version);

    // Phase 2: Switch to versioned codec (preserves buffered bytes — pipelined packets survive).
    let mut framed = framed.map_codec(|_| CodecPair::new(version));

    // Phase 3: Read CONNECT under the shared handshake budget.
    let packet = match monoio::time::timeout(remaining(), framed.next()).await {
        Ok(Some(Ok((p, _id)))) => p,
        Ok(Some(Err(e))) => {
            // Decoder-level InvalidClientId → version-matched refusal CONNACK.
            if let Some(DecodeError::InvalidClientId) =
                e.get_ref().and_then(|s| s.downcast_ref::<DecodeError>())
            {
                tracing::warn!("CONNECT refused: invalid client id from {peer_addr}");
                let refusal: MqttPacket = match version {
                    ProtocolVersion::MQTT3 => MqttPacket::V3(PacketV3::ConnectAck(ConnectAck {
                        return_code: ConnectAckReason::IdentifierRejected,
                        session_present: false,
                    })),
                    ProtocolVersion::MQTT5 => MqttPacket::V5(PacketV5::ConnectAck(Box::new(
                        handshake::honest_v5_connack(V5ConnectAckReason::ClientIdentifierNotValid),
                    ))),
                };
                bounded_send(
                    &mut framed,
                    refusal,
                    remaining(),
                    peer_addr,
                    TIMEOUT_CTX_CONNACK_FLUSH,
                )
                .await?;
                return Ok(SessionOutcome::Refused);
            }
            if e.get_ref()
                .and_then(|s| s.downcast_ref::<DecodeError>())
                .is_some()
            {
                // Every other decoder rejection is a protocol violation closed without a
                // reply; Q8 requires it visible at default level, not only at debug.
                tracing::warn!("handshake violation: malformed CONNECT from {peer_addr}: {e}");
                return Err(Error::Io(e));
            }
            let err = Error::Io(e);
            log_handshake_transport_error(peer_addr, &err);
            return Err(err);
        }
        Ok(None) => return Err(Error::ClientClosed),
        Err(_) => {
            tracing::debug!("handshake timeout during CONNECT read ({peer_addr})");
            return Err(Error::Timeout);
        }
    };
    // Idle-window anchor: CONNECT receipt, so a slow CONNACK flush cannot
    // extend the first idle interval.
    let connect_received_at = std::time::Instant::now();

    // Phase 4: Evaluate CONNECT, then release the packet — its will payload,
    // credentials, and properties must not stay allocated for the whole connection.
    let decision = handshake::evaluate_connect(&packet, idle_timeout_secs, peer_addr);
    drop(packet);

    let outcome = match decision {
        ConnectDecision::NotConnect => {
            tracing::warn!("handshake violation: first packet was not CONNECT ({peer_addr})");
            return Err(Error::Protocol("first packet was not CONNECT".into()));
        }
        ConnectDecision::Refuse { connack } => {
            let reason = match &connack {
                MqttPacket::V3(PacketV3::ConnectAck(a)) => format!("{:?}", a.return_code),
                MqttPacket::V5(PacketV5::ConnectAck(a)) => format!("{:?}", a.reason_code),
                _ => "unknown".to_string(),
            };
            tracing::warn!("CONNECT refused: {reason} from {peer_addr}");
            bounded_send(
                &mut framed,
                connack,
                remaining(),
                peer_addr,
                TIMEOUT_CTX_CONNACK_FLUSH,
            )
            .await?;
            SessionOutcome::Refused
        }
        ConnectDecision::Accept {
            connack,
            client_id,
            keep_alive_secs,
            idle_timeout,
            max_packet_size,
        } => {
            bounded_send(
                &mut framed,
                connack,
                remaining(),
                peer_addr,
                TIMEOUT_CTX_CONNACK_FLUSH,
            )
            .await?;
            state.client_id = client_id;
            state.keep_alive = keep_alive_secs;
            state.last_packet_time = connect_received_at;

            run_packet_loop(
                &mut framed,
                &mut state,
                event_tx,
                idle_timeout,
                peer_addr,
                max_packet_size,
            )
            .await?
        }
    };

    Ok(outcome)
}

/// Main packet loop — idle deadline anchored to the last received packet.
///
/// `max_packet_size` is the client's declared receive limit from CONNECT: every
/// reply is gated against it, because a SUBACK's size follows the request's
/// filter count and MQTT 5 forbids sending past the declared limit.
async fn run_packet_loop<IO>(
    framed: &mut Framed<IO, CodecPair>,
    state: &mut ConnectionState,
    event_tx: EventSender,
    idle_timeout: Duration,
    peer_addr: std::net::SocketAddr,
    max_packet_size: Option<NonZeroU32>,
) -> Result<SessionOutcome, Error>
where
    IO: AsyncReadRent + AsyncWriteRent,
{
    loop {
        let remaining_idle = idle_timeout.saturating_sub(state.last_packet_time.elapsed());
        let packet_result = monoio::time::timeout(remaining_idle, framed.next()).await;
        match packet_result {
            Ok(Some(Ok((packet, _id)))) => {
                state.update_activity();
                let reply_deadline = idle_timeout.saturating_sub(state.last_packet_time.elapsed());
                let disposition = super::packet::dispatch(&packet);
                // The ingest effect happens per arm; the reply every arm owes,
                // if any, leaves through the single gated send below.
                let owed_reply = match disposition {
                    Disposition::Deliver(publish) => {
                        deliver_publish(&event_tx, publish);
                        None
                    }
                    Disposition::DeliverThenAck(publish, packet_id) => {
                        deliver_publish(&event_tx, publish);
                        Some(Reply::PublishAck(packet_id))
                    }
                    Disposition::Reply(reply) => Some(reply),
                    Disposition::Close => {
                        tracing::debug!("Client requested disconnect");
                        break Ok(SessionOutcome::Served);
                    }
                    Disposition::Violation(violation) => {
                        break Ok(close_with_violation(
                            framed,
                            violation,
                            reply_deadline,
                            peer_addr,
                            max_packet_size,
                        )
                        .await);
                    }
                };
                if let Some(reply) = owed_reply {
                    let version = framed.codec().version();
                    if let Some(violation) =
                        super::packet::reply_over_max_packet_size(reply, version, max_packet_size)
                    {
                        break Ok(close_with_violation(
                            framed,
                            violation,
                            reply_deadline,
                            peer_addr,
                            max_packet_size,
                        )
                        .await);
                    }
                    bounded_send(
                        framed,
                        reply.render(version),
                        reply_deadline,
                        peer_addr,
                        reply.timeout_context(),
                    )
                    .await?;
                }
            }
            Ok(Some(Err(e))) => {
                if let Some(violation) = super::packet::classify_read_error(&e) {
                    let reply_deadline =
                        idle_timeout.saturating_sub(state.last_packet_time.elapsed());
                    break Ok(close_with_violation(
                        framed,
                        violation,
                        reply_deadline,
                        peer_addr,
                        max_packet_size,
                    )
                    .await);
                }
                tracing::debug!("Packet error: {:?}", e);
                break Ok(SessionOutcome::Served);
            }
            Ok(None) => {
                tracing::debug!("Client closed connection");
                break Ok(SessionOutcome::Served);
            }
            Err(_) => {
                tracing::debug!("client idle timeout ({peer_addr})");
                break Ok(SessionOutcome::Served);
            }
        }
    }
}

/// Hands a PUBLISH's payload to the worker's ingest seam. A payload
/// `parse_sensor_data` cannot read is discarded here; a reading the event
/// channel cannot take is discarded inside `EventSender::send`, which
/// counts it. Neither outcome reaches the caller, because neither changes
/// what the caller does: the PUBACK is sent either way, and QoS 0 has no
/// ack to send at all.
fn deliver_publish(event_tx: &EventSender, publish: &rmqtt_codec::types::Publish) {
    tracing::debug!("PUBLISH {} len: {}", publish.topic, publish.payload.len());
    if let Some(event) = parse_sensor_data(&publish.payload) {
        event_tx.send(event);
    }
}

/// Emit the one warn line, send the v5 DISCONNECT best-effort, and report
/// `Violation`. The DISCONNECT flush is best-effort by design: a write
/// failure to a dead peer must not convert a countable violation into an
/// anonymous `Err`. `max_packet_size` is the client's declared receive limit:
/// a DISCONNECT that does not fit under it is omitted rather than written, so
/// the close never breaks the limit it may itself be enforcing.
async fn close_with_violation<IO>(
    framed: &mut Framed<IO, CodecPair>,
    violation: super::packet::Violation,
    deadline: Duration,
    peer_addr: std::net::SocketAddr,
    max_packet_size: Option<NonZeroU32>,
) -> SessionOutcome
where
    IO: AsyncWriteRent,
{
    tracing::warn!("{} ({peer_addr}) — closing", violation.reason());
    if let Some(pkt) = violation.disconnect(framed.codec().version(), max_packet_size) {
        drop(
            bounded_send(
                framed,
                pkt,
                deadline,
                peer_addr,
                TIMEOUT_CTX_DISCONNECT_FLUSH,
            )
            .await,
        );
    }
    SessionOutcome::Violation
}

/// Bounded `send_and_flush` under a deadline (handshake or idle).
async fn bounded_send<IO>(
    framed: &mut Framed<IO, CodecPair>,
    pkt: MqttPacket,
    deadline: Duration,
    peer_addr: std::net::SocketAddr,
    timeout_context: &'static str,
) -> Result<(), Error>
where
    IO: AsyncWriteRent,
{
    let send_fut = SinkExt::send_and_flush(framed, pkt);
    match monoio::time::timeout(deadline, send_fut).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(Error::Io(e)),
        Err(_) => {
            tracing::debug!("{timeout_context} ({peer_addr})");
            Err(Error::Timeout)
        }
    }
}

/// Combined codec for both encoding and decoding MQTT packets.
struct CodecPair {
    version: ProtocolVersion,
    decoder: crate::codec::mqtt::MqttDecoder,
    encoder: MqttEncoder,
}

impl CodecPair {
    fn new(version: ProtocolVersion) -> Self {
        Self {
            version,
            decoder: crate::codec::mqtt::MqttDecoder::new(version),
            encoder: MqttEncoder::new(version),
        }
    }

    fn version(&self) -> ProtocolVersion {
        self.version
    }
}

impl monoio_codec::Decoder for CodecPair {
    type Item = (MqttPacket, u32);
    type Error = std::io::Error;

    fn decode(
        &mut self,
        src: &mut bytes::BytesMut,
    ) -> Result<monoio_codec::Decoded<Self::Item>, Self::Error> {
        self.decoder.decode(src)
    }
}

impl monoio_codec::Encoder<MqttPacket> for CodecPair {
    type Error = std::io::Error;

    fn encode(&mut self, item: MqttPacket, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        self.encoder.encode(item, dst)
    }
}

/// Parse binary sensor data (SensorV1 format).
///
/// Format: 4 bytes big-endian
/// - bytes 0-1: temperature (i16, scale 0.01°C)
/// - bytes 2-3: pressure (u16, scale 1.0 hPa)
#[inline(always)]
fn parse_sensor_data(data: &[u8]) -> Option<Event> {
    if data.len() < 4 {
        return None;
    }
    Some(Event::SensorV1 {
        temperature: i16::from_be_bytes([data[0], data[1]]),
        pressure: u16::from_be_bytes([data[2], data[3]]),
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::broker::worker::{event_channel, EventReceiver, EVENT_CHANNEL_CAPACITY};
    use bytes::BytesMut;
    use monoio::buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut};
    use monoio::io::{AsyncReadRent, AsyncWriteRent};
    use monoio::net::{TcpListener, TcpStream};
    use monoio_codec::Encoder as _;
    use rmqtt_codec::types::Publish as TypesPublish;
    use rmqtt_codec::types::QoS;
    use std::num::NonZeroU16;

    // Wire fixtures (byte literals — server's own encoder never produces expected values).
    /// v3 CONNECT: ka=60, id "test", flags 0x00. Mirrors `src/codec/version.rs:76-86`.
    const V3_CONNECT_TEST: [u8; 18] = [
        0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x00, 0x00, 0x3C, 0x00, 0x04, b't',
        b'e', b's', b't',
    ];
    /// v3 CONNECT: ka=1, id "test", flags 0x00 — for keep-alive-expiry tests.
    const V3_CONNECT_TEST_KA1: [u8; 18] = [
        0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x00, 0x00, 0x01, 0x00, 0x04, b't',
        b'e', b's', b't',
    ];
    /// v3 CONNECT: ka=60, id "test", reserved flag set (0x01) → decoder emits
    /// `ConnectReservedFlagSet`. Byte index 9 is the only difference from `V3_CONNECT_TEST`.
    const V3_CONNECT_RESERVED_FLAG: [u8; 18] = [
        0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x01, 0x00, 0x3C, 0x00, 0x04, b't',
        b'e', b's', b't',
    ];
    /// v3 CONNECT shape with protocol name "MQTX" → `VersionCodec` rejects with
    /// `DecodeError::InvalidProtocol`. Byte index 7 is the only difference from
    /// `V3_CONNECT_TEST`.
    const V3_CONNECT_BAD_PROTOCOL_NAME: [u8; 18] = [
        0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'X', 0x04, 0x00, 0x00, 0x3C, 0x00, 0x04, b't',
        b'e', b's', b't',
    ];
    /// v3 CONNECT: empty id, clean_session=false → decoder emits `InvalidClientId`.
    const V3_CONNECT_EMPTY_NO_CLEAN: [u8; 14] = [
        0x10, 0x0C, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x00, 0x00, 0x3C, 0x00, 0x00,
    ];
    /// v3 PUBLISH QoS 0: topic "t", payload SensorV1 25.00°C / 1013 hPa.
    const PUBLISH_QOS0_T: [u8; 9] = [0x30, 0x07, 0x00, 0x01, b't', 0x09, 0xC4, 0x03, 0xF5];
    /// v3 PUBLISH QoS 1: topic "t", packet id 1, payload SensorV1 25.00°C / 1013 hPa.
    const PUBLISH_QOS1_T: [u8; 11] = [
        0x32, 0x09, 0x00, 0x01, b't', 0x00, 0x01, 0x09, 0xC4, 0x03, 0xF5,
    ];
    /// v3 PUBLISH QoS 1: topic "t", packet id 1, payload too short for `parse_sensor_data`.
    const PUBLISH_QOS1_T_BAD_PAYLOAD: [u8; 10] =
        [0x32, 0x08, 0x00, 0x01, b't', 0x00, 0x01, 0xAA, 0xBB, 0xCC];
    const PINGREQ: [u8; 2] = [0xC0, 0x00];
    const DISCONNECT: [u8; 2] = [0xE0, 0x00];
    /// v3 SUBSCRIBE: packet id 37, single filter "t", QoS 0.
    const V3_SUBSCRIBE_T_ID37: [u8; 8] = [0x82, 0x06, 0x00, 0x25, 0x00, 0x01, b't', 0x00];
    /// v3 UNSUBSCRIBE: packet id 37, single filter "t" — non-wildcard, so the
    /// dispatch never short-circuits to the violation branch. The id is
    /// deliberately not 1: an arm that hardcoded `NonZeroU16::new(1).unwrap()`
    /// instead of propagating `*packet_id` would still satisfy an id-1 case.
    const V3_UNSUBSCRIBE_T_ID37: [u8; 7] = [0xA2, 0x05, 0x00, 0x25, 0x00, 0x01, b't'];

    fn build_runtime() -> monoio::FusionRuntime<
        monoio::time::TimeDriver<monoio::IoUringDriver>,
        monoio::time::TimeDriver<monoio::LegacyDriver>,
    > {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
    }

    async fn spawn_handler(
        connection_timeout_secs: u64,
        idle_timeout_secs: u64,
    ) -> (
        TcpStream,
        EventReceiver,
        monoio::task::JoinHandle<Result<SessionOutcome, Error>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let (tx, rx) = event_channel(0);
        let handle = monoio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            handle_client(stream, tx, connection_timeout_secs, idle_timeout_secs).await
        });
        let client = TcpStream::connect(addr).await.expect("connect");
        (client, rx, handle)
    }

    /// Identical to `spawn_handler` but pre-fills the event channel to capacity
    /// before the accept task starts, so every `EventSender::send` the handler
    /// makes takes the full-channel branch (`worker.rs:104`). The returned `rx`
    /// stays alive and unpolled — the caller MUST keep it — so the sender's
    /// `tx.is_closed()` check stays false and depth stays at capacity.
    async fn spawn_handler_full_ingest(
        connection_timeout_secs: u64,
        idle_timeout_secs: u64,
    ) -> (
        TcpStream,
        EventReceiver,
        monoio::task::JoinHandle<Result<SessionOutcome, Error>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let (tx, rx) = event_channel(0);
        for i in 0..EVENT_CHANNEL_CAPACITY {
            tx.send(Event::SensorV1 {
                temperature: i16::try_from(i % 1000).expect("fits i16"),
                pressure: 1013,
            });
        }
        let handle = monoio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            handle_client(stream, tx, connection_timeout_secs, idle_timeout_secs).await
        });
        let client = TcpStream::connect(addr).await.expect("connect");
        (client, rx, handle)
    }

    /// Test IO wrapper: delays every write (slow flush) and optionally serves a
    /// scripted payload before the first socket read, so "a packet is already
    /// buffered when the flush completes" is a precondition the test sets rather
    /// than TCP luck.
    struct TestIo<S> {
        first_read: Option<Vec<u8>>,
        /// Bytes of `first_read` already handed out.
        consumed: usize,
        write_delay: Duration,
        /// This many initial writes skip `write_delay` (e.g. a fast CONNACK
        /// before a stalled PINGRESP).
        undelayed_writes: usize,
        /// If `Some(kind)`, the first read AFTER the `first_read` prefix has
        /// been fully drained yields this io error and clears the field.
        /// Lets tests inject an arbitrary transport failure mid-handshake.
        read_error: Option<std::io::ErrorKind>,
        inner: S,
    }

    impl<S: AsyncReadRent> AsyncReadRent for TestIo<S> {
        async fn read<T: IoBufMut>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
            let pending = match self.first_read.as_ref() {
                Some(prefix) if self.consumed < prefix.len() => {
                    Some(prefix[self.consumed..].to_vec())
                }
                _ => None,
            };
            if let Some(pending) = pending {
                // Delegate to monoio's own `impl AsyncReadRent for &[u8]` — the
                // crate forbids unsafe, so no hand-filling of the IoBufMut.
                let mut slice: &[u8] = &pending;
                let (res, buf) = slice.read(buf).await;
                if let Ok(n) = res {
                    self.consumed += n;
                }
                return (res, buf);
            }
            if let Some(kind) = self.read_error.take() {
                return (Err(std::io::Error::from(kind)), buf);
            }
            self.inner.read(buf).await
        }

        async fn readv<T: IoVecBufMut>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
            self.inner.readv(buf).await
        }
    }

    impl<S: AsyncWriteRent> AsyncWriteRent for TestIo<S> {
        async fn write<T: IoBuf>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
            if self.undelayed_writes > 0 {
                self.undelayed_writes -= 1;
            } else {
                monoio::time::sleep(self.write_delay).await;
            }
            self.inner.write(buf).await
        }

        async fn writev<T: IoVecBuf>(&mut self, buf_vec: T) -> monoio::BufResult<usize, T> {
            if self.undelayed_writes > 0 {
                self.undelayed_writes -= 1;
            } else {
                monoio::time::sleep(self.write_delay).await;
            }
            self.inner.writev(buf_vec).await
        }

        async fn flush(&mut self) -> std::io::Result<()> {
            self.inner.flush().await
        }

        async fn shutdown(&mut self) -> std::io::Result<()> {
            self.inner.shutdown().await
        }
    }

    /// `spawn_handler` over the `handle_client_io` seam with a `TestIo` stream.
    async fn spawn_handler_test_io(
        connection_timeout_secs: u64,
        idle_timeout_secs: u64,
        write_delay: Duration,
        undelayed_writes: usize,
        first_read: Option<Vec<u8>>,
        read_error: Option<std::io::ErrorKind>,
    ) -> (
        TcpStream,
        EventReceiver,
        monoio::task::JoinHandle<Result<SessionOutcome, Error>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let (tx, rx) = event_channel(0);
        let handle = monoio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let peer_addr = stream.peer_addr().map_err(Error::Io)?;
            let io = TestIo {
                first_read,
                consumed: 0,
                write_delay,
                undelayed_writes,
                read_error,
                inner: stream,
            };
            handle_client_io(
                io,
                peer_addr,
                tx,
                connection_timeout_secs,
                idle_timeout_secs,
            )
            .await
        });
        let client = TcpStream::connect(addr).await.expect("connect");
        (client, rx, handle)
    }

    async fn tcp_write_all(client: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
        let mut written = 0;
        while written < data.len() {
            let buf: Vec<u8> = data[written..].to_vec();
            let (res, _) = client.write(buf).await;
            let n = res?;
            if n == 0 {
                return Err(std::io::ErrorKind::WriteZero.into());
            }
            written += n;
        }
        Ok(())
    }

    async fn tcp_read_n(client: &mut TcpStream, n: usize) -> std::io::Result<Vec<u8>> {
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            let need = n - out.len();
            let buf = vec![0u8; need];
            let (res, buf) = client.read(buf).await;
            let got = res?;
            if got == 0 {
                break;
            }
            out.extend_from_slice(&buf[..got]);
        }
        Ok(out)
    }

    async fn tcp_read_to_eof(client: &mut TcpStream) -> std::io::Result<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            let buf = vec![0u8; 4096];
            let (res, buf) = client.read(buf).await;
            let got = res?;
            if got == 0 {
                break;
            }
            out.extend_from_slice(&buf[..got]);
        }
        Ok(out)
    }

    /// Buffered v5 packet reader. Owns the read buffer so bytes that arrive
    /// coalesced with the packet being decoded survive for the next read.
    struct V5Reader {
        decoder: crate::codec::mqtt::MqttDecoder,
        buf: BytesMut,
    }

    impl V5Reader {
        fn new() -> Self {
            Self {
                decoder: crate::codec::mqtt::MqttDecoder::v5(),
                buf: BytesMut::new(),
            }
        }

        /// Next decoded packet, or `None` on EOF before a complete packet.
        async fn next(&mut self, client: &mut TcpStream) -> Option<MqttPacket> {
            loop {
                match monoio_codec::Decoder::decode(&mut self.decoder, &mut self.buf) {
                    Ok(monoio_codec::Decoded::Some((packet, _))) => return Some(packet),
                    Ok(_) => {}
                    Err(e) => panic!("v5 decode error: {e}"),
                }
                let buf = vec![0u8; 256];
                let (res, buf) = client.read(buf).await;
                let got = res.expect("read chunk");
                if got == 0 {
                    return None;
                }
                self.buf.extend_from_slice(&buf[..got]);
            }
        }

        /// Exactly `n` raw bytes — already-buffered bytes first, then the socket.
        async fn raw(&mut self, client: &mut TcpStream, n: usize) -> Vec<u8> {
            while self.buf.len() < n {
                let buf = vec![0u8; n - self.buf.len()];
                let (res, buf) = client.read(buf).await;
                let got = res.expect("read raw");
                if got == 0 {
                    break;
                }
                self.buf.extend_from_slice(&buf[..got]);
            }
            let take = n.min(self.buf.len());
            self.buf.split_to(take).to_vec()
        }
    }

    fn encode_v5_connect(keep_alive: u16, client_id: &str) -> Vec<u8> {
        encode_v5_connect_with_max_packet_size(keep_alive, client_id, None)
    }

    /// v5 CONNECT carrying the Maximum Packet Size property when one is given.
    fn encode_v5_connect_with_max_packet_size(
        keep_alive: u16,
        client_id: &str,
        max_packet_size: Option<NonZeroU32>,
    ) -> Vec<u8> {
        let mut enc = MqttEncoder::v5();
        let connect = rmqtt_codec::v5::Connect {
            client_id: client_id.to_string().into(),
            keep_alive,
            max_packet_size,
            ..Default::default()
        };
        let pkt = MqttPacket::V5(PacketV5::Connect(Box::new(connect)));
        let mut buf = BytesMut::new();
        enc.encode(pkt, &mut buf).expect("encode v5 CONNECT");
        buf.to_vec()
    }

    fn encode_v5_publish(topic: &str, payload: &[u8]) -> Vec<u8> {
        let mut enc = MqttEncoder::v5();
        let pkt = MqttPacket::V5(PacketV5::Publish(Box::new(TypesPublish {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            topic: topic.to_string().into(),
            packet_id: None,
            payload: bytes::Bytes::copy_from_slice(payload),
            properties: None,
        })));
        let mut buf = BytesMut::new();
        enc.encode(pkt, &mut buf).expect("encode v5 PUBLISH");
        buf.to_vec()
    }

    #[test]
    fn pipelined_connect_publish_pingreq_gets_connack_then_pingresp() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, mut rx, handle) = spawn_handler(2, 30).await;

            // One coalesced write: CONNECT + PUBLISH + PINGREQ.
            let mut coalesced = Vec::new();
            coalesced.extend_from_slice(&V3_CONNECT_TEST);
            coalesced.extend_from_slice(&PUBLISH_QOS0_T);
            coalesced.extend_from_slice(&PINGREQ);
            tcp_write_all(&mut client, &coalesced)
                .await
                .expect("coalesced write");

            // Read exactly 6 bytes: CONNACK (4) + PINGRESP (2).
            let got = tcp_read_n(&mut client, 6).await.expect("read 6 bytes");
            assert_eq!(
                got,
                vec![0x20, 0x02, 0x00, 0x00, 0xD0, 0x00],
                "expected flushed CONNACK then PINGRESP"
            );

            // Exactly one event delivered.
            let evt = monoio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("event timeout")
                .expect("event Some");
            match evt {
                Event::SensorV1 {
                    temperature,
                    pressure,
                } => {
                    assert_eq!(temperature, 2500);
                    assert_eq!(pressure, 1013);
                }
            }

            // Send DISCONNECT; handler should close.
            tcp_write_all(&mut client, &DISCONNECT)
                .await
                .expect("disconnect write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Ok(SessionOutcome::Served)),
                "expected Ok(SessionOutcome::Served), got {join_res:?}"
            );

            // No second event in the next 300 ms.
            let second = monoio::time::timeout(Duration::from_millis(300), rx.recv()).await;
            assert!(
                matches!(second, Ok(None)),
                "expected no second event, got {second:?}"
            );

            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            for level in ["DEBUG", "WARN", "INFO", "ERROR"] {
                assert_eq!(
                    count_lines_at(&logs, level, "timeout"),
                    0,
                    "healthy DISCONNECT-terminated session emitted a {level}-level timeout line, got: {logs}"
                );
            }
        });
    }

    /// AC-1 — a QoS 1 PUBLISH receives a PUBACK carrying its packet id,
    /// immediately after the CONNACK in the response byte stream. The client
    /// writes CONNECT and PUBLISH coalesced, the way the brief's
    /// pipelined-connect test does; the assertion pins reply order in the
    /// stream, not how the replies are split across TCP segments.
    #[test]
    fn acks_v3_qos1_publish_with_puback() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;

            // One coalesced write: CONNECT + QoS 1 PUBLISH.
            let mut coalesced = Vec::new();
            coalesced.extend_from_slice(&V3_CONNECT_TEST);
            coalesced.extend_from_slice(&PUBLISH_QOS1_T);
            tcp_write_all(&mut client, &coalesced)
                .await
                .expect("coalesced write");

            // Read exactly 8 bytes: CONNACK (4) + PUBACK (4).
            let got = tcp_read_n_bounded(&mut client, 8, Duration::from_secs(2))
                .await
                .expect("read 8 bytes");
            assert_eq!(
                got,
                vec![0x20, 0x02, 0x00, 0x00, 0x40, 0x02, 0x00, 0x01],
                "expected flushed CONNACK then PUBACK"
            );

            tcp_write_all(&mut client, &DISCONNECT)
                .await
                .expect("disconnect write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Ok(SessionOutcome::Served)),
                "expected Ok(SessionOutcome::Served), got {join_res:?}"
            );
        });
    }

    /// AC-2 — the event reaches the receiver while the PUBACK write is still
    /// stalled. `spawn_handler_test_io`'s `undelayed_writes: 1` lets the
    /// CONNACK flush immediately while every later write sleeps for a second;
    /// if `DeliverThenAck` ever sends the PUBACK before calling
    /// `deliver_publish`, the event never arrives in the 300 ms window.
    #[test]
    fn qos1_publish_event_arrives_before_puback_flush() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let mut script = Vec::new();
            script.extend_from_slice(&V3_CONNECT_TEST);
            script.extend_from_slice(&PUBLISH_QOS1_T);
            let (mut client, mut rx, handle) =
                spawn_handler_test_io(2, 30, Duration::from_secs(1), 1, Some(script), None).await;

            // The event must arrive within 300 ms even though the PUBACK
            // flush is stalled for 1 s.
            let evt = monoio::time::timeout(Duration::from_millis(300), rx.recv())
                .await
                .expect("event timeout")
                .expect("event Some");
            match evt {
                Event::SensorV1 {
                    temperature,
                    pressure,
                } => {
                    assert_eq!(temperature, 2500);
                    assert_eq!(pressure, 1013);
                }
            }

            // Drain CONNACK + PUBACK once the stalled write completes.
            let got = tcp_read_n_bounded(&mut client, 8, Duration::from_secs(2))
                .await
                .expect("read 8 bytes");
            assert_eq!(got, vec![0x20, 0x02, 0x00, 0x00, 0x40, 0x02, 0x00, 0x01]);

            // DISCONNECT so the handler leaves `run_packet_loop`.
            tcp_write_all(&mut client, &DISCONNECT)
                .await
                .expect("disconnect write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Ok(SessionOutcome::Served)),
                "expected Ok(SessionOutcome::Served), got {join_res:?}"
            );
        });
    }

    /// AC-1, AC-29 — when the event channel is full, the QoS 1 PUBACK still
    /// arrives. `spawn_handler_full_ingest` keeps `rx` alive and unpolled so
    /// every `EventSender::send` reaches the full-channel branch at
    /// `worker.rs:104`. Draining `rx` here would silently move the test off
    /// the branch it exists to cover.
    #[test]
    fn acks_qos1_publish_when_ingest_channel_is_full() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler_full_ingest(2, 30).await;

            tcp_write_all(&mut client, &V3_CONNECT_TEST)
                .await
                .expect("CONNECT write");
            let connack = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(2))
                .await
                .expect("read CONNACK");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);

            tcp_write_all(&mut client, &PUBLISH_QOS1_T)
                .await
                .expect("PUBLISH write");

            // PUBACK must arrive even though the channel was full and the
            // payload was dropped on the worker side.
            let puback = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(2))
                .await
                .expect("read PUBACK");
            assert_eq!(puback, vec![0x40, 0x02, 0x00, 0x01]);

            // Session is still alive — PINGREQ → PINGRESP round trip.
            tcp_write_all(&mut client, &PINGREQ)
                .await
                .expect("PINGREQ write");
            let pingresp = tcp_read_n_bounded(&mut client, 2, Duration::from_secs(2))
                .await
                .expect("read PINGRESP");
            assert_eq!(pingresp, vec![0xD0, 0x00]);

            tcp_write_all(&mut client, &DISCONNECT)
                .await
                .expect("disconnect write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(matches!(join_res, Ok(SessionOutcome::Served)));
        });
    }

    /// AC-29 — when the event receiver has gone away, the QoS 1 PUBACK still
    /// arrives. The closed-receiver branch (`worker.rs:97`) is the other
    /// branch the full-channel branch is paired with; both gates have to
    /// ack the publisher or the device stalls.
    #[test]
    fn acks_qos1_publish_when_ingest_receiver_is_closed() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, rx, handle) = spawn_handler(2, 30).await;
            drop(rx);

            tcp_write_all(&mut client, &V3_CONNECT_TEST)
                .await
                .expect("CONNECT write");
            let connack = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(2))
                .await
                .expect("read CONNACK");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);

            tcp_write_all(&mut client, &PUBLISH_QOS1_T)
                .await
                .expect("PUBLISH write");

            // PUBACK must arrive even though the receiver is closed.
            let puback = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(2))
                .await
                .expect("read PUBACK");
            assert_eq!(puback, vec![0x40, 0x02, 0x00, 0x01]);

            tcp_write_all(&mut client, &PINGREQ)
                .await
                .expect("PINGREQ write");
            let pingresp = tcp_read_n_bounded(&mut client, 2, Duration::from_secs(2))
                .await
                .expect("read PINGRESP");
            assert_eq!(pingresp, vec![0xD0, 0x00]);

            tcp_write_all(&mut client, &DISCONNECT)
                .await
                .expect("disconnect write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(matches!(join_res, Ok(SessionOutcome::Served)));
        });
    }

    /// AC-4 — a v3 SUBSCRIBE carrying one filter receives a spec-valid SUBACK
    /// whose packet id echoes the request (here, 37), one `Failure` (0x80) per
    /// filter, and the session keeps reading afterwards.
    #[test]
    fn refuses_v3_subscribe_with_failure_suback() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;

            // Coalesced: v3 CONNECT + v3 SUBSCRIBE (packet id 37, filter "t", QoS 0).
            let mut coalesced = Vec::new();
            coalesced.extend_from_slice(&V3_CONNECT_TEST);
            coalesced.extend_from_slice(&V3_SUBSCRIBE_T_ID37);
            tcp_write_all(&mut client, &coalesced)
                .await
                .expect("coalesced write");

            // CONNACK (4) + SUBACK (5): the id is echoed back, not hardcoded 1.
            let got = tcp_read_n_bounded(&mut client, 9, Duration::from_secs(2))
                .await
                .expect("read 9 bytes");
            assert_eq!(
                got,
                vec![0x20, 0x02, 0x00, 0x00, 0x90, 0x03, 0x00, 0x25, 0x80],
                "expected CONNACK then SUBACK carrying the request's packet id"
            );

            // Session keeps reading: PINGREQ → PINGRESP.
            tcp_write_all(&mut client, &PINGREQ)
                .await
                .expect("pingreq write");
            let pingresp = tcp_read_n_bounded(&mut client, 2, Duration::from_secs(2))
                .await
                .expect("read pingresp");
            assert_eq!(pingresp, vec![0xD0, 0x00]);

            tcp_write_all(&mut client, &DISCONNECT)
                .await
                .expect("disconnect write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(matches!(join_res, Ok(SessionOutcome::Served)));
        });
    }

    /// AC-5 — a non-wildcard UNSUBSCRIBE receives an UNSUBACK carrying its
    /// packet id (here, 37, not 1), the connection stays open, and the
    /// session still answers a PINGREQ afterwards. CONNACK bytes come first;
    /// the assertion pins reply order in the stream, not how the replies are
    /// split across TCP segments. The id is deliberately not 1: this is the
    /// only end-to-end observation of the propagated id.
    #[test]
    fn acks_v3_unsubscribe_with_unsuback() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;

            // Coalesced: v3 CONNECT + v3 UNSUBSCRIBE (packet id 37, filter "t").
            let mut coalesced = Vec::new();
            coalesced.extend_from_slice(&V3_CONNECT_TEST);
            coalesced.extend_from_slice(&V3_UNSUBSCRIBE_T_ID37);
            tcp_write_all(&mut client, &coalesced)
                .await
                .expect("coalesced write");

            // CONNACK (4) + UNSUBACK (4): the id is echoed back, not hardcoded 1.
            let got = tcp_read_n_bounded(&mut client, 8, Duration::from_secs(2))
                .await
                .expect("read 8 bytes");
            assert_eq!(
                got,
                vec![0x20, 0x02, 0x00, 0x00, 0xB0, 0x02, 0x00, 0x25],
                "expected flushed CONNACK then UNSUBACK carrying the request's packet id"
            );

            // Session keeps reading: PINGREQ → PINGRESP.
            tcp_write_all(&mut client, &PINGREQ)
                .await
                .expect("pingreq write");
            let pingresp = tcp_read_n_bounded(&mut client, 2, Duration::from_secs(2))
                .await
                .expect("read pingresp");
            assert_eq!(pingresp, vec![0xD0, 0x00]);

            tcp_write_all(&mut client, &DISCONNECT)
                .await
                .expect("disconnect write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(matches!(join_res, Ok(SessionOutcome::Served)));
        });
    }

    /// AC-5 — the v5 half of the UNSUBSCRIBE path end to end: two filters and
    /// packet id 37 come back as one UNSUBACK carrying one
    /// `NoSubscriptionExisted` (0x11) byte per filter, and the session keeps
    /// reading. The v5 renderer is the one that carries per-filter status, so
    /// the count reaching the encoder off a real socket is what this pins;
    /// `acks_v3_unsubscribe_with_unsuback` covers the version that drops it.
    /// `V5Reader` is used because the UNSUBACK may coalesce with the CONNACK.
    #[test]
    fn acks_v5_unsubscribe_with_unsuback() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;

            tcp_write_all(&mut client, &encode_v5_connect(30, "test"))
                .await
                .expect("v5 CONNECT write");

            let mut reader = V5Reader::new();
            let connack = monoio::time::timeout(Duration::from_secs(2), reader.next(&mut client))
                .await
                .expect("connack timeout");
            match connack {
                Some(MqttPacket::V5(PacketV5::ConnectAck(_))) => {}
                other => panic!("expected v5 CONNACK, got {other:?}"),
            }

            // v5 UNSUBSCRIBE, packet id 37, two filters — built directly via
            // the encoder, the same way the v5 SUBSCRIBE fixture below is.
            let mut enc = MqttEncoder::v5();
            let pkt = MqttPacket::V5(PacketV5::Unsubscribe(rmqtt_codec::v5::Unsubscribe {
                packet_id: NonZeroU16::new(37).expect("non-zero"),
                user_properties: Vec::new(),
                topic_filters: vec!["a".into(), "b/c".into()],
            }));
            let mut buf = BytesMut::new();
            enc.encode(pkt, &mut buf).expect("encode v5 UNSUBSCRIBE");
            tcp_write_all(&mut client, &buf)
                .await
                .expect("v5 UNSUBSCRIBE write");

            let unsuback =
                monoio::time::timeout(Duration::from_secs(2), reader.raw(&mut client, 7))
                    .await
                    .expect("unsuback timeout");
            assert_eq!(
                unsuback,
                vec![0xB0, 0x05, 0x00, 0x25, 0x00, 0x11, 0x11],
                "expected v5 UNSUBACK with id 37 and one 0x11 status per filter"
            );

            // Session keeps reading — PINGREQ → PINGRESP.
            tcp_write_all(&mut client, &PINGREQ)
                .await
                .expect("pingreq write");
            let pingresp =
                monoio::time::timeout(Duration::from_secs(2), reader.raw(&mut client, 2))
                    .await
                    .expect("pingresp timeout");
            assert_eq!(pingresp, vec![0xD0, 0x00]);

            tcp_write_all(&mut client, &DISCONNECT)
                .await
                .expect("disconnect write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(matches!(join_res, Ok(SessionOutcome::Served)));
        });
    }

    /// AC-26 — a v5 device asking with a wildcard SUBSCRIBE still gets its
    /// SUBACK, and the session keeps reading afterwards. The CONNACK and the
    /// SUBACK are read through `V5Reader` because the SUBACK may coalesce with
    /// the CONNACK into a single TCP segment; a fixed byte count would lose
    /// either packet when that happens.
    #[test]
    fn v5_wildcard_subscribe_is_refused_and_session_continues() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;

            tcp_write_all(&mut client, &encode_v5_connect(30, "test"))
                .await
                .expect("v5 CONNECT write");

            let mut reader = V5Reader::new();
            let connack = monoio::time::timeout(Duration::from_secs(2), reader.next(&mut client))
                .await
                .expect("connack timeout");
            match connack {
                Some(MqttPacket::V5(PacketV5::ConnectAck(_))) => {}
                other => panic!("expected v5 CONNACK, got {other:?}"),
            }

            // v5 SUBSCRIBE carrying a wildcard filter — built directly via the
            // encoder to keep the test independent of the codec path under
            // exercise elsewhere in this module.
            let mut enc = MqttEncoder::v5();
            let pkt = MqttPacket::V5(PacketV5::Subscribe(rmqtt_codec::v5::Subscribe {
                packet_id: NonZeroU16::new(1).expect("non-zero"),
                id: None,
                user_properties: Vec::new(),
                topic_filters: vec![(
                    "t/#".into(),
                    rmqtt_codec::v5::SubscriptionOptions::default(),
                )],
            }));
            let mut buf = BytesMut::new();
            enc.encode(pkt, &mut buf).expect("encode v5 SUBSCRIBE");
            tcp_write_all(&mut client, &buf)
                .await
                .expect("v5 SUBSCRIBE write");

            let suback = monoio::time::timeout(Duration::from_secs(2), reader.raw(&mut client, 6))
                .await
                .expect("suback timeout");
            assert_eq!(suback, vec![0x90, 0x04, 0x00, 0x01, 0x00, 0x83]);

            // Session keeps reading — PINGREQ → PINGRESP.
            tcp_write_all(&mut client, &PINGREQ)
                .await
                .expect("pingreq write");
            let pingresp =
                monoio::time::timeout(Duration::from_secs(2), reader.raw(&mut client, 2))
                    .await
                    .expect("pingresp timeout");
            assert_eq!(pingresp, vec![0xD0, 0x00]);

            tcp_write_all(&mut client, &DISCONNECT)
                .await
                .expect("disconnect write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(matches!(join_res, Ok(SessionOutcome::Served)));
        });
    }

    /// AC-30 — an unparseable QoS 1 payload still gets its PUBACK, because
    /// no retransmission could ever make it parse.
    #[test]
    fn acks_qos1_publish_with_unparseable_payload() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;

            let mut coalesced = Vec::new();
            coalesced.extend_from_slice(&V3_CONNECT_TEST);
            coalesced.extend_from_slice(&PUBLISH_QOS1_T_BAD_PAYLOAD);
            tcp_write_all(&mut client, &coalesced)
                .await
                .expect("coalesced write");

            let got = tcp_read_n_bounded(&mut client, 8, Duration::from_secs(2))
                .await
                .expect("read 8 bytes");
            assert_eq!(
                got,
                vec![0x20, 0x02, 0x00, 0x00, 0x40, 0x02, 0x00, 0x01],
                "expected CONNACK then PUBACK even with an unparseable payload"
            );

            tcp_write_all(&mut client, &DISCONNECT)
                .await
                .expect("disconnect write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(matches!(join_res, Ok(SessionOutcome::Served)));
        });
    }

    #[test]
    fn v5_connect_publish_pingreq_full_roundtrip() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, mut rx, handle) = spawn_handler(2, 30).await;

            // Encoder-built v5 CONNECT + v5 PUBLISH + PINGREQ.
            let mut coalesced = Vec::new();
            coalesced.extend_from_slice(&encode_v5_connect(60, "dev5"));
            coalesced.extend_from_slice(&encode_v5_publish("t", &[0x09, 0xC4, 0x03, 0xF5]));
            coalesced.extend_from_slice(&PINGREQ);
            tcp_write_all(&mut client, &coalesced)
                .await
                .expect("coalesced write");

            // Decode the reply CONNACK with the v5 decoder.
            let mut reader = V5Reader::new();
            match reader.next(&mut client).await {
                Some(MqttPacket::V5(PacketV5::ConnectAck(ack))) => {
                    assert!(matches!(ack.reason_code, V5ConnectAckReason::Success));
                    assert!(!ack.session_present);
                    assert_eq!(ack.max_qos, QoS::AtLeastOnce);
                }
                other => panic!("expected v5 CONNACK, got {other:?}"),
            }

            // PINGRESP (2 bytes).
            let pingresp = reader.raw(&mut client, 2).await;
            assert_eq!(pingresp, vec![0xD0, 0x00]);

            // Event delivered.
            let evt = monoio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("event timeout")
                .expect("event Some");
            assert!(matches!(evt, Event::SensorV1 { .. }));

            tcp_write_all(&mut client, &DISCONNECT)
                .await
                .expect("disconnect write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");
        });
    }

    #[test]
    fn closes_connection_when_keepalive_expires_at_1_5x() {
        let mut rt = build_runtime();
        let start = std::time::Instant::now();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            // CONNECT ka=1 → negotiated 1.5 s idle.
            tcp_write_all(&mut client, &V3_CONNECT_TEST_KA1)
                .await
                .expect("connect write");
            let connack = tcp_read_n(&mut client, 4).await.expect("read connack");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);
            // Stay silent; expect handler to close at ~1.5 s.
            let _ = tcp_read_to_eof(&mut client).await.expect("read to eof");
            let join_res = monoio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");
        });
        let elapsed = start.elapsed();
        assert!(
            (Duration::from_millis(1300)..=Duration::from_millis(2300)).contains(&elapsed),
            "elapsed {elapsed:?} outside 1.3s..=2.3s window (1.5x rule)"
        );
    }

    #[test]
    fn replies_identifier_rejected_to_v3_empty_id_without_clean_session() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            tcp_write_all(&mut client, &V3_CONNECT_EMPTY_NO_CLEAN)
                .await
                .expect("write");
            let got = tcp_read_n(&mut client, 4).await.expect("read 4");
            assert_eq!(got, vec![0x20, 0x02, 0x00, 0x02]);
            let _ = tcp_read_to_eof(&mut client).await.expect("eof");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");
        });
    }

    #[test]
    fn replies_client_identifier_not_valid_to_v5_empty_id_without_clean_start() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            // Build a v5 CONNECT with empty id and no clean_start via MqttEncoder::v5().
            let mut enc = MqttEncoder::v5();
            let pkt = MqttPacket::V5(PacketV5::Connect(Box::<rmqtt_codec::v5::Connect>::default()));
            let mut buf = BytesMut::new();
            enc.encode(pkt, &mut buf).expect("encode v5 CONNECT empty");
            tcp_write_all(&mut client, &buf).await.expect("write");

            let mut reader = V5Reader::new();
            match reader.next(&mut client).await {
                Some(MqttPacket::V5(PacketV5::ConnectAck(ack))) => {
                    assert!(matches!(
                        ack.reason_code,
                        V5ConnectAckReason::ClientIdentifierNotValid
                    ));
                    assert_eq!(ack.max_qos, QoS::AtLeastOnce);
                    assert!(ack.wildcard_subscription_available);
                    assert!(ack.shared_subscription_available);
                    assert!(ack.subscription_identifiers_available);
                    assert_eq!(ack.session_expiry_interval_secs, Some(0));
                    assert!(ack.retain_available);
                }
                other => panic!("expected v5 CONNACK, got {other:?}"),
            }
            let _ = tcp_read_to_eof(&mut client).await.expect("eof");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");
        });
    }

    #[test]
    fn refuses_v5_auth_method_connect_with_bad_auth_reason() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            // Build v5 CONNECT carrying auth_method "PLAIN".
            let mut enc = MqttEncoder::v5();
            let connect = rmqtt_codec::v5::Connect {
                client_id: "dev5".to_string().into(),
                auth_method: Some("PLAIN".to_string().into()),
                keep_alive: 60,
                ..Default::default()
            };
            let mut buf = BytesMut::new();
            enc.encode(
                MqttPacket::V5(PacketV5::Connect(Box::new(connect))),
                &mut buf,
            )
            .expect("encode");
            tcp_write_all(&mut client, &buf).await.expect("write");

            let mut reader = V5Reader::new();
            match reader.next(&mut client).await {
                Some(MqttPacket::V5(PacketV5::ConnectAck(ack))) => {
                    assert!(matches!(
                        ack.reason_code,
                        V5ConnectAckReason::BadAuthenticationMethod
                    ));
                }
                other => panic!("expected v5 CONNACK with BadAuthenticationMethod, got {other:?}"),
            }
            let _ = tcp_read_to_eof(&mut client).await.expect("eof");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Ok(SessionOutcome::Refused)),
                "expected Ok(SessionOutcome::Refused), got {join_res:?}"
            );
        });
    }

    /// AC-23 — decoder-level `InvalidClientId` refusal yields
    /// `SessionOutcome::Refused`. Asserts the outcome enum discriminates the
    /// refusal path (the wire CONNACK is identical to the existing
    /// `replies_identifier_rejected_to_v3_empty_id_without_clean_session`
    /// test).
    #[test]
    fn invalid_client_id_refusal_yields_refused_outcome() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            tcp_write_all(&mut client, &V3_CONNECT_EMPTY_NO_CLEAN)
                .await
                .expect("write empty-id no-clean");
            let got = tcp_read_n(&mut client, 4).await.expect("read refusal");
            assert_eq!(
                got,
                vec![0x20, 0x02, 0x00, 0x02],
                "expected refusal CONNACK"
            );
            let _ = tcp_read_to_eof(&mut client).await.expect("eof");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Ok(SessionOutcome::Refused)),
                "expected Ok(SessionOutcome::Refused), got {join_res:?}"
            );
        });
    }

    #[test]
    fn closes_without_connack_when_first_packet_is_not_connect() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            tcp_write_all(&mut client, &PINGREQ)
                .await
                .expect("write pingreq");
            let got = tcp_read_to_eof(&mut client).await.expect("read to eof");
            assert!(got.is_empty(), "expected zero response bytes, got {got:?}");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_err(), "expected Err, got Ok");
        });
    }

    pub(crate) type LogSink = std::sync::Arc<std::sync::Mutex<Vec<u8>>>;

    thread_local! {
        /// Per-test-thread capture buffer; `None` on threads not asserting logs.
        static LOG_SINK: std::cell::RefCell<Option<LogSink>> = const { std::cell::RefCell::new(None) };
    }

    /// Writer routing `tracing` output to the calling thread's capture buffer.
    #[derive(Clone, Default)]
    struct ThreadLogWriter;

    impl std::io::Write for ThreadLogWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            LOG_SINK.with(|sink| {
                if let Some(sink) = sink.borrow().as_ref() {
                    sink.lock().expect("log buffer").extend_from_slice(buf);
                }
            });
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for ThreadLogWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            Self
        }
    }

    /// Start capturing this thread's logs down to DEBUG. The subscriber is installed
    /// globally and only once: tests run in parallel and `tracing` caches callsite
    /// interest process-wide, so a thread-scoped subscriber loses that race and the
    /// callsite stays disabled. DEBUG is the capture floor — each test asserts the
    /// parsed level of the lines it cares about via `has_line_at` / `count_lines_at`,
    /// so message text can never fake a level.
    pub(crate) fn capture_logs() -> LogSink {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_ansi(false)
                .with_writer(ThreadLogWriter)
                .finish();
            tracing::subscriber::set_global_default(subscriber).expect("global subscriber");
        });
        let sink: LogSink = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        LOG_SINK.with(|slot| *slot.borrow_mut() = Some(sink.clone()));
        sink
    }

    /// True iff at least one captured line parses to `level` as its second
    /// whitespace-separated field (the tracing-subscriber full format is
    /// `<timestamp> <LEVEL> <target>: <message>`) AND contains every needle.
    /// Parsing the level field prevents message text from faking a level.
    pub(crate) fn has_line_at(logs: &str, level: &str, needles: &[&str]) -> bool {
        logs.lines().any(|l| {
            l.split_whitespace().nth(1) == Some(level) && needles.iter().all(|n| l.contains(n))
        })
    }

    /// Count captured lines that parse to `level` and contain `needle`.
    /// Parsing the level field prevents message text from faking a level.
    pub(crate) fn count_lines_at(logs: &str, level: &str, needle: &str) -> usize {
        logs.lines()
            .filter(|l| l.split_whitespace().nth(1) == Some(level) && l.contains(needle))
            .count()
    }

    #[test]
    fn drop_warns_on_first_and_every_hundredth() {
        let sink = capture_logs();
        let (tx, _rx) = event_channel(7);

        // Fill the bounded facade to capacity.
        for i in 0..crate::broker::worker::EVENT_CHANNEL_CAPACITY {
            tx.send(Event::SensorV1 {
                temperature: i16::try_from(i).expect("fits i16"),
                pressure: 0,
            });
        }
        // First overflow: drop #1 → warn.
        tx.send(Event::SensorV1 {
            temperature: -1,
            pressure: 0,
        });
        // Overflow #2..#100: only #100 should additionally warn.
        for _ in 0..99 {
            tx.send(Event::SensorV1 {
                temperature: -1,
                pressure: 0,
            });
        }
        assert_eq!(tx.dropped_total(), 100);

        // Give the tracing subscriber a moment to flush any pending writes
        // (the writer is synchronous and inline, but the sink mutex and a
        // possible scheduling handoff want one extra boundary check).
        let logs = {
            let bytes = sink.lock().expect("log buffer").clone();
            String::from_utf8(bytes).expect("utf8 logs")
        };
        assert!(
            logs.contains("worker 7") && logs.contains("1 dropped total"),
            "first-drop warn missing worker id or count, got: {logs}"
        );
        assert!(
            logs.contains("100 dropped total"),
            "every-100th warn missing, got: {logs}"
        );
        let drop_warn_lines = count_lines_at(&logs, "WARN", "dropped total");
        assert_eq!(
            drop_warn_lines, 2,
            "expected exactly two drop-warn lines, got {drop_warn_lines}: {logs}"
        );
    }

    #[test]
    fn closed_receiver_discard_is_not_counted_or_warned() {
        // Case (a): fresh channel, drop receiver, send.
        let sink_a = capture_logs();
        {
            let (tx, rx) = event_channel(11);
            drop(rx);
            tx.send(Event::SensorV1 {
                temperature: 0,
                pressure: 0,
            });
            assert_eq!(
                tx.dropped_total(),
                0,
                "closed-receiver discard must not increment the counter"
            );
        }
        let logs_a =
            String::from_utf8(sink_a.lock().expect("log buffer").clone()).expect("utf8 logs");
        assert!(
            !logs_a.contains("dropped total"),
            "closed-receiver discard must not emit a drop-warn, got: {logs_a}"
        );

        // Case (b): overflow into a still-open channel, then drop the receiver
        // and confirm further sends do not raise the counter.
        let sink_b = capture_logs();
        {
            let (tx, rx) = event_channel(11);
            for i in 0..crate::broker::worker::EVENT_CHANNEL_CAPACITY {
                tx.send(Event::SensorV1 {
                    temperature: i16::try_from(i).expect("fits i16"),
                    pressure: 0,
                });
            }
            tx.send(Event::SensorV1 {
                temperature: -1,
                pressure: 0,
            });
            tx.send(Event::SensorV1 {
                temperature: -2,
                pressure: 0,
            });
            assert_eq!(tx.dropped_total(), 2, "two overflows → counter 2");

            drop(rx);
            // Receiver gone: closure check must take precedence over fullness.
            tx.send(Event::SensorV1 {
                temperature: -3,
                pressure: 0,
            });
            assert_eq!(
                tx.dropped_total(),
                2,
                "closed-receiver discards must not add to dropped total"
            );
        }
        let logs_b =
            String::from_utf8(sink_b.lock().expect("log buffer").clone()).expect("utf8 logs");
        // Two overflows: warn at #1, next cadence boundary is #100 → exactly
        // one warn line containing "dropped total" in this channel's lifetime.
        let drop_warn_lines_b = count_lines_at(&logs_b, "WARN", "dropped total");
        assert_eq!(
            drop_warn_lines_b, 1,
            "expected exactly one drop-warn line for case (b), got {drop_warn_lines_b}: {logs_b}"
        );
    }

    #[test]
    fn warns_at_default_level_when_first_packet_is_not_connect() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            let peer = client.local_addr().expect("local_addr").to_string();
            tcp_write_all(&mut client, &PINGREQ)
                .await
                .expect("write pingreq");
            let got = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(2))
                .await
                .expect("read to eof");
            assert!(got.is_empty(), "expected zero response bytes, got {got:?}");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_err(), "expected Err, got Ok");
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert!(
                has_line_at(
                    &logs,
                    "WARN",
                    &["handshake violation: first packet was not CONNECT", &peer],
                ),
                "missing WARN-level violation log with reason and complete peer address on one line, got: {logs}"
            );
            assert!(
                !logs.contains("handshake transport error"),
                "a decoder rejection must not also be labelled a transport error, got: {logs}"
            );
            assert!(
                !logs.contains("client disconnected during handshake"),
                "a decoder rejection must not be labelled a routine disconnect, got: {logs}"
            );
        });
    }

    #[test]
    fn warns_at_default_level_when_connect_is_malformed() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            let peer = client.local_addr().expect("local_addr").to_string();
            tcp_write_all(&mut client, &V3_CONNECT_RESERVED_FLAG)
                .await
                .expect("write malformed connect");
            let got = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(2))
                .await
                .expect("read to eof");
            assert!(got.is_empty(), "expected zero response bytes, got {got:?}");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_err(), "expected Err, got Ok");
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert!(
                has_line_at(
                    &logs,
                    "WARN",
                    &["handshake violation: malformed CONNECT", &peer],
                ),
                "missing WARN-level malformed-CONNECT log with reason and complete peer address on one line, got: {logs}"
            );
            assert!(
                !logs.contains("handshake transport error"),
                "a decoder rejection must not also be labelled a transport error, got: {logs}"
            );
            assert!(
                !logs.contains("client disconnected during handshake"),
                "a decoder rejection must not be labelled a routine disconnect, got: {logs}"
            );
        });
    }

    /// Phase-1 transport anomaly: 8 bytes written (version cannot resolve until
    /// the 9th byte), client then drops (FIN). monoio-codec's `decode_eof` yields
    /// `Io(Other, "bytes remaining on stream")` — non-routine, so the arm must
    /// warn as `"handshake transport error"` and NOT as a violation.
    #[test]
    fn first_read_anomaly_warns_as_transport_error_not_violation() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            let peer = client.local_addr().expect("local_addr").to_string();
            tcp_write_all(&mut client, &V3_CONNECT_FIRST_8)
                .await
                .expect("write 8-byte prefix");
            // FIN — the version-decoder's next read sees EOF mid-packet.
            drop(client);
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::Other),
                "expected Err(Error::Io(Other)) preserving monoio-codec's decode_eof kind, got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert!(
                has_line_at(&logs, "WARN", &["handshake transport error", &peer]),
                "missing WARN-level transport-error log with complete peer address on one line, got: {logs}"
            );
            assert!(
                !logs.contains("handshake violation"),
                "transport anomaly must not be labelled a handshake violation, got: {logs}"
            );
        });
    }

    /// Phase-1 transport routine hangup: nothing written, the read sees a
    /// peer reset (ECONNRESET). Classified routine by `is_routine_disconnect`,
    /// so the arm must debug-log the disconnect and emit no info-or-higher
    /// event — regardless of message wording.
    #[test]
    fn first_read_reset_is_routine_and_not_a_violation() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (client, _rx, handle) = spawn_handler_test_io(
                2,
                30,
                Duration::ZERO,
                0,
                None,
                Some(std::io::ErrorKind::ConnectionReset),
            )
            .await;
            let peer = client.local_addr().expect("local_addr").to_string();
            drop(client);
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::ConnectionReset),
                "expected Err(Error::Io(ConnectionReset)), got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert!(
                has_line_at(&logs, "DEBUG", &["client disconnected during handshake", &peer]),
                "missing DEBUG-level routine-disconnect log with complete peer address, got: {logs}"
            );
            // No info-or-higher event regardless of wording: handler is the only
            // code running on this thread, so any warn/info/error line is a
            // regression.
            for level in ["WARN", "INFO", "ERROR"] {
                let any = logs
                    .lines()
                    .any(|l| l.split_whitespace().nth(1) == Some(level));
                assert!(
                    !any,
                    "routine handshake hangup emitted a {level}-level line, got: {logs}"
                );
            }
        });
    }

    /// Phase-3 transport routine hangup: 9-byte prefix resolves the version,
    /// then the CONNECT read sees a peer reset. Routine, so the arm must
    /// debug-log and emit no info-or-higher event.
    #[test]
    fn connect_read_reset_is_routine_and_not_malformed() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (client, _rx, handle) = spawn_handler_test_io(
                2,
                30,
                Duration::ZERO,
                0,
                Some(V3_CONNECT_FIRST_9.to_vec()),
                Some(std::io::ErrorKind::ConnectionReset),
            )
            .await;
            let peer = client.local_addr().expect("local_addr").to_string();
            drop(client);
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::ConnectionReset),
                "expected Err(Error::Io(ConnectionReset)), got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert!(
                has_line_at(&logs, "DEBUG", &["client disconnected during handshake", &peer]),
                "missing DEBUG-level routine-disconnect log with complete peer address, got: {logs}"
            );
            for level in ["WARN", "INFO", "ERROR"] {
                let any = logs
                    .lines()
                    .any(|l| l.split_whitespace().nth(1) == Some(level));
                assert!(
                    !any,
                    "routine CONNECT-read hangup emitted a {level}-level line, got: {logs}"
                );
            }
        });
    }

    /// Phase-3 timeout: 9-byte prefix resolves the version, then the CONNECT
    /// read never completes. Must debug-log the phase and peer, and emit no
    /// info-or-higher event, before returning `Err(Error::Timeout)`.
    #[test]
    fn connect_read_timeout_is_logged_at_debug_with_peer() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (client, _rx, handle) = spawn_handler_test_io(
                1,
                30,
                Duration::ZERO,
                0,
                Some(V3_CONNECT_FIRST_9.to_vec()),
                None,
            )
            .await;
            let peer = client.local_addr().expect("local_addr").to_string();
            let join_res = monoio::time::timeout(Duration::from_secs(3), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Timeout)),
                "expected Err(Error::Timeout), got {join_res:?}"
            );
            drop(client);
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "timeout"),
                1,
                "expected exactly one DEBUG timeout line, got: {logs}"
            );
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "handshake timeout during CONNECT read"),
                1,
                "expected exactly one DEBUG CONNECT-read timeout line, got: {logs}"
            );
            assert!(
                has_line_at(&logs, "DEBUG", &["handshake timeout during CONNECT read", &peer]),
                "missing DEBUG-level CONNECT-read timeout log with complete peer address, got: {logs}"
            );
            for level in ["WARN", "INFO", "ERROR"] {
                let any = logs
                    .lines()
                    .any(|l| l.split_whitespace().nth(1) == Some(level));
                assert!(
                    !any,
                    "CONNECT-read timeout emitted a {level}-level line, got: {logs}"
                );
            }
        });
    }

    /// Phase-3 transport anomaly: 9-byte prefix resolves the version, the
    /// CONNECT read sees `ConnectionAborted` (non-routine). Must warn as
    /// "handshake transport error" with the peer address — never as a
    /// violation — and preserve the kind on the returned `Error::Io`.
    #[test]
    fn connect_read_abort_warns_as_transport_error_not_malformed() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (client, _rx, handle) = spawn_handler_test_io(
                2,
                30,
                Duration::ZERO,
                0,
                Some(V3_CONNECT_FIRST_9.to_vec()),
                Some(std::io::ErrorKind::ConnectionAborted),
            )
            .await;
            let peer = client.local_addr().expect("local_addr").to_string();
            drop(client);
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::ConnectionAborted),
                "expected Err(Error::Io(ConnectionAborted)), got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert!(
                has_line_at(&logs, "WARN", &["handshake transport error", &peer]),
                "missing WARN-level transport-error log with complete peer address, got: {logs}"
            );
            assert!(
                !logs.contains("handshake violation"),
                "transport anomaly must not be labelled a handshake violation, got: {logs}"
            );
        });
    }

    /// Phase-1 routine hangup on the OTHER routine kind: `BrokenPipe` is the
    /// second `io::ErrorKind` `Error::is_routine_disconnect` accepts, so it must
    /// take the same debug path as `ConnectionReset` (AC-3, AC-9) and propagate
    /// its kind unchanged (AC-8).
    #[test]
    fn first_read_broken_pipe_is_routine_and_not_a_violation() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (client, _rx, handle) = spawn_handler_test_io(
                2,
                30,
                Duration::ZERO,
                0,
                None,
                Some(std::io::ErrorKind::BrokenPipe),
            )
            .await;
            let peer = client.local_addr().expect("local_addr").to_string();
            drop(client);
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::BrokenPipe),
                "expected Err(Error::Io(BrokenPipe)), got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert!(
                has_line_at(
                    &logs,
                    "DEBUG",
                    &["client disconnected during handshake", &peer],
                ),
                "missing DEBUG-level routine-disconnect log with complete peer address, got: {logs}"
            );
            for level in ["WARN", "INFO", "ERROR"] {
                let any = logs
                    .lines()
                    .any(|l| l.split_whitespace().nth(1) == Some(level));
                assert!(
                    !any,
                    "routine handshake hangup emitted a {level}-level line, got: {logs}"
                );
            }
        });
    }

    /// Phase-3 routine hangup on `BrokenPipe`: the 9-byte prefix resolves the
    /// version, then the CONNECT read breaks. Same debug path as the reset case
    /// (AC-6, AC-9), kind preserved (AC-8).
    #[test]
    fn connect_read_broken_pipe_is_routine_and_not_malformed() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (client, _rx, handle) = spawn_handler_test_io(
                2,
                30,
                Duration::ZERO,
                0,
                Some(V3_CONNECT_FIRST_9.to_vec()),
                Some(std::io::ErrorKind::BrokenPipe),
            )
            .await;
            let peer = client.local_addr().expect("local_addr").to_string();
            drop(client);
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::BrokenPipe),
                "expected Err(Error::Io(BrokenPipe)), got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert!(
                has_line_at(
                    &logs,
                    "DEBUG",
                    &["client disconnected during handshake", &peer],
                ),
                "missing DEBUG-level routine-disconnect log with complete peer address, got: {logs}"
            );
            for level in ["WARN", "INFO", "ERROR"] {
                let any = logs
                    .lines()
                    .any(|l| l.split_whitespace().nth(1) == Some(level));
                assert!(
                    !any,
                    "routine CONNECT-read hangup emitted a {level}-level line, got: {logs}"
                );
            }
        });
    }

    /// The Phase-1 split must key on the typed `DecodeError` source, never on
    /// `io::ErrorKind`. A transport failure carrying the SAME kind the decoder
    /// uses (`InvalidData`) but no typed source is still a transport error: an
    /// `InvalidData`-kind heuristic would mislabel it a handshake violation.
    #[test]
    fn first_read_invalid_data_transport_error_is_not_a_violation() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (client, _rx, handle) = spawn_handler_test_io(
                2,
                30,
                Duration::ZERO,
                0,
                None,
                Some(std::io::ErrorKind::InvalidData),
            )
            .await;
            let peer = client.local_addr().expect("local_addr").to_string();
            drop(client);
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::InvalidData),
                "expected Err(Error::Io(InvalidData)), got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert!(
                has_line_at(&logs, "WARN", &["handshake transport error", &peer]),
                "missing WARN-level transport-error log with complete peer address, got: {logs}"
            );
            assert!(
                !logs.contains("handshake violation"),
                "a sourceless InvalidData transport error must not be labelled a handshake violation, got: {logs}"
            );
        });
    }

    /// Same typed-source proof at the Phase-3 arm: `InvalidData` without a
    /// `DecodeError` source is a transport error, not a malformed CONNECT.
    #[test]
    fn connect_read_invalid_data_transport_error_is_not_malformed() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (client, _rx, handle) = spawn_handler_test_io(
                2,
                30,
                Duration::ZERO,
                0,
                Some(V3_CONNECT_FIRST_9.to_vec()),
                Some(std::io::ErrorKind::InvalidData),
            )
            .await;
            let peer = client.local_addr().expect("local_addr").to_string();
            drop(client);
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::InvalidData),
                "expected Err(Error::Io(InvalidData)), got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert!(
                has_line_at(&logs, "WARN", &["handshake transport error", &peer]),
                "missing WARN-level transport-error log with complete peer address, got: {logs}"
            );
            assert!(
                !logs.contains("handshake violation"),
                "a sourceless InvalidData transport error must not be labelled a handshake violation, got: {logs}"
            );
        });
    }

    /// The second decoder-rejection variant reaching the Phase-1 arm on the real
    /// wire: protocol name "MQTX" → `DecodeError::InvalidProtocol`. It is a
    /// decoder rejection, so it keeps the (frozen, Q4) violation warn and must
    /// NOT be routed through the transport helper.
    #[test]
    fn bad_protocol_name_warns_as_violation_not_transport_error() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            let peer = client.local_addr().expect("local_addr").to_string();
            tcp_write_all(&mut client, &V3_CONNECT_BAD_PROTOCOL_NAME)
                .await
                .expect("write bad protocol name");
            let got = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(2))
                .await
                .expect("read to eof");
            assert!(got.is_empty(), "expected zero response bytes, got {got:?}");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::InvalidData),
                "expected Err(Error::Io(InvalidData)) from the decoder rejection, got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert!(
                has_line_at(
                    &logs,
                    "WARN",
                    &["handshake violation: first packet was not CONNECT", &peer],
                ),
                "missing WARN-level violation log with reason and complete peer address, got: {logs}"
            );
            assert!(
                !logs.contains("handshake transport error"),
                "a decoder rejection must not be labelled a transport error, got: {logs}"
            );
            assert!(
                !logs.contains("client disconnected during handshake"),
                "a decoder rejection must not be labelled a routine disconnect, got: {logs}"
            );
        });
    }

    #[test]
    fn closes_connection_on_duplicate_connect() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            tcp_write_all(&mut client, &V3_CONNECT_TEST)
                .await
                .expect("connect write");
            let connack = tcp_read_n(&mut client, 4).await.expect("read connack");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);
            // Send the same CONNECT again.
            tcp_write_all(&mut client, &V3_CONNECT_TEST)
                .await
                .expect("dup write");
            let _ = tcp_read_to_eof(&mut client).await.expect("eof");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");
        });
    }

    /// AC-16 — a v3 duplicate CONNECT closes with `SessionOutcome::Violation`.
    #[test]
    fn duplicate_connect_closes_as_violation_outcome() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            tcp_write_all(&mut client, &V3_CONNECT_TEST)
                .await
                .expect("first CONNECT write");
            let connack = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(2))
                .await
                .expect("read CONNACK");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);
            tcp_write_all(&mut client, &V3_CONNECT_TEST)
                .await
                .expect("second CONNECT write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            match join_res {
                Ok(super::SessionOutcome::Violation) => {}
                other => panic!("expected SessionOutcome::Violation, got {other:?}"),
            }
        });
    }

    /// AC-13 — a v3 duplicate CONNECT emits exactly one WARN line whose reason
    /// clause is "protocol violation: duplicate CONNECT" and carries the peer
    /// address. Asserted via `has_line_at` / `count_lines_at` (not raw
    /// `contains("WARN")`), per the epic's log-test convention.
    #[test]
    fn duplicate_connect_warn_is_labelled_protocol_violation() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            // Client's local socket address is the same port the broker
            // observes as its peer, so we assert on the client side and the
            // handler's warn line matches.
            let peer = client.local_addr().expect("local_addr").to_string();
            tcp_write_all(&mut client, &V3_CONNECT_TEST)
                .await
                .expect("first CONNECT write");
            let connack = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(2))
                .await
                .expect("read CONNACK");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);
            tcp_write_all(&mut client, &V3_CONNECT_TEST)
                .await
                .expect("second CONNECT write");
            let _ = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            let logs =
                String::from_utf8(sink.lock().expect("log lock").clone()).expect("log UTF-8");
            assert!(
                has_line_at(
                    &logs,
                    "WARN",
                    &["protocol violation: duplicate CONNECT", &peer]
                ),
                "expected one WARN line with the violation reason and peer {peer}; logs:\n{logs}"
            );
            assert_eq!(
                count_lines_at(&logs, "WARN", "protocol violation"),
                1,
                "expected exactly one WARN line mentioning 'protocol violation'"
            );
        });
    }

    /// AC-17 — a v3 PUBLISH whose QoS bits are 3 mid-session closes with
    /// `SessionOutcome::Violation`. The QoS-3 fixture is rejected by the
    /// decoder as `DecodeError::MalformedPacket`; the handler must surface
    /// that as a violation rather than the inherited `Ok(Served)` quiet path.
    #[test]
    fn qos3_publish_mid_session_closes_as_violation() {
        // v3 PUBLISH QoS=3: first byte 0x36, topic "t" (len 1), packet id 1,
        // payload SensorV1 25.00°C / 1013 hPa. Same wire shape as
        // `PUBLISH_QOS1_T` (0x32...) with QoS bits set to the reserved value.
        const V3_PUBLISH_QOS3_T: [u8; 11] = [
            0x36, 0x09, 0x00, 0x01, b't', 0x00, 0x01, 0x09, 0xC4, 0x03, 0xF5,
        ];
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            let peer = client.local_addr().expect("local_addr").to_string();
            tcp_write_all(&mut client, &V3_CONNECT_TEST)
                .await
                .expect("CONNECT write");
            let connack = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(2))
                .await
                .expect("read CONNACK");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);
            tcp_write_all(&mut client, &V3_PUBLISH_QOS3_T)
                .await
                .expect("QoS-3 PUBLISH write");
            // v3 has no DISCONNECT on malformed; server-side close should mean
            // EOF after CONNACK. Bound the read so a stuck server does not
            // hang the test.
            let post = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(2))
                .await
                .expect("read to eof (bounded)");
            assert!(
                post.is_empty(),
                "v3 close on malformed must not emit extra bytes, got {post:?}"
            );
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            match join_res {
                Ok(super::SessionOutcome::Violation) => {}
                other => panic!("expected SessionOutcome::Violation, got {other:?}"),
            }
            let logs =
                String::from_utf8(sink.lock().expect("log lock").clone()).expect("log UTF-8");
            assert!(
                has_line_at(
                    &logs,
                    "WARN",
                    &["protocol violation: malformed packet", &peer]
                ),
                "expected one WARN line with the malformed-packet reason and peer {peer}; logs:\n{logs}"
            );
        });
    }

    /// AC-14 — a v5 duplicate CONNECT receives the ProtocolError DISCONNECT
    /// before the handler closes. `V5Reader` is wrapped in `monoio::time::timeout`
    /// per the brief because it has no internal deadline.
    #[test]
    fn v5_duplicate_connect_receives_disconnect_before_close() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            tcp_write_all(&mut client, &encode_v5_connect(30, "test"))
                .await
                .expect("first v5 CONNECT write");
            let mut reader = V5Reader::new();
            let connack = monoio::time::timeout(Duration::from_secs(2), reader.next(&mut client))
                .await
                .expect("v5 CONNACK read timeout")
                .expect("v5 CONNACK decoded");
            let MqttPacket::V5(PacketV5::ConnectAck(_)) = connack else {
                panic!("expected v5 CONNACK, got {connack:?}")
            };
            tcp_write_all(&mut client, &encode_v5_connect(30, "test"))
                .await
                .expect("second v5 CONNECT write");
            let disconnect_bytes =
                monoio::time::timeout(Duration::from_secs(2), reader.raw(&mut client, 4))
                    .await
                    .expect("v5 DISCONNECT read timeout");
            assert_eq!(
                disconnect_bytes,
                vec![0xE0, 0x02, 0x82, 0x00],
                "expected v5 DISCONNECT carrying ProtocolError (0x82)"
            );
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            match join_res {
                Ok(super::SessionOutcome::Violation) => {}
                other => panic!("expected SessionOutcome::Violation, got {other:?}"),
            }
        });
    }

    /// AC-14 — a v5 mid-session decoder rejection receives the `MalformedPacket`
    /// (0x81) DISCONNECT before close. The v3 test above proves the close
    /// but no DISCONNECT bytes, so without this test a `close_with_violation`
    /// that sent nothing on v5 would pass every other check.
    #[test]
    fn v5_malformed_packet_mid_session_disconnects_with_81() {
        // Build a real v5 PUBLISH, then mutate the fixed-header QoS bits to
        // 3 (reserved) so the v5 decoder rejects it as `MalformedPacket`.
        // The QoS bits are rejected before any further bytes are parsed, so
        // the rest of the buffer is irrelevant.
        let mut publish = encode_v5_publish("t", &[0x09, 0xC4, 0x03, 0xF5]);
        publish[0] = 0x36;
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            tcp_write_all(&mut client, &encode_v5_connect(30, "test"))
                .await
                .expect("v5 CONNECT write");
            let mut reader = V5Reader::new();
            let connack = monoio::time::timeout(Duration::from_secs(2), reader.next(&mut client))
                .await
                .expect("v5 CONNACK read timeout")
                .expect("v5 CONNACK decoded");
            let MqttPacket::V5(PacketV5::ConnectAck(_)) = connack else {
                panic!("expected v5 CONNACK, got {connack:?}")
            };
            tcp_write_all(&mut client, &publish)
                .await
                .expect("malformed v5 PUBLISH write");
            let disconnect_bytes =
                monoio::time::timeout(Duration::from_secs(2), reader.raw(&mut client, 4))
                    .await
                    .expect("v5 DISCONNECT read timeout");
            assert_eq!(
                disconnect_bytes,
                vec![0xE0, 0x02, 0x81, 0x00],
                "expected v5 DISCONNECT carrying MalformedPacket (0x81)"
            );
            // The DISCONNECT is the last thing written: the socket reaches EOF
            // with no trailing bytes. A zero bound here would time out before
            // the close and assert nothing.
            let rest = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(2))
                .await
                .expect("EOF after the v5 DISCONNECT");
            assert!(
                rest.is_empty(),
                "expected EOF after the DISCONNECT, got {rest:02X?}"
            );
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            match join_res {
                Ok(super::SessionOutcome::Violation) => {}
                other => panic!("expected SessionOutcome::Violation, got {other:?}"),
            }
        });
    }

    /// AC-18 — a mid-session transport error must keep the inherited quiet
    /// `Served` path. The injected `InvalidData` has no `DecodeError` source,
    /// so `classify_read_error` returns `None` and the `debug` + `Served`
    /// arm survives. `ConnectionReset` would not exercise this branch — see
    /// the brief note on the discriminator.
    #[test]
    fn mid_session_transport_error_stays_served() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler_test_io(
                2,
                30,
                Duration::ZERO,
                0,
                Some(V3_CONNECT_TEST.to_vec()),
                Some(std::io::ErrorKind::InvalidData),
            )
            .await;
            let peer = client.local_addr().expect("local_addr").to_string();
            // Read the CONNACK so the handshake completes, then drop the
            // client so the next read on the server side yields the injected
            // transport error — the mid-session branch.
            let connack = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(2))
                .await
                .expect("read CONNACK");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);
            drop(client);
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Ok(super::SessionOutcome::Served)),
                "mid-session InvalidData (no DecodeError source) must stay Served, got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert_eq!(
                count_lines_at(&logs, "WARN", "protocol violation"),
                0,
                "a transport failure must not emit a WARN line; logs:\n{logs}"
            );
            // Sanity: the WARN-level transport line that the handshake path
            // emits is not present here either — the packet loop has no such
            // line, the WARN silence is the property AC-18 asks for.
            let _ = peer;
        });
    }

    /// AC-7, AC-13 — a v5 client that sends a PINGRESP (a server-only packet)
    /// receives a v5 DISCONNECT carrying `ProtocolError` (0x82) and the
    /// handler reports `SessionOutcome::Violation`, with exactly one WARN
    /// line carrying the violation reason.
    #[test]
    fn v5_pingresp_from_client_closes_with_disconnect() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            tcp_write_all(&mut client, &encode_v5_connect(30, "test"))
                .await
                .expect("v5 CONNECT write");

            let mut reader = V5Reader::new();
            let connack = monoio::time::timeout(Duration::from_secs(2), reader.next(&mut client))
                .await
                .expect("v5 CONNACK read timeout")
                .expect("v5 CONNACK decoded");
            let MqttPacket::V5(PacketV5::ConnectAck(_)) = connack else {
                panic!("expected v5 CONNACK, got {connack:?}")
            };

            // PINGRESP from a client: server-only packet, must close as a violation.
            tcp_write_all(&mut client, &[0xD0, 0x00])
                .await
                .expect("client PINGRESP write");

            let disconnect_bytes =
                monoio::time::timeout(Duration::from_secs(2), reader.raw(&mut client, 4))
                    .await
                    .expect("v5 DISCONNECT read timeout");
            assert_eq!(
                disconnect_bytes,
                vec![0xE0, 0x02, 0x82, 0x00],
                "expected v5 DISCONNECT carrying ProtocolError (0x82)"
            );

            // EOF on the socket after the DISCONNECT flush, with no trailing
            // bytes. A zero bound here would time out before the close and
            // assert nothing.
            let rest = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(2))
                .await
                .expect("EOF after the v5 DISCONNECT");
            assert!(
                rest.is_empty(),
                "expected EOF after the DISCONNECT, got {rest:02X?}"
            );

            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            match join_res {
                Ok(super::SessionOutcome::Violation) => {}
                other => panic!("expected SessionOutcome::Violation, got {other:?}"),
            }

            let logs =
                String::from_utf8(sink.lock().expect("log lock").clone()).expect("utf8 logs");
            assert_eq!(
                count_lines_at(&logs, "WARN", "protocol violation"),
                1,
                "exactly one WARN protocol-violation line expected; logs were:\n{logs}"
            );
        });
    }

    /// A SUBACK the client's declared Maximum Packet Size cannot hold is never
    /// written: 200 filters render a 206-byte refusal, so a client that
    /// declared 128 gets the `PacketTooLarge` (0x95) DISCONNECT and a
    /// `Violation` outcome instead of an oversized packet.
    #[test]
    fn v5_suback_over_client_max_packet_size_closes_with_packet_too_large() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            let connect = encode_v5_connect_with_max_packet_size(
                30,
                "test",
                Some(NonZeroU32::new(128).expect("non-zero")),
            );
            tcp_write_all(&mut client, &connect)
                .await
                .expect("v5 CONNECT write");

            let mut reader = V5Reader::new();
            let connack = monoio::time::timeout(Duration::from_secs(2), reader.next(&mut client))
                .await
                .expect("v5 CONNACK read timeout")
                .expect("v5 CONNACK decoded");
            let MqttPacket::V5(PacketV5::ConnectAck(_)) = connack else {
                panic!("expected v5 CONNACK, got {connack:?}")
            };

            let mut enc = MqttEncoder::v5();
            let pkt = MqttPacket::V5(PacketV5::Subscribe(rmqtt_codec::v5::Subscribe {
                packet_id: NonZeroU16::new(1).expect("non-zero"),
                id: None,
                user_properties: Vec::new(),
                topic_filters: (0..200)
                    .map(|i| {
                        (
                            format!("t/{i}").into(),
                            rmqtt_codec::v5::SubscriptionOptions::default(),
                        )
                    })
                    .collect(),
            }));
            let mut buf = BytesMut::new();
            enc.encode(pkt, &mut buf).expect("encode v5 SUBSCRIBE");
            tcp_write_all(&mut client, &buf)
                .await
                .expect("v5 SUBSCRIBE write");

            let disconnect_bytes =
                monoio::time::timeout(Duration::from_secs(2), reader.raw(&mut client, 4))
                    .await
                    .expect("v5 DISCONNECT read timeout");
            assert_eq!(
                disconnect_bytes,
                vec![0xE0, 0x02, 0x95, 0x00],
                "expected v5 DISCONNECT carrying PacketTooLarge (0x95), not a 206-byte SUBACK"
            );
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            match join_res {
                Ok(super::SessionOutcome::Violation) => {}
                other => panic!("expected SessionOutcome::Violation, got {other:?}"),
            }
        });
    }

    /// The oversize close obeys the limit it enforces. A client declaring a
    /// Maximum Packet Size of 3 cannot receive the 4-byte `PacketTooLarge`
    /// DISCONNECT either, so its one-filter SUBSCRIBE closes the connection
    /// silently: zero bytes after the CONNACK, still `SessionOutcome::Violation`.
    #[test]
    fn v5_oversize_close_under_four_byte_limit_sends_no_disconnect() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            let connect = encode_v5_connect_with_max_packet_size(
                30,
                "test",
                Some(NonZeroU32::new(3).expect("non-zero")),
            );
            tcp_write_all(&mut client, &connect)
                .await
                .expect("v5 CONNECT write");

            let mut reader = V5Reader::new();
            let connack = monoio::time::timeout(Duration::from_secs(2), reader.next(&mut client))
                .await
                .expect("v5 CONNACK read timeout")
                .expect("v5 CONNACK decoded");
            let MqttPacket::V5(PacketV5::ConnectAck(_)) = connack else {
                panic!("expected v5 CONNACK, got {connack:?}")
            };

            let mut enc = MqttEncoder::v5();
            let pkt = MqttPacket::V5(PacketV5::Subscribe(rmqtt_codec::v5::Subscribe {
                packet_id: NonZeroU16::new(1).expect("non-zero"),
                id: None,
                user_properties: Vec::new(),
                topic_filters: vec![(
                    "t/0".into(),
                    rmqtt_codec::v5::SubscriptionOptions::default(),
                )],
            }));
            let mut buf = BytesMut::new();
            enc.encode(pkt, &mut buf).expect("encode v5 SUBSCRIBE");
            tcp_write_all(&mut client, &buf)
                .await
                .expect("v5 SUBSCRIBE write");

            let got = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(2))
                .await
                .expect("read to eof");
            assert!(
                got.is_empty(),
                "expected no packet after the CONNACK — neither the 6-byte SUBACK nor the \
                 4-byte DISCONNECT fits a 3-byte limit — got {got:?}"
            );
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            match join_res {
                Ok(super::SessionOutcome::Violation) => {}
                other => panic!("expected SessionOutcome::Violation, got {other:?}"),
            }
        });
    }

    /// v3 CONNECT: ka=2, id "test", flags 0x00 — for AC-3 keep-alive hardening.
    const V3_CONNECT_TEST_KA2: [u8; 18] = [
        0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x00, 0x00, 0x02, 0x00, 0x04, b't',
        b'e', b's', b't',
    ];

    /// First 8 bytes of the v3 CONNECT fixture — version cannot resolve until
    /// the 9th byte (protocol level) is also present.
    const V3_CONNECT_FIRST_8: [u8; 8] = [0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T'];

    /// The 9th byte of the v3 CONNECT fixture — the MQTT 3.1.1 protocol level.
    const V3_CONNECT_LEVEL_BYTE: [u8; 1] = [0x04];

    /// First 9 bytes of the v3 CONNECT fixture — version known, CONNECT still
    /// incomplete (the level byte triggers `VersionDecoder::decode`, but the
    /// remaining-length payload is missing for the CONNECT codec).
    const V3_CONNECT_FIRST_9: [u8; 9] = [0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04];

    /// Last 9 bytes of the v3 CONNECT fixture — completes the CONNECT payload.
    const V3_CONNECT_LAST_9: [u8; 9] = [0x00, 0x00, 0x3C, 0x00, 0x04, b't', b'e', b's', b't'];

    /// Last 9 bytes of the ka=1 v3 CONNECT fixture — `V3_CONNECT_FIRST_9` is the
    /// shared prefix, so the two together are `V3_CONNECT_TEST_KA1`.
    const V3_CONNECT_KA1_LAST_9: [u8; 9] = [0x00, 0x00, 0x01, 0x00, 0x04, b't', b'e', b's', b't'];

    /// Bound a `read` so a stuck server cannot hang the test. On expiry
    /// returns `Ok(empty Vec)` so callers asserting "no premature bytes
    /// within bound" interpret the elapse as success, not as an error.
    async fn tcp_read_n_bounded(
        client: &mut TcpStream,
        n: usize,
        bound: Duration,
    ) -> std::io::Result<Vec<u8>> {
        match monoio::time::timeout(bound, tcp_read_n(client, n)).await {
            Ok(r) => r,
            Err(_) => Ok(Vec::new()),
        }
    }

    /// Bound `tcp_read_to_eof` for the same reason.
    async fn tcp_read_to_eof_bounded(
        client: &mut TcpStream,
        bound: Duration,
    ) -> std::io::Result<Vec<u8>> {
        match monoio::time::timeout(bound, tcp_read_to_eof(client)).await {
            Ok(r) => r,
            Err(_) => Err(std::io::ErrorKind::TimedOut.into()),
        }
    }

    #[test]
    fn handshake_budget_spans_version_detect_and_connect_read() {
        let mut rt = build_runtime();
        let start = std::time::Instant::now();
        rt.block_on(async {
            let (mut client, _rx, _handle) = spawn_handler(3, 30).await;

            // t=0: write the first 8 bytes — version decoder cannot resolve yet.
            tcp_write_all(&mut client, &V3_CONNECT_FIRST_8)
                .await
                .expect("write first 8 bytes");

            // Drive the scenario: write the level byte after a 2s wait so the
            // budget, if shared, has only ~1s left for the CONNECT read.
            monoio::time::sleep(Duration::from_secs(2)).await;
            // Sanity: 2s actually elapsed — a no-op sleep would make the bound
            // unsatisfiable, so assert the wait took before continuing.
            assert!(
                start.elapsed() >= Duration::from_secs(2),
                "2s sleep did not elapse; elapsed {:?}",
                start.elapsed()
            );
            tcp_write_all(&mut client, &V3_CONNECT_LEVEL_BYTE)
                .await
                .expect("write level byte");

            // Silence: handshake must be abandoned when the shared budget runs
            // out. Bound the read so a stuck server doesn't hang the test.
            let got = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(6))
                .await
                .expect("read to eof (bounded)");
            assert!(
                got.is_empty(),
                "expected zero response bytes after shared budget expiry, got {got:?}"
            );
        });
        let elapsed = start.elapsed();
        assert!(
            (Duration::from_millis(2700)..=Duration::from_millis(4200)).contains(&elapsed),
            "elapsed {elapsed:?} outside 2.7s..=4.2s window (shared handshake budget)"
        );
    }

    #[test]
    fn accepts_connect_fragmented_across_two_writes() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;

            // 9-byte prefix: version resolves, CONNECT payload still missing.
            tcp_write_all(&mut client, &V3_CONNECT_FIRST_9)
                .await
                .expect("write 9-byte prefix");

            // Read must elapse (no premature CONNACK) before the rest arrives.
            let empty = tcp_read_n_bounded(&mut client, 1, Duration::from_millis(300))
                .await
                .expect("read after prefix");
            assert!(
                empty.is_empty(),
                "expected no premature CONNACK, got {empty:?}"
            );

            // Send the remaining 9 bytes.
            tcp_write_all(&mut client, &V3_CONNECT_LAST_9)
                .await
                .expect("write remaining 9 bytes");

            // Expect the v3 CONNACK.
            let connack = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(2))
                .await
                .expect("read connack");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);

            // Clean up so the handler doesn't sit idle for ~3 minutes.
            tcp_write_all(&mut client, &DISCONNECT)
                .await
                .expect("disconnect write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");
        });
    }

    #[test]
    fn huge_connection_timeout_does_not_panic() {
        let mut rt = build_runtime();
        let start = std::time::Instant::now();
        rt.block_on(async {
            // u64::MAX must be clamped internally — no panic converting to a
            // Duration or summing onto an Instant.
            let (mut client, _rx, handle) = spawn_handler(u64::MAX, 30).await;

            tcp_write_all(&mut client, &V3_CONNECT_FIRST_9)
                .await
                .expect("write 9-byte prefix");

            // Wait so the pending CONNECT read polls the timer at the
            // (previously) overflowing Instant sum.
            monoio::time::sleep(Duration::from_millis(500)).await;
            assert!(
                start.elapsed() >= Duration::from_millis(500),
                "500ms sleep did not elapse; elapsed {:?}",
                start.elapsed()
            );

            tcp_write_all(&mut client, &V3_CONNECT_LAST_9)
                .await
                .expect("write remaining 9 bytes");

            let connack = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(2))
                .await
                .expect("read connack");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);

            tcp_write_all(&mut client, &DISCONNECT)
                .await
                .expect("disconnect write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");
        });
    }

    #[test]
    fn closes_at_1_5x_of_two_second_keepalive() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        let start = std::time::Instant::now();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            let peer = client.local_addr().expect("local_addr").to_string();
            // CONNECT ka=2 → negotiated idle deadline = 2 × 1.5 = 3s.
            tcp_write_all(&mut client, &V3_CONNECT_TEST_KA2)
                .await
                .expect("connect write");
            let connack = tcp_read_n(&mut client, 4).await.expect("read connack");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);
            // Stay silent; expect handler to close at ~3s.
            let _ = tcp_read_to_eof(&mut client).await.expect("read to eof");
            let join_res = monoio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "timeout"),
                1,
                "expected exactly one DEBUG timeout line, got: {logs}"
            );
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "client idle timeout"),
                1,
                "expected exactly one DEBUG client-idle-timeout line, got: {logs}"
            );
            assert!(
                has_line_at(&logs, "DEBUG", &["client idle timeout", &peer]),
                "missing DEBUG-level client-idle-timeout log with complete peer address, got: {logs}"
            );
            for level in ["WARN", "INFO", "ERROR"] {
                let any = logs
                    .lines()
                    .any(|l| l.split_whitespace().nth(1) == Some(level));
                assert!(
                    !any,
                    "idle-deadline expiry emitted a {level}-level line, got: {logs}"
                );
            }
        });
        let elapsed = start.elapsed();
        // Disjoint from the Task-2 ka=1 window (1.3s..=2.3s): a single hardcoded
        // constant cannot satisfy both ranges.
        assert!(
            (Duration::from_millis(2600)..=Duration::from_millis(4600)).contains(&elapsed),
            "elapsed {elapsed:?} outside 2.6s..=4.6s window (1.5x of ka=2 rule)"
        );
    }

    #[test]
    fn packet_activity_resets_idle_deadline() {
        let mut rt = build_runtime();
        let start = std::time::Instant::now();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            // CONNECT ka=1 → idle deadline = 1.5s.
            tcp_write_all(&mut client, &V3_CONNECT_TEST_KA1)
                .await
                .expect("connect write");
            let connack = tcp_read_n(&mut client, 4).await.expect("read connack");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);
            let connack_at = std::time::Instant::now();

            // ~0.8s later (inside the 1.5s deadline): send PINGREQ, read PINGRESP.
            monoio::time::sleep(Duration::from_millis(800)).await;
            tcp_write_all(&mut client, &PINGREQ)
                .await
                .expect("pingreq write");
            let pingresp = tcp_read_n(&mut client, 2).await.expect("read pingresp");
            assert_eq!(pingresp, vec![0xD0, 0x00]);

            // Now stay silent; the idle deadline must be measured from the
            // PINGREQ, not the CONNACK. Expected close ~ 0.8s + 1.5s ≈ 2.3s
            // from CONNACK; an unreset timer would close at ~1.5s.
            let _ = tcp_read_to_eof(&mut client).await.expect("read to eof");
            let join_res = monoio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");
            let from_connack = connack_at.elapsed();
            assert!(
                (Duration::from_millis(2000)..=Duration::from_millis(3500)).contains(&from_connack),
                "elapsed from CONNACK {from_connack:?} outside 2.0s..=3.5s window"
            );
        });
        let _ = start.elapsed();
    }

    #[test]
    fn v5_capped_keepalive_enforced_at_announced_deadline() {
        let mut rt = build_runtime();
        let start = std::time::Instant::now();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 2).await;
            // Encoder-built v5 CONNECT ka=60, idle_timeout_secs=2 → capped.
            tcp_write_all(&mut client, &encode_v5_connect(60, "dev5"))
                .await
                .expect("v5 connect write");

            // Decode CONNACK and confirm the announced deadline.
            let mut reader = V5Reader::new();
            match reader.next(&mut client).await {
                Some(MqttPacket::V5(PacketV5::ConnectAck(ack))) => {
                    assert_eq!(ack.server_keepalive_sec, Some(1));
                }
                other => panic!("expected v5 CONNACK, got {other:?}"),
            }

            // Stay silent; close must land at 1 × 1.5 = 1.5s (deadline anchored
            // to the announced keep-alive, not the raw 60s or the 2s config).
            let _ = tcp_read_to_eof(&mut client).await.expect("read to eof");
            let join_res = monoio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");
        });
        let elapsed = start.elapsed();
        // Excludes both 90s (uncapped) and 2.0s (raw config): the deadline is
        // 1.5 × announced = 1.5 × 1 = 1.5s.
        assert!(
            (Duration::from_millis(1350)..=Duration::from_millis(1850)).contains(&elapsed),
            "elapsed {elapsed:?} outside 1.35s..=1.85s window (1.5x of announced ka=1)"
        );
    }

    #[test]
    fn closes_silent_client_when_handshake_budget_expires() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        let start = std::time::Instant::now();
        rt.block_on(async {
            // connection_timeout_secs=1; the client connects and never speaks,
            // so the budget must expire during version detection.
            let (mut client, _rx, handle) = spawn_handler(1, 30).await;
            let peer = client.local_addr().expect("local_addr").to_string();

            let got = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(5))
                .await
                .expect("read to eof (bounded)");
            assert!(got.is_empty(), "expected zero response bytes, got {got:?}");

            let join_res = monoio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Timeout)),
                "expected Err(Error::Timeout), got {join_res:?}"
            );

            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "timeout"),
                1,
                "expected exactly one DEBUG timeout line, got: {logs}"
            );
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "handshake timeout during version detect"),
                1,
                "expected exactly one DEBUG version-detect timeout line, got: {logs}"
            );
            assert!(
                has_line_at(&logs, "DEBUG", &["handshake timeout during version detect", &peer]),
                "missing DEBUG-level version-detect timeout log with complete peer address, got: {logs}"
            );
            for level in ["WARN", "INFO", "ERROR"] {
                let any = logs
                    .lines()
                    .any(|l| l.split_whitespace().nth(1) == Some(level));
                assert!(
                    !any,
                    "handshake budget expiry emitted a {level}-level line, got: {logs}"
                );
            }
        });
        let elapsed = start.elapsed();
        assert!(
            (Duration::from_millis(700)..=Duration::from_millis(2500)).contains(&elapsed),
            "elapsed {elapsed:?} outside 0.7s..=2.5s window (1s handshake budget)"
        );
    }

    #[test]
    fn closes_when_client_disconnects_mid_handshake() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (client, _rx, handle) = spawn_handler(5, 30).await;
            // Hang up before sending anything — the version read sees EOF, and
            // the handler must fail fast rather than wait out the 5s budget.
            drop(client);
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::ClientClosed)),
                "expected Err(Error::ClientClosed), got {join_res:?}"
            );
        });
    }

    #[test]
    fn truncated_connect_close_is_io_error_not_client_closed() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(5, 30).await;
            // First 14 of V3_CONNECT_TEST's 18 bytes: past version detection
            // (byte 8 = level byte), mid-CONNECT (waiting on client_id bytes
            // 14..18). EOF arrives with partial CONNECT buffered, so
            // monoio-codec's `decode_eof` yields Io(Other, ...) — not the
            // clean-EOF ClientClosed variant.
            let prefix: [u8; 14] = V3_CONNECT_TEST[..14]
                .try_into()
                .expect("V3_CONNECT_TEST prefix fits [u8; 14]");
            tcp_write_all(&mut client, &prefix)
                .await
                .expect("write truncated connect");
            drop(client);
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Io(_))),
                "expected Err(Error::Io(_)), got {join_res:?}"
            );
        });
    }

    #[test]
    fn closes_blocked_writer_when_idle_deadline_passes() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            // Scripted CONNECT (ka=1 → idle 1.5s) plus one PINGREQ; the CONNACK
            // write is undelayed, then the PINGRESP write stalls for 10s — a
            // blocked writer as a precondition, not TCP-buffer luck (the old
            // flood variant went green or hung with the runner's buffer sizes).
            let mut prefix = V3_CONNECT_TEST_KA1.to_vec();
            prefix.extend_from_slice(&PINGREQ);
            let (client, _rx, handle) =
                spawn_handler_test_io(5, 30, Duration::from_secs(10), 1, Some(prefix), None).await;
            let peer = client.local_addr().expect("local_addr").to_string();

            // Idle deadline (1.5s) elapses mid-flush → bounded send_and_flush
            // returns Elapsed → handler returns Err(Error::Timeout).
            let start = std::time::Instant::now();
            let join_res = monoio::time::timeout(Duration::from_secs(4), handle)
                .await
                .expect("join timeout");
            let elapsed = start.elapsed();
            assert!(
                matches!(join_res, Err(Error::Timeout)),
                "expected Err(Error::Timeout), got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "timeout"),
                1,
                "expected exactly one DEBUG timeout line, got: {logs}"
            );
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "idle timeout during PINGRESP flush"),
                1,
                "expected exactly one DEBUG PINGRESP-flush timeout line, got: {logs}"
            );
            assert!(
                has_line_at(&logs, "DEBUG", &["idle timeout during PINGRESP flush", &peer]),
                "missing DEBUG-level PINGRESP-flush timeout log with complete peer address, got: {logs}"
            );
            for level in ["WARN", "INFO", "ERROR"] {
                let any = logs
                    .lines()
                    .any(|l| l.split_whitespace().nth(1) == Some(level));
                assert!(
                    !any,
                    "blocked PINGRESP-flush timeout emitted a {level}-level line, got: {logs}"
                );
            }
            assert!(
                (Duration::from_millis(1300)..=Duration::from_millis(2500)).contains(&elapsed),
                "elapsed {elapsed:?} outside 1.3s..=2.5s window (1.5s idle deadline)"
            );

            drop(client);
        });
    }

    #[test]
    fn closes_at_1_5x_from_connect_receipt_under_slow_connack_flush() {
        let mut rt = build_runtime();
        rt.block_on(async {
            // 1s write delay → the CONNACK flush completes ~1s after the CONNECT
            // decode, inside the 1.5s negotiated window (ka=1).
            let (mut client, _rx, handle) =
                spawn_handler_test_io(5, 30, Duration::from_secs(1), 0, None, None).await;

            tcp_write_all(&mut client, &V3_CONNECT_FIRST_9)
                .await
                .expect("write 9-byte prefix");
            monoio::time::sleep(Duration::from_secs(1)).await;
            let connect_done = std::time::Instant::now();
            tcp_write_all(&mut client, &V3_CONNECT_KA1_LAST_9)
                .await
                .expect("write remaining 9 bytes");

            let connack = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(3))
                .await
                .expect("read connack");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);

            let _ = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(6))
                .await
                .expect("read to eof (bounded)");
            let join_res = monoio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");

            let from_connect = connect_done.elapsed();
            // Excludes both wrong anchors: connection creation closes at ~1.0s,
            // CONNACK completion at ~2.5s.
            assert!(
                (Duration::from_millis(1300)..=Duration::from_millis(2100)).contains(&from_connect),
                "elapsed from CONNECT receipt {from_connect:?} outside 1.3s..=2.1s window"
            );
        });
    }

    #[test]
    fn processes_publish_buffered_with_connect_under_overlong_flush() {
        let mut rt = build_runtime();
        rt.block_on(async {
            // CONNECT + PUBLISH served as one scripted read; the 2s flush delay
            // exhausts the 1.5s window before the CONNACK completes.
            let mut buffered = Vec::new();
            buffered.extend_from_slice(&V3_CONNECT_TEST_KA1);
            buffered.extend_from_slice(&PUBLISH_QOS0_T);
            assert_eq!(buffered.len(), 27);
            let (mut client, mut rx, handle) =
                spawn_handler_test_io(5, 30, Duration::from_secs(2), 0, Some(buffered), None).await;

            let connack = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(4))
                .await
                .expect("read connack");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);

            let evt = monoio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("event timeout")
                .expect("event Some");
            match evt {
                Event::SensorV1 {
                    temperature,
                    pressure,
                } => {
                    assert_eq!(temperature, 2500);
                    assert_eq!(pressure, 1013);
                }
            }

            let _ = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(6))
                .await
                .expect("read to eof (bounded)");
            let join_res = monoio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");
        });
    }

    #[test]
    fn closes_immediately_after_overlong_connack_flush_with_nothing_buffered() {
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) =
                spawn_handler_test_io(5, 30, Duration::from_secs(2), 0, None, None).await;

            tcp_write_all(&mut client, &V3_CONNECT_TEST_KA1)
                .await
                .expect("connect write");

            let connack = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(4))
                .await
                .expect("read connack");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);
            let connack_at = std::time::Instant::now();

            let _ = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(3))
                .await
                .expect("read to eof (bounded)");
            let join_res = monoio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");

            let after_connack = connack_at.elapsed();
            // A fresh post-flush window would close ~1.5s after the CONNACK.
            assert!(
                after_connack <= Duration::from_secs(1),
                "close took {after_connack:?} after CONNACK — a fresh idle window was granted"
            );
        });
    }

    #[test]
    fn closes_after_overlong_flush_when_only_incomplete_packet_buffered() {
        let mut rt = build_runtime();
        rt.block_on(async {
            // Complete CONNECT plus one byte of a PUBLISH header — an incomplete
            // frame is not a packet, so it grants no fresh window.
            let mut buffered = Vec::new();
            buffered.extend_from_slice(&V3_CONNECT_TEST_KA1);
            buffered.push(0x30);
            assert_eq!(buffered.len(), 19);
            let (mut client, mut rx, handle) =
                spawn_handler_test_io(5, 30, Duration::from_secs(2), 0, Some(buffered), None).await;

            let connack = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(4))
                .await
                .expect("read connack");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);
            let connack_at = std::time::Instant::now();

            let _ = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(3))
                .await
                .expect("read to eof (bounded)");
            let join_res = monoio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");

            let after_connack = connack_at.elapsed();
            assert!(
                after_connack <= Duration::from_secs(1),
                "close took {after_connack:?} after CONNACK — a fresh idle window was granted"
            );

            let evt = monoio::time::timeout(Duration::from_millis(300), rx.recv()).await;
            assert!(
                matches!(evt, Ok(None)),
                "expected no event from an incomplete frame, got {evt:?}"
            );
        });
    }

    #[test]
    fn closes_at_config_capped_window_from_connect_receipt_under_slow_flush() {
        let mut rt = build_runtime();
        rt.block_on(async {
            // v3 ka=60 with idle_timeout_secs=2 → negotiated window is the config
            // cap (2s), not 1.5×60. The 1s write delay keeps the flush inside it.
            let (mut client, _rx, handle) =
                spawn_handler_test_io(5, 2, Duration::from_secs(1), 0, None, None).await;

            tcp_write_all(&mut client, &V3_CONNECT_FIRST_9)
                .await
                .expect("write 9-byte prefix");
            monoio::time::sleep(Duration::from_secs(1)).await;
            let connect_done = std::time::Instant::now();
            tcp_write_all(&mut client, &V3_CONNECT_LAST_9)
                .await
                .expect("write remaining 9 bytes");

            let connack = tcp_read_n_bounded(&mut client, 4, Duration::from_secs(3))
                .await
                .expect("read connack");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);

            let _ = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(6))
                .await
                .expect("read to eof (bounded)");
            let join_res = monoio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");

            let from_connect = connect_done.elapsed();
            // Excludes both wrong anchors: connection creation closes at ~1.0s,
            // CONNACK completion at ~3.0s. An uncapped 90s window never closes.
            assert!(
                (Duration::from_millis(1700)..=Duration::from_millis(2600)).contains(&from_connect),
                "elapsed from CONNECT receipt {from_connect:?} outside 1.7s..=2.6s window"
            );
        });
    }

    #[test]
    fn v5_refusal_connack_capabilities_match_accept_connack() {
        let mut rt = build_runtime();
        rt.block_on(async {
            // Accept path: v5 CONNECT with a client id.
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            tcp_write_all(&mut client, &encode_v5_connect(60, "dev5"))
                .await
                .expect("v5 connect write");
            let mut reader = V5Reader::new();
            let accept = match reader.next(&mut client).await {
                Some(MqttPacket::V5(PacketV5::ConnectAck(ack))) => ack,
                other => panic!("expected v5 CONNACK, got {other:?}"),
            };
            assert!(matches!(accept.reason_code, V5ConnectAckReason::Success));
            tcp_write_all(&mut client, &DISCONNECT)
                .await
                .expect("disconnect write");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");

            // Decoder-level refusal path: v5 CONNECT with empty id, no clean start.
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            let mut enc = MqttEncoder::v5();
            let pkt = MqttPacket::V5(PacketV5::Connect(Box::<rmqtt_codec::v5::Connect>::default()));
            let mut buf = BytesMut::new();
            enc.encode(pkt, &mut buf).expect("encode v5 CONNECT empty");
            tcp_write_all(&mut client, &buf).await.expect("write");
            let mut reader = V5Reader::new();
            let refusal = match reader.next(&mut client).await {
                Some(MqttPacket::V5(PacketV5::ConnectAck(ack))) => ack,
                other => panic!("expected v5 CONNACK, got {other:?}"),
            };
            assert!(matches!(
                refusal.reason_code,
                V5ConnectAckReason::ClientIdentifierNotValid
            ));
            let _ = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(2))
                .await
                .expect("eof");
            let join_res = monoio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("join timeout");
            assert!(join_res.is_ok(), "handler returned error: {join_res:?}");

            // One constructor owns the capability policy — the two wire CONNACKs
            // cannot diverge, whatever value a later feature gives max_qos.
            assert_eq!(refusal.max_qos, accept.max_qos, "max_qos diverged");
            assert_eq!(
                refusal.retain_available, accept.retain_available,
                "retain_available diverged"
            );
            assert_eq!(
                refusal.wildcard_subscription_available, accept.wildcard_subscription_available,
                "wildcard_subscription_available diverged"
            );
            assert_eq!(
                refusal.shared_subscription_available, accept.shared_subscription_available,
                "shared_subscription_available diverged"
            );
            assert_eq!(
                refusal.subscription_identifiers_available,
                accept.subscription_identifiers_available,
                "subscription_identifiers_available diverged"
            );
            assert_eq!(
                refusal.session_expiry_interval_secs, accept.session_expiry_interval_secs,
                "session_expiry_interval_secs diverged"
            );
            assert_eq!(
                refusal.session_present, accept.session_present,
                "session_present diverged"
            );
        });
    }

    #[test]
    fn connack_flush_timeout_respects_shared_handshake_budget() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        let start = std::time::Instant::now();
        rt.block_on(async {
            // 3s budget consumed by a 2s fragmentation wait; the 1.5s write delay
            // then exceeds what is left, so the CONNACK never reaches the client.
            let (mut client, _rx, handle) =
                spawn_handler_test_io(3, 30, Duration::from_millis(1500), 0, None, None).await;
            let peer = client.local_addr().expect("local_addr").to_string();

            tcp_write_all(&mut client, &V3_CONNECT_FIRST_9)
                .await
                .expect("write 9-byte prefix");
            monoio::time::sleep(Duration::from_secs(2)).await;
            tcp_write_all(&mut client, &V3_CONNECT_KA1_LAST_9)
                .await
                .expect("write remaining 9 bytes");

            let got = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(6))
                .await
                .expect("read to eof (bounded)");
            assert!(
                got.is_empty(),
                "expected zero response bytes after shared budget expiry, got {got:?}"
            );

            let join_res = monoio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Timeout)),
                "expected Err(Error::Timeout), got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            // Phase-agnostic on purpose: the remaining 9 bytes are written after a
            // real-clock sleep, so a scheduling slip past the budget expires the
            // CONNECT read instead of the flush. The phase-specific CONNACK needle
            // lives on the deterministic fixture below.
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "timeout"),
                1,
                "expected exactly one DEBUG timeout line, got: {logs}"
            );
            assert!(
                has_line_at(&logs, "DEBUG", &["timeout", &peer]),
                "missing DEBUG-level timeout log with complete peer address, got: {logs}"
            );
            for level in ["WARN", "INFO", "ERROR"] {
                let any = logs
                    .lines()
                    .any(|l| l.split_whitespace().nth(1) == Some(level));
                assert!(
                    !any,
                    "shared-budget timeout emitted a {level}-level line, got: {logs}"
                );
            }
        });
        let elapsed = start.elapsed();
        assert!(
            (Duration::from_millis(2700)..=Duration::from_millis(4200)).contains(&elapsed),
            "elapsed {elapsed:?} outside 2.7s..=4.2s window (shared handshake budget)"
        );
    }

    /// Accept-path CONNACK flush (`bounded_send` at the `Accept` arm) with the
    /// phase pinned structurally: the complete CONNECT is served from memory, so
    /// version detect and the CONNECT read consume no wall clock and the 1s
    /// budget can only expire inside the 3s-delayed flush.
    #[test]
    fn accept_connack_flush_timeout_debug_logs_connack_context_with_peer() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (client, _rx, handle) = spawn_handler_test_io(
                1,
                30,
                Duration::from_secs(3),
                0,
                Some(V3_CONNECT_TEST.to_vec()),
                None,
            )
            .await;
            let peer = client.local_addr().expect("local_addr").to_string();

            let join_res = monoio::time::timeout(Duration::from_secs(4), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Timeout)),
                "expected Err(Error::Timeout), got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "timeout"),
                1,
                "expected exactly one DEBUG timeout line, got: {logs}"
            );
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "handshake timeout during CONNACK flush"),
                1,
                "expected exactly one DEBUG CONNACK-flush timeout line, got: {logs}"
            );
            assert!(
                has_line_at(
                    &logs,
                    "DEBUG",
                    &["handshake timeout during CONNACK flush", &peer]
                ),
                "missing DEBUG-level CONNACK-flush timeout log with complete peer address, got: {logs}"
            );
            for level in ["WARN", "INFO", "ERROR"] {
                let any = logs
                    .lines()
                    .any(|l| l.split_whitespace().nth(1) == Some(level));
                assert!(
                    !any,
                    "accept CONNACK-flush timeout emitted a {level}-level line, got: {logs}"
                );
            }

            drop(client);
        });
    }

    #[test]
    fn decoder_refusal_connack_flush_timeout_debug_logs_timeout_and_keeps_refusal_warn() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (client, _rx, handle) = spawn_handler_test_io(
                1,
                30,
                Duration::from_secs(3),
                0,
                Some(V3_CONNECT_EMPTY_NO_CLEAN.to_vec()),
                None,
            )
            .await;
            let peer = client.local_addr().expect("local_addr").to_string();

            let join_res = monoio::time::timeout(Duration::from_secs(4), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Timeout)),
                "expected Err(Error::Timeout), got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert!(
                has_line_at(&logs, "WARN", &["CONNECT refused: invalid client id", &peer]),
                "missing WARN-level refusal log with complete peer address, got: {logs}"
            );
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "timeout"),
                1,
                "expected exactly one DEBUG timeout line, got: {logs}"
            );
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "handshake timeout during CONNACK flush"),
                1,
                "expected exactly one DEBUG CONNACK-flush timeout line, got: {logs}"
            );
            assert!(
                has_line_at(
                    &logs,
                    "DEBUG",
                    &["handshake timeout during CONNACK flush", &peer]
                ),
                "missing DEBUG-level CONNACK-flush timeout log with complete peer address, got: {logs}"
            );
            for level in ["WARN", "INFO", "ERROR"] {
                assert_eq!(
                    count_lines_at(&logs, level, "timeout"),
                    0,
                    "decoder-refusal CONNACK-flush timeout emitted a {level}-level timeout line, got: {logs}"
                );
            }

            drop(client);
        });
    }

    #[test]
    fn policy_refusal_connack_flush_timeout_debug_logs_timeout_and_keeps_refusal_warn() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let mut enc = MqttEncoder::v5();
            let connect = rmqtt_codec::v5::Connect {
                client_id: "dev5".to_string().into(),
                auth_method: Some("PLAIN".to_string().into()),
                keep_alive: 60,
                ..Default::default()
            };
            let mut buf = BytesMut::new();
            enc.encode(
                MqttPacket::V5(PacketV5::Connect(Box::new(connect))),
                &mut buf,
            )
            .expect("encode");

            let (client, _rx, handle) = spawn_handler_test_io(
                1,
                30,
                Duration::from_secs(3),
                0,
                Some(buf.to_vec()),
                None,
            )
            .await;
            let peer = client.local_addr().expect("local_addr").to_string();

            let join_res = monoio::time::timeout(Duration::from_secs(4), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Timeout)),
                "expected Err(Error::Timeout), got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert!(
                has_line_at(
                    &logs,
                    "WARN",
                    &["CONNECT refused: BadAuthenticationMethod", &peer]
                ),
                "missing WARN-level refusal log with complete peer address, got: {logs}"
            );
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "timeout"),
                1,
                "expected exactly one DEBUG timeout line, got: {logs}"
            );
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "handshake timeout during CONNACK flush"),
                1,
                "expected exactly one DEBUG CONNACK-flush timeout line, got: {logs}"
            );
            assert!(
                has_line_at(
                    &logs,
                    "DEBUG",
                    &["handshake timeout during CONNACK flush", &peer]
                ),
                "missing DEBUG-level CONNACK-flush timeout log with complete peer address, got: {logs}"
            );
            for level in ["WARN", "INFO", "ERROR"] {
                assert_eq!(
                    count_lines_at(&logs, level, "timeout"),
                    0,
                    "policy-refusal CONNACK-flush timeout emitted a {level}-level timeout line, got: {logs}"
                );
            }

            drop(client);
        });
    }

    #[test]
    fn v5_pingresp_flush_timeout_debug_logs_idle_context_with_peer() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let mut prefix = encode_v5_connect(1, "dev5");
            prefix.extend_from_slice(&PINGREQ);
            let (client, _rx, handle) =
                spawn_handler_test_io(5, 30, Duration::from_secs(10), 1, Some(prefix), None).await;
            let peer = client.local_addr().expect("local_addr").to_string();

            let join_res = monoio::time::timeout(Duration::from_secs(4), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Err(Error::Timeout)),
                "expected Err(Error::Timeout), got {join_res:?}"
            );
            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "timeout"),
                1,
                "expected exactly one DEBUG timeout line, got: {logs}"
            );
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "idle timeout during PINGRESP flush"),
                1,
                "expected exactly one DEBUG PINGRESP-flush timeout line, got: {logs}"
            );
            assert!(
                has_line_at(&logs, "DEBUG", &["idle timeout during PINGRESP flush", &peer]),
                "missing DEBUG-level PINGRESP-flush timeout log with complete peer address, got: {logs}"
            );
            for level in ["WARN", "INFO", "ERROR"] {
                let any = logs
                    .lines()
                    .any(|l| l.split_whitespace().nth(1) == Some(level));
                assert!(
                    !any,
                    "v5 PINGRESP-flush timeout emitted a {level}-level line, got: {logs}"
                );
            }

            drop(client);
        });
    }

    /// Peer attribution has to identify WHICH connection timed out: two silent
    /// clients on the same runtime must produce two version-detect lines, each
    /// carrying its own address. Catches what no single-connection test can — a
    /// peer value shared across connections, or emission deduplicated
    /// process-wide so the second connection's expiry goes unlogged.
    #[test]
    fn concurrent_handshake_timeouts_attribute_each_line_to_its_own_peer() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (client_a, _rx_a, handle_a) = spawn_handler(1, 30).await;
            let (client_b, _rx_b, handle_b) = spawn_handler(1, 30).await;
            let peer_a = client_a.local_addr().expect("local_addr").to_string();
            let peer_b = client_b.local_addr().expect("local_addr").to_string();
            assert_ne!(peer_a, peer_b, "both clients bound the same local address");

            // Neither client speaks: both budgets expire during version detect.
            let join_a = monoio::time::timeout(Duration::from_secs(5), handle_a)
                .await
                .expect("join timeout a");
            let join_b = monoio::time::timeout(Duration::from_secs(5), handle_b)
                .await
                .expect("join timeout b");
            assert!(
                matches!(join_a, Err(Error::Timeout)),
                "expected Err(Error::Timeout) for connection a, got {join_a:?}"
            );
            assert!(
                matches!(join_b, Err(Error::Timeout)),
                "expected Err(Error::Timeout) for connection b, got {join_b:?}"
            );

            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "timeout"),
                2,
                "expected exactly one DEBUG timeout line per connection, got: {logs}"
            );
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "handshake timeout during version detect"),
                2,
                "expected exactly two DEBUG version-detect timeout lines, got: {logs}"
            );
            assert!(
                has_line_at(
                    &logs,
                    "DEBUG",
                    &["handshake timeout during version detect", &peer_a]
                ),
                "missing DEBUG-level version-detect timeout log for peer a, got: {logs}"
            );
            assert!(
                has_line_at(
                    &logs,
                    "DEBUG",
                    &["handshake timeout during version detect", &peer_b]
                ),
                "missing DEBUG-level version-detect timeout log for peer b, got: {logs}"
            );
            for level in ["WARN", "INFO", "ERROR"] {
                let any = logs
                    .lines()
                    .any(|l| l.split_whitespace().nth(1) == Some(level));
                assert!(
                    !any,
                    "concurrent handshake expiry emitted a {level}-level line, got: {logs}"
                );
            }

            drop(client_a);
            drop(client_b);
        });
    }

    /// A PINGRESP that flushes inside its idle window must stay silent: the one
    /// timeout line the session emits is the later idle-deadline expiry, with no
    /// PINGRESP-flush context anywhere. Logging on `bounded_send`'s success path
    /// (or attributing the idle close to the flush) fails this.
    #[test]
    fn successful_pingresp_flush_is_silent_and_idle_close_logs_once() {
        let sink = capture_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            // ka=2 → negotiated idle deadline 3s, re-anchored by the PINGREQ.
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
            let peer = client.local_addr().expect("local_addr").to_string();

            tcp_write_all(&mut client, &V3_CONNECT_TEST_KA2)
                .await
                .expect("connect write");
            let connack = tcp_read_n(&mut client, 4).await.expect("read connack");
            assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);

            tcp_write_all(&mut client, &PINGREQ)
                .await
                .expect("pingreq write");
            let pingresp = tcp_read_n(&mut client, 2).await.expect("read pingresp");
            assert_eq!(pingresp, vec![0xD0, 0x00], "expected flushed PINGRESP");

            // Stay silent from here: the idle deadline is the only expiry left.
            let rest = tcp_read_to_eof_bounded(&mut client, Duration::from_secs(6))
                .await
                .expect("read to eof (bounded)");
            assert!(
                rest.is_empty(),
                "expected no further bytes after PINGRESP, got {rest:?}"
            );
            let join_res = monoio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("join timeout");
            assert!(
                matches!(join_res, Ok(SessionOutcome::Served)),
                "expected Ok(SessionOutcome::Served), got {join_res:?}"
            );

            let logs =
                String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "timeout"),
                1,
                "expected exactly one DEBUG timeout line, got: {logs}"
            );
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "client idle timeout"),
                1,
                "expected exactly one DEBUG client-idle-timeout line, got: {logs}"
            );
            assert!(
                has_line_at(&logs, "DEBUG", &["client idle timeout", &peer]),
                "missing DEBUG-level client-idle-timeout log with complete peer address, got: {logs}"
            );
            assert_eq!(
                count_lines_at(&logs, "DEBUG", "PINGRESP flush"),
                0,
                "a PINGRESP that flushed in time emitted a flush-timeout line, got: {logs}"
            );
            for level in ["WARN", "INFO", "ERROR"] {
                let any = logs
                    .lines()
                    .any(|l| l.split_whitespace().nth(1) == Some(level));
                assert!(
                    !any,
                    "served session closing on idle emitted a {level}-level line, got: {logs}"
                );
            }
        });
    }
}
