use monoio::io::sink::SinkExt;
use monoio::io::stream::Stream;
use monoio::io::{AsyncReadRent, AsyncWriteRent};
use monoio::net::TcpStream;
use monoio_codec::Framed;
use std::time::Duration;

use super::worker::EventSender;
use super::Event;
use crate::broker::handshake::{self, ConnectDecision};
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
}

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

/// IO-generic body of `handle_client`. The public entry keeps the
/// monomorphic `TcpStream` signature; tests substitute a `TestIo` wrapper.
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
            // VersionCodec rejects any first packet that is not CONNECT, so this is
            // the boundary where "first packet was not CONNECT" actually surfaces.
            tracing::warn!("handshake violation: first packet was not CONNECT ({peer_addr}): {e}");
            return Err(Error::Io(e));
        }
        Ok(None) => return Err(Error::ClientClosed),
        Err(_) => return Err(Error::Timeout),
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
                bounded_send(&mut framed, refusal, remaining()).await?;
                return Ok(SessionOutcome::Refused);
            }
            // Every other decoder rejection is a protocol violation closed without a
            // reply; Q8 requires it visible at default level, not only at debug.
            tracing::warn!("handshake violation: malformed CONNECT from {peer_addr}: {e}");
            return Err(Error::Io(e));
        }
        Ok(None) => return Err(Error::ClientClosed),
        Err(_) => return Err(Error::Timeout),
    };
    // Idle-window anchor: CONNECT receipt, so a slow CONNACK flush cannot
    // extend the first idle interval.
    let connect_received_at = std::time::Instant::now();

    // Phase 4: Evaluate CONNECT, then release the packet — its will payload,
    // credentials, and properties must not stay allocated for the whole connection.
    let decision = handshake::evaluate_connect(&packet, idle_timeout_secs, peer_addr);
    drop(packet);

    match decision {
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
            bounded_send(&mut framed, connack, remaining()).await?;
            return Ok(SessionOutcome::Refused);
        }
        ConnectDecision::Accept {
            connack,
            client_id,
            keep_alive_secs,
            idle_timeout,
        } => {
            bounded_send(&mut framed, connack, remaining()).await?;
            state.client_id = client_id;
            state.keep_alive = keep_alive_secs;
            state.last_packet_time = connect_received_at;

            run_packet_loop(&mut framed, &mut state, event_tx, idle_timeout, peer_addr).await?;
        }
    }

    Ok(SessionOutcome::Served)
}

/// Main packet loop — idle deadline anchored to the last received packet.
async fn run_packet_loop<IO>(
    framed: &mut Framed<IO, CodecPair>,
    state: &mut ConnectionState,
    event_tx: EventSender,
    idle_timeout: Duration,
    peer_addr: std::net::SocketAddr,
) -> Result<(), Error>
where
    IO: AsyncReadRent + AsyncWriteRent,
{
    loop {
        let remaining_idle = idle_timeout.saturating_sub(state.last_packet_time.elapsed());
        let packet_result = monoio::time::timeout(remaining_idle, framed.next()).await;
        match packet_result {
            Ok(Some(Ok((packet, _id)))) => {
                state.update_activity();
                match packet {
                    MqttPacket::V3(PacketV3::Publish(pub_pkt)) => {
                        tracing::debug!(
                            "v3 PUBLISH {} len: {}",
                            pub_pkt.topic,
                            pub_pkt.payload.len()
                        );
                        if let Some(event) = parse_sensor_data(&pub_pkt.payload) {
                            event_tx.send(event);
                        }
                    }
                    MqttPacket::V5(PacketV5::Publish(pub_pkt)) => {
                        tracing::debug!(
                            "v5 PUBLISH {} len: {}",
                            pub_pkt.topic,
                            pub_pkt.payload.len()
                        );
                        if let Some(event) = parse_sensor_data(&pub_pkt.payload) {
                            event_tx.send(event);
                        }
                    }
                    MqttPacket::V3(PacketV3::PingRequest) => {
                        let remaining =
                            idle_timeout.saturating_sub(state.last_packet_time.elapsed());
                        bounded_send(framed, MqttPacket::V3(PacketV3::PingResponse), remaining)
                            .await?;
                    }
                    MqttPacket::V5(PacketV5::PingRequest) => {
                        let remaining =
                            idle_timeout.saturating_sub(state.last_packet_time.elapsed());
                        bounded_send(framed, MqttPacket::V5(PacketV5::PingResponse), remaining)
                            .await?;
                    }
                    MqttPacket::V3(PacketV3::Connect(_)) | MqttPacket::V5(PacketV5::Connect(_)) => {
                        tracing::warn!(
                            "handshake violation: duplicate CONNECT ({peer_addr}) — closing"
                        );
                        break;
                    }
                    MqttPacket::V3(PacketV3::Disconnect)
                    | MqttPacket::V5(PacketV5::Disconnect(_)) => {
                        tracing::debug!("Client requested disconnect");
                        break;
                    }
                    _ => {
                        tracing::trace!("Unhandled packet type");
                    }
                }
            }
            Ok(Some(Err(e))) => {
                tracing::debug!("Packet error: {:?}", e);
                break;
            }
            Ok(None) => {
                tracing::debug!("Client closed connection");
                break;
            }
            Err(_) => {
                tracing::debug!("Client idle timeout");
                break;
            }
        }
    }
    Ok(())
}

/// Bounded `send_and_flush` under a deadline (handshake or idle).
async fn bounded_send<IO>(
    framed: &mut Framed<IO, CodecPair>,
    pkt: MqttPacket,
    deadline: Duration,
) -> Result<(), Error>
where
    IO: AsyncWriteRent,
{
    let send_fut = SinkExt::send_and_flush(framed, pkt);
    match monoio::time::timeout(deadline, send_fut).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(Error::Io(e)),
        Err(_) => Err(Error::Timeout),
    }
}

/// Combined codec for both encoding and decoding MQTT packets.
struct CodecPair {
    decoder: crate::codec::mqtt::MqttDecoder,
    encoder: MqttEncoder,
}

impl CodecPair {
    fn new(version: ProtocolVersion) -> Self {
        Self {
            decoder: crate::codec::mqtt::MqttDecoder::new(version),
            encoder: MqttEncoder::new(version),
        }
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
mod tests {
    use super::*;
    use crate::broker::worker::{event_channel, EventReceiver};
    use bytes::BytesMut;
    use monoio::buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut};
    use monoio::io::{AsyncReadRent, AsyncWriteRent};
    use monoio::net::{TcpListener, TcpStream};
    use monoio_codec::Encoder as _;
    use rmqtt_codec::types::Publish as TypesPublish;
    use rmqtt_codec::types::QoS;

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
    /// v3 CONNECT: empty id, clean_session=false → decoder emits `InvalidClientId`.
    const V3_CONNECT_EMPTY_NO_CLEAN: [u8; 14] = [
        0x10, 0x0C, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x00, 0x00, 0x3C, 0x00, 0x00,
    ];
    /// v3 PUBLISH QoS 0: topic "t", payload SensorV1 25.00°C / 1013 hPa.
    const PUBLISH_QOS0_T: [u8; 9] = [0x30, 0x07, 0x00, 0x01, b't', 0x09, 0xC4, 0x03, 0xF5];
    const PINGREQ: [u8; 2] = [0xC0, 0x00];
    const DISCONNECT: [u8; 2] = [0xE0, 0x00];

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
            match pending {
                // Delegate to monoio's own `impl AsyncReadRent for &[u8]` — the
                // crate forbids unsafe, so no hand-filling of the IoBufMut.
                Some(pending) => {
                    let mut slice: &[u8] = &pending;
                    let (res, buf) = slice.read(buf).await;
                    if let Ok(n) = res {
                        self.consumed += n;
                    }
                    (res, buf)
                }
                None => self.inner.read(buf).await,
            }
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
        let mut enc = MqttEncoder::v5();
        let connect = rmqtt_codec::v5::Connect {
            client_id: client_id.to_string().into(),
            keep_alive,
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
                    assert!(matches!(ack.max_qos, QoS::AtMostOnce));
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
                    assert!(matches!(ack.max_qos, QoS::AtMostOnce));
                    assert!(!ack.wildcard_subscription_available);
                    assert!(!ack.shared_subscription_available);
                    assert!(!ack.subscription_identifiers_available);
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

    type LogSink = std::sync::Arc<std::sync::Mutex<Vec<u8>>>;

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

    /// Start capturing this thread's warn-level logs. The subscriber is installed
    /// globally and only once: tests run in parallel and `tracing` caches callsite
    /// interest process-wide, so a thread-scoped subscriber loses that race and the
    /// callsite stays disabled. Max level WARN also proves default-level visibility.
    fn capture_warn_logs() -> LogSink {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::WARN)
                .with_ansi(false)
                .with_writer(ThreadLogWriter)
                .finish();
            tracing::subscriber::set_global_default(subscriber).expect("global subscriber");
        });
        let sink: LogSink = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        LOG_SINK.with(|slot| *slot.borrow_mut() = Some(sink.clone()));
        sink
    }

    #[test]
    fn drop_warns_on_first_and_every_hundredth() {
        let sink = capture_warn_logs();
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
        let drop_warn_lines = logs.lines().filter(|l| l.contains("dropped total")).count();
        assert_eq!(
            drop_warn_lines, 2,
            "expected exactly two drop-warn lines, got {drop_warn_lines}: {logs}"
        );
    }

    #[test]
    fn closed_receiver_discard_is_not_counted_or_warned() {
        // Case (a): fresh channel, drop receiver, send.
        let sink_a = capture_warn_logs();
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
        let sink_b = capture_warn_logs();
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
        let drop_warn_lines_b = logs_b
            .lines()
            .filter(|l| l.contains("dropped total"))
            .count();
        assert_eq!(
            drop_warn_lines_b, 1,
            "expected exactly one drop-warn line for case (b), got {drop_warn_lines_b}: {logs_b}"
        );
    }

    #[test]
    fn warns_at_default_level_when_first_packet_is_not_connect() {
        let sink = capture_warn_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
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
        });
        let logs = String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
        assert!(
            logs.contains("handshake violation: first packet was not CONNECT"),
            "missing warn-level violation log, got: {logs}"
        );
        assert!(
            logs.contains("127.0.0.1:"),
            "violation log lacks peer address, got: {logs}"
        );
    }

    #[test]
    fn warns_at_default_level_when_connect_is_malformed() {
        let sink = capture_warn_logs();
        let mut rt = build_runtime();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
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
        });
        let logs = String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
        assert!(
            logs.contains("handshake violation: malformed CONNECT"),
            "missing warn-level malformed-CONNECT log, got: {logs}"
        );
        assert!(
            logs.contains("127.0.0.1:"),
            "malformed-CONNECT log lacks peer address, got: {logs}"
        );
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
        let mut rt = build_runtime();
        let start = std::time::Instant::now();
        rt.block_on(async {
            let (mut client, _rx, handle) = spawn_handler(2, 30).await;
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
        let mut rt = build_runtime();
        let start = std::time::Instant::now();
        rt.block_on(async {
            // connection_timeout_secs=1; the client connects and never speaks,
            // so the budget must expire during version detection.
            let (mut client, _rx, handle) = spawn_handler(1, 30).await;

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
        let mut rt = build_runtime();
        rt.block_on(async {
            // Scripted CONNECT (ka=1 → idle 1.5s) plus one PINGREQ; the CONNACK
            // write is undelayed, then the PINGRESP write stalls for 10s — a
            // blocked writer as a precondition, not TCP-buffer luck (the old
            // flood variant went green or hung with the runner's buffer sizes).
            let mut prefix = V3_CONNECT_TEST_KA1.to_vec();
            prefix.extend_from_slice(&PINGREQ);
            let (client, _rx, handle) =
                spawn_handler_test_io(5, 30, Duration::from_secs(10), 1, Some(prefix)).await;

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
                spawn_handler_test_io(5, 30, Duration::from_secs(1), 0, None).await;

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
                spawn_handler_test_io(5, 30, Duration::from_secs(2), 0, Some(buffered)).await;

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
                spawn_handler_test_io(5, 30, Duration::from_secs(2), 0, None).await;

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
                spawn_handler_test_io(5, 30, Duration::from_secs(2), 0, Some(buffered)).await;

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
                spawn_handler_test_io(5, 2, Duration::from_secs(1), 0, None).await;

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
        let mut rt = build_runtime();
        let start = std::time::Instant::now();
        rt.block_on(async {
            // 3s budget consumed by a 2s fragmentation wait; the 1.5s write delay
            // then exceeds what is left, so the CONNACK never reaches the client.
            let (mut client, _rx, handle) =
                spawn_handler_test_io(3, 30, Duration::from_millis(1500), 0, None).await;

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
        });
        let elapsed = start.elapsed();
        assert!(
            (Duration::from_millis(2700)..=Duration::from_millis(4200)).contains(&elapsed),
            "elapsed {elapsed:?} outside 2.7s..=4.2s window (shared handshake budget)"
        );
    }
}
