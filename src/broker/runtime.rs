//! Runtime-generic IO/timer seam for the session loop and the event-channel
//! admission. The shipped broker compiles its monoio adapters inline;
//! `bench/src/twin.rs` implements `Timer`, `HandshakeIo`, `PacketIo` on tokio.
use std::future::Future;
use std::io;
use std::time::Duration;

use monoio::io::sink::Sink;
use monoio::io::stream::Stream;
use monoio::io::{AsyncReadRent, AsyncWriteRent};
use monoio_codec::Framed;

use crate::codec::mqtt::{MaxInboundPacketSize, MqttDecoder, MqttEncoder, MqttPacket};
use crate::codec::version::{ProtocolVersion, VersionDecoder};

/// Returned from `Timer::timeout` when the duration elapsed before the future
/// completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimedOut;

/// Race a future against a wall-clock deadline. Adapters wrap their runtime's
/// timer; `bench/src/twin.rs` adapts tokio's.
pub trait Timer {
    fn timeout<F: Future>(
        duration: Duration,
        future: F,
    ) -> impl Future<Output = Result<F::Output, TimedOut>>;
}

/// IO used during the version-detect + CONNECT handshake, then morphed into
/// the packet codec for the rest of the session.
pub trait HandshakeIo {
    type Packets: PacketIo;
    fn next_version(&mut self) -> impl Future<Output = Option<io::Result<ProtocolVersion>>>;
    fn into_packets(
        self,
        version: ProtocolVersion,
        max_inbound_packet_size: MaxInboundPacketSize,
    ) -> Self::Packets;
}

/// MQTT-codec-agnostic IO for the steady-state packet loop.
pub trait PacketIo {
    fn next_packet(&mut self) -> impl Future<Output = Option<io::Result<(MqttPacket, u32)>>>;
    /// Encode `packet` into the write buffer. Writes to the socket only when
    /// the buffer already holds the codec's backpressure boundary.
    fn send(&mut self, packet: MqttPacket) -> impl Future<Output = io::Result<()>>;
    fn flush(&mut self) -> impl Future<Output = io::Result<()>>;
    fn read_buffer(&self) -> &[u8];
    /// Bytes encoded by `send` and not yet written to the socket.
    fn write_buffer(&self) -> &[u8];
    fn version(&self) -> ProtocolVersion;
}

/// Monoio adapter: `monoio::time::timeout` and `monoio_codec::Framed`.
pub(crate) struct MonoioTimer;

impl Timer for MonoioTimer {
    async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output, TimedOut> {
        monoio::time::timeout(duration, future)
            .await
            .map_err(|_| TimedOut)
    }
}

/// Monoio handshake IO: a `Framed<IO, VersionDecoder>`.
pub(crate) struct MonoioHandshake<IO>(Framed<IO, VersionDecoder>);

impl<IO: AsyncReadRent + AsyncWriteRent> MonoioHandshake<IO> {
    pub(crate) fn new(io: IO, max_inbound_packet_size: MaxInboundPacketSize) -> Self {
        Self(Framed::new(
            io,
            VersionDecoder::new(max_inbound_packet_size),
        ))
    }
}

impl<IO: AsyncReadRent + AsyncWriteRent> HandshakeIo for MonoioHandshake<IO> {
    type Packets = MonoioPackets<IO>;

    fn next_version(&mut self) -> impl Future<Output = Option<io::Result<ProtocolVersion>>> {
        self.0.next()
    }

    fn into_packets(
        self,
        version: ProtocolVersion,
        max_inbound_packet_size: MaxInboundPacketSize,
    ) -> Self::Packets {
        MonoioPackets(
            self.0
                .map_codec(|_| CodecPair::new(version, max_inbound_packet_size)),
        )
    }
}

/// Monoio packet IO: a `Framed<IO, CodecPair>`.
pub(crate) struct MonoioPackets<IO>(Framed<IO, CodecPair>);

impl<IO: AsyncReadRent + AsyncWriteRent> PacketIo for MonoioPackets<IO> {
    fn next_packet(&mut self) -> impl Future<Output = Option<io::Result<(MqttPacket, u32)>>> {
        self.0.next()
    }

    fn send(&mut self, packet: MqttPacket) -> impl Future<Output = io::Result<()>> {
        Sink::send(&mut self.0, packet)
    }

    fn flush(&mut self) -> impl Future<Output = io::Result<()>> {
        Sink::<MqttPacket>::flush(&mut self.0)
    }

    fn read_buffer(&self) -> &[u8] {
        self.0.read_buffer()
    }

    fn write_buffer(&self) -> &[u8] {
        self.0.write_buffer()
    }

    fn version(&self) -> ProtocolVersion {
        self.0.codec().version()
    }
}

/// Combined codec for both encoding and decoding MQTT packets.
pub(crate) struct CodecPair {
    version: ProtocolVersion,
    decoder: MqttDecoder,
    encoder: MqttEncoder,
}

impl CodecPair {
    fn new(version: ProtocolVersion, max_inbound_packet_size: MaxInboundPacketSize) -> Self {
        Self {
            version,
            decoder: MqttDecoder::new(version, max_inbound_packet_size),
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

    fn decode_eof(
        &mut self,
        src: &mut bytes::BytesMut,
    ) -> Result<monoio_codec::Decoded<Self::Item>, Self::Error> {
        monoio_codec::Decoder::decode_eof(&mut self.decoder, src)
    }
}

impl monoio_codec::Encoder<MqttPacket> for CodecPair {
    type Error = std::io::Error;

    fn encode(&mut self, item: MqttPacket, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        self.encoder.encode(item, dst)
    }
}
