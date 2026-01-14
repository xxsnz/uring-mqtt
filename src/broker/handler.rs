use monoio::io::sink::Sink;
use monoio::io::stream::Stream;
use monoio::net::TcpStream;
use monoio_codec::Framed;
use std::time::Duration;

use super::worker::LocalSender;
use super::Event;
use crate::codec::mqtt::{
    ConnectAck, ConnectAckReason, MqttDecoder, MqttEncoder, MqttPacket, PacketV3, PacketV5,
};
use crate::codec::version::{ProtocolVersion, VersionDecoder};
use crate::connection::ConnectionState;
use crate::error::Error;

/// Handle a single MQTT client connection.
pub async fn handle_client(
    stream: TcpStream,
    event_tx: LocalSender<Event>,
    connection_timeout_secs: u64,
    idle_timeout_secs: u64,
) -> Result<(), Error> {
    let mut state = ConnectionState::new();

    // Phase 1: Detect protocol version
    let mut framed = Framed::new(stream, VersionDecoder::new());

    let version =
        match monoio::time::timeout(Duration::from_secs(connection_timeout_secs), framed.next())
            .await
        {
            Ok(Some(Ok(v))) => v,
            Ok(Some(Err(e))) => return Err(Error::Io(e)),
            Ok(None) => return Err(Error::Protocol("Connection closed during handshake".into())),
            Err(_) => return Err(Error::Timeout),
        };

    tracing::debug!("Detected protocol version: {:?}", version);

    // Phase 2: Switch to versioned codec
    let stream = framed.into_inner();
    let codec = CodecPair::new(version);
    let mut framed = Framed::new(stream, codec);

    // Phase 3: Send CONNACK
    let connack = match version {
        ProtocolVersion::MQTT3 => MqttPacket::V3(PacketV3::ConnectAck(ConnectAck {
            session_present: false,
            return_code: ConnectAckReason::ConnectionAccepted,
        })),
        ProtocolVersion::MQTT5 => MqttPacket::V5(PacketV5::ConnectAck(Box::default())),
    };

    framed.send(connack).await.map_err(Error::Io)?;

    // Phase 4: Main packet loop
    loop {
        let packet_result =
            monoio::time::timeout(Duration::from_secs(idle_timeout_secs), framed.next()).await;

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
                        let response = MqttPacket::V3(PacketV3::PingResponse);
                        framed.send(response).await.map_err(Error::Io)?;
                    }
                    MqttPacket::V5(PacketV5::PingRequest) => {
                        let response = MqttPacket::V5(PacketV5::PingResponse);
                        framed.send(response).await.map_err(Error::Io)?;
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

/// Combined codec for both encoding and decoding MQTT packets.
struct CodecPair {
    decoder: MqttDecoder,
    encoder: MqttEncoder,
}

impl CodecPair {
    fn new(version: ProtocolVersion) -> Self {
        Self {
            decoder: MqttDecoder::new(version),
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
