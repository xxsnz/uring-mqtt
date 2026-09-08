use bytes::BytesMut;
use monoio_codec::{Decoded, Decoder, Encoder};
use rmqtt_codec::v3::Codec as V3Codec;
use rmqtt_codec::v5::Codec as V5Codec;
use rmqtt_codec::MqttCodec;
use std::io;
use tokio_util::codec::Decoder as TokioDecoder;
use tokio_util::codec::Encoder as TokioEncoder;

use super::version::ProtocolVersion;

// Re-export commonly used types
pub use rmqtt_codec::error::DecodeError;
pub use rmqtt_codec::v3::{ConnectAck, ConnectAckReason, Packet as PacketV3};
pub use rmqtt_codec::v5::Packet as PacketV5;
pub use rmqtt_codec::MqttPacket;

/// Wrapper around rmqtt-codec's MqttCodec for monoio-codec compatibility.
///
/// Handles encoding/decoding of MQTT v3.1.1 and v5.0 packets.
pub struct MqttDecoder {
    inner: MqttCodec,
}

impl MqttDecoder {
    /// Create a new decoder for the specified MQTT protocol version.
    pub fn new(version: ProtocolVersion) -> Self {
        let inner = match version {
            ProtocolVersion::MQTT3 => MqttCodec::V3(V3Codec::default()),
            ProtocolVersion::MQTT5 => MqttCodec::V5(V5Codec::default()),
        };
        Self { inner }
    }

    /// Create a decoder for MQTT v3.1.1.
    #[cfg(test)]
    pub fn v3() -> Self {
        Self {
            inner: MqttCodec::V3(V3Codec::default()),
        }
    }

    /// Create a decoder for MQTT v5.0.
    #[cfg(test)]
    pub fn v5() -> Self {
        Self {
            inner: MqttCodec::V5(V5Codec::default()),
        }
    }
}

impl Decoder for MqttDecoder {
    type Item = (MqttPacket, u32);
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Decoded<Self::Item>, Self::Error> {
        match TokioDecoder::decode(&mut self.inner, src) {
            Ok(Some(packet)) => Ok(Decoded::Some(packet)),
            Ok(None) => Ok(Decoded::Insufficient),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        }
    }
}

/// Wrapper around rmqtt-codec's MqttCodec for encoding packets.
pub struct MqttEncoder {
    inner: MqttCodec,
}

impl MqttEncoder {
    /// Create a new encoder for the specified MQTT protocol version.
    pub fn new(version: ProtocolVersion) -> Self {
        let inner = match version {
            ProtocolVersion::MQTT3 => MqttCodec::V3(V3Codec::default()),
            ProtocolVersion::MQTT5 => MqttCodec::V5(V5Codec::default()),
        };
        Self { inner }
    }

    /// Create an encoder for MQTT v3.1.1.
    #[cfg(test)]
    pub fn v3() -> Self {
        Self {
            inner: MqttCodec::V3(V3Codec::default()),
        }
    }

    /// Create an encoder for MQTT 5.0.
    #[cfg(test)]
    pub fn v5() -> Self {
        Self {
            inner: MqttCodec::V5(V5Codec::default()),
        }
    }
}

impl Encoder<MqttPacket> for MqttEncoder {
    type Error = io::Error;

    fn encode(&mut self, item: MqttPacket, dst: &mut BytesMut) -> Result<(), Self::Error> {
        TokioEncoder::encode(&mut self.inner, item, dst)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use monoio_codec::{Decoder as MonoioDecoder, Encoder as MonoioEncoder};

    #[test]
    fn test_mqtt_decoder_new_v3() {
        let decoder = MqttDecoder::new(ProtocolVersion::MQTT3);
        // Just verify it creates without panicking
        assert!(matches!(decoder.inner, MqttCodec::V3(_)));
    }

    #[test]
    fn test_mqtt_decoder_new_v5() {
        let decoder = MqttDecoder::new(ProtocolVersion::MQTT5);
        assert!(matches!(decoder.inner, MqttCodec::V5(_)));
    }

    #[test]
    fn test_mqtt_decoder_v3_shortcut() {
        let decoder = MqttDecoder::v3();
        assert!(matches!(decoder.inner, MqttCodec::V3(_)));
    }

    #[test]
    fn test_mqtt_decoder_v5_shortcut() {
        let decoder = MqttDecoder::v5();
        assert!(matches!(decoder.inner, MqttCodec::V5(_)));
    }

    #[test]
    fn test_mqtt_encoder_new_v3() {
        let encoder = MqttEncoder::new(ProtocolVersion::MQTT3);
        assert!(matches!(encoder.inner, MqttCodec::V3(_)));
    }

    #[test]
    fn test_mqtt_encoder_new_v5() {
        let encoder = MqttEncoder::new(ProtocolVersion::MQTT5);
        assert!(matches!(encoder.inner, MqttCodec::V5(_)));
    }

    #[test]
    fn test_encode_decode_pingreq_v3() {
        let mut encoder = MqttEncoder::v3();
        let mut decoder = MqttDecoder::v3();

        // Encode a PINGREQ
        let mut buf = BytesMut::new();
        let packet = MqttPacket::V3(PacketV3::PingRequest);
        MonoioEncoder::encode(&mut encoder, packet, &mut buf).unwrap();

        // Should have encoded bytes
        assert!(!buf.is_empty());

        // Decode it back
        let decoded = MonoioDecoder::decode(&mut decoder, &mut buf).unwrap();
        match decoded {
            Decoded::Some((MqttPacket::V3(PacketV3::PingRequest), _)) => {}
            _ => panic!("Expected PINGREQ, got {decoded:?}"),
        }
    }

    #[test]
    fn test_encode_decode_pingresp_v3() {
        let mut encoder = MqttEncoder::v3();
        let mut decoder = MqttDecoder::v3();

        let mut buf = BytesMut::new();
        let packet = MqttPacket::V3(PacketV3::PingResponse);
        MonoioEncoder::encode(&mut encoder, packet, &mut buf).unwrap();

        let decoded = MonoioDecoder::decode(&mut decoder, &mut buf).unwrap();
        match decoded {
            Decoded::Some((MqttPacket::V3(PacketV3::PingResponse), _)) => {}
            _ => panic!("Expected PINGRESP, got {decoded:?}"),
        }
    }

    #[test]
    fn test_encode_decode_connack_v3() {
        let mut encoder = MqttEncoder::v3();
        let mut decoder = MqttDecoder::v3();

        let mut buf = BytesMut::new();
        let packet = MqttPacket::V3(PacketV3::ConnectAck(ConnectAck {
            session_present: false,
            return_code: ConnectAckReason::ConnectionAccepted,
        }));
        MonoioEncoder::encode(&mut encoder, packet, &mut buf).unwrap();

        let decoded = MonoioDecoder::decode(&mut decoder, &mut buf).unwrap();
        match decoded {
            Decoded::Some((MqttPacket::V3(PacketV3::ConnectAck(ack)), _)) => {
                assert!(!ack.session_present);
                assert!(matches!(
                    ack.return_code,
                    ConnectAckReason::ConnectionAccepted
                ));
            }
            _ => panic!("Expected CONNACK, got {decoded:?}"),
        }
    }

    #[test]
    fn test_decode_insufficient_data() {
        let mut decoder = MqttDecoder::v3();
        let mut buf = BytesMut::new();

        // Empty buffer should return Insufficient
        let result = MonoioDecoder::decode(&mut decoder, &mut buf).unwrap();
        assert!(matches!(result, Decoded::Insufficient));
    }

    #[test]
    fn test_decode_partial_packet() {
        let mut decoder = MqttDecoder::v3();
        let mut buf = BytesMut::new();

        // Only put the first byte of a PINGREQ (0xC0)
        buf.extend_from_slice(&[0xC0]);

        // Should return Insufficient
        let result = MonoioDecoder::decode(&mut decoder, &mut buf).unwrap();
        assert!(matches!(result, Decoded::Insufficient));
    }

    #[test]
    fn test_encode_v5_connack_client_identifier_not_valid() {
        use rmqtt_codec::types::QoS;
        use rmqtt_codec::v5::{ConnectAck, ConnectAckReason};
        let ack = ConnectAck {
            reason_code: ConnectAckReason::ClientIdentifierNotValid,
            session_present: false,
            session_expiry_interval_secs: Some(0),
            max_qos: QoS::AtMostOnce,
            retain_available: true,
            wildcard_subscription_available: false,
            subscription_identifiers_available: false,
            shared_subscription_available: false,
            ..Default::default()
        };
        let mut encoder = MqttEncoder::v5();
        let mut buf = BytesMut::new();
        let pkt = MqttPacket::V5(PacketV5::ConnectAck(Box::new(ack)));
        MonoioEncoder::encode(&mut encoder, pkt, &mut buf).expect("encode");
        eprintln!("v5 connack bytes ({}): {:02x?}", buf.len(), &buf[..]);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_v3_connack_identifier_rejected() {
        use rmqtt_codec::v3::ConnectAckReason;
        let ack = super::ConnectAck {
            return_code: ConnectAckReason::IdentifierRejected,
            session_present: false,
        };
        let mut encoder = MqttEncoder::v3();
        let mut buf = BytesMut::new();
        let pkt = MqttPacket::V3(PacketV3::ConnectAck(ack));
        MonoioEncoder::encode(&mut encoder, pkt, &mut buf).expect("encode");
        eprintln!("v3 connack bytes ({}): {:02x?}", buf.len(), &buf[..]);
        assert_eq!(buf.len(), 4);
    }
}
