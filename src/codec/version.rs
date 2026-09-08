use bytes::BytesMut;
use monoio_codec::{Decoded, Decoder};
use std::io;
use tokio_util::codec::Decoder as TokioDecoder;

pub use rmqtt_codec::version::ProtocolVersion;
use rmqtt_codec::version::VersionCodec;

/// Wrapper around rmqtt-codec's VersionCodec for monoio-codec compatibility.
///
/// Detects MQTT protocol version from the initial CONNECT packet.
pub struct VersionDecoder {
    inner: VersionCodec,
}

impl VersionDecoder {
    pub fn new() -> Self {
        Self {
            inner: VersionCodec,
        }
    }
}

impl Default for VersionDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for VersionDecoder {
    type Item = ProtocolVersion;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Decoded<Self::Item>, Self::Error> {
        match TokioDecoder::decode(&mut self.inner, src) {
            Ok(Some(version)) => Ok(Decoded::Some(version)),
            Ok(None) => Ok(Decoded::Insufficient),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use monoio_codec::Decoder as MonoioDecoder;

    #[test]
    fn test_version_decoder_new() {
        let _decoder = VersionDecoder::new();
        // Just verify it creates without panicking
    }

    #[test]
    fn test_version_decoder_default() {
        let _decoder = VersionDecoder::default();
    }

    #[test]
    fn test_decode_empty_buffer_returns_insufficient() {
        let mut decoder = VersionDecoder::new();
        let mut buf = BytesMut::new();

        let result = MonoioDecoder::decode(&mut decoder, &mut buf).unwrap();
        assert!(matches!(result, Decoded::Insufficient));
    }

    #[test]
    fn test_decode_mqtt311_connect() {
        let mut decoder = VersionDecoder::new();
        let mut buf = BytesMut::new();

        // MQTT 3.1.1 CONNECT packet (minimal)
        // Fixed header: 0x10 (CONNECT), remaining length
        // Variable header: Protocol Name "MQTT", Protocol Level 4
        buf.extend_from_slice(&[
            0x10, // CONNECT packet type
            0x10, // Remaining length (16 bytes)
            0x00, 0x04, // Protocol name length
            b'M', b'Q', b'T', b'T', // Protocol name
            0x04, // Protocol level (4 = MQTT 3.1.1)
            0x00, // Connect flags
            0x00, 0x3C, // Keep alive (60 seconds)
            0x00, 0x04, // Client ID length
            b't', b'e', b's', b't', // Client ID "test"
        ]);

        let result = MonoioDecoder::decode(&mut decoder, &mut buf).unwrap();
        match result {
            Decoded::Some(version) => {
                assert!(matches!(version, ProtocolVersion::MQTT3));
            }
            _ => panic!("Expected MQTT3 version, got {result:?}"),
        }
    }

    #[test]
    fn test_decode_mqtt5_connect() {
        let mut decoder = VersionDecoder::new();
        let mut buf = BytesMut::new();

        // MQTT 5.0 CONNECT packet (minimal)
        buf.extend_from_slice(&[
            0x10, // CONNECT packet type
            0x11, // Remaining length (17 bytes)
            0x00, 0x04, // Protocol name length
            b'M', b'Q', b'T', b'T', // Protocol name
            0x05, // Protocol level (5 = MQTT 5.0)
            0x00, // Connect flags
            0x00, 0x3C, // Keep alive (60 seconds)
            0x00, // Properties length (0)
            0x00, 0x04, // Client ID length
            b't', b'e', b's', b't', // Client ID "test"
        ]);

        let result = MonoioDecoder::decode(&mut decoder, &mut buf).unwrap();
        match result {
            Decoded::Some(version) => {
                assert!(matches!(version, ProtocolVersion::MQTT5));
            }
            _ => panic!("Expected MQTT5 version, got {result:?}"),
        }
    }

    #[test]
    fn test_decode_partial_connect_returns_insufficient() {
        let mut decoder = VersionDecoder::new();
        let mut buf = BytesMut::new();

        // Only partial CONNECT header
        buf.extend_from_slice(&[0x10, 0x10, 0x00, 0x04]);

        let result = MonoioDecoder::decode(&mut decoder, &mut buf).unwrap();
        assert!(matches!(result, Decoded::Insufficient));
    }

    #[test]
    fn test_protocol_version_debug() {
        let v3 = ProtocolVersion::MQTT3;
        let v5 = ProtocolVersion::MQTT5;

        let v3_str = format!("{v3:?}");
        let v5_str = format!("{v5:?}");

        assert!(v3_str.contains("MQTT3"));
        assert!(v5_str.contains("MQTT5"));
    }

    #[test]
    fn rejected_first_byte_error_carries_typed_decode_error_source() {
        let mut decoder = VersionDecoder::new();
        let mut buf = BytesMut::from(&[0xC0_u8, 0x00][..]);

        let err = MonoioDecoder::decode(&mut decoder, &mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        let source = err
            .get_ref()
            .and_then(|s| s.downcast_ref::<rmqtt_codec::error::DecodeError>());
        assert!(
            matches!(
                source,
                Some(rmqtt_codec::error::DecodeError::UnsupportedPacketType)
            ),
            "expected originating UnsupportedPacketType variant, got {source:?}"
        );
    }

    #[test]
    fn invalid_protocol_name_error_carries_invalid_protocol_source() {
        let mut decoder = VersionDecoder::new();
        let mut buf = BytesMut::from(
            &[
                0x10_u8, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'X', 0x04, 0x00, 0x00, 0x3C, 0x00,
                0x04, b't', b'e', b's', b't',
            ][..],
        );

        let err = MonoioDecoder::decode(&mut decoder, &mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        let source = err
            .get_ref()
            .and_then(|s| s.downcast_ref::<rmqtt_codec::error::DecodeError>());
        assert!(
            matches!(
                source,
                Some(rmqtt_codec::error::DecodeError::InvalidProtocol)
            ),
            "expected originating InvalidProtocol variant, got {source:?}"
        );
    }
}
