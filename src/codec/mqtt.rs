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

/// The inbound packet-size bound: the largest MQTT packet, in total wire bytes
/// (fixed header + variable header + payload), the server reads from a client.
/// Enforced from the fixed header before any body-sized allocation, and
/// advertised as the v5 CONNACK Maximum Packet Size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxInboundPacketSize(u32);

/// Smallest bound whose derived Remaining Length limit is non-zero
/// (rmqtt-codec reads a limit of 0 as "unlimited").
const MIN_INBOUND_PACKET_SIZE: u32 = 3;
/// Largest Remaining Length MQTT can encode (four varint bytes).
const MAX_REMAINING_LENGTH: u32 = 268_435_455;
/// Largest packet MQTT can frame: 1 type byte + 4 length bytes + MAX_REMAINING_LENGTH.
const MAX_INBOUND_PACKET_SIZE: u32 = 268_435_460;
/// Payload bits per Remaining Length varint byte.
const VARINT_PAYLOAD_BITS: u32 = 7;
/// Payload mask and continuation bit of a Remaining Length varint byte.
const VARINT_PAYLOAD_MASK: u8 = 0x7F;
const VARINT_CONTINUATION_BIT: u8 = 0x80;
/// Most bytes a Remaining Length varint may use.
const MAX_VARINT_BYTES: usize = 4;

/// Length in bytes of the varint that encodes `r` (1 to 4).
fn varint_len(r: u32) -> u32 {
    let mut len = 1;
    let mut rest = r >> VARINT_PAYLOAD_BITS;
    while rest > 0 {
        len += 1;
        rest >>= VARINT_PAYLOAD_BITS;
    }
    len
}

impl MaxInboundPacketSize {
    /// 128 KiB.
    pub const DEFAULT: Self = Self(131_072);

    /// `None` below [`MIN_INBOUND_PACKET_SIZE`] or above [`MAX_INBOUND_PACKET_SIZE`].
    /// The lower floor is 3 bytes because the rmqtt-codec limit it accepts
    /// is `r` from `MqttDecoder::new` (≥ 0); it reads `0` as "unlimited", so
    /// any `r` derived from a smaller bound would invite the very allocation
    /// the bound is meant to cap.
    pub const fn new(bytes: u32) -> Option<Self> {
        if bytes < MIN_INBOUND_PACKET_SIZE || bytes > MAX_INBOUND_PACKET_SIZE {
            None
        } else {
            Some(Self(bytes))
        }
    }

    /// The bound, in total wire bytes.
    pub const fn get(self) -> u32 {
        self.0
    }

    /// Largest Remaining Length `r` with `1 + varint_len(r) + r <= self.get()`; never 0.
    pub(crate) fn remaining_length_limit(self) -> u32 {
        let mut r = (self.0 - 2).min(MAX_REMAINING_LENGTH);
        while 1 + varint_len(r) + r > self.0 {
            r -= 1;
        }
        r
    }

    /// `true` when `src` starts with a complete fixed header whose packet,
    /// counted at the Remaining Length's actual encoded width, exceeds the
    /// bound. `false` while the header is incomplete, and for a varint longer
    /// than 4 bytes (rmqtt-codec rejects that as `InvalidLength`).
    pub(crate) fn rejects_fixed_header(self, src: &[u8]) -> bool {
        let mut remaining_length: u32 = 0;
        #[allow(clippy::cast_possible_truncation)] // i < MAX_VARINT_BYTES by the `take`
        for (i, &byte) in src.iter().skip(1).take(MAX_VARINT_BYTES).enumerate() {
            remaining_length |=
                u32::from(byte & VARINT_PAYLOAD_MASK) << (VARINT_PAYLOAD_BITS * i as u32);
            if byte & VARINT_CONTINUATION_BIT == 0 {
                #[allow(clippy::cast_possible_truncation)] // i < MAX_VARINT_BYTES by the `take`
                let header_len = 2 + i as u64;
                return header_len + u64::from(remaining_length) > u64::from(self.0);
            }
        }
        false
    }
}

/// Wrapper around rmqtt-codec's MqttCodec for monoio-codec compatibility.
///
/// Handles encoding/decoding of MQTT v3.1.1 and v5.0 packets.
pub struct MqttDecoder {
    inner: MqttCodec,
    max_inbound_packet_size: MaxInboundPacketSize,
    /// The buffer starts at a fixed header: true initially and after every
    /// decoded packet, false after a call that consumed a header and returned
    /// `Insufficient` while the body is still arriving.
    at_frame_start: bool,
}

impl MqttDecoder {
    /// Create a new decoder for the specified MQTT protocol version, bounded
    /// to `max_inbound_packet_size` total wire bytes per packet.
    pub fn new(version: ProtocolVersion, max_inbound_packet_size: MaxInboundPacketSize) -> Self {
        let limit = max_inbound_packet_size.remaining_length_limit();
        let inner = match version {
            ProtocolVersion::MQTT3 => MqttCodec::V3(V3Codec::new(limit)),
            ProtocolVersion::MQTT5 => MqttCodec::V5(V5Codec::new(limit, 0)),
        };
        Self {
            inner,
            max_inbound_packet_size,
            at_frame_start: true,
        }
    }

    /// Create a decoder for MQTT v3.1.1, bounded only by the largest packet
    /// MQTT can frame: tests use it to read the server's own output.
    #[cfg(test)]
    pub fn v3() -> Self {
        Self::new(
            ProtocolVersion::MQTT3,
            MaxInboundPacketSize(MAX_INBOUND_PACKET_SIZE),
        )
    }

    /// Create a decoder for MQTT v5.0, bounded only by the largest packet
    /// MQTT can frame: tests use it to read the server's own output.
    #[cfg(test)]
    pub fn v5() -> Self {
        Self::new(
            ProtocolVersion::MQTT5,
            MaxInboundPacketSize(MAX_INBOUND_PACKET_SIZE),
        )
    }
}

impl Decoder for MqttDecoder {
    type Item = (MqttPacket, u32);
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Decoded<Self::Item>, Self::Error> {
        if self.at_frame_start && self.max_inbound_packet_size.rejects_fixed_header(src) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                DecodeError::MaxSizeExceeded,
            ));
        }
        let before = src.len();
        match TokioDecoder::decode(&mut self.inner, src) {
            Ok(Some(packet)) => {
                self.at_frame_start = true;
                Ok(Decoded::Some(packet))
            }
            Ok(None) => {
                if src.len() < before {
                    self.at_frame_start = false;
                }
                Ok(Decoded::Insufficient)
            }
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
        let decoder = MqttDecoder::new(ProtocolVersion::MQTT3, MaxInboundPacketSize::DEFAULT);
        // Just verify it creates without panicking
        assert!(matches!(decoder.inner, MqttCodec::V3(_)));
    }

    #[test]
    fn test_mqtt_decoder_new_v5() {
        let decoder = MqttDecoder::new(ProtocolVersion::MQTT5, MaxInboundPacketSize::DEFAULT);
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

    /// AC-1, AC-2 — `MaxInboundPacketSize::new` rejects everything below 3 or
    /// above 268 435 460, and `get()` round-trips every accepted value.
    #[test]
    fn inbound_bound_accepts_only_the_framable_range() {
        assert_eq!(MaxInboundPacketSize::new(0), None);
        assert_eq!(MaxInboundPacketSize::new(2), None);
        assert_eq!(MaxInboundPacketSize::new(268_435_461), None);

        let lo = MaxInboundPacketSize::new(3).expect("3 in range");
        assert_eq!(lo.get(), 3);

        let default = MaxInboundPacketSize::new(131_072).expect("131072 in range");
        assert_eq!(default.get(), 131_072);

        let hi = MaxInboundPacketSize::new(268_435_460).expect("max in range");
        assert_eq!(hi.get(), 268_435_460);
    }

    /// (bound at which the packet is exactly full, Remaining Length varint
    /// bytes, Remaining Length) for each Remaining Length encoding width 1-4.
    const WIDTH_CASES: [(u32, &[u8], u32); 4] = [
        (129, &[0x7F], 127),
        (16_386, &[0xFF, 0x7F], 16_383),
        (2_097_155, &[0xFF, 0xFF, 0x7F], 2_097_151),
        (2_097_157, &[0x80, 0x80, 0x80, 0x01], 2_097_152),
    ];
    const HEADER_ONLY_CAPACITY: usize = 16;

    /// AC-4 — the largest Remaining Length that still fits the bound.
    #[test]
    fn inbound_bound_derives_the_largest_remaining_length_that_fits() {
        let cases: [(u32, u32); 12] = [
            (3, 1),
            (128, 126),
            (129, 127),
            (130, 127),
            (131, 128),
            (16_385, 16_382),
            (16_386, 16_383),
            (2_097_155, 2_097_151),
            (2_097_156, 2_097_151),
            (2_097_157, 2_097_152),
            (131_072, 131_068),
            (268_435_460, 268_435_455),
        ];
        for (bound, expected) in cases {
            let size = MaxInboundPacketSize::new(bound).expect("in range");
            assert_eq!(size.remaining_length_limit(), expected, "bound {bound}");
        }
    }

    fn assert_max_size_exceeded(err: &io::Error) {
        assert!(matches!(
            err.get_ref().and_then(|s| s.downcast_ref::<DecodeError>()),
            Some(DecodeError::MaxSizeExceeded)
        ));
    }

    /// AC-5 — a fixed header declaring one byte over the bound is rejected
    /// before the body-sized buffer is reserved, for every Remaining Length
    /// width, the maximum bound, and a non-minimal encoding.
    #[test]
    fn decoder_rejects_one_byte_over_the_bound_from_the_fixed_header() {
        for version in [ProtocolVersion::MQTT3, ProtocolVersion::MQTT5] {
            for (bound, varint, remaining_length) in WIDTH_CASES {
                let max = MaxInboundPacketSize::new(bound - 1).expect("in range");
                let mut decoder = MqttDecoder::new(version, max);
                let mut buf = BytesMut::with_capacity(HEADER_ONLY_CAPACITY);
                buf.extend_from_slice(&[0x30]);
                buf.extend_from_slice(varint);

                let err = MonoioDecoder::decode(&mut decoder, &mut buf)
                    .expect_err("over-bound header must be rejected");
                assert_max_size_exceeded(&err);
                assert!(buf.capacity() < remaining_length as usize);
            }

            let default = MaxInboundPacketSize::DEFAULT;
            let mut decoder = MqttDecoder::new(version, default);
            let mut buf = BytesMut::with_capacity(HEADER_ONLY_CAPACITY);
            buf.extend_from_slice(&[0x30, 0xFF, 0xFF, 0xFF, 0x7F]);
            let remaining_length: usize = 268_435_455;
            let err = MonoioDecoder::decode(&mut decoder, &mut buf)
                .expect_err("over-bound header must be rejected");
            assert_max_size_exceeded(&err);
            assert!(buf.capacity() < remaining_length);

            let bound64 = MaxInboundPacketSize::new(64).expect("in range");
            let mut decoder = MqttDecoder::new(version, bound64);
            let mut buf = BytesMut::with_capacity(HEADER_ONLY_CAPACITY);
            buf.extend_from_slice(&[0x30, 0xBE, 0x00]);
            let err = MonoioDecoder::decode(&mut decoder, &mut buf)
                .expect_err("non-minimal over-bound header must be rejected");
            assert_max_size_exceeded(&err);
            assert!(buf.capacity() <= HEADER_ONLY_CAPACITY);
        }
    }

    /// AC-6 — a packet whose total wire size equals the bound decodes, for
    /// every Remaining Length encoding width 1-4.
    #[test]
    fn decoder_accepts_a_packet_exactly_at_the_bound_for_every_length_width() {
        for (bound, varint, remaining_length) in WIDTH_CASES {
            let max = MaxInboundPacketSize::new(bound).expect("in range");

            let mut decoder = MqttDecoder::new(ProtocolVersion::MQTT3, max);
            let mut buf = BytesMut::new();
            buf.extend_from_slice(&[0x30]);
            buf.extend_from_slice(varint);
            buf.extend_from_slice(&[0x00, 0x01, b't']);
            buf.resize(buf.len() + (remaining_length as usize - 3), 0);
            match MonoioDecoder::decode(&mut decoder, &mut buf).expect("decode") {
                Decoded::Some((MqttPacket::V3(PacketV3::Publish(p)), _)) => {
                    assert_eq!(p.payload.len(), remaining_length as usize - 3);
                }
                other => panic!("expected v3 PUBLISH at bound {bound}, got {other:?}"),
            }

            let mut decoder = MqttDecoder::new(ProtocolVersion::MQTT5, max);
            let mut buf = BytesMut::new();
            buf.extend_from_slice(&[0x30]);
            buf.extend_from_slice(varint);
            buf.extend_from_slice(&[0x00, 0x01, b't', 0x00]);
            buf.resize(buf.len() + (remaining_length as usize - 4), 0);
            match MonoioDecoder::decode(&mut decoder, &mut buf).expect("decode") {
                Decoded::Some((MqttPacket::V5(PacketV5::Publish(p)), _)) => {
                    assert_eq!(p.payload.len(), remaining_length as usize - 4);
                }
                other => panic!("expected v5 PUBLISH at bound {bound}, got {other:?}"),
            }
        }
    }

    /// AC-5 — the header check re-arms on every frame boundary: a split read
    /// mid-body must not be mistaken for a new fixed header, and the header
    /// of the packet after a decoded frame is checked again.
    #[test]
    fn decoder_checks_every_frame_header_across_split_reads() {
        const SPLIT_BOUND: u32 = 256;

        for version in [ProtocolVersion::MQTT3, ProtocolVersion::MQTT5] {
            let max = MaxInboundPacketSize::new(SPLIT_BOUND).expect("in range");
            let mut decoder = MqttDecoder::new(version, max);
            let mut buf = BytesMut::new();

            buf.extend_from_slice(&[0x30, 0xFD, 0x01]);
            let result = MonoioDecoder::decode(&mut decoder, &mut buf).expect("decode");
            assert!(matches!(result, Decoded::Insufficient));

            buf.extend_from_slice(&[0x00, 0x80, b't']);
            let result = MonoioDecoder::decode(&mut decoder, &mut buf).expect("decode");
            assert!(matches!(result, Decoded::Insufficient));

            let mut rest: Vec<u8> = vec![b't'; 127];
            match version {
                ProtocolVersion::MQTT3 => rest.extend(std::iter::repeat_n(0u8, 123)),
                ProtocolVersion::MQTT5 => {
                    rest.push(0x00);
                    rest.extend(std::iter::repeat_n(0u8, 122));
                }
            }
            buf.extend_from_slice(&rest);
            let result = MonoioDecoder::decode(&mut decoder, &mut buf).expect("decode");
            let expected_topic = vec![b't'; 128];
            match (version, result) {
                (
                    ProtocolVersion::MQTT3,
                    Decoded::Some((MqttPacket::V3(PacketV3::Publish(p)), _)),
                ) => {
                    assert_eq!(p.topic.as_bytes(), expected_topic.as_slice());
                    assert_eq!(p.payload.len(), 123);
                }
                (
                    ProtocolVersion::MQTT5,
                    Decoded::Some((MqttPacket::V5(PacketV5::Publish(p)), _)),
                ) => {
                    assert_eq!(p.topic.as_bytes(), expected_topic.as_slice());
                    assert_eq!(p.payload.len(), 122);
                }
                (v, other) => panic!("expected PUBLISH for {v:?}, got {other:?}"),
            }

            buf.extend_from_slice(&[0x30, 0xFD, 0x81, 0x00]);
            let err = MonoioDecoder::decode(&mut decoder, &mut buf)
                .expect_err("re-armed header check must reject non-minimal over-bound header");
            assert_max_size_exceeded(&err);
        }
    }

    /// AC-5 — a Remaining Length varint longer than four bytes cannot smuggle
    /// a packet past the bound. `rejects_fixed_header` reads at most four
    /// varint bytes and reports no rejection for such a header, so the
    /// decoder's own rejection (`InvalidLength`) is what must close it — still
    /// before any body-sized reservation.
    #[test]
    fn decoder_rejects_an_over_long_remaining_length_varint_without_reserving() {
        /// Type byte plus five continuation-flagged varint bytes: more than
        /// the four bytes MQTT allows for a Remaining Length.
        const OVER_LONG_VARINT_HEADER: [u8; 5] = [0x30, 0xFF, 0xFF, 0xFF, 0xFF];

        for version in [ProtocolVersion::MQTT3, ProtocolVersion::MQTT5] {
            let mut decoder = MqttDecoder::new(version, MaxInboundPacketSize::DEFAULT);
            let mut buf = BytesMut::with_capacity(HEADER_ONLY_CAPACITY);
            buf.extend_from_slice(&OVER_LONG_VARINT_HEADER);

            let err = MonoioDecoder::decode(&mut decoder, &mut buf)
                .expect_err("an over-long Remaining Length varint must be rejected");
            assert!(
                matches!(
                    err.get_ref().and_then(|s| s.downcast_ref::<DecodeError>()),
                    Some(DecodeError::InvalidLength)
                ),
                "expected InvalidLength for {version:?}, got {err:?}"
            );
            assert!(buf.capacity() <= HEADER_ONLY_CAPACITY);
        }
    }

    /// AC-6 — a non-minimal Remaining Length encoding whose total wire size is
    /// exactly the bound is accepted. The exact header check and the derived
    /// rmqtt-codec limit must agree at the boundary, so no honest client is
    /// closed for its encoding width alone.
    #[test]
    fn decoder_accepts_a_non_minimal_remaining_length_exactly_at_the_bound() {
        /// Bound the non-minimal rejection case above also uses.
        const NON_MINIMAL_BOUND: u32 = 64;
        /// Remaining Length 61 in two bytes: 1 + 2 + 61 = 64 total wire bytes.
        const NON_MINIMAL_HEADER: [u8; 3] = [0x30, 0xBD, 0x00];
        /// Remaining Length the header declares.
        const NON_MINIMAL_REMAINING_LENGTH: usize = 61;

        let max = MaxInboundPacketSize::new(NON_MINIMAL_BOUND).expect("in range");

        let mut decoder = MqttDecoder::new(ProtocolVersion::MQTT3, max);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&NON_MINIMAL_HEADER);
        buf.extend_from_slice(&[0x00, 0x01, b't']);
        buf.resize(buf.len() + (NON_MINIMAL_REMAINING_LENGTH - 3), 0);
        match MonoioDecoder::decode(&mut decoder, &mut buf).expect("decode") {
            Decoded::Some((MqttPacket::V3(PacketV3::Publish(p)), _)) => {
                assert_eq!(p.payload.len(), NON_MINIMAL_REMAINING_LENGTH - 3);
            }
            other => panic!("expected v3 PUBLISH at the bound, got {other:?}"),
        }

        let mut decoder = MqttDecoder::new(ProtocolVersion::MQTT5, max);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&NON_MINIMAL_HEADER);
        buf.extend_from_slice(&[0x00, 0x01, b't', 0x00]);
        buf.resize(buf.len() + (NON_MINIMAL_REMAINING_LENGTH - 4), 0);
        match MonoioDecoder::decode(&mut decoder, &mut buf).expect("decode") {
            Decoded::Some((MqttPacket::V5(PacketV5::Publish(p)), _)) => {
                assert_eq!(p.payload.len(), NON_MINIMAL_REMAINING_LENGTH - 4);
            }
            other => panic!("expected v5 PUBLISH at the bound, got {other:?}"),
        }
    }

    /// AC-4, AC-5, AC-6 — the smallest valid bound still bounds. Its derived
    /// Remaining Length limit is 1, never the rmqtt-codec "unlimited"
    /// sentinel 0, so a 2-byte PINGREQ decodes and the next header declaring a
    /// 4-byte packet is rejected.
    #[test]
    fn decoder_enforces_the_bound_at_its_smallest_valid_value() {
        let max = MaxInboundPacketSize::new(MIN_INBOUND_PACKET_SIZE).expect("in range");
        assert_eq!(max.remaining_length_limit(), 1);

        let mut decoder = MqttDecoder::new(ProtocolVersion::MQTT3, max);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[0xC0, 0x00]);
        match MonoioDecoder::decode(&mut decoder, &mut buf).expect("decode") {
            Decoded::Some((MqttPacket::V3(PacketV3::PingRequest), _)) => {}
            other => panic!("expected v3 PINGREQ under the smallest bound, got {other:?}"),
        }

        buf.extend_from_slice(&[0x30, 0x02]);
        let err = MonoioDecoder::decode(&mut decoder, &mut buf)
            .expect_err("a 4-byte packet must be rejected under a 3-byte bound");
        assert_max_size_exceeded(&err);
    }

    /// AC-5 — the bound is judged only once the fixed header is complete. A
    /// header arriving one byte at a time must not be closed as over-bound
    /// while its Remaining Length varint is unfinished, and the check must
    /// still fire on the byte that completes it.
    #[test]
    fn decoder_waits_for_a_complete_fixed_header_before_judging_the_bound() {
        /// Bound every piece below is measured against.
        const PARTIAL_HEADER_BOUND: u32 = 64;

        for version in [ProtocolVersion::MQTT3, ProtocolVersion::MQTT5] {
            let max = MaxInboundPacketSize::new(PARTIAL_HEADER_BOUND).expect("in range");
            let mut decoder = MqttDecoder::new(version, max);
            let mut buf = BytesMut::with_capacity(HEADER_ONLY_CAPACITY);

            buf.extend_from_slice(&[0x30]);
            let result = MonoioDecoder::decode(&mut decoder, &mut buf).expect("decode");
            assert!(
                matches!(result, Decoded::Insufficient),
                "a type byte alone must wait, got {result:?}"
            );

            buf.extend_from_slice(&[0xFF]);
            let result = MonoioDecoder::decode(&mut decoder, &mut buf).expect("decode");
            assert!(
                matches!(result, Decoded::Insufficient),
                "an unfinished Remaining Length varint must wait, got {result:?}"
            );

            // 0xFF 0xFF 0x7F completes the varint at Remaining Length
            // 2 097 151 — far over the bound.
            buf.extend_from_slice(&[0xFF, 0x7F]);
            let err = MonoioDecoder::decode(&mut decoder, &mut buf)
                .expect_err("the completed over-bound header must be rejected");
            assert_max_size_exceeded(&err);
            assert!(buf.capacity() <= HEADER_ONLY_CAPACITY);
        }
    }
}
