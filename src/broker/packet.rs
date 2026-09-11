//! Pure packet-decision seam. No IO, no `tracing`, no time.
//!
//! `dispatch` maps every variant of `MqttPacket` to a `Disposition` without a
//! catch-all arm, so a new packet type is a compile error here — exactly the
//! regression guard the task's expand → migrate → contract shape exists to
//! provide. `Reply::render` is the only constructor of an outbound packet in
//! the packet loop, called with the codec that will encode it, so the
//! cross-version `EncodeError::MalformedPacket` is unreachable by construction.

use crate::codec::mqtt::{MqttEncoder, MqttPacket, PacketV3, PacketV5};
use crate::codec::version::ProtocolVersion;
use bytes::BytesMut;
use monoio_codec::Encoder as _;
use rmqtt_codec::types::QoS;
use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};

/// A reply the packet loop owes the client, stated without a protocol version.
/// Rendered once, at the send site, from the codec that will encode it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reply {
    PingResponse,
    PublishAck(NonZeroU16),
    /// Packet id + requested-filter count. `NonZeroUsize` because a SUBACK
    /// with an empty payload is malformed and must not be constructible.
    SubscribeRefusal(NonZeroU16, NonZeroUsize),
    /// Packet id + requested-filter count. v3 UNSUBACK carries no per-filter
    /// status, so the v3 renderer discards the count.
    UnsubscribeAck(NonZeroU16, NonZeroUsize),
}

impl Reply {
    /// The `bounded_send` timeout context for this reply's flush. Returns the
    /// `super::handler::TIMEOUT_CTX_*` constant itself — never a copy of its
    /// text — so the wording lives in exactly one place.
    pub(crate) fn timeout_context(self) -> &'static str {
        match self {
            Reply::PingResponse => super::handler::TIMEOUT_CTX_PINGRESP_FLUSH,
            Reply::PublishAck(_) => super::handler::TIMEOUT_CTX_PUBACK_FLUSH,
            Reply::SubscribeRefusal(_, _) => super::handler::TIMEOUT_CTX_SUBACK_FLUSH,
            Reply::UnsubscribeAck(_, _) => super::handler::TIMEOUT_CTX_UNSUBACK_FLUSH,
        }
    }

    /// Wire size of this reply in bytes, or `None` when it cannot be encoded
    /// at all. Measured by encoding the rendered packet rather than derived
    /// from the packet layout, so the size and the bytes the send site writes
    /// can never drift apart.
    fn encoded_len(self, version: ProtocolVersion) -> Option<usize> {
        encoded_len(self.render(version), version)
    }

    /// The only constructor of an outbound packet in the packet loop.
    pub(crate) fn render(self, version: ProtocolVersion) -> MqttPacket {
        match (self, version) {
            (Reply::PingResponse, ProtocolVersion::MQTT3) => MqttPacket::V3(PacketV3::PingResponse),
            (Reply::PingResponse, ProtocolVersion::MQTT5) => MqttPacket::V5(PacketV5::PingResponse),
            (Reply::PublishAck(packet_id), ProtocolVersion::MQTT3) => {
                MqttPacket::V3(PacketV3::PublishAck { packet_id })
            }
            (Reply::PublishAck(packet_id), ProtocolVersion::MQTT5) => {
                MqttPacket::V5(PacketV5::PublishAck(rmqtt_codec::v5::PublishAck {
                    packet_id,
                    reason_code: rmqtt_codec::v5::PublishAckReason::Success,
                    properties: Vec::new(),
                    reason_string: None,
                }))
            }
            (Reply::SubscribeRefusal(packet_id, filters), ProtocolVersion::MQTT3) => {
                let filters = filters.get();
                MqttPacket::V3(PacketV3::SubscribeAck {
                    packet_id,
                    status: vec![rmqtt_codec::v3::SubscribeReturnCode::Failure; filters],
                })
            }
            (Reply::SubscribeRefusal(packet_id, filters), ProtocolVersion::MQTT5) => {
                let filters = filters.get();
                MqttPacket::V5(PacketV5::SubscribeAck(rmqtt_codec::v5::SubscribeAck {
                    packet_id,
                    properties: Vec::new(),
                    reason_string: None,
                    status: vec![
                        rmqtt_codec::v5::SubscribeAckReason::ImplementationSpecificError;
                        filters
                    ],
                }))
            }
            (Reply::UnsubscribeAck(packet_id, _filters), ProtocolVersion::MQTT3) => {
                MqttPacket::V3(PacketV3::UnsubscribeAck { packet_id })
            }
            (Reply::UnsubscribeAck(packet_id, filters), ProtocolVersion::MQTT5) => {
                let filters = filters.get();
                MqttPacket::V5(PacketV5::UnsubscribeAck(rmqtt_codec::v5::UnsubscribeAck {
                    packet_id,
                    properties: Vec::new(),
                    reason_string: None,
                    status: vec![
                        rmqtt_codec::v5::UnsubscribeAckReason::NoSubscriptionExisted;
                        filters
                    ],
                }))
            }
        }
    }
}

/// A protocol violation: the connection closes and the session reports
/// `SessionOutcome::Violation`. Added one variant at a time by the task that
/// uses it (CI denies `dead_code`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Violation {
    /// A second CONNECT on an established session.
    DuplicateConnect,
    /// PUBLISH at QoS >= 1 with no packet id. Not producible by the shipped
    /// decoders; policied rather than unwrapped.
    PublishMissingPacketId,
    /// SUBSCRIBE / UNSUBSCRIBE whose topic-filter list is empty.
    EmptyTopicFilterList,
    /// The reply the request obliges the server to send is larger than the
    /// Maximum Packet Size the client declared it can receive.
    ReplyOverMaxPacketSize,
    /// CONNACK / SUBACK / UNSUBACK / PINGRESP arriving from a client.
    ServerOnlyPacket,
    /// PUBREC / PUBREL / PUBCOMP. F2.2 turns these into the receiver flow.
    Qos2FlowNotImplemented,
    /// PUBACK from a client — this server never publishes.
    PublishAckFromClient,
    /// v5 AUTH with no authentication method negotiated at CONNECT.
    AuthNotNegotiated,
    /// PUBLISH whose QoS exceeds `handshake::MAX_QOS`.
    PublishQosAboveMaximum,
    /// The framed stream yielded a decoder rejection mid-session.
    MalformedPacket,
    /// `MqttPacket::Version(_)`. Unreachable through `CodecPair`; closes
    /// loudly rather than panicking.
    VersionProbe,
}

impl Violation {
    /// Reason clause for the loop's single warn line. A `&'static str` literal
    /// per variant — no formatting on the adversarial path.
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Violation::DuplicateConnect => "protocol violation: duplicate CONNECT",
            Violation::PublishMissingPacketId => {
                "protocol violation: PUBLISH at QoS 1 or above without a packet id"
            }
            Violation::EmptyTopicFilterList => "protocol violation: empty topic filter list",
            Violation::ReplyOverMaxPacketSize => {
                "protocol violation: reply exceeds the client's maximum packet size"
            }
            Violation::ServerOnlyPacket => {
                "protocol violation: server-to-client packet from a client"
            }
            Violation::Qos2FlowNotImplemented => {
                "protocol violation: QoS 2 flow packet, receiver flow not implemented"
            }
            Violation::PublishAckFromClient => {
                "protocol violation: PUBACK from a client this server never publishes to"
            }
            Violation::AuthNotNegotiated => {
                "protocol violation: AUTH without a negotiated authentication method"
            }
            Violation::PublishQosAboveMaximum => {
                "protocol violation: PUBLISH above the advertised maximum QoS"
            }
            Violation::MalformedPacket => "protocol violation: malformed packet",
            Violation::VersionProbe => {
                "protocol violation: codec yielded a bare protocol-version item"
            }
        }
    }

    /// The reason code this violation closes with. `None` on MQTT 3.1.1,
    /// which has no server-sent DISCONNECT.
    fn disconnect_reason_code(
        self,
        version: ProtocolVersion,
    ) -> Option<rmqtt_codec::v5::DisconnectReasonCode> {
        match (self, version) {
            (
                Violation::DuplicateConnect
                | Violation::PublishMissingPacketId
                | Violation::EmptyTopicFilterList
                | Violation::ReplyOverMaxPacketSize
                | Violation::ServerOnlyPacket
                | Violation::Qos2FlowNotImplemented
                | Violation::PublishAckFromClient
                | Violation::AuthNotNegotiated
                | Violation::PublishQosAboveMaximum
                | Violation::MalformedPacket
                | Violation::VersionProbe,
                ProtocolVersion::MQTT3,
            ) => None,
            (
                Violation::DuplicateConnect
                | Violation::PublishMissingPacketId
                | Violation::EmptyTopicFilterList
                | Violation::ServerOnlyPacket
                | Violation::PublishAckFromClient
                | Violation::AuthNotNegotiated
                | Violation::VersionProbe,
                ProtocolVersion::MQTT5,
            ) => Some(rmqtt_codec::v5::DisconnectReasonCode::ProtocolError),
            (Violation::MalformedPacket, ProtocolVersion::MQTT5) => {
                Some(rmqtt_codec::v5::DisconnectReasonCode::MalformedPacket)
            }
            (Violation::ReplyOverMaxPacketSize, ProtocolVersion::MQTT5) => {
                Some(rmqtt_codec::v5::DisconnectReasonCode::PacketTooLarge)
            }
            (
                Violation::Qos2FlowNotImplemented | Violation::PublishQosAboveMaximum,
                ProtocolVersion::MQTT5,
            ) => Some(rmqtt_codec::v5::DisconnectReasonCode::QosNotSupported),
        }
    }

    /// The DISCONNECT the close sends, if any. `None` on MQTT 3.1.1, which has
    /// no server-sent DISCONNECT, and `None` when the DISCONNECT would itself
    /// exceed the client's declared Maximum Packet Size — the close must obey
    /// the same limit the `ReplyOverMaxPacketSize` policy enforces, so a client
    /// declaring fewer bytes than the 4-byte DISCONNECT is closed silently.
    pub(crate) fn disconnect(
        self,
        version: ProtocolVersion,
        max_packet_size: Option<NonZeroU32>,
    ) -> Option<MqttPacket> {
        let reason_code = self.disconnect_reason_code(version)?;
        let render = || {
            MqttPacket::V5(PacketV5::Disconnect(rmqtt_codec::v5::Disconnect::new(
                reason_code,
            )))
        };
        fits_within(encoded_len(render(), version), max_packet_size).then(render)
    }
}

/// Wire size of `packet` under the codec that will send it, or `None` when it
/// cannot be encoded at all. Measured by encoding rather than derived from the
/// packet layout, so the size and the bytes the send site writes can never
/// drift apart.
fn encoded_len(packet: MqttPacket, version: ProtocolVersion) -> Option<usize> {
    let mut buf = BytesMut::new();
    MqttEncoder::new(version)
        .encode(packet, &mut buf)
        .ok()
        .map(|()| buf.len())
}

/// Whether a packet measuring `len` may be sent to a client that declared
/// `max_packet_size`. `None` for `max_packet_size` means "no client-side
/// limit" and is never gated; an unmeasurable packet never fits under a limit.
fn fits_within(len: Option<usize>, max_packet_size: Option<NonZeroU32>) -> bool {
    let Some(limit) = max_packet_size else {
        return true;
    };
    let limit = u64::from(limit.get());
    matches!(len, Some(len) if u64::try_from(len).is_ok_and(|len| len <= limit))
}

/// The gate between `dispatch` and the send: MQTT 5 forbids sending a packet
/// larger than the Maximum Packet Size the client declared on CONNECT, and the
/// SUBACK's size follows the request's filter count, so a refusal the client
/// cannot receive closes the connection instead of going out oversized.
/// `max_packet_size` is `None` for every v3 client and for a v5 CONNECT that
/// omits the property — both mean "no client-side limit", and are never gated.
pub(crate) fn reply_over_max_packet_size(
    reply: Reply,
    version: ProtocolVersion,
    max_packet_size: Option<NonZeroU32>,
) -> Option<Violation> {
    if fits_within(reply.encoded_len(version), max_packet_size) {
        None
    } else {
        Some(Violation::ReplyOverMaxPacketSize)
    }
}

/// Split a packet-loop read error into a protocol violation and a transport
/// failure via `downcast_ref::<DecodeError>()`. `None` = transport failure,
/// which keeps its inherited quiet `Served` path; `Some(MalformedPacket)`
/// closes the session under the violation policy. Branches on the typed
/// source rather than the IO `ErrorKind`, because `src/codec/mqtt.rs:60`
/// gives every genuine decoder rejection and every transport error the same
/// `InvalidData` kind — only the preserved source distinguishes them.
pub(crate) fn classify_read_error(err: &std::io::Error) -> Option<Violation> {
    err.get_ref()
        .and_then(|s| s.downcast_ref::<crate::codec::mqtt::DecodeError>())
        .map(|_| Violation::MalformedPacket)
}

/// What the packet loop owes for one decoded packet. Pure: the caller performs
/// every effect. Borrows the packet, so the QoS 0 path allocates nothing.
#[derive(Debug)]
pub(crate) enum Disposition<'a> {
    /// Hand the payload to the event seam, then keep reading.
    Deliver(&'a rmqtt_codec::types::Publish),
    /// Hand the payload to the event seam, THEN flush the PUBACK. The variant
    /// name carries the ordering decision (AC-2).
    DeliverThenAck(&'a rmqtt_codec::types::Publish, NonZeroU16),
    /// Flush this reply, then keep reading.
    Reply(Reply),
    /// Client asked to close. The session reports `SessionOutcome::Served`.
    Close,
    /// Close under the violation policy.
    Violation(Violation),
}

/// Decide one post-handshake packet. IO-free, tracing-free, panic-free, and
/// total over `MqttPacket` with no `_` arm.
#[allow(clippy::too_many_lines)] // one total match over three exhaustive enums; splitting buys nothing
pub(crate) fn dispatch(packet: &MqttPacket) -> Disposition<'_> {
    match packet {
        MqttPacket::V3(PacketV3::Connect(_)) | MqttPacket::V5(PacketV5::Connect(_)) => {
            Disposition::Violation(Violation::DuplicateConnect)
        }
        MqttPacket::V3(PacketV3::Publish(publish)) | MqttPacket::V5(PacketV5::Publish(publish)) => {
            if publish.qos > crate::broker::handshake::MAX_QOS {
                return Disposition::Violation(Violation::PublishQosAboveMaximum);
            }
            match (publish.qos, publish.packet_id) {
                (QoS::AtMostOnce, _) => Disposition::Deliver(publish),
                (QoS::AtLeastOnce, Some(id)) => Disposition::DeliverThenAck(publish, id),
                (QoS::AtLeastOnce, None) => {
                    Disposition::Violation(Violation::PublishMissingPacketId)
                }
                (QoS::ExactlyOnce, _) => Disposition::Violation(Violation::Qos2FlowNotImplemented),
            }
        }
        MqttPacket::V3(PacketV3::PingRequest) | MqttPacket::V5(PacketV5::PingRequest) => {
            Disposition::Reply(Reply::PingResponse)
        }
        MqttPacket::V3(PacketV3::Disconnect) | MqttPacket::V5(PacketV5::Disconnect(_)) => {
            Disposition::Close
        }
        MqttPacket::V3(PacketV3::Subscribe {
            packet_id,
            topic_filters,
        }) => match NonZeroUsize::new(topic_filters.len()) {
            Some(n) => Disposition::Reply(Reply::SubscribeRefusal(*packet_id, n)),
            None => Disposition::Violation(Violation::EmptyTopicFilterList),
        },
        MqttPacket::V5(PacketV5::Subscribe(s)) => match NonZeroUsize::new(s.topic_filters.len()) {
            Some(n) => Disposition::Reply(Reply::SubscribeRefusal(s.packet_id, n)),
            None => Disposition::Violation(Violation::EmptyTopicFilterList),
        },
        MqttPacket::V3(PacketV3::Unsubscribe {
            packet_id,
            topic_filters,
        }) => match NonZeroUsize::new(topic_filters.len()) {
            Some(n) => Disposition::Reply(Reply::UnsubscribeAck(*packet_id, n)),
            None => Disposition::Violation(Violation::EmptyTopicFilterList),
        },
        MqttPacket::V5(PacketV5::Unsubscribe(s)) => {
            match NonZeroUsize::new(s.topic_filters.len()) {
                Some(n) => Disposition::Reply(Reply::UnsubscribeAck(s.packet_id, n)),
                None => Disposition::Violation(Violation::EmptyTopicFilterList),
            }
        }
        MqttPacket::V3(
            PacketV3::ConnectAck(_)
            | PacketV3::SubscribeAck { .. }
            | PacketV3::UnsubscribeAck { .. }
            | PacketV3::PingResponse,
        )
        | MqttPacket::V5(
            PacketV5::ConnectAck(_)
            | PacketV5::SubscribeAck(_)
            | PacketV5::UnsubscribeAck(_)
            | PacketV5::PingResponse,
        ) => Disposition::Violation(Violation::ServerOnlyPacket),
        MqttPacket::V3(
            PacketV3::PublishReceived { .. }
            | PacketV3::PublishRelease { .. }
            | PacketV3::PublishComplete { .. },
        )
        | MqttPacket::V5(
            PacketV5::PublishReceived(_)
            | PacketV5::PublishRelease(_)
            | PacketV5::PublishComplete(_),
        ) => Disposition::Violation(Violation::Qos2FlowNotImplemented),
        MqttPacket::V3(PacketV3::PublishAck { .. }) | MqttPacket::V5(PacketV5::PublishAck(_)) => {
            Disposition::Violation(Violation::PublishAckFromClient)
        }
        MqttPacket::V5(PacketV5::Auth(_)) => Disposition::Violation(Violation::AuthNotNegotiated),
        MqttPacket::Version(_) => Disposition::Violation(Violation::VersionProbe),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::mqtt::{MqttEncoder, PacketV3, PacketV5};
    use crate::codec::version::ProtocolVersion;
    use bytes::BytesMut;
    use rmqtt_codec::types::{Publish as TypesPublish, QoS};
    use rmqtt_codec::v5::{
        Disconnect as V5Disconnect, DisconnectReasonCode, PublishAckReason,
        Subscribe as V5Subscribe, SubscriptionOptions, Unsubscribe as V5Unsubscribe,
        UnsubscribeAckReason,
    };
    use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};

    fn v3_publish_fixture() -> MqttPacket {
        let publish = TypesPublish {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            topic: "t".into(),
            packet_id: None,
            payload: bytes::Bytes::from_static(&[0x09, 0xC4, 0x03, 0xF5]),
            properties: None,
        };
        MqttPacket::V3(PacketV3::Publish(Box::new(publish)))
    }

    fn v5_publish_fixture() -> MqttPacket {
        let publish = TypesPublish {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            topic: "t".into(),
            packet_id: None,
            payload: bytes::Bytes::from_static(&[0x09, 0xC4, 0x03, 0xF5]),
            properties: None,
        };
        MqttPacket::V5(PacketV5::Publish(Box::new(publish)))
    }

    fn encode(packet: MqttPacket, version: ProtocolVersion) -> Vec<u8> {
        let mut enc = match version {
            ProtocolVersion::MQTT3 => MqttEncoder::v3(),
            ProtocolVersion::MQTT5 => MqttEncoder::v5(),
        };
        let mut buf = BytesMut::new();
        enc.encode(packet, &mut buf).expect("encode");
        buf.to_vec()
    }

    /// AC-3 — a QoS 0 PUBLISH is handed to the event seam and produces no reply.
    #[test]
    fn dispatch_qos0_publish_delivers() {
        let binding = v3_publish_fixture();
        match dispatch(&binding) {
            Disposition::Deliver(_) => {}
            other => panic!("expected Deliver, got {other:?}"),
        }
        let binding = v5_publish_fixture();
        match dispatch(&binding) {
            Disposition::Deliver(_) => {}
            other => panic!("expected Deliver, got {other:?}"),
        }
    }

    /// AC-12 — a second CONNECT on an established session is a violation.
    #[test]
    fn dispatch_duplicate_connect_is_violation() {
        let binding = MqttPacket::V3(PacketV3::Connect(Box::<rmqtt_codec::v3::Connect>::default()));
        let v3 = dispatch(&binding);
        assert!(matches!(
            v3,
            Disposition::Violation(Violation::DuplicateConnect)
        ));

        let binding = MqttPacket::V5(PacketV5::Connect(Box::<rmqtt_codec::v5::Connect>::default()));
        let v5 = dispatch(&binding);
        assert!(matches!(
            v5,
            Disposition::Violation(Violation::DuplicateConnect)
        ));
    }

    /// AC-12 — the warn-line reason is the verbatim phrase the task brief cites.
    #[test]
    fn duplicate_connect_reason_names_protocol_violation() {
        assert_eq!(
            Violation::DuplicateConnect.reason(),
            "protocol violation: duplicate CONNECT"
        );
    }

    /// AC-15 — v3 has no server-sent DISCONNECT, so the disconnect builder returns `None`.
    #[test]
    fn v3_violation_disconnect_is_none() {
        let got = Violation::DuplicateConnect.disconnect(ProtocolVersion::MQTT3, None);
        assert!(got.is_none());
    }

    /// AC-14 — the v5 DISCONNECT encodes to the literal bytes the brief pins.
    #[test]
    fn v5_violation_disconnect_encodes_protocol_error() {
        let pkt = Violation::DuplicateConnect
            .disconnect(ProtocolVersion::MQTT5, None)
            .expect("v5 disconnect");
        let bytes = encode(pkt, ProtocolVersion::MQTT5);
        assert_eq!(bytes, vec![0xE0, 0x02, 0x82, 0x00]);
    }

    /// AC-25 — `Reply::render` returns a packet whose `MqttPacket` variant matches the
    /// `ProtocolVersion` argument; both versions encode PINGRESP to `[0xD0, 0x00]`.
    #[test]
    fn ping_response_renders_per_version() {
        let v3 = Reply::PingResponse.render(ProtocolVersion::MQTT3);
        match v3 {
            MqttPacket::V3(PacketV3::PingResponse) => {}
            other => panic!("expected MqttPacket::V3(PingResponse), got {other:?}"),
        }
        assert_eq!(encode(v3, ProtocolVersion::MQTT3), vec![0xD0, 0x00]);

        let v5 = Reply::PingResponse.render(ProtocolVersion::MQTT5);
        match v5 {
            MqttPacket::V5(PacketV5::PingResponse) => {}
            other => panic!("expected MqttPacket::V5(PingResponse), got {other:?}"),
        }
        assert_eq!(encode(v5, ProtocolVersion::MQTT5), vec![0xD0, 0x00]);
    }

    /// Smoke test for the remaining variants so a future enum variant is a
    /// compile error here too, not just at the production site. Each fixture
    /// pins the specific `Violation` variant the dispatch arm produces.
    #[test]
    fn remaining_variants_are_violations() {
        let v3_suback = MqttPacket::V3(PacketV3::SubscribeAck {
            packet_id: NonZeroU16::new(1).expect("non-zero"),
            status: vec![rmqtt_codec::v3::SubscribeReturnCode::Failure],
        });
        assert!(matches!(
            dispatch(&v3_suback),
            Disposition::Violation(Violation::ServerOnlyPacket)
        ));

        let v5_puback = MqttPacket::V5(PacketV5::PublishAck(rmqtt_codec::v5::PublishAck {
            packet_id: NonZeroU16::new(1).expect("non-zero"),
            reason_code: PublishAckReason::Success,
            properties: Vec::new(),
            reason_string: None,
        }));
        assert!(matches!(
            dispatch(&v5_puback),
            Disposition::Violation(Violation::PublishAckFromClient)
        ));

        let probe = MqttPacket::Version(ProtocolVersion::MQTT3);
        assert!(matches!(
            dispatch(&probe),
            Disposition::Violation(Violation::VersionProbe)
        ));
    }

    /// The v5 disconnect reason-code encoding round-trip keeps the brief's
    /// literal (`0x82` for ProtocolError). Independent of the codec under test.
    #[test]
    fn v5_disconnect_protocol_error_is_0x82() {
        let pkt = MqttPacket::V5(PacketV5::Disconnect(V5Disconnect::new(
            DisconnectReasonCode::ProtocolError,
        )));
        let bytes = encode(pkt, ProtocolVersion::MQTT5);
        assert_eq!(bytes, vec![0xE0, 0x02, 0x82, 0x00]);
    }

    /// AC-7 — every server-only packet the codec can produce (v3 + v5 of
    /// CONNACK, SUBACK, UNSUBACK, PINGRESP) closes the connection under
    /// `Violation::ServerOnlyPacket`, never silently swallowed. The named
    /// variant is what AC-7 asks for, and it is what selects the close's
    /// reason string and v5 reason code — `Violation(_)` would pass on any
    /// other variant and let a mis-routed arm ship the wrong DISCONNECT.
    #[test]
    fn dispatch_server_only_packets_are_violations() {
        let id = NonZeroU16::new(1).expect("non-zero");

        let v3_connack = MqttPacket::V3(PacketV3::ConnectAck(rmqtt_codec::v3::ConnectAck {
            return_code: rmqtt_codec::v3::ConnectAckReason::ConnectionAccepted,
            session_present: false,
        }));
        let v3_suback = MqttPacket::V3(PacketV3::SubscribeAck {
            packet_id: id,
            status: vec![rmqtt_codec::v3::SubscribeReturnCode::Failure],
        });
        let v3_unsuback = MqttPacket::V3(PacketV3::UnsubscribeAck { packet_id: id });
        let v3_pingresp = MqttPacket::V3(PacketV3::PingResponse);
        let v5_connack = MqttPacket::V5(PacketV5::ConnectAck(
            Box::<rmqtt_codec::v5::ConnectAck>::default(),
        ));
        let v5_suback = MqttPacket::V5(PacketV5::SubscribeAck(rmqtt_codec::v5::SubscribeAck {
            packet_id: id,
            properties: Vec::new(),
            reason_string: None,
            status: vec![rmqtt_codec::v5::SubscribeAckReason::ImplementationSpecificError],
        }));
        let v5_unsuback =
            MqttPacket::V5(PacketV5::UnsubscribeAck(rmqtt_codec::v5::UnsubscribeAck {
                packet_id: id,
                properties: Vec::new(),
                reason_string: None,
                status: vec![rmqtt_codec::v5::UnsubscribeAckReason::NoSubscriptionExisted],
            }));
        let v5_pingresp = MqttPacket::V5(PacketV5::PingResponse);

        for packet in [
            v3_connack,
            v3_suback,
            v3_unsuback,
            v3_pingresp,
            v5_connack,
            v5_suback,
            v5_unsuback,
            v5_pingresp,
        ] {
            match dispatch(&packet) {
                Disposition::Violation(Violation::ServerOnlyPacket) => {}
                other => panic!("expected ServerOnlyPacket for {packet:?}, got {other:?}"),
            }
        }
    }

    fn v3_qos1_publish(packet_id: Option<NonZeroU16>) -> MqttPacket {
        let publish = TypesPublish {
            dup: false,
            retain: false,
            qos: QoS::AtLeastOnce,
            topic: "t".into(),
            packet_id,
            payload: bytes::Bytes::from_static(&[0x09, 0xC4, 0x03, 0xF5]),
            properties: None,
        };
        MqttPacket::V3(PacketV3::Publish(Box::new(publish)))
    }

    fn v5_qos1_publish(packet_id: Option<NonZeroU16>) -> MqttPacket {
        let publish = TypesPublish {
            dup: false,
            retain: false,
            qos: QoS::AtLeastOnce,
            topic: "t".into(),
            packet_id,
            payload: bytes::Bytes::from_static(&[0x09, 0xC4, 0x03, 0xF5]),
            properties: None,
        };
        MqttPacket::V5(PacketV5::Publish(Box::new(publish)))
    }

    /// AC-1 — a QoS 1 PUBLISH routes to `DeliverThenAck` carrying its packet id.
    #[test]
    fn dispatch_qos1_publish_delivers_then_acks() {
        let id = NonZeroU16::new(1).expect("non-zero");
        let binding = v3_qos1_publish(Some(id));
        match dispatch(&binding) {
            Disposition::DeliverThenAck(_, got_id) => assert_eq!(got_id, id),
            other => panic!("expected DeliverThenAck, got {other:?}"),
        }
        let binding = v5_qos1_publish(Some(id));
        match dispatch(&binding) {
            Disposition::DeliverThenAck(_, got_id) => assert_eq!(got_id, id),
            other => panic!("expected DeliverThenAck, got {other:?}"),
        }
    }

    /// Regression guard — fuzz input reaching `dispatch` directly with a
    /// missing packet id becomes a violation, never a `unwrap` in the caller.
    #[test]
    fn dispatch_qos1_publish_without_packet_id_is_violation() {
        let binding = v5_qos1_publish(None);
        match dispatch(&binding) {
            Disposition::Violation(Violation::PublishMissingPacketId) => {}
            other => panic!("expected Violation(PublishMissingPacketId), got {other:?}"),
        }
    }

    /// AC-1, AC-25 — PUBACK renders to `[0x40, 0x02, 0x00, 0x01]` on v3 and
    /// `[0x40, 0x04, 0x00, 0x01, 0x00, 0x00]` on v5, and the variant matches
    /// the requested version.
    #[test]
    fn publish_ack_renders_per_version() {
        let id = NonZeroU16::new(1).expect("non-zero");
        let v3 = Reply::PublishAck(id).render(ProtocolVersion::MQTT3);
        match &v3 {
            MqttPacket::V3(PacketV3::PublishAck { packet_id }) => assert_eq!(*packet_id, id),
            other => panic!("expected MqttPacket::V3(PublishAck), got {other:?}"),
        }
        assert_eq!(
            encode(v3, ProtocolVersion::MQTT3),
            vec![0x40, 0x02, 0x00, 0x01]
        );

        let v5 = Reply::PublishAck(id).render(ProtocolVersion::MQTT5);
        match &v5 {
            MqttPacket::V5(PacketV5::PublishAck(puback)) => assert_eq!(puback.packet_id, id),
            other => panic!("expected MqttPacket::V5(PublishAck), got {other:?}"),
        }
        assert_eq!(
            encode(v5, ProtocolVersion::MQTT5),
            vec![0x40, 0x04, 0x00, 0x01, 0x00, 0x00]
        );
    }

    fn v3_subscribe(packet_id: NonZeroU16, filters: Vec<(&'static str, QoS)>) -> MqttPacket {
        MqttPacket::V3(PacketV3::Subscribe {
            packet_id,
            topic_filters: filters.into_iter().map(|(s, q)| (s.into(), q)).collect(),
        })
    }

    fn v5_subscribe(
        packet_id: NonZeroU16,
        id: Option<NonZeroU32>,
        filters: Vec<(&'static str, SubscriptionOptions)>,
    ) -> MqttPacket {
        let s = V5Subscribe {
            packet_id,
            id,
            user_properties: Vec::new(),
            topic_filters: filters
                .into_iter()
                .map(|(s, opts)| (s.into(), opts))
                .collect(),
        };
        MqttPacket::V5(PacketV5::Subscribe(s))
    }

    fn v3_unsubscribe(packet_id: NonZeroU16, filters: Vec<&'static str>) -> MqttPacket {
        MqttPacket::V3(PacketV3::Unsubscribe {
            packet_id,
            topic_filters: filters.into_iter().map(std::convert::Into::into).collect(),
        })
    }

    fn v5_unsubscribe(packet_id: NonZeroU16, filters: Vec<&'static str>) -> MqttPacket {
        let s = V5Unsubscribe {
            packet_id,
            user_properties: Vec::new(),
            topic_filters: filters.into_iter().map(std::convert::Into::into).collect(),
        };
        MqttPacket::V5(PacketV5::Unsubscribe(s))
    }

    /// AC-4 — every SUBSCRIBE filter is refused, and the refusal's count
    /// matches the request's filter list length.
    #[test]
    fn dispatch_subscribe_refuses_every_filter() {
        let id = NonZeroU16::new(1).expect("non-zero");
        let binding = v3_subscribe(id, vec![("a", QoS::AtMostOnce), ("b/c", QoS::AtMostOnce)]);
        match dispatch(&binding) {
            Disposition::Reply(Reply::SubscribeRefusal(got_id, n)) => {
                assert_eq!(got_id, id);
                assert_eq!(n.get(), 2);
            }
            other => panic!("expected SubscribeRefusal, got {other:?}"),
        }

        let binding = v5_subscribe(id, None, vec![("x", SubscriptionOptions::default())]);
        match dispatch(&binding) {
            Disposition::Reply(Reply::SubscribeRefusal(got_id, n)) => {
                assert_eq!(got_id, id);
                assert_eq!(n.get(), 1);
            }
            other => panic!("expected SubscribeRefusal, got {other:?}"),
        }
    }

    /// AC-6 — a filter-less SUBSCRIBE is a violation (its payload would be a
    /// malformed SUBACK and so is unconstructible, hence the `Violate` branch
    /// rather than an impossible `SubscribeRefusal(NonZeroU16, NonZeroUsize)`).
    #[test]
    fn dispatch_empty_filter_subscribe_is_violation() {
        let id = NonZeroU16::new(1).expect("non-zero");
        let binding = v3_subscribe(id, vec![]);
        assert!(matches!(
            dispatch(&binding),
            Disposition::Violation(Violation::EmptyTopicFilterList)
        ));

        let binding = v5_subscribe(id, None, vec![]);
        assert!(matches!(
            dispatch(&binding),
            Disposition::Violation(Violation::EmptyTopicFilterList)
        ));
    }

    /// AC-4, AC-25 — one failure code per filter on each version, the version
    /// variant matches the codec, and a non-1 packet id is propagated (a
    /// hardcoded `NonZeroU16::new(1).unwrap()` in `render` would satisfy every
    /// id-1 row but fail the id-37 ones).
    #[test]
    fn subscribe_refusal_renders_one_failure_code_per_filter() {
        let id1 = NonZeroU16::new(1).expect("non-zero");
        let id37 = NonZeroU16::new(37).expect("non-zero");

        // v3: one filter, id 1 → 90 03 00 01 80
        let v3_one = Reply::SubscribeRefusal(id1, NonZeroUsize::new(1).expect("n"))
            .render(ProtocolVersion::MQTT3);
        match &v3_one {
            MqttPacket::V3(PacketV3::SubscribeAck { packet_id, status }) => {
                assert_eq!(*packet_id, id1);
                assert_eq!(status.len(), 1);
                assert!(matches!(
                    status[0],
                    rmqtt_codec::v3::SubscribeReturnCode::Failure
                ));
            }
            other => panic!("expected MqttPacket::V3(SubscribeAck), got {other:?}"),
        }
        assert_eq!(
            encode(v3_one, ProtocolVersion::MQTT3),
            vec![0x90, 0x03, 0x00, 0x01, 0x80]
        );

        // v3: two filters, id 1 → 90 04 00 01 80 80
        let v3_two = Reply::SubscribeRefusal(id1, NonZeroUsize::new(2).expect("n"))
            .render(ProtocolVersion::MQTT3);
        assert_eq!(
            encode(v3_two, ProtocolVersion::MQTT3),
            vec![0x90, 0x04, 0x00, 0x01, 0x80, 0x80]
        );

        // v5: one filter, id 1 → 90 04 00 01 00 83
        let v5_one = Reply::SubscribeRefusal(id1, NonZeroUsize::new(1).expect("n"))
            .render(ProtocolVersion::MQTT5);
        match &v5_one {
            MqttPacket::V5(PacketV5::SubscribeAck(ack)) => {
                assert_eq!(ack.packet_id, id1);
                assert_eq!(ack.status.len(), 1);
                assert_eq!(
                    ack.status[0],
                    rmqtt_codec::v5::SubscribeAckReason::ImplementationSpecificError
                );
            }
            other => panic!("expected MqttPacket::V5(SubscribeAck), got {other:?}"),
        }
        assert_eq!(
            encode(v5_one, ProtocolVersion::MQTT5),
            vec![0x90, 0x04, 0x00, 0x01, 0x00, 0x83]
        );

        // v5: two filters, id 1 → 90 05 00 01 00 83 83
        let v5_two = Reply::SubscribeRefusal(id1, NonZeroUsize::new(2).expect("n"))
            .render(ProtocolVersion::MQTT5);
        assert_eq!(
            encode(v5_two, ProtocolVersion::MQTT5),
            vec![0x90, 0x05, 0x00, 0x01, 0x00, 0x83, 0x83]
        );

        // v3: one filter, id 37 → 90 03 00 25 80 (propagated, not hardcoded 1)
        let v3_id37 = Reply::SubscribeRefusal(id37, NonZeroUsize::new(1).expect("n"))
            .render(ProtocolVersion::MQTT3);
        assert_eq!(
            encode(v3_id37, ProtocolVersion::MQTT3),
            vec![0x90, 0x03, 0x00, 0x25, 0x80]
        );

        // v5: one filter, id 37 → 90 04 00 25 00 83
        let v5_id37 = Reply::SubscribeRefusal(id37, NonZeroUsize::new(1).expect("n"))
            .render(ProtocolVersion::MQTT5);
        assert_eq!(
            encode(v5_id37, ProtocolVersion::MQTT5),
            vec![0x90, 0x04, 0x00, 0x25, 0x00, 0x83]
        );
    }

    /// A refusal larger than the client's declared Maximum Packet Size is a
    /// violation rather than an oversized write: 200 filters render a 206-byte
    /// v5 SUBACK, which a client that declared 128 must not be sent. The same
    /// refusal under a limit that holds it, and under no limit at all, passes.
    #[test]
    fn subscribe_refusal_over_client_max_packet_size_is_violation() {
        let id = NonZeroU16::new(1).expect("non-zero");
        let refusal = Reply::SubscribeRefusal(id, NonZeroUsize::new(200).expect("n"));
        assert_eq!(
            refusal.encoded_len(ProtocolVersion::MQTT5),
            Some(206),
            "3-byte fixed header + 2 packet id + 1 property length + 200 reason codes"
        );

        assert_eq!(
            reply_over_max_packet_size(
                refusal,
                ProtocolVersion::MQTT5,
                Some(NonZeroU32::new(128).expect("non-zero")),
            ),
            Some(Violation::ReplyOverMaxPacketSize)
        );
        assert_eq!(
            reply_over_max_packet_size(
                refusal,
                ProtocolVersion::MQTT5,
                Some(NonZeroU32::new(4096).expect("non-zero")),
            ),
            None
        );
        assert_eq!(
            reply_over_max_packet_size(refusal, ProtocolVersion::MQTT5, None),
            None
        );
    }

    /// The gate compares the measured reply against the limit inclusively — a
    /// reply of exactly the declared size is sendable, one byte more is not.
    #[test]
    fn reply_exactly_at_max_packet_size_is_sendable() {
        let two = NonZeroU32::new(2).expect("non-zero");
        let one = NonZeroU32::new(1).expect("non-zero");
        assert_eq!(
            Reply::PingResponse.encoded_len(ProtocolVersion::MQTT3),
            Some(2)
        );
        assert_eq!(
            reply_over_max_packet_size(Reply::PingResponse, ProtocolVersion::MQTT3, Some(two)),
            None
        );
        assert_eq!(
            reply_over_max_packet_size(Reply::PingResponse, ProtocolVersion::MQTT3, Some(one)),
            Some(Violation::ReplyOverMaxPacketSize)
        );
    }

    /// The oversize close carries `PacketTooLarge` (0x95) on v5 and, as for
    /// every other violation, no packet at all on v3.
    #[test]
    fn reply_over_max_packet_size_disconnects_with_packet_too_large() {
        let pkt = Violation::ReplyOverMaxPacketSize
            .disconnect(ProtocolVersion::MQTT5, None)
            .expect("v5 disconnect");
        assert_eq!(
            encode(pkt, ProtocolVersion::MQTT5),
            vec![0xE0, 0x02, 0x95, 0x00]
        );
        assert!(Violation::ReplyOverMaxPacketSize
            .disconnect(ProtocolVersion::MQTT3, None)
            .is_none());
        assert_eq!(
            Violation::ReplyOverMaxPacketSize.reason(),
            "protocol violation: reply exceeds the client's maximum packet size"
        );
    }

    /// The close obeys the limit it enforces: the 4-byte v5 DISCONNECT is sent
    /// to a client declaring 4 and omitted for one declaring 3, so a close
    /// triggered by an oversize reply cannot itself go out oversized. The gate
    /// covers every violation, not only `ReplyOverMaxPacketSize`.
    #[test]
    fn violation_disconnect_under_the_clients_max_packet_size_is_omitted() {
        let four = NonZeroU32::new(4).expect("non-zero");
        let three = NonZeroU32::new(3).expect("non-zero");
        for violation in [
            Violation::ReplyOverMaxPacketSize,
            Violation::DuplicateConnect,
            Violation::PublishMissingPacketId,
            Violation::EmptyTopicFilterList,
        ] {
            let pkt = violation
                .disconnect(ProtocolVersion::MQTT5, Some(four))
                .expect("v5 disconnect fits a 4-byte limit");
            assert_eq!(encode(pkt, ProtocolVersion::MQTT5).len(), 4);
            assert!(
                violation
                    .disconnect(ProtocolVersion::MQTT5, Some(three))
                    .is_none(),
                "{violation:?} must not send a 4-byte DISCONNECT under a 3-byte limit"
            );
        }
    }

    /// AC-26 — every filter SHAPE the CONNACK now says is available takes the
    /// ordinary refusal path. Wildcards, `$share/` and a Subscription
    /// Identifier ride together on one v5 SUBSCRIBE; no per-shape branch
    /// exists to regress.
    #[test]
    fn dispatch_v5_wildcard_subscribe_refuses_with_suback() {
        let id = NonZeroU16::new(1).expect("non-zero");
        let sub_id = NonZeroU32::new(1).expect("non-zero");
        let binding = v5_subscribe(
            id,
            Some(sub_id),
            vec![
                ("t/#", SubscriptionOptions::default()),
                ("t/+", SubscriptionOptions::default()),
                ("$share/g/t", SubscriptionOptions::default()),
            ],
        );
        match dispatch(&binding) {
            Disposition::Reply(Reply::SubscribeRefusal(got_id, n)) => {
                assert_eq!(got_id, id);
                assert_eq!(n.get(), 3);
            }
            other => panic!("expected SubscribeRefusal with 3 filters, got {other:?}"),
        }
    }

    /// AC-5 — an UNSUBSCRIBE acknowledges every filter and propagates the
    /// request's packet id. Two distinct ids (1 and 37) are sent through
    /// dispatch; an arm that hardcoded `NonZeroU16::new(1).unwrap()` instead
    /// of `*packet_id` / `s.packet_id` would satisfy id 1 but fail id 37, and
    /// a LAN client would then wait forever on its second unsubscribe.
    #[test]
    fn dispatch_unsubscribe_acknowledges_every_filter() {
        let id1 = NonZeroU16::new(1).expect("non-zero");
        let id37 = NonZeroU16::new(37).expect("non-zero");

        let binding = v3_unsubscribe(id1, vec!["a", "b/c"]);
        match dispatch(&binding) {
            Disposition::Reply(Reply::UnsubscribeAck(got_id, n)) => {
                assert_eq!(got_id, id1);
                assert_eq!(n.get(), 2);
            }
            other => panic!("expected UnsubscribeAck with id 1, got {other:?}"),
        }

        let binding = v3_unsubscribe(id37, vec!["t"]);
        match dispatch(&binding) {
            Disposition::Reply(Reply::UnsubscribeAck(got_id, n)) => {
                assert_eq!(got_id, id37, "id must be propagated, not hardcoded 1");
                assert_eq!(n.get(), 1);
            }
            other => panic!("expected UnsubscribeAck with id 37, got {other:?}"),
        }

        let binding = v5_unsubscribe(id1, vec!["x"]);
        match dispatch(&binding) {
            Disposition::Reply(Reply::UnsubscribeAck(got_id, n)) => {
                assert_eq!(got_id, id1);
                assert_eq!(n.get(), 1);
            }
            other => panic!("expected v5 UnsubscribeAck with id 1, got {other:?}"),
        }
    }

    /// AC-5 — the v5 UNSUBSCRIBE arm propagates the request's own packet id
    /// and filter count, not the id-1/one-filter shape every other v5 case in
    /// this module happens to use: an arm reading `s.packet_id` from the wrong
    /// field or counting `1` would pass those and fail this one, and the v5
    /// UNSUBACK carries one status byte per filter, so a wrong count is a
    /// malformed reply rather than a cosmetic slip.
    #[test]
    fn dispatch_v5_unsubscribe_propagates_id_and_filter_count() {
        let id37 = NonZeroU16::new(37).expect("non-zero");
        let binding = v5_unsubscribe(id37, vec!["a", "b/c"]);
        match dispatch(&binding) {
            Disposition::Reply(Reply::UnsubscribeAck(got_id, n)) => {
                assert_eq!(got_id, id37, "id must be propagated, not hardcoded 1");
                assert_eq!(n.get(), 2, "one status byte per requested filter");
            }
            other => panic!("expected v5 UnsubscribeAck with id 37 and 2 filters, got {other:?}"),
        }
    }

    /// Every reply flushes under its OWN timeout context, and each context is
    /// the `handler` constant rather than a copy of its text. A swapped arm
    /// costs no test elsewhere — the wordings are parsed contracts for the
    /// per-close `debug` line, so a PUBACK stall reported as a PINGRESP stall
    /// misattributes the stalled reply in every diagnostic that reads them.
    #[test]
    fn reply_timeout_context_matches_its_flush_constant() {
        let id = NonZeroU16::new(1).expect("non-zero");
        let one = NonZeroUsize::new(1).expect("non-zero");

        assert_eq!(
            Reply::PingResponse.timeout_context(),
            super::super::handler::TIMEOUT_CTX_PINGRESP_FLUSH
        );
        assert_eq!(
            Reply::PublishAck(id).timeout_context(),
            super::super::handler::TIMEOUT_CTX_PUBACK_FLUSH
        );
        assert_eq!(
            Reply::SubscribeRefusal(id, one).timeout_context(),
            super::super::handler::TIMEOUT_CTX_SUBACK_FLUSH
        );
        assert_eq!(
            Reply::UnsubscribeAck(id, one).timeout_context(),
            super::super::handler::TIMEOUT_CTX_UNSUBACK_FLUSH
        );

        let contexts = [
            Reply::PingResponse.timeout_context(),
            Reply::PublishAck(id).timeout_context(),
            Reply::SubscribeRefusal(id, one).timeout_context(),
            Reply::UnsubscribeAck(id, one).timeout_context(),
        ];
        let mut distinct = contexts.to_vec();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(
            distinct.len(),
            contexts.len(),
            "each reply must flush under a distinct context: {contexts:?}"
        );
    }

    /// AC-6 — an UNSUBSCRIBE carrying zero topic filters is a violation, same
    /// as the SUBSCRIBE half: a payload-less UNSUBACK is malformed, so the
    /// `None` branch of `NonZeroUsize::new` is the only legal arm.
    #[test]
    fn dispatch_empty_filter_unsubscribe_is_violation() {
        let id = NonZeroU16::new(1).expect("non-zero");
        let binding = v3_unsubscribe(id, vec![]);
        assert!(matches!(
            dispatch(&binding),
            Disposition::Violation(Violation::EmptyTopicFilterList)
        ));

        let binding = v5_unsubscribe(id, vec![]);
        assert!(matches!(
            dispatch(&binding),
            Disposition::Violation(Violation::EmptyTopicFilterList)
        ));
    }

    /// MQTT 5 §§3.8.3 and 3.10.3 classify a filter-less SUBSCRIBE /
    /// UNSUBSCRIBE as a Protocol Error, whose reason code is `0x82` under
    /// §4.13.1 — not the `MalformedPacket` (`0x81`) the plan assumed before the
    /// specification text was available. Driven end to end from the two v5
    /// requests (`82 03 00 01 00` and `A2 03 00 01 00` on the wire) so a
    /// remapped arm cannot pass on the dispatch half alone.
    #[test]
    fn empty_filter_list_disconnect_encodes_protocol_error() {
        let id = NonZeroU16::new(1).expect("non-zero");
        for binding in [v5_subscribe(id, None, vec![]), v5_unsubscribe(id, vec![])] {
            let violation = match dispatch(&binding) {
                Disposition::Violation(v @ Violation::EmptyTopicFilterList) => v,
                other => panic!("expected Violation(EmptyTopicFilterList), got {other:?}"),
            };
            let pkt = violation
                .disconnect(ProtocolVersion::MQTT5, None)
                .expect("v5 disconnect");
            assert_eq!(
                encode(pkt, ProtocolVersion::MQTT5),
                vec![0xE0, 0x02, 0x82, 0x00],
                "empty filter list is a Protocol Error (0x82), not MalformedPacket (0x81)"
            );
            assert!(violation
                .disconnect(ProtocolVersion::MQTT3, None)
                .is_none());
        }
    }

    /// AC-5, AC-25 — UNSUBACK renders to `[0xB0, 0x02, 0x00, 0x01]` on v3 and
    /// `[0xB0, 0x04, 0x00, 0x01, 0x00, 0x11]` on v5, with the v3 two-filter row
    /// dropping the count (proving the v3 renderer discards it) and the v5
    /// two-filter row emitting one status byte per filter (proving v5 does not
    /// hardcode a single status). Non-1 packet id (37, big-endian `0x0025`) is
    /// propagated on both versions — a hardcoded id-1 in `render` would fail
    /// these rows even when the dispatch already passed id 37 through.
    #[test]
    fn unsubscribe_ack_renders_per_version() {
        let id1 = NonZeroU16::new(1).expect("non-zero");
        let id37 = NonZeroU16::new(37).expect("non-zero");

        // v3: one filter, id 1 → B0 02 00 01
        let v3_one = Reply::UnsubscribeAck(id1, NonZeroUsize::new(1).expect("n"))
            .render(ProtocolVersion::MQTT3);
        match &v3_one {
            MqttPacket::V3(PacketV3::UnsubscribeAck { packet_id }) => {
                assert_eq!(*packet_id, id1);
            }
            other => panic!("expected MqttPacket::V3(UnsubscribeAck), got {other:?}"),
        }
        assert_eq!(
            encode(v3_one, ProtocolVersion::MQTT3),
            vec![0xB0, 0x02, 0x00, 0x01]
        );

        // v3: two filters, id 1 → B0 02 00 01 (count dropped on v3).
        let v3_two = Reply::UnsubscribeAck(id1, NonZeroUsize::new(2).expect("n"))
            .render(ProtocolVersion::MQTT3);
        assert_eq!(
            encode(v3_two, ProtocolVersion::MQTT3),
            vec![0xB0, 0x02, 0x00, 0x01]
        );

        // v5: one filter, id 1 → B0 04 00 01 00 11
        let v5_one = Reply::UnsubscribeAck(id1, NonZeroUsize::new(1).expect("n"))
            .render(ProtocolVersion::MQTT5);
        match &v5_one {
            MqttPacket::V5(PacketV5::UnsubscribeAck(ack)) => {
                assert_eq!(ack.packet_id, id1);
                assert_eq!(ack.status.len(), 1);
                assert_eq!(ack.status[0], UnsubscribeAckReason::NoSubscriptionExisted);
            }
            other => panic!("expected MqttPacket::V5(UnsubscribeAck), got {other:?}"),
        }
        assert_eq!(
            encode(v5_one, ProtocolVersion::MQTT5),
            vec![0xB0, 0x04, 0x00, 0x01, 0x00, 0x11]
        );

        // v5: two filters, id 1 → B0 05 00 01 00 11 11 (one status byte per filter).
        let v5_two = Reply::UnsubscribeAck(id1, NonZeroUsize::new(2).expect("n"))
            .render(ProtocolVersion::MQTT5);
        assert_eq!(
            encode(v5_two, ProtocolVersion::MQTT5),
            vec![0xB0, 0x05, 0x00, 0x01, 0x00, 0x11, 0x11]
        );

        // v3: one filter, id 37 → B0 02 00 25 (id propagated).
        let v3_id37 = Reply::UnsubscribeAck(id37, NonZeroUsize::new(1).expect("n"))
            .render(ProtocolVersion::MQTT3);
        assert_eq!(
            encode(v3_id37, ProtocolVersion::MQTT3),
            vec![0xB0, 0x02, 0x00, 0x25]
        );

        // v5: one filter, id 37 → B0 04 00 25 00 11.
        let v5_id37 = Reply::UnsubscribeAck(id37, NonZeroUsize::new(1).expect("n"))
            .render(ProtocolVersion::MQTT5);
        assert_eq!(
            encode(v5_id37, ProtocolVersion::MQTT5),
            vec![0xB0, 0x04, 0x00, 0x25, 0x00, 0x11]
        );
    }

    /// AC-8 — PUBREC / PUBREL / PUBCOMP close under `Qos2FlowNotImplemented`.
    /// Both versions of each, three packet types: six inputs, one variant.
    #[test]
    fn dispatch_qos2_flow_packets_are_violations() {
        let id = NonZeroU16::new(1).expect("non-zero");

        let v3_pubrec = MqttPacket::V3(PacketV3::PublishReceived { packet_id: id });
        assert!(matches!(
            dispatch(&v3_pubrec),
            Disposition::Violation(Violation::Qos2FlowNotImplemented)
        ));
        let v3_pubrel = MqttPacket::V3(PacketV3::PublishRelease { packet_id: id });
        assert!(matches!(
            dispatch(&v3_pubrel),
            Disposition::Violation(Violation::Qos2FlowNotImplemented)
        ));
        let v3_pubcomp = MqttPacket::V3(PacketV3::PublishComplete { packet_id: id });
        assert!(matches!(
            dispatch(&v3_pubcomp),
            Disposition::Violation(Violation::Qos2FlowNotImplemented)
        ));

        let v5_pubrec = MqttPacket::V5(PacketV5::PublishReceived(rmqtt_codec::v5::PublishAck {
            packet_id: id,
            reason_code: PublishAckReason::Success,
            properties: Vec::new(),
            reason_string: None,
        }));
        assert!(matches!(
            dispatch(&v5_pubrec),
            Disposition::Violation(Violation::Qos2FlowNotImplemented)
        ));
        let v5_pubrel = MqttPacket::V5(PacketV5::PublishRelease(rmqtt_codec::v5::PublishAck2 {
            packet_id: id,
            reason_code: rmqtt_codec::v5::PublishAck2Reason::Success,
            properties: Vec::new(),
            reason_string: None,
        }));
        assert!(matches!(
            dispatch(&v5_pubrel),
            Disposition::Violation(Violation::Qos2FlowNotImplemented)
        ));
        let v5_pubcomp = MqttPacket::V5(PacketV5::PublishComplete(rmqtt_codec::v5::PublishAck2 {
            packet_id: id,
            reason_code: rmqtt_codec::v5::PublishAck2Reason::Success,
            properties: Vec::new(),
            reason_string: None,
        }));
        assert!(matches!(
            dispatch(&v5_pubcomp),
            Disposition::Violation(Violation::Qos2FlowNotImplemented)
        ));
    }

    /// AC-9 — a QoS 2 PUBLISH exceeds `MAX_QOS` and is rejected before the
    // `(qos, packet_id)` match. The arm that names QoS 2 specifically
    // (`Qos2FlowNotImplemented`) is reserved for F2.2, when `MAX_QOS` rises
    // to `ExactlyOnce` and the guard above no longer fires.
    #[test]
    fn dispatch_qos2_publish_exceeds_max_qos() {
        let id = NonZeroU16::new(1).expect("non-zero");
        let publish = TypesPublish {
            dup: false,
            retain: false,
            qos: QoS::ExactlyOnce,
            topic: "t".into(),
            packet_id: Some(id),
            payload: bytes::Bytes::from_static(&[0x09, 0xC4, 0x03, 0xF5]),
            properties: None,
        };

        let v3 = MqttPacket::V3(PacketV3::Publish(Box::new(publish.clone())));
        assert!(matches!(
            dispatch(&v3),
            Disposition::Violation(Violation::PublishQosAboveMaximum)
        ));
        let v5 = MqttPacket::V5(PacketV5::Publish(Box::new(publish)));
        assert!(matches!(
            dispatch(&v5),
            Disposition::Violation(Violation::PublishQosAboveMaximum)
        ));
    }

    /// AC-10 — v5 AUTH without a negotiated method closes as
    /// `AuthNotNegotiated`. v3 has no AUTH packet.
    #[test]
    fn dispatch_v5_auth_is_violation() {
        let auth = rmqtt_codec::v5::Auth {
            reason_code: rmqtt_codec::v5::AuthReasonCode::ReAuth,
            auth_method: None,
            auth_data: None,
            reason_string: None,
            user_properties: Vec::new(),
        };
        let binding = MqttPacket::V5(PacketV5::Auth(auth));
        assert!(matches!(
            dispatch(&binding),
            Disposition::Violation(Violation::AuthNotNegotiated)
        ));
    }

    /// AC-11 — a PUBACK arriving from a client closes as
    /// `PublishAckFromClient`. This server never publishes, so a PUBACK is
    /// only ever server-sent.
    #[test]
    fn dispatch_client_puback_is_violation() {
        let id = NonZeroU16::new(1).expect("non-zero");

        let v3 = MqttPacket::V3(PacketV3::PublishAck { packet_id: id });
        assert!(matches!(
            dispatch(&v3),
            Disposition::Violation(Violation::PublishAckFromClient)
        ));

        let v5 = MqttPacket::V5(PacketV5::PublishAck(rmqtt_codec::v5::PublishAck {
            packet_id: id,
            reason_code: PublishAckReason::Success,
            properties: Vec::new(),
            reason_string: None,
        }));
        assert!(matches!(
            dispatch(&v5),
            Disposition::Violation(Violation::PublishAckFromClient)
        ));
    }

    /// AC-9 — the v5 close for an above-maximum-QoS PUBLISH carries
    /// `QosNotSupported` (0x9B), distinct from the `ProtocolError` (0x82) the
    /// other violations use.
    #[test]
    fn qos_not_supported_disconnect_encodes_9b() {
        let pkt = Violation::PublishQosAboveMaximum
            .disconnect(ProtocolVersion::MQTT5, None)
            .expect("v5 disconnect");
        assert_eq!(
            encode(pkt, ProtocolVersion::MQTT5),
            vec![0xE0, 0x02, 0x9B, 0x00]
        );

        let pkt = Violation::Qos2FlowNotImplemented
            .disconnect(ProtocolVersion::MQTT5, None)
            .expect("v5 disconnect");
        assert_eq!(
            encode(pkt, ProtocolVersion::MQTT5),
            vec![0xE0, 0x02, 0x9B, 0x00]
        );
    }

    /// AC-17 — a decoder rejection carrying the typed source flags as a
    /// protocol violation. The `DecodeError` is preserved through the IO
    /// adapter at `src/codec/mqtt.rs:60`, so the probe can find it.
    #[test]
    fn classify_read_error_flags_decoder_rejection() {
        let err = std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            crate::codec::mqtt::DecodeError::MalformedPacket,
        );
        assert_eq!(classify_read_error(&err), Some(Violation::MalformedPacket));
    }

    /// AC-18 — a sourceless `InvalidData` is a transport error, NOT a decoder
    /// rejection. Same `ErrorKind` as the decoder path, which is the only
    /// thing that distinguishes this test from the one above and proves the
    /// probe is typed and not a kind heuristic.
    #[test]
    fn classify_read_error_ignores_transport_error() {
        let err = std::io::Error::from(std::io::ErrorKind::InvalidData);
        assert_eq!(classify_read_error(&err), None);
    }

    /// AC-14 — the v5 close for a mid-session decoder rejection carries
    /// `MalformedPacket` (0x81), distinct from `ProtocolError` (0x82).
    #[test]
    fn malformed_packet_disconnect_encodes_81() {
        let pkt = Violation::MalformedPacket
            .disconnect(ProtocolVersion::MQTT5, None)
            .expect("v5 disconnect");
        assert_eq!(
            encode(pkt, ProtocolVersion::MQTT5),
            vec![0xE0, 0x02, 0x81, 0x00]
        );
        assert!(Violation::MalformedPacket
            .disconnect(ProtocolVersion::MQTT3, None)
            .is_none());
    }
}
