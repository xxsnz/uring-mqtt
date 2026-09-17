use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::time::Duration;

use crate::codec::mqtt::{ConnectAck, ConnectAckReason, MqttPacket, PacketV3, PacketV5};
use crate::codec::version::ProtocolVersion;
use rmqtt_codec::types::QoS;
use rmqtt_codec::v5::ConnectAckReason as V5ConnectAckReason;

/// Spec factor: idle deadline = 1.5 × keep_alive. Expressed in ms per keep-alive second.
const KEEP_ALIVE_GRACE_MILLIS_PER_SEC: u64 = 1500;

/// Supported ceiling for config-supplied timeouts (1 year). Larger u64 values are
/// clamped: monoio's timer panics converting absurd Durations to millis, and an
/// Instant sum can overflow. Clamping defines the supported-configuration contract.
pub(crate) const MAX_TIMEOUT_SECS: u64 = 31_536_000;

/// Outcome of evaluating the first decoded packet of a connection. Pure — no IO.
pub(crate) enum ConnectDecision {
    /// CONNECT accepted: send `connack`, adopt session values.
    Accept {
        connack: MqttPacket,
        /// None = anonymous v3 client (empty id + clean_session).
        client_id: Option<String>,
        /// Negotiated keep-alive (v5 capped: the announced value; else the client's).
        keep_alive_secs: u16,
        /// Effective packet-loop deadline per the Q1 decision.
        idle_timeout: Duration,
        /// Largest packet the client declared it can receive (v5 Maximum
        /// Packet Size). `None` — every v3 client, and a v5 CONNECT without
        /// the property — means the client set no limit.
        max_packet_size: Option<NonZeroU32>,
    },
    /// CONNECT refused: `connack` names the refusal reason and is written only when
    /// `sendable`. `sendable` is false when the CONNACK is larger than the Maximum
    /// Packet Size the CONNECT declared: it is then withheld and the connection
    /// closes with no reply, the same silent shape the v3 violation close uses.
    /// Always true on v3 and whenever the CONNECT declared no limit.
    Refuse { connack: MqttPacket, sendable: bool },
    /// First packet was not CONNECT: close without CONNACK.
    NotConnect,
}

/// A v5 refusal decision. The CONNACK carries `reason`; it is sendable only
/// when it fits the Maximum Packet Size the CONNECT declared. Built from a
/// clone because `MqttPacket` is not `Clone` and measuring a packet
/// consumes it.
fn refuse_v5(reason: V5ConnectAckReason, max_packet_size: Option<NonZeroU32>) -> ConnectDecision {
    let ack = honest_v5_connack(reason);
    let sendable = super::packet::fits_max_packet_size(
        MqttPacket::V5(PacketV5::ConnectAck(Box::new(ack.clone()))),
        ProtocolVersion::MQTT5,
        max_packet_size,
    );
    ConnectDecision::Refuse {
        connack: MqttPacket::V5(PacketV5::ConnectAck(Box::new(ack))),
        sendable,
    }
}

/// Evaluate the first decoded packet of a connection against the broker's policy.
///
/// Pure function: no IO, no tracing. Decisions are driven by the normative table in
/// `PLAN.md` (`## Interface Contracts`).
#[allow(clippy::cast_possible_truncation)] // clamp guarantees u16 fit
pub(crate) fn evaluate_connect(
    packet: &MqttPacket,
    config_idle_timeout_secs: u64,
    peer_addr: SocketAddr, // source for v5 assigned_client_id
) -> ConnectDecision {
    let cfg = config_idle_timeout_secs.min(MAX_TIMEOUT_SECS);
    let cfg_duration = Duration::from_secs(cfg);

    match packet {
        MqttPacket::V3(PacketV3::Connect(c)) => {
            if c.client_id.is_empty() && !c.clean_session {
                return ConnectDecision::Refuse {
                    connack: MqttPacket::V3(PacketV3::ConnectAck(ConnectAck {
                        return_code: ConnectAckReason::IdentifierRejected,
                        session_present: false,
                    })),
                    sendable: true,
                };
            }
            let client_id = if c.client_id.is_empty() {
                None
            } else {
                Some(c.client_id.to_string())
            };
            let idle_timeout = if c.keep_alive == 0 {
                cfg_duration
            } else {
                Duration::from_millis(u64::from(c.keep_alive) * KEEP_ALIVE_GRACE_MILLIS_PER_SEC)
                    .min(cfg_duration)
            };
            ConnectDecision::Accept {
                connack: MqttPacket::V3(PacketV3::ConnectAck(ConnectAck {
                    return_code: ConnectAckReason::ConnectionAccepted,
                    session_present: false,
                })),
                client_id,
                keep_alive_secs: c.keep_alive,
                idle_timeout,
                max_packet_size: None,
            }
        }
        MqttPacket::V5(PacketV5::Connect(c)) => {
            if c.auth_method.is_some() {
                return refuse_v5(
                    V5ConnectAckReason::BadAuthenticationMethod,
                    c.max_packet_size,
                );
            }
            let assigned = if c.client_id.is_empty() {
                Some(format!("auto-{peer_addr}"))
            } else {
                None
            };
            let (negotiated_ka, idle, announce) = if c.keep_alive == 0 {
                (0_u16, cfg_duration, None)
            } else {
                let ka_ms = Duration::from_millis(
                    u64::from(c.keep_alive) * KEEP_ALIVE_GRACE_MILLIS_PER_SEC,
                );
                if ka_ms <= cfg_duration {
                    (c.keep_alive, ka_ms, None)
                } else {
                    // Announce clamp(cfg × 2/3, 1, u16::MAX) so 1.5 × announced fits the ceiling.
                    // The floor of 1 is a recorded epic exception — see EPIC-SPEC.md §4, Bounded exception.
                    // At cfg < 2 the enforced 1.5s exceeds the ceiling by at most 1.5s: announcing a
                    // keep-alive the server then refuses to honor would close a conforming client.
                    // Do not "fix" this to min(_, cfg_duration).
                    let a = (cfg * 2 / 3).clamp(1, u64::from(u16::MAX)) as u16;
                    (
                        a,
                        Duration::from_millis(u64::from(a) * KEEP_ALIVE_GRACE_MILLIS_PER_SEC),
                        Some(a),
                    )
                }
            };
            let client_id = Some(assigned.clone().unwrap_or_else(|| c.client_id.to_string()));
            let mut connack = honest_v5_connack(V5ConnectAckReason::Success);
            connack.assigned_client_id = assigned.map(Into::into);
            connack.server_keepalive_sec = announce;
            // A client whose declared Maximum Packet Size cannot hold the CONNACK it
            // is owed cannot be served: the session's first packet would already
            // break the limit. Refuse before the session begins rather than accept
            // and close on the first reply. See EPIC-SPEC.md §4, "Recorded exception
            // — Maximum Packet Size".
            if !super::packet::fits_max_packet_size(
                MqttPacket::V5(PacketV5::ConnectAck(Box::new(connack.clone()))),
                ProtocolVersion::MQTT5,
                c.max_packet_size,
            ) {
                return refuse_v5(
                    V5ConnectAckReason::ImplementationSpecificError,
                    c.max_packet_size,
                );
            }
            ConnectDecision::Accept {
                connack: MqttPacket::V5(PacketV5::ConnectAck(Box::new(connack))),
                client_id,
                keep_alive_secs: negotiated_ka,
                idle_timeout: idle,
                max_packet_size: c.max_packet_size,
            }
        }
        _ => ConnectDecision::NotConnect,
    }
}

/// Highest QoS the server can presently acknowledge. `honest_v5_connack`
/// advertises exactly this value and `packet::dispatch` refuses any PUBLISH
/// above it, so the CONNACK cannot promise what the packet loop rejects.
/// F2.2 raises this to `QoS::ExactlyOnce` when the receiver flow lands.
pub(crate) const MAX_QOS: QoS = QoS::AtLeastOnce;

/// Honest v5 CONNACK: truthful capability announcement per AC-15.
///
/// Single constructor for both the accept path (this module's
/// `evaluate_connect`) and the decoder-level refusal path in
/// `handler::handle_client_io` — capability fields cannot diverge
/// between the two when later features raise `max_qos`.
///
/// `max_qos` is `MAX_QOS`, currently `AtLeastOnce`, because the packet loop
/// acknowledges QoS 1 and not QoS 2; F2.2 raises it to `ExactlyOnce` as its own
/// Done-when. `retain_available` stays `true` as a recorded exception in the
/// same section: it promises the flag is accepted, not that a retained copy
/// survives.
pub(crate) fn honest_v5_connack(reason: V5ConnectAckReason) -> rmqtt_codec::v5::ConnectAck {
    rmqtt_codec::v5::ConnectAck {
        reason_code: reason,
        max_qos: MAX_QOS,
        retain_available: true,
        wildcard_subscription_available: true,
        subscription_identifiers_available: true,
        shared_subscription_available: true,
        session_expiry_interval_secs: Some(0),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::mqtt::MqttEncoder;
    use bytes::Bytes;
    use monoio_codec::Encoder as _;
    use rmqtt_codec::v3::{Connect, LastWill};

    fn peer() -> SocketAddr {
        "127.0.0.1:9999".parse().unwrap()
    }

    fn v3_connect_with(client_id: &str, keep_alive: u16, clean_session: bool) -> MqttPacket {
        let c = Connect {
            keep_alive,
            clean_session,
            ..Connect::default()
        }
        .client_id(client_id.to_string());
        MqttPacket::V3(PacketV3::Connect(Box::new(c)))
    }

    fn v5_connect_with(
        client_id: &str,
        keep_alive: u16,
        clean_start: bool,
        auth_method: Option<&str>,
    ) -> MqttPacket {
        let mut c = rmqtt_codec::v5::Connect {
            client_id: client_id.to_string().into(),
            keep_alive,
            clean_start,
            ..rmqtt_codec::v5::Connect::default()
        };
        if let Some(m) = auth_method {
            c.auth_method = Some(m.to_string().into());
        }
        MqttPacket::V5(PacketV5::Connect(Box::new(c)))
    }

    #[test]
    fn accepts_v3_connect_with_client_id_and_keepalive() {
        let packet = v3_connect_with("dev-1", 60, false);
        let d = evaluate_connect(&packet, 300, peer());
        let ConnectDecision::Accept {
            client_id,
            keep_alive_secs,
            idle_timeout,
            ..
        } = d
        else {
            panic!("expected Accept");
        };
        assert_eq!(client_id.as_deref(), Some("dev-1"));
        assert_eq!(keep_alive_secs, 60);
        assert_eq!(idle_timeout, Duration::from_secs(90));
    }

    #[test]
    fn preserves_supplied_v5_client_id() {
        let packet = v5_connect_with("dev5", 60, false, None);
        let d = evaluate_connect(&packet, 300, peer());
        let ConnectDecision::Accept {
            client_id, connack, ..
        } = d
        else {
            panic!("expected Accept");
        };
        assert_eq!(client_id.as_deref(), Some("dev5"));
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert_eq!(ack.assigned_client_id, None);
    }

    #[test]
    fn keepalive_zero_falls_back_to_config_idle_timeout() {
        let packet = v3_connect_with("dev-1", 0, false);
        let d = evaluate_connect(&packet, 300, peer());
        let ConnectDecision::Accept { idle_timeout, .. } = d else {
            panic!("expected Accept");
        };
        assert_eq!(idle_timeout, Duration::from_secs(300));
    }

    #[test]
    fn v3_keepalive_capped_at_config() {
        let packet = v3_connect_with("dev-1", 400, false);
        let d = evaluate_connect(&packet, 300, peer());
        let ConnectDecision::Accept {
            idle_timeout,
            keep_alive_secs,
            ..
        } = d
        else {
            panic!("expected Accept");
        };
        assert_eq!(idle_timeout, Duration::from_secs(300));
        assert_eq!(keep_alive_secs, 400);
    }

    #[test]
    fn v3_zero_config_caps_idle_at_zero() {
        let packet = v3_connect_with("dev-1", 60, false);
        let d = evaluate_connect(&packet, 0, peer());
        let ConnectDecision::Accept { idle_timeout, .. } = d else {
            panic!("expected Accept");
        };
        assert_eq!(idle_timeout, Duration::ZERO);
    }

    #[test]
    fn v5_capped_keepalive_announces_consistent_server_keepalive() {
        let packet = v5_connect_with("dev5", 400, true, None);
        let d = evaluate_connect(&packet, 300, peer());
        let ConnectDecision::Accept {
            connack,
            keep_alive_secs,
            idle_timeout,
            ..
        } = d
        else {
            panic!("expected Accept");
        };
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert_eq!(ack.server_keepalive_sec, Some(200));
        assert_eq!(keep_alive_secs, 200);
        assert_eq!(idle_timeout, Duration::from_secs(300));
    }

    #[test]
    fn v5_cap_non_divisible_by_three_rounds_down() {
        let packet = v5_connect_with("dev5", 200, true, None);
        let d = evaluate_connect(&packet, 100, peer());
        let ConnectDecision::Accept {
            connack,
            idle_timeout,
            ..
        } = d
        else {
            panic!("expected Accept");
        };
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert_eq!(ack.server_keepalive_sec, Some(66));
        assert_eq!(idle_timeout, Duration::from_secs(99));
    }

    #[test]
    fn v5_degenerate_cap_announces_at_least_one_second() {
        let packet = v5_connect_with("dev5", 60, true, None);
        let d = evaluate_connect(&packet, 1, peer());
        let ConnectDecision::Accept {
            connack,
            idle_timeout,
            ..
        } = d
        else {
            panic!("expected Accept");
        };
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert_eq!(ack.server_keepalive_sec, Some(1));
        assert_eq!(idle_timeout, Duration::from_millis(1500));
    }

    #[test]
    fn v5_zero_config_with_positive_keepalive_announces_one_second() {
        let packet = v5_connect_with("dev5", 60, true, None);
        let d = evaluate_connect(&packet, 0, peer());
        let ConnectDecision::Accept {
            connack,
            idle_timeout,
            ..
        } = d
        else {
            panic!("expected Accept");
        };
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert_eq!(ack.server_keepalive_sec, Some(1));
        assert_eq!(idle_timeout, Duration::from_millis(1500));
    }

    #[test]
    fn v5_uncapped_keepalive_has_no_announcement() {
        let packet = v5_connect_with("dev5", 2, true, None);
        let d = evaluate_connect(&packet, 30, peer());
        let ConnectDecision::Accept {
            connack,
            keep_alive_secs,
            idle_timeout,
            ..
        } = d
        else {
            panic!("expected Accept");
        };
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert_eq!(keep_alive_secs, 2);
        assert_eq!(idle_timeout, Duration::from_secs(3));
        assert_eq!(ack.server_keepalive_sec, None);
    }

    #[test]
    fn huge_config_idle_clamped_to_supported_max() {
        let packet = v3_connect_with("dev-1", 0, false);
        let d = evaluate_connect(&packet, u64::MAX, peer());
        let ConnectDecision::Accept { idle_timeout, .. } = d else {
            panic!("expected Accept");
        };
        assert_eq!(idle_timeout, Duration::from_secs(31_536_000));
    }

    #[test]
    fn rejects_v5_connect_with_auth_method() {
        let packet = v5_connect_with("dev5", 60, true, Some("PLAIN"));
        let d = evaluate_connect(&packet, 300, peer());
        let ConnectDecision::Refuse { connack, .. } = d else {
            panic!("expected Refuse");
        };
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert!(matches!(
            ack.reason_code,
            V5ConnectAckReason::BadAuthenticationMethod
        ));
    }

    #[test]
    fn refuses_v3_empty_client_id_without_clean_session() {
        let packet = MqttPacket::V3(PacketV3::Connect(Box::<Connect>::default()));
        let d = evaluate_connect(&packet, 300, peer());
        let ConnectDecision::Refuse { connack, .. } = d else {
            panic!("expected Refuse");
        };
        let MqttPacket::V3(PacketV3::ConnectAck(ack)) = connack else {
            panic!("expected v3 CONNACK");
        };
        assert!(matches!(
            ack.return_code,
            ConnectAckReason::IdentifierRejected
        ));
    }

    #[test]
    fn accepts_v3_empty_client_id_with_clean_session_as_anonymous() {
        let packet = v3_connect_with("", 60, true);
        let d = evaluate_connect(&packet, 300, peer());
        let ConnectDecision::Accept { client_id, .. } = d else {
            panic!("expected Accept");
        };
        assert_eq!(client_id, None);
    }

    #[test]
    fn assigns_client_id_to_v5_empty_client_id() {
        let packet = v5_connect_with("", 0, true, None);
        let d = evaluate_connect(&packet, 60, peer());
        let ConnectDecision::Accept {
            client_id, connack, ..
        } = d
        else {
            panic!("expected Accept");
        };
        let expected = "auto-127.0.0.1:9999";
        assert_eq!(client_id.as_deref(), Some(expected));
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert_eq!(ack.assigned_client_id.as_deref(), Some(expected));
    }

    #[test]
    fn v5_accept_connack_is_honest_about_capabilities() {
        let packet = v5_connect_with("dev5", 60, true, None);
        let d = evaluate_connect(&packet, 60, peer());
        let ConnectDecision::Accept { connack, .. } = d else {
            panic!("expected Accept");
        };
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert_eq!(ack.max_qos, QoS::AtLeastOnce);
        assert!(ack.wildcard_subscription_available);
        assert!(ack.shared_subscription_available);
        assert!(ack.subscription_identifiers_available);
        assert_eq!(ack.session_expiry_interval_secs, Some(0));
        assert!(!ack.session_present);
        assert!(ack.retain_available);
    }

    /// The v5 Maximum Packet Size survives the CONNECT's drop, so the packet
    /// loop can refuse to send a reply the client cannot receive. A v5 CONNECT
    /// without the property and every v3 CONNECT declare no limit.
    #[test]
    fn retains_client_max_packet_size() {
        let mut c = rmqtt_codec::v5::Connect {
            client_id: "dev5".to_string().into(),
            keep_alive: 60,
            ..rmqtt_codec::v5::Connect::default()
        };
        c.max_packet_size = NonZeroU32::new(128);
        let packet = MqttPacket::V5(PacketV5::Connect(Box::new(c)));
        let ConnectDecision::Accept {
            max_packet_size, ..
        } = evaluate_connect(&packet, 300, peer())
        else {
            panic!("expected Accept");
        };
        assert_eq!(max_packet_size, NonZeroU32::new(128));

        let packet = v5_connect_with("dev5", 60, true, None);
        let ConnectDecision::Accept {
            max_packet_size, ..
        } = evaluate_connect(&packet, 300, peer())
        else {
            panic!("expected Accept");
        };
        assert_eq!(max_packet_size, None);

        let packet = v3_connect_with("dev-1", 60, false);
        let ConnectDecision::Accept {
            max_packet_size, ..
        } = evaluate_connect(&packet, 300, peer())
        else {
            panic!("expected Accept");
        };
        assert_eq!(max_packet_size, None);
    }

    #[test]
    fn v5_keepalive_zero_falls_back_to_config_without_announcement() {
        let packet = v5_connect_with("dev5", 0, true, None);
        let d = evaluate_connect(&packet, 300, peer());
        let ConnectDecision::Accept {
            connack,
            keep_alive_secs,
            idle_timeout,
            ..
        } = d
        else {
            panic!("expected Accept");
        };
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert_eq!(keep_alive_secs, 0);
        assert_eq!(idle_timeout, Duration::from_secs(300));
        assert_eq!(ack.server_keepalive_sec, None);
    }

    #[test]
    fn v5_keepalive_exactly_at_config_is_not_capped() {
        // 2 × 1.5 = 3s, config 3s — the cap comparison is `<=`, so no cap fires.
        let packet = v5_connect_with("dev5", 2, true, None);
        let d = evaluate_connect(&packet, 3, peer());
        let ConnectDecision::Accept {
            connack,
            keep_alive_secs,
            idle_timeout,
            ..
        } = d
        else {
            panic!("expected Accept");
        };
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert_eq!(keep_alive_secs, 2);
        assert_eq!(idle_timeout, Duration::from_secs(3));
        assert_eq!(ack.server_keepalive_sec, None);
    }

    #[test]
    fn v5_keepalive_one_second_over_config_is_capped() {
        // 2 × 1.5 = 3s against config 2s — one second past the boundary above.
        let packet = v5_connect_with("dev5", 2, true, None);
        let d = evaluate_connect(&packet, 2, peer());
        let ConnectDecision::Accept {
            connack,
            keep_alive_secs,
            idle_timeout,
            ..
        } = d
        else {
            panic!("expected Accept");
        };
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert_eq!(ack.server_keepalive_sec, Some(1));
        assert_eq!(keep_alive_secs, 1);
        assert_eq!(idle_timeout, Duration::from_millis(1500));
    }

    #[test]
    fn v5_huge_config_idle_clamped_to_supported_max() {
        let packet = v5_connect_with("dev5", 0, true, None);
        let d = evaluate_connect(&packet, u64::MAX, peer());
        let ConnectDecision::Accept {
            connack,
            idle_timeout,
            ..
        } = d
        else {
            panic!("expected Accept");
        };
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert_eq!(idle_timeout, Duration::from_secs(31_536_000));
        assert_eq!(ack.server_keepalive_sec, None);
    }

    #[test]
    fn v5_maximum_keepalive_under_generous_config_is_not_capped() {
        // 65535 × 1.5 = 98302.5s — the largest keep-alive a client can request.
        let packet = v5_connect_with("dev5", u16::MAX, true, None);
        let d = evaluate_connect(&packet, 200_000, peer());
        let ConnectDecision::Accept {
            connack,
            keep_alive_secs,
            idle_timeout,
            ..
        } = d
        else {
            panic!("expected Accept");
        };
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert_eq!(keep_alive_secs, 65535);
        assert_eq!(idle_timeout, Duration::from_millis(98_302_500));
        assert_eq!(ack.server_keepalive_sec, None);
    }

    #[test]
    fn v3_odd_keepalive_keeps_half_second_precision() {
        // 3 × 1.5 = 4.5s — proves the grace factor is not truncated to seconds.
        let packet = v3_connect_with("dev-1", 3, false);
        let d = evaluate_connect(&packet, 300, peer());
        let ConnectDecision::Accept { idle_timeout, .. } = d else {
            panic!("expected Accept");
        };
        assert_eq!(idle_timeout, Duration::from_millis(4500));
    }

    #[test]
    fn refuses_v5_auth_method_even_with_empty_client_id() {
        // The auth_method refusal outranks client-id assignment.
        let packet = v5_connect_with("", 60, true, Some("PLAIN"));
        let d = evaluate_connect(&packet, 300, peer());
        let ConnectDecision::Refuse { connack, .. } = d else {
            panic!("expected Refuse");
        };
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert!(matches!(
            ack.reason_code,
            V5ConnectAckReason::BadAuthenticationMethod
        ));
        assert_eq!(ack.assigned_client_id, None);
    }

    #[test]
    fn non_connect_v5_packet_yields_not_connect() {
        let packet = MqttPacket::V5(PacketV5::PingRequest);
        let d = evaluate_connect(&packet, 300, peer());
        assert!(matches!(d, ConnectDecision::NotConnect));
    }

    #[test]
    fn accepts_connect_carrying_will_flag() {
        let c = Connect {
            last_will: Some(LastWill {
                qos: QoS::AtMostOnce,
                retain: false,
                topic: "will/topic".to_string().into(),
                message: Bytes::from_static(b"goodbye"),
            }),
            ..Connect::default()
        }
        .client_id("dev-1".to_string());
        let packet = MqttPacket::V3(PacketV3::Connect(Box::new(c)));
        let d = evaluate_connect(&packet, 300, peer());
        assert!(matches!(d, ConnectDecision::Accept { .. }));
    }

    #[test]
    fn non_connect_packet_yields_not_connect() {
        let packet = MqttPacket::V3(PacketV3::PingRequest);
        let d = evaluate_connect(&packet, 300, peer());
        assert!(matches!(d, ConnectDecision::NotConnect));
    }

    /// AC-1 — a v5 client that declared a Maximum Packet Size smaller than
    /// the CONNACK it is owed is refused before any session begins, with a
    /// reason code the brief pins so a default-level warning carries it.
    /// Written against the `{ connack, .. }` pattern so the test compiles
    /// before the implementation step adds `sendable`. Today this returns
    /// `Accept` for any v5 CONNECT whose declared limit is non-zero, which is
    /// the exact RED behaviour: the size gate has not been wired.
    #[test]
    fn refuses_v5_connect_whose_declared_limit_cannot_hold_the_connack() {
        let c = rmqtt_codec::v5::Connect {
            client_id: "dev5".to_string().into(),
            keep_alive: 60,
            max_packet_size: NonZeroU32::new(11),
            ..rmqtt_codec::v5::Connect::default()
        };
        let packet = MqttPacket::V5(PacketV5::Connect(Box::new(c)));
        let d = evaluate_connect(&packet, 300, peer());
        let ConnectDecision::Refuse { connack, sendable } = d else {
            panic!("expected Refuse; today evaluate_connect returns Accept for v5 CONNECTs that fit the CONNACK");
        };
        let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
            panic!("expected v5 CONNACK");
        };
        assert!(
            matches!(
                ack.reason_code,
                V5ConnectAckReason::ImplementationSpecificError
            ),
            "size-policy refusal is ImplementationSpecificError, got {:?}",
            ack.reason_code,
        );
        assert!(
            !sendable,
            "refusal CONNACK is 12 bytes over a declared 11 — must be withheld"
        );
    }

    /// AC-1, AC-2 — the boundary between accept and refuse lives at the
    /// measured CONNACK length, not at any hardcoded threshold. Three shapes
    /// (bare CONNACK, server-announced keep-alive, and `assigned_client_id`)
    /// all pin their own Accept-length boundary; `max_packet_size == len` is
    /// the inclusive accept side, `len - 1` is the refuse side. A
    /// hardcoded threshold constant cannot satisfy all three because the
    /// three lengths are pairwise distinct, and the bare-CONNACK length is
    /// pinned to 12 so a constant that only happens to line up with one
    /// shape fails the other two.
    #[test]
    #[allow(clippy::too_many_lines)] // three CONNACK shapes each measured end-to-end, splitting buys nothing
    fn accepts_at_exactly_the_connack_length_for_every_connack_shape() {
        // (a) bare CONNACK — id "dev5", keep_alive 60 under config 300.
        let pkt_a = v5_connect_with("dev5", 60, false, None);
        let ConnectDecision::Accept {
            connack: a_connack, ..
        } = evaluate_connect(&pkt_a, 300, peer())
        else {
            panic!("shape (a) expected Accept at max_packet_size = None");
        };
        let a_len = {
            let mut buf = bytes::BytesMut::new();
            MqttEncoder::v5()
                .encode(a_connack, &mut buf)
                .expect("encode a");
            buf.len()
        };
        assert_eq!(a_len, 12, "bare CONNACK length pins the lower threshold");

        // (b) capped keep-alive — id "dev5", keep_alive 30 under config 30.
        let pkt_b = v5_connect_with("dev5", 30, false, None);
        let ConnectDecision::Accept {
            connack: b_connack, ..
        } = evaluate_connect(&pkt_b, 30, peer())
        else {
            panic!("shape (b) expected Accept at max_packet_size = None");
        };
        let b_len = {
            let mut buf = bytes::BytesMut::new();
            MqttEncoder::v5()
                .encode(b_connack, &mut buf)
                .expect("encode b");
            buf.len()
        };

        // (c) assigned client id — id "" + clean_start, keep_alive 60 under config 300.
        let pkt_c = v5_connect_with("", 60, true, None);
        let ConnectDecision::Accept {
            connack: c_connack, ..
        } = evaluate_connect(&pkt_c, 300, peer())
        else {
            panic!("shape (c) expected Accept at max_packet_size = None");
        };
        let c_len = {
            let mut buf = bytes::BytesMut::new();
            MqttEncoder::v5()
                .encode(c_connack, &mut buf)
                .expect("encode c");
            buf.len()
        };

        // Distinct — a hardcoded single threshold cannot satisfy all three.
        assert_ne!(
            a_len, b_len,
            "shapes (a) and (b) must encode to distinct lengths"
        );
        assert_ne!(
            a_len, c_len,
            "shapes (a) and (c) must encode to distinct lengths"
        );
        assert_ne!(
            b_len, c_len,
            "shapes (b) and (c) must encode to distinct lengths"
        );

        // At max_packet_size == len the implementation must keep Accept.
        let c_a = rmqtt_codec::v5::Connect {
            client_id: "dev5".to_string().into(),
            keep_alive: 60,
            max_packet_size: NonZeroU32::new(a_len.try_into().unwrap()),
            ..rmqtt_codec::v5::Connect::default()
        };
        let packet = MqttPacket::V5(PacketV5::Connect(Box::new(c_a)));
        assert!(
            matches!(
                evaluate_connect(&packet, 300, peer()),
                ConnectDecision::Accept { .. }
            ),
            "shape (a) at max_packet_size = a_len ({a_len}) must accept",
        );

        let c_b = rmqtt_codec::v5::Connect {
            client_id: "dev5".to_string().into(),
            keep_alive: 30,
            max_packet_size: NonZeroU32::new(b_len.try_into().unwrap()),
            ..rmqtt_codec::v5::Connect::default()
        };
        let packet = MqttPacket::V5(PacketV5::Connect(Box::new(c_b)));
        assert!(
            matches!(
                evaluate_connect(&packet, 30, peer()),
                ConnectDecision::Accept { .. }
            ),
            "shape (b) at max_packet_size = b_len ({b_len}) must accept",
        );

        let c_c = rmqtt_codec::v5::Connect {
            client_id: String::new().into(),
            keep_alive: 60,
            clean_start: true,
            max_packet_size: NonZeroU32::new(c_len.try_into().unwrap()),
            ..rmqtt_codec::v5::Connect::default()
        };
        let packet = MqttPacket::V5(PacketV5::Connect(Box::new(c_c)));
        assert!(
            matches!(
                evaluate_connect(&packet, 300, peer()),
                ConnectDecision::Accept { .. }
            ),
            "shape (c) at max_packet_size = c_len ({c_len}) must accept",
        );

        // At max_packet_size = len - 1 the implementation must Refuse.
        let a_under = u32::try_from(a_len - 1).unwrap();
        let c_a = rmqtt_codec::v5::Connect {
            client_id: "dev5".to_string().into(),
            keep_alive: 60,
            max_packet_size: NonZeroU32::new(a_under),
            ..rmqtt_codec::v5::Connect::default()
        };
        let packet = MqttPacket::V5(PacketV5::Connect(Box::new(c_a)));
        assert!(
            matches!(
                evaluate_connect(&packet, 300, peer()),
                ConnectDecision::Refuse { .. }
            ),
            "shape (a) at max_packet_size = {a_under} must refuse",
        );
    }

    /// AC-3 — v3 carries no Maximum Packet Size property, so the new size
    /// rule cannot reach a v3 CONNECT. Pure regression guard against an
    /// implementation that mis-targets the gate at `MqttPacket` rather than
    /// at the v5 CONNECT only.
    #[test]
    fn v3_connect_is_never_refused_for_packet_size() {
        let packet = v3_connect_with("dev-1", 60, false);
        let d = evaluate_connect(&packet, 300, peer());
        assert!(
            matches!(d, ConnectDecision::Accept { .. }),
            "v3 CONNECT cannot be refused for packet size"
        );
    }

    /// AC-11 — authentication-method refusal outranks the size refusal so a
    /// misconfigured client that violates both rules sees the more useful
    /// `BadAuthenticationMethod` diagnosis. Regression guard against an
    /// implementation that places the size check before the auth check (or
    /// that overwrites the auth reason with `ImplementationSpecificError`).
    /// The v5 CONNECT carries both `auth_method = Some("PLAIN")` AND a
    /// `max_packet_size` set to each of 11, 12, and `None`; all three must
    /// refuse with `BadAuthenticationMethod`, not `ImplementationSpecificError`.
    #[test]
    fn authentication_refusal_outranks_the_packet_size_refusal() {
        for (label, max_packet_size, expected_sendable) in [
            ("max=11", NonZeroU32::new(11), false),
            ("max=12", NonZeroU32::new(12), true),
            ("max=None", None, true),
        ] {
            let mut c = rmqtt_codec::v5::Connect {
                client_id: "dev5".to_string().into(),
                keep_alive: 60,
                clean_start: true,
                auth_method: Some("PLAIN".to_string().into()),
                max_packet_size,
                ..rmqtt_codec::v5::Connect::default()
            };
            let packet = MqttPacket::V5(PacketV5::Connect(Box::new(c.clone())));
            let d = evaluate_connect(&packet, 300, peer());
            let ConnectDecision::Refuse { connack, sendable } = d else {
                panic!("{label}: expected Refuse (auth_method outranks size)");
            };
            let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
                panic!("{label}: expected v5 CONNACK");
            };
            assert!(
                matches!(ack.reason_code, V5ConnectAckReason::BadAuthenticationMethod),
                "{label}: size policy must not overwrite the auth refusal, got {:?}",
                ack.reason_code,
            );
            assert_eq!(
                sendable, expected_sendable,
                "{label}: auth-refusal sendable must follow the size rule (false at 11, true at 12/None)",
            );
            // Shape under test: see brief.
            let _ = &mut c;
        }
    }

    /// AC-1, AC-2 — the below-limit half of the boundary for the two CONNACK
    /// shapes that `accepts_at_exactly_the_connack_length_for_every_connack_shape`
    /// only proves the accept side of. That test refuses at `len - 1` for the
    /// bare 12-byte CONNACK alone, so a gate hardcoded to 12 still passes it:
    /// the server-announced keep-alive (15) and assigned-client-id (~34)
    /// shapes both clear a constant 12 at their own `len - 1`. Refusing there
    /// is what a constant cannot do. Both cases also assert the refusal is
    /// sendable, because both thresholds sit above the 12-byte refusal CONNACK.
    #[test]
    fn refuses_one_byte_below_the_keepalive_and_assigned_id_connack_lengths() {
        let measure = |packet: &MqttPacket, cfg: u64| -> usize {
            let ConnectDecision::Accept { connack, .. } = evaluate_connect(packet, cfg, peer())
            else {
                panic!("expected Accept at max_packet_size = None");
            };
            let mut buf = bytes::BytesMut::new();
            MqttEncoder::v5()
                .encode(connack, &mut buf)
                .expect("encode CONNACK");
            buf.len()
        };
        let b_len = measure(&v5_connect_with("dev5", 30, false, None), 30);
        let c_len = measure(&v5_connect_with("", 60, true, None), 300);

        for (label, client_id, keep_alive, clean_start, cfg, len) in [
            (
                "server-announced keep-alive",
                "dev5",
                30_u16,
                false,
                30_u64,
                b_len,
            ),
            ("assigned client id", "", 60, true, 300, c_len),
        ] {
            assert!(
                len > 12,
                "{label}: this shape must be larger than the bare CONNACK or it proves nothing; got {len}",
            );
            let under = u32::try_from(len - 1).expect("len - 1 fits in u32");
            let c = rmqtt_codec::v5::Connect {
                client_id: client_id.to_string().into(),
                keep_alive,
                clean_start,
                max_packet_size: NonZeroU32::new(under),
                ..rmqtt_codec::v5::Connect::default()
            };
            let packet = MqttPacket::V5(PacketV5::Connect(Box::new(c)));
            let ConnectDecision::Refuse { connack, sendable } =
                evaluate_connect(&packet, cfg, peer())
            else {
                panic!("{label}: max_packet_size {under}, one below its {len}-byte CONNACK, must refuse");
            };
            let MqttPacket::V5(PacketV5::ConnectAck(ack)) = connack else {
                panic!("{label}: expected v5 CONNACK");
            };
            assert!(
                matches!(
                    ack.reason_code,
                    V5ConnectAckReason::ImplementationSpecificError
                ),
                "{label}: size-policy refusal is ImplementationSpecificError, got {:?}",
                ack.reason_code,
            );
            assert!(
                sendable,
                "{label}: the 12-byte refusal CONNACK fits {under} and must be sent",
            );
        }
    }

    /// `sendable` is unconditionally true on v3: MQTT 3.1.1 carries no Maximum
    /// Packet Size property, so a v3 refusal can never be gated by one.
    /// Regression guard against an implementation that lets the new field
    /// default to false on this arm, which would silently turn every v3
    /// `IdentifierRejected` refusal into a bare close — a behaviour change
    /// this feature does not make, and one no other test would catch.
    #[test]
    fn v3_refusal_connack_is_always_sendable() {
        let packet = MqttPacket::V3(PacketV3::Connect(Box::<Connect>::default()));
        let ConnectDecision::Refuse { connack, sendable } = evaluate_connect(&packet, 300, peer())
        else {
            panic!("expected Refuse");
        };
        let MqttPacket::V3(PacketV3::ConnectAck(ack)) = connack else {
            panic!("expected v3 CONNACK");
        };
        assert!(matches!(
            ack.return_code,
            ConnectAckReason::IdentifierRejected
        ));
        assert!(
            sendable,
            "a v3 refusal CONNACK is never gated by a Maximum Packet Size"
        );
    }
}
