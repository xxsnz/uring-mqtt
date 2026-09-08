use std::net::SocketAddr;
use std::time::Duration;

use crate::codec::mqtt::{ConnectAck, ConnectAckReason, MqttPacket, PacketV3, PacketV5};
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
    },
    /// CONNECT refused: send `connack` (v3 error code / v5 reason >= 0x80), then close.
    Refuse { connack: MqttPacket },
    /// First packet was not CONNECT: close without CONNACK.
    NotConnect,
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
            }
        }
        MqttPacket::V5(PacketV5::Connect(c)) => {
            if c.auth_method.is_some() {
                return ConnectDecision::Refuse {
                    connack: MqttPacket::V5(PacketV5::ConnectAck(Box::new(honest_v5_connack(
                        V5ConnectAckReason::BadAuthenticationMethod,
                    )))),
                };
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
            ConnectDecision::Accept {
                connack: MqttPacket::V5(PacketV5::ConnectAck(Box::new(connack))),
                client_id,
                keep_alive_secs: negotiated_ka,
                idle_timeout: idle,
            }
        }
        _ => ConnectDecision::NotConnect,
    }
}

/// Honest v5 CONNACK: truthful capability announcement per AC-15.
///
/// Single constructor for both the accept path (this module's
/// `evaluate_connect`) and the decoder-level refusal path in
/// `handler::handle_client_io` — capability fields cannot diverge
/// between the two when later features raise `max_qos`.
pub(crate) fn honest_v5_connack(reason: V5ConnectAckReason) -> rmqtt_codec::v5::ConnectAck {
    rmqtt_codec::v5::ConnectAck {
        reason_code: reason,
        max_qos: QoS::AtMostOnce,
        retain_available: true,
        wildcard_subscription_available: false,
        subscription_identifiers_available: false,
        shared_subscription_available: false,
        session_expiry_interval_secs: Some(0),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
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
        let ConnectDecision::Refuse { connack } = d else {
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
        let ConnectDecision::Refuse { connack } = d else {
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
        assert!(matches!(ack.max_qos, QoS::AtMostOnce));
        assert!(!ack.wildcard_subscription_available);
        assert!(!ack.shared_subscription_available);
        assert!(!ack.subscription_identifiers_available);
        assert_eq!(ack.session_expiry_interval_secs, Some(0));
        assert!(!ack.session_present);
        assert!(ack.retain_available);
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
        let ConnectDecision::Refuse { connack } = d else {
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
}
