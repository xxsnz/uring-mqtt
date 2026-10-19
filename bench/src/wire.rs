//! Hand-encoded MQTT 3.1.1 wire bytes for the load generator (task 5 of the
//! benchmark-harness feature). Signatures copied from
//! `features/benchmark-harness/PLAN.md` `## Interface Contracts`
//! (`### Bench — bench/src/wire.rs`).

use std::num::NonZeroU16;

use crate::payload::{PAYLOAD_BYTES, TOPIC};

/// Largest MQTT Remaining Length a bench client accepts from the broker.
/// `Codec::default()` is an unlimited codec, and the decoder reserves the
/// advertised remaining length before the body arrives, so an unbounded
/// codec lets a broker make a 64-byte-payload client reserve a 268 MB
/// frame. The widest frame the benchmark protocol delivers is a QoS 1
/// PUBLISH on `bench/load` (2 topic-length + 10 topic + 2 packet-id + 64
/// payload = 78 bytes); 1 KiB leaves slack for any control packet.
pub const INBOUND_REMAINING_LENGTH_MAX: u32 = 1024;

/// Remaining Length of every MQTT 3.1.1 CONNACK: the acknowledge-flags
/// byte and the return code. `rmqtt-codec` 0.2.2 accepts a longer body
/// and ignores its tail, so each bench client checks this itself.
pub const CONNACK_REMAINING_LENGTH: u32 = 2;

/// MQTT 3.1.1 CONNECT (clean session = 1, keep-alive = 0). The body is
/// `2 + 4 + 1 + 1 + 2 + 2 + client_id.len()` = `12 + client_id.len()` bytes.
///
/// Keep-alive 0 disables the server's idle deadline for the session (v3.1.1
/// §3.1.2.10), which is what a benchmark client wants: `bench sub` only
/// receives, so a non-zero keep-alive would have it closed after 1.5 ×
/// keep-alive seconds of sending nothing, turning a measured run into a
/// disconnection.
pub fn encode_connect(client_id: &str) -> Vec<u8> {
    let body_len = 12 + client_id.len();
    let mut out = Vec::with_capacity(2 + body_len + 4);
    out.push(0x10);
    write_varint(body_len, &mut out);
    out.extend_from_slice(b"\x00\x04MQTT");
    out.push(0x04); // protocol level 4 (MQTT 3.1.1)
    out.push(0x02); // flags: clean session
    out.extend_from_slice(&[0x00, 0x00]); // keep-alive 0
    out.extend_from_slice(&(u16::try_from(client_id.len()).expect("client_id fits in u16")).to_be_bytes());
    out.extend_from_slice(client_id.as_bytes());
    out
}

/// MQTT v3 PUBLISH with `retain = false`, `dup = false`, topic `TOPIC`,
/// `payload.len() == PAYLOAD_BYTES`. `packet_id` is required for QoS 1 and
/// forbidden for QoS 0 (the broker closes the connection otherwise).
pub fn encode_publish(
    dst: &mut Vec<u8>,
    qos: u8,
    packet_id: Option<NonZeroU16>,
    payload: &[u8; PAYLOAD_BYTES],
) {
    let fixed = 0x30 | (qos << 1);
    let body_len = 2 + TOPIC.len() + payload.len() + if qos > 0 { 2 } else { 0 };
    dst.push(fixed);
    write_varint(body_len, dst);
    dst.extend_from_slice(&(u16::try_from(TOPIC.len()).expect("topic fits in u16")).to_be_bytes());
    dst.extend_from_slice(TOPIC.as_bytes());
    if qos > 0 {
        let id = packet_id.expect("QoS > 0 requires packet_id").get();
        dst.extend_from_slice(&id.to_be_bytes());
    }
    dst.extend_from_slice(payload);
}

/// MQTT v3 SUBSCRIBE for the single filter `TOPIC` at `qos`. Flags 0x02 (the
/// SUBSCRIBE bits 1..0) per the v3.1.1 spec.
pub fn encode_subscribe(packet_id: NonZeroU16, qos: u8) -> Vec<u8> {
    let body_len = 2 + 2 + TOPIC.len() + 1;
    let mut out = Vec::with_capacity(2 + body_len + 4);
    out.push(0x82);
    write_varint(body_len, &mut out);
    out.extend_from_slice(&packet_id.get().to_be_bytes());
    out.extend_from_slice(&(u16::try_from(TOPIC.len()).expect("topic fits in u16")).to_be_bytes());
    out.extend_from_slice(TOPIC.as_bytes());
    out.push(qos);
    out
}

/// MQTT v3 PUBACK for one packet id (no payload).
pub fn encode_puback(packet_id: NonZeroU16) -> [u8; 4] {
    let id = packet_id.get();
    [0x40, 0x02, (id >> 8) as u8, (id & 0xFF) as u8]
}

pub const DISCONNECT: [u8; 2] = [0xE0, 0x00];

/// Encode an MQTT variable-length integer (1..4 bytes, continuation bit = 0x80).
/// Validated against the spec: values above `0x0FFF_FFFF` (268 MiB) cannot be
/// encoded; the load generator's frames are far smaller.
fn write_varint(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let mut encoded = u8::try_from(value & 0x7F).expect("low 7 bits fit in u8");
        value >>= 7;
        if value > 0 {
            encoded |= 0x80;
        }
        out.push(encoded);
        if value == 0 {
            return;
        }
    }
}