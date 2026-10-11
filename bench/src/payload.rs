use nix::time::{clock_gettime, ClockId};

pub const PAYLOAD_BYTES: usize = 64;
pub const TOPIC: &str = "bench/load";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    pub conn_id: u32,
    pub seq: u64,
    pub scheduled_ns: u64,
}

impl Stamp {
    pub fn encode(self) -> [u8; PAYLOAD_BYTES] {
        let mut buf = [0u8; PAYLOAD_BYTES];
        buf[0..4].copy_from_slice(&self.conn_id.to_le_bytes());
        buf[4..12].copy_from_slice(&self.seq.to_le_bytes());
        buf[12..20].copy_from_slice(&self.scheduled_ns.to_le_bytes());
        // bytes 20..64 already zero
        buf
    }

    pub fn decode(payload: &[u8]) -> Option<Stamp> {
        if payload.len() != PAYLOAD_BYTES {
            return None;
        }
        if payload[20..PAYLOAD_BYTES].iter().any(|&b| b != 0) {
            return None;
        }
        let conn_id = u32::from_le_bytes(payload[0..4].try_into().ok()?);
        let seq = u64::from_le_bytes(payload[4..12].try_into().ok()?);
        let scheduled_ns = u64::from_le_bytes(payload[12..20].try_into().ok()?);
        Some(Stamp {
            conn_id,
            seq,
            scheduled_ns,
        })
    }
}

pub fn monotonic_now_ns() -> u64 {
    let ts = clock_gettime(ClockId::CLOCK_MONOTONIC)
        .expect("CLOCK_MONOTONIC is always available on Linux");
    let sec: u64 = u64::try_from(ts.tv_sec()).expect("tv_sec non-negative");
    let nsec: u64 = u64::try_from(ts.tv_nsec()).expect("tv_nsec non-negative");
    sec * 1_000_000_000 + nsec
}
