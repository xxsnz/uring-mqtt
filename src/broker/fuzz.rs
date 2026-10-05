//! Fuzzing entry: one client session of attacker-chosen bytes through the
//! production connection handler. Compiled only under `cfg(test)` and
//! cargo-fuzz's `cfg(fuzzing)`, so a normal build exposes none of it.

use std::cell::RefCell;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use monoio::buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut};
use monoio::io::{AsyncReadRent, AsyncWriteRent};

use super::handler::{handle_client_io, SessionOutcome};
use super::worker::event_channel;
use super::{Publish, DEFAULT_CONNECTION_TIMEOUT_SECS, DEFAULT_IDLE_TIMEOUT_SECS};
use crate::codec::mqtt::MaxInboundPacketSize;
use crate::error::Error;

/// Peer address the scripted connection reports, in logs and in a v5
/// assigned client id.
const FUZZ_PEER_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1883);

/// Script prefix value meaning "no per-read cap".
const UNCHUNKED: u8 = 0;

type FuzzRuntime = monoio::Runtime<monoio::time::TimeDriver<monoio::LegacyDriver>>;

thread_local! {
    /// One runtime per thread, reused across inputs. The legacy driver
    /// because a scripted stream submits no io_uring operation; the timer
    /// because `handle_client_io` bounds every read and write with one.
    static RUNTIME: RefCell<FuzzRuntime> = RefCell::new(
        monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio legacy runtime"),
    );
}

/// libFuzzer entry. Byte 0 of `data` caps the bytes each read returns
/// (`0` = no cap); the rest is what the client sends.
pub fn session(data: &[u8]) {
    drop(run_session(data));
}

/// What one scripted session did.
#[cfg_attr(not(test), allow(dead_code))] // fields are read by this module's tests only
pub(crate) struct SessionTrace {
    pub(crate) result: Result<SessionOutcome, Error>,
    pub(crate) bytes_written: usize,
    pub(crate) delivered: Vec<Publish>,
}

/// Run `data` (format as for [`session`]) as one client connection through
/// `handle_client_io`, then drain every publish the session delivered.
/// Must not be called from inside a running monoio runtime.
pub(crate) fn run_session(data: &[u8]) -> SessionTrace {
    let (max_read, wire) = match data.split_first() {
        Some((&max_read, wire)) => (max_read, wire),
        None => (UNCHUNKED, &[][..]),
    };
    RUNTIME.with(|runtime| {
        runtime.borrow_mut().block_on(async {
            let (tx, mut rx) = event_channel(0, Arc::default());
            let mut io = ScriptedIo {
                wire,
                max_read,
                bytes_written: 0,
            };
            let result = handle_client_io(
                &mut io,
                FUZZ_PEER_ADDR,
                tx,
                DEFAULT_CONNECTION_TIMEOUT_SECS,
                DEFAULT_IDLE_TIMEOUT_SECS,
                MaxInboundPacketSize::DEFAULT,
            )
            .await;
            let mut delivered = Vec::new();
            while let Some(publish) = rx.recv().await {
                delivered.push(publish);
            }
            SessionTrace {
                result,
                bytes_written: io.bytes_written,
                delivered,
            }
        })
    })
}

/// In-memory client. Reads hand out the script, at most `max_read` bytes
/// each (`UNCHUNKED` = no cap), then end of stream; writes are counted and
/// discarded. `Framed` calls only `read` and `write`, so the vectored forms
/// report `Unsupported` instead of pretending.
struct ScriptedIo<'a> {
    wire: &'a [u8],
    max_read: u8,
    bytes_written: usize,
}

impl AsyncReadRent for ScriptedIo<'_> {
    async fn read<T: IoBufMut>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        let cap = if self.max_read == UNCHUNKED {
            self.wire.len()
        } else {
            usize::from(self.max_read).min(self.wire.len())
        };
        // Delegate to monoio's own `impl AsyncReadRent for &[u8]`, as the
        // handler tests' `TestIo` does: the crate forbids unsafe.
        let mut chunk: &[u8] = &self.wire[..cap];
        let (res, buf) = chunk.read(buf).await;
        if let Ok(n) = res {
            self.wire = &self.wire[n..];
        }
        (res, buf)
    }

    fn readv<T: IoVecBufMut>(
        &mut self,
        buf: T,
    ) -> impl Future<Output = monoio::BufResult<usize, T>> {
        std::future::ready((Err(std::io::ErrorKind::Unsupported.into()), buf))
    }
}

impl AsyncWriteRent for ScriptedIo<'_> {
    fn write<T: IoBuf>(&mut self, buf: T) -> impl Future<Output = monoio::BufResult<usize, T>> {
        let n = buf.bytes_init();
        self.bytes_written += n;
        std::future::ready((Ok(n), buf))
    }

    fn writev<T: IoVecBuf>(
        &mut self,
        buf_vec: T,
    ) -> impl Future<Output = monoio::BufResult<usize, T>> {
        std::future::ready((Err(std::io::ErrorKind::Unsupported.into()), buf_vec))
    }

    fn flush(&mut self) -> impl Future<Output = std::io::Result<()>> {
        std::future::ready(Ok(()))
    }

    fn shutdown(&mut self) -> impl Future<Output = std::io::Result<()>> {
        std::future::ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::handler::tests::{capture_logs, has_line_at};
    use crate::broker::QoS;
    use monoio::buf::VecBuf;

    const SEED_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fuzz_seeds/session");
    const V3_CONNACK_LEN: usize = 4;
    const V5_CONNACK_LEN: usize = 15;

    // Wire fixtures (byte literals — the server's own codec never produces expected values).
    /// v3 CONNECT: ka=60, id "test", flags 0x02 (Clean Session) — the handshake
    /// the hand-written scripts below start with.
    const V3_CONNECT_TEST: [u8; 18] = [
        0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x04, b't',
        b'e', b's', b't',
    ];
    /// v3 PUBLISH fixed header declaring Remaining Length 268 435 455 (the
    /// largest MQTT can frame), far above `MaxInboundPacketSize::DEFAULT`. The
    /// body never arrives: a decoder that reserved the declared length before
    /// checking the bound is the F2.4 denial of service.
    const V3_PUBLISH_DECLARING_MAX_REMAINING_LENGTH: [u8; 5] = [0x30, 0xFF, 0xFF, 0xFF, 0x7F];
    /// Script bytes (prefix included) whose packets are malformed in ways the
    /// committed seeds do not cover. `session` must return on each of them.
    const ADVERSARIAL_SCRIPTS: &[(&str, &[u8])] = &[
        ("prefix-only", &[UNCHUNKED]),
        ("one-byte-reads-prefix-only", &[1]),
        (
            "truncated-v3-connect",
            &[UNCHUNKED, 0x10, 0x10, 0x00, 0x04, b'M'],
        ),
        (
            "connect-remaining-length-overshoots",
            &[
                UNCHUNKED, 0x10, 0x7F, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04,
            ],
        ),
        (
            "five-byte-remaining-length-varint",
            &[UNCHUNKED, 0x10, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
        ),
        ("empty-protocol-name", &[UNCHUNKED, 0x10, 0x02, 0x00, 0x00]),
        (
            "v5-connect-declaring-unterminated-properties",
            &[
                UNCHUNKED, 0x10, 0x11, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C,
                0x7F, 0x00, 0x04, b't', b'e', b's', b't',
            ],
        ),
        (
            "v3-publish-topic-not-utf8",
            &[
                UNCHUNKED, 0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C,
                0x00, 0x04, b't', b'e', b's', b't', 0x30, 0x04, 0x00, 0x02, 0xFF, 0xFE,
            ],
        ),
        (
            "v3-subscribe-with-no-filter",
            &[
                UNCHUNKED, 0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C,
                0x00, 0x04, b't', b'e', b's', b't', 0x82, 0x02, 0x00, 0x25,
            ],
        ),
        (
            "second-connect-after-handshake",
            &[
                UNCHUNKED, 0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C,
                0x00, 0x04, b't', b'e', b's', b't', 0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T',
                0x04, 0x02, 0x00, 0x3C, 0x00, 0x04, b't', b'e', b's', b't',
            ],
        ),
    ];
    /// Remaining Length of a header-only first packet, for the packet-type sweep.
    const EMPTY_REMAINING_LENGTH: u8 = 0x00;
    /// Mutated copies made of each committed seed by the deterministic sweep.
    const MUTATIONS_PER_SEED: usize = 64;
    /// Fixed start state, so the sweep runs the same inputs on every machine.
    const MUTATION_SEED: u64 = 0x5DEE_CE66_D5CA_FE01;
    /// Numerical Recipes 64-bit LCG parameters, with the low bits discarded.
    const LCG_MULTIPLIER: u64 = 6_364_136_223_846_793_005;
    const LCG_INCREMENT: u64 = 1_442_695_040_888_963_407;
    const LCG_DISCARD_BITS: u32 = 33;
    /// Distinct values of one byte.
    const BYTE_VALUES: u64 = 256;
    /// Extra replays of each seed in the repeatability check.
    const REPLAY_ROUNDS: usize = 2;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Expected {
        Served,
        Refused,
        Violation,
        ClientClosed,
        Io,
    }

    struct SeedTrace {
        file: &'static str,
        outcome: Expected,
        bytes_written: usize,
        delivered: &'static [(&'static str, QoS)],
    }

    /// The 11 committed seeds and their recorded traces. Numbers come from
    /// PLAN.md `## Data Model` and the byte-count sources it cites.
    const SEED_TRACES: &[SeedTrace] = &[
        SeedTrace {
            file: "v3-qos0-publish",
            outcome: Expected::Served,
            bytes_written: 4,
            delivered: &[("t", QoS::AtMostOnce)],
        },
        SeedTrace {
            file: "v3-qos1-publish",
            outcome: Expected::Served,
            bytes_written: 8,
            delivered: &[("t", QoS::AtLeastOnce)],
        },
        SeedTrace {
            file: "v3-qos2-flow",
            outcome: Expected::Served,
            bytes_written: 12,
            delivered: &[("t", QoS::ExactlyOnce)],
        },
        SeedTrace {
            file: "v3-qos2-flow-1-byte-reads",
            outcome: Expected::Served,
            bytes_written: 12,
            delivered: &[("t", QoS::ExactlyOnce)],
        },
        SeedTrace {
            file: "v3-subscribe-unsubscribe-ping",
            outcome: Expected::Served,
            bytes_written: 15,
            delivered: &[],
        },
        SeedTrace {
            file: "v3-puback-from-client",
            outcome: Expected::Violation,
            bytes_written: 4,
            delivered: &[],
        },
        SeedTrace {
            file: "v3-persistent-session",
            outcome: Expected::Refused,
            bytes_written: 0,
            delivered: &[],
        },
        SeedTrace {
            file: "first-packet-not-connect",
            outcome: Expected::Io,
            bytes_written: 0,
            delivered: &[],
        },
        SeedTrace {
            file: "v5-session",
            outcome: Expected::Served,
            bytes_written: 41,
            delivered: &[("t", QoS::AtLeastOnce), ("t", QoS::ExactlyOnce)],
        },
        SeedTrace {
            file: "v5-wildcard-topic",
            outcome: Expected::Violation,
            bytes_written: 19,
            delivered: &[],
        },
        SeedTrace {
            file: "empty",
            outcome: Expected::ClientClosed,
            bytes_written: 0,
            delivered: &[],
        },
    ];

    /// A `SessionTrace` in comparable form: outcome, reply byte count and the
    /// delivered (topic, QoS) list.
    type ComparableTrace = (Expected, usize, Vec<(String, QoS)>);

    fn comparable(trace: &SessionTrace) -> ComparableTrace {
        (
            observed(&trace.result),
            trace.bytes_written,
            trace
                .delivered
                .iter()
                .map(|p| (p.topic().to_owned(), p.qos()))
                .collect(),
        )
    }

    fn observed(result: &Result<SessionOutcome, Error>) -> Expected {
        match result {
            Ok(SessionOutcome::Served) => Expected::Served,
            Ok(SessionOutcome::Refused) => Expected::Refused,
            Ok(SessionOutcome::Violation) => Expected::Violation,
            Err(Error::ClientClosed) => Expected::ClientClosed,
            Err(Error::Io(_)) => Expected::Io,
            other => panic!("unexpected session result {other:?}"),
        }
    }

    fn seed(name: &str) -> Vec<u8> {
        std::fs::read(format!("{SEED_DIR}/{name}"))
            .unwrap_or_else(|e| panic!("read seed {name}: {e}"))
    }

    /// AC-1 + AC-5 — every committed seed replays to its recorded trace.
    #[test]
    fn every_committed_seed_replays_to_its_recorded_trace() {
        let entries = std::fs::read_dir(SEED_DIR)
            .unwrap_or_else(|e| panic!("read_dir {SEED_DIR}: {e}"))
            .collect::<Result<Vec<_>, _>>()
            .expect("collect seed dir entries");
        for entry in entries {
            let name = entry.file_name();
            let name = name.to_string_lossy().into_owned();
            let row = SEED_TRACES
                .iter()
                .find(|r| r.file == name)
                .unwrap_or_else(|| panic!("seed {name} has no SEED_TRACES row"));
            let bytes = seed(&name);
            let trace = run_session(&bytes);
            let observed = observed(&trace.result);
            assert_eq!(
                observed, row.outcome,
                "seed {name}: outcome mismatch (got {observed:?}, want {:?})",
                row.outcome
            );
            assert_eq!(
                trace.bytes_written, row.bytes_written,
                "seed {name}: bytes_written mismatch (got {}, want {})",
                trace.bytes_written, row.bytes_written
            );
            let delivered: Vec<(&str, QoS)> = trace
                .delivered
                .iter()
                .map(|p| (p.topic(), p.qos()))
                .collect();
            assert_eq!(
                delivered,
                row.delivered.to_vec(),
                "seed {name}: delivered mismatch (got {delivered:?}, want {:?})",
                row.delivered
            );
        }
    }

    /// AC-2 — every seed file is named in `SEED_TRACES` exactly once, and
    /// `SEED_TRACES` names no file that is absent from the directory.
    #[test]
    fn every_seed_file_has_a_recorded_trace_and_every_trace_a_file() {
        let mut on_disk: Vec<String> = std::fs::read_dir(SEED_DIR)
            .unwrap_or_else(|e| panic!("read_dir {SEED_DIR}: {e}"))
            .map(|e| {
                e.expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        on_disk.sort_unstable();
        let mut in_table: Vec<&'static str> = SEED_TRACES.iter().map(|r| r.file).collect();
        in_table.sort_unstable();
        let on_disk_refs: Vec<&str> = on_disk.iter().map(String::as_str).collect();
        assert_eq!(
            on_disk_refs, in_table,
            "seed files on disk and SEED_TRACES rows disagree"
        );
        let pre = in_table.len();
        in_table.dedup();
        assert_eq!(pre, in_table.len(), "SEED_TRACES contains a duplicate row");
    }

    /// AC-3 — committed seeds reach v3 decode, v5 dispatch and partial reads.
    #[test]
    fn committed_seeds_reach_v3_v5_dispatch_and_partial_reads() {
        for (name, connack_len) in [
            ("v3-qos2-flow", V3_CONNACK_LEN),
            ("v3-qos2-flow-1-byte-reads", V3_CONNACK_LEN),
            ("v5-session", V5_CONNACK_LEN),
        ] {
            let bytes = seed(name);
            let trace = run_session(&bytes);
            assert!(
                matches!(trace.result, Ok(SessionOutcome::Served)),
                "{name}: expected Ok(Served), got {:?}",
                trace.result
            );
            assert!(
                trace.bytes_written > connack_len,
                "{name}: expected bytes_written > {connack_len} (CONNACK), got {}",
                trace.bytes_written
            );
            assert!(
                !trace.delivered.is_empty(),
                "{name}: expected at least one delivered Publish"
            );
        }
        assert_eq!(
            seed("v3-qos2-flow-1-byte-reads")[0],
            1,
            "v3-qos2-flow-1-byte-reads prefix byte must be 1 (one-byte reads)"
        );
        assert_eq!(
            seed("v3-qos2-flow")[0],
            0,
            "v3-qos2-flow prefix byte must be 0 (no per-read cap)"
        );
    }

    /// AC-4 — a script's first byte caps each read's return size.
    #[test]
    fn scripted_reads_never_exceed_the_prefix_cap() {
        RUNTIME.with(|rt| {
            rt.borrow_mut().block_on(async {
                let mut io = ScriptedIo {
                    wire: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                    max_read: 3,
                    bytes_written: 0,
                };
                for (expected, idx) in [
                    (&[1u8, 2, 3][..], 0usize),
                    (&[4u8, 5, 6][..], 1),
                    (&[7u8, 8, 9][..], 2),
                    (&[10u8][..], 3),
                ] {
                    let buf = Vec::with_capacity(4096);
                    let (res, buf) = io.read(buf).await;
                    let n = res.expect("read ok");
                    assert_eq!(
                        &buf[..n],
                        expected,
                        "read {idx}: got {:?}, want {expected:?}",
                        &buf[..n]
                    );
                }
                let buf = Vec::with_capacity(4096);
                let (res, _) = io.read(buf).await;
                assert_eq!(res.expect("eof ok"), 0, "fifth read must be EOF");

                let mut io = ScriptedIo {
                    wire: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                    max_read: UNCHUNKED,
                    bytes_written: 0,
                };
                let buf = Vec::with_capacity(4096);
                let (res, buf) = io.read(buf).await;
                let n = res.expect("read ok");
                assert_eq!(
                    &buf[..n],
                    &[1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10][..],
                    "unchunked read must return every wire byte"
                );
                let buf = Vec::with_capacity(4096);
                let (res, _) = io.read(buf).await;
                assert_eq!(
                    res.expect("eof ok"),
                    0,
                    "second read after exhaustion must be EOF"
                );

                let mut io = ScriptedIo {
                    wire: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                    max_read: UNCHUNKED,
                    bytes_written: 0,
                };
                let buf = Vec::with_capacity(4);
                let (res, buf) = io.read(buf).await;
                let n = res.expect("read ok");
                assert_eq!(
                    &buf[..n],
                    &[1u8, 2, 3, 4][..],
                    "buffer capacity still bounds an UNCHUNKED read"
                );
            });
        });
    }

    /// AC-9 — the public `session` entry (not `run_session`) causes the
    /// dispatch violation WARN lines for v3 and v5 adversarial inputs.
    #[test]
    fn session_drives_v3_and_v5_bytes_through_dispatch() {
        let sink = capture_logs();
        session(&seed("v3-puback-from-client"));
        session(&seed("v5-wildcard-topic"));
        let logs = String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
        assert!(
            has_line_at(
                &logs,
                "WARN",
                &[
                    "protocol violation: PUBACK, PUBREC or PUBCOMP from a client",
                    "127.0.0.1:1883"
                ],
            ),
            "missing v3 PUBACK-from-client WARN with peer 127.0.0.1:1883, got: {logs}"
        );
        assert!(
            has_line_at(
                &logs,
                "WARN",
                &[
                    "protocol violation: PUBLISH topic name empty or containing a wildcard",
                    "127.0.0.1:1883"
                ],
            ),
            "missing v5 wildcard-topic WARN with peer 127.0.0.1:1883, got: {logs}"
        );
    }

    /// AC-5 — a script that carries a prefix byte but no wire bytes is still an
    /// empty client stream, for a capped and an uncapped prefix alike.
    #[test]
    fn a_script_with_no_wire_bytes_reports_a_closed_client() {
        for prefix in [UNCHUNKED, 1] {
            let trace = run_session(&[prefix]);
            assert_eq!(
                observed(&trace.result),
                Expected::ClientClosed,
                "prefix {prefix}: expected ClientClosed, got {:?}",
                trace.result
            );
            assert_eq!(
                trace.bytes_written, 0,
                "prefix {prefix}: expected no reply bytes"
            );
            assert!(
                trace.delivered.is_empty(),
                "prefix {prefix}: expected no delivered Publish"
            );
        }
    }

    /// The thread-local runtime and the event channel are rebuilt per input, so
    /// replaying inputs in any order reports the same trace every time.
    /// libFuzzer calls `session` millions of times on one thread: state kept
    /// between calls would make a crash depend on its predecessors.
    #[test]
    fn replaying_seeds_in_any_order_repeats_every_trace() {
        let names = ["v5-session", "v3-qos2-flow", "v5-wildcard-topic"];
        let first: Vec<ComparableTrace> = names
            .iter()
            .map(|name| comparable(&run_session(&seed(name))))
            .collect();
        for round in 0..REPLAY_ROUNDS {
            for (name, expected) in names.iter().zip(&first) {
                assert_eq!(
                    comparable(&run_session(&seed(name))),
                    *expected,
                    "seed {name}, replay round {round}: trace differs from the first run"
                );
            }
        }
    }

    /// A packet whose fixed header declares more bytes than
    /// `MaxInboundPacketSize::DEFAULT` ends the session on the header alone:
    /// the body is never awaited and the declared length is never reserved.
    /// This is the allocation the fuzz runs' `-malloc_limit_mb` flag watches.
    #[test]
    fn a_packet_declaring_more_than_the_inbound_bound_ends_the_session() {
        let mut script = vec![UNCHUNKED];
        script.extend_from_slice(&V3_CONNECT_TEST);
        script.extend_from_slice(&V3_PUBLISH_DECLARING_MAX_REMAINING_LENGTH);
        let sink = capture_logs();
        let trace = run_session(&script);
        assert_eq!(
            observed(&trace.result),
            Expected::Violation,
            "expected the bound to end the session as a violation, got {:?}",
            trace.result
        );
        let logs = String::from_utf8(sink.lock().expect("log buffer").clone()).expect("utf8 logs");
        assert!(
            has_line_at(
                &logs,
                "WARN",
                &[
                    "protocol violation: packet exceeds the inbound packet-size bound",
                    "127.0.0.1:1883"
                ],
            ),
            "expected the inbound-bound violation WARN, not a generic rejection, got: {logs}"
        );
        assert_eq!(
            trace.bytes_written, V3_CONNACK_LEN,
            "expected the CONNACK and nothing else"
        );
        assert!(
            trace.delivered.is_empty(),
            "expected no delivered Publish, got {:?}",
            trace.delivered
        );
    }

    /// The fuzz oracle on stable CI: `session` returns, rather than panicking,
    /// on hand-written malformed scripts and on a header-only first packet of
    /// every one of the 256 first-byte values.
    #[test]
    fn session_returns_on_every_malformed_script() {
        for (name, script) in ADVERSARIAL_SCRIPTS {
            session(script);
            assert!(!script.is_empty(), "script {name} must carry a prefix byte");
        }
        for first_byte in u8::MIN..=u8::MAX {
            session(&[UNCHUNKED, first_byte, EMPTY_REMAINING_LENGTH]);
        }
    }

    /// The same oracle over mutations of the committed seeds: one byte of each
    /// seed is flipped `MUTATIONS_PER_SEED` times from a fixed start state, so
    /// every machine runs the same inputs. Both outcome classes must appear —
    /// a sweep whose every input died at the first byte would prove nothing.
    #[test]
    fn run_session_returns_on_mutated_committed_seeds() {
        let mut state = MUTATION_SEED;
        let mut runs = 0usize;
        let mut served = 0usize;
        let mut failed = 0usize;
        for row in SEED_TRACES {
            let bytes = seed(row.file);
            for _ in 0..MUTATIONS_PER_SEED {
                let mut mutated = bytes.clone();
                let position = next_random(&mut state);
                let value = next_random(&mut state);
                let value = u8::try_from(value % BYTE_VALUES).expect("remainder below 256");
                if mutated.is_empty() {
                    mutated.push(value);
                } else {
                    let len = u64::try_from(mutated.len()).expect("seed length fits u64");
                    let index =
                        usize::try_from(position % len).expect("index below the seed length");
                    mutated[index] ^= value;
                }
                if run_session(&mutated).result.is_ok() {
                    served += 1;
                } else {
                    failed += 1;
                }
                runs += 1;
            }
        }
        assert_eq!(
            runs,
            SEED_TRACES.len() * MUTATIONS_PER_SEED,
            "the sweep must run every mutation of every seed"
        );
        assert!(
            served > 0 && failed > 0,
            "the sweep must reach both outcome classes, got {served} Ok and {failed} Err"
        );
    }

    /// A read cap above the bytes left hands out the whole remainder, and a cap
    /// of 1 hands out one byte per read — the two ends of AC-4's bound.
    #[test]
    fn a_read_cap_bounds_each_read_without_losing_wire_bytes() {
        RUNTIME.with(|rt| {
            rt.borrow_mut().block_on(async {
                let mut io = ScriptedIo {
                    wire: &[1, 2, 3],
                    max_read: u8::MAX,
                    bytes_written: 0,
                };
                let (res, buf) = io.read(Vec::with_capacity(4096)).await;
                let n = res.expect("read ok");
                assert_eq!(
                    &buf[..n],
                    &[1u8, 2, 3][..],
                    "a cap above the remaining wire must return every byte"
                );

                let mut io = ScriptedIo {
                    wire: &[1, 2, 3],
                    max_read: 1,
                    bytes_written: 0,
                };
                let mut seen = Vec::new();
                loop {
                    let (res, buf) = io.read(Vec::with_capacity(4096)).await;
                    let n = res.expect("read ok");
                    if n == 0 {
                        break;
                    }
                    assert_eq!(n, 1, "a cap of 1 must return one byte per read");
                    seen.extend_from_slice(&buf[..n]);
                }
                assert_eq!(
                    seen,
                    vec![1u8, 2, 3],
                    "one-byte reads must still deliver the whole script in order"
                );
            });
        });
    }

    /// `ScriptedIo` counts written bytes, succeeds on `flush` and `shutdown`,
    /// and reports `Unsupported` for the vectored forms instead of pretending
    /// to have moved bytes. `Framed` calls only `read` and `write`; a vectored
    /// caller must see the refusal, not a silent loss.
    #[test]
    fn scripted_io_counts_writes_and_refuses_the_vectored_forms() {
        RUNTIME.with(|rt| {
            rt.borrow_mut().block_on(async {
                let mut io = ScriptedIo {
                    wire: &[1, 2, 3],
                    max_read: UNCHUNKED,
                    bytes_written: 0,
                };

                let (res, _) = io.write(vec![1u8, 2, 3, 4]).await;
                assert_eq!(res.expect("write ok"), 4, "write must report every byte");
                let (res, _) = io.write(Vec::<u8>::new()).await;
                assert_eq!(res.expect("write ok"), 0, "an empty write moves no byte");
                assert_eq!(io.bytes_written, 4, "writes must accumulate");

                let (res, _) = io.writev(VecBuf::from(vec![vec![1u8, 2, 3]])).await;
                assert_eq!(
                    res.expect_err("writev must fail").kind(),
                    std::io::ErrorKind::Unsupported,
                    "writev must report Unsupported"
                );
                assert_eq!(
                    io.bytes_written, 4,
                    "a refused writev must not count any byte"
                );

                let (res, _) = io.readv(VecBuf::from(vec![vec![0u8; 4]])).await;
                assert_eq!(
                    res.expect_err("readv must fail").kind(),
                    std::io::ErrorKind::Unsupported,
                    "readv must report Unsupported"
                );

                io.flush().await.expect("flush ok");
                io.shutdown().await.expect("shutdown ok");

                let (res, buf) = io.read(Vec::with_capacity(4096)).await;
                let n = res.expect("read ok");
                assert_eq!(
                    &buf[..n],
                    &[1u8, 2, 3][..],
                    "a refused readv must leave the script unconsumed"
                );
            });
        });
    }

    /// Pseudo-random, so the mutation sweep is reproducible without a dependency.
    fn next_random(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(LCG_MULTIPLIER)
            .wrapping_add(LCG_INCREMENT);
        *state >> LCG_DISCARD_BITS
    }
}
