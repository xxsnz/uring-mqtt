use rmqtt_codec::error::{DecodeError, EncodeError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("MQTT decode error: {0}")]
    Decode(#[from] DecodeError),

    #[error("MQTT encode error: {0}")]
    Encode(#[from] EncodeError),

    #[error("Connection timeout")]
    Timeout,

    #[error("Client closed connection during handshake")]
    ClientClosed,

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Worker error: {0}")]
    Worker(String),
}

impl Error {
    /// True when the underlying cause is a routine client-side disconnect
    /// (clean hangup, peer reset, broken pipe) that should not produce a log
    /// line at the worker level. Typed: matches variants and `io::ErrorKind`
    /// only — never message text.
    pub(crate) fn is_routine_disconnect(&self) -> bool {
        match self {
            Self::Timeout | Self::ClientClosed => true,
            Self::Io(e) => matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ),
            Self::Decode(_) | Self::Encode(_) | Self::Protocol(_) | Self::Worker(_) => false,
        }
    }

    /// `true` for an I/O error whose source is `TruncatedPacket`: the client
    /// closed its connection inside a packet. Typed: a downcast on the
    /// source, never message text.
    pub(crate) fn is_truncated_packet(&self) -> bool {
        matches!(
            self,
            Self::Io(e) if e
                .get_ref()
                .and_then(|s| s.downcast_ref::<crate::codec::mqtt::TruncatedPacket>())
                .is_some()
        )
    }
}

/// [`Error::is_routine_disconnect`] for the benchmark's tokio twin, which
/// reports session errors the way the shipped worker logs them. Reachable
/// only through `runtime_seam`, so the shipped API does not grow.
#[cfg(feature = "runtime-seam")]
pub fn is_routine_disconnect(err: &Error) -> bool {
    err.is_routine_disconnect()
}

/// [`Error::is_truncated_packet`] for the benchmark's tokio twin, whose
/// own load generator ends stalled connections mid-packet. Reachable
/// only through `runtime_seam`, so the shipped API does not grow.
#[cfg(feature = "runtime-seam")]
pub fn is_truncated_packet(err: &Error) -> bool {
    err.is_truncated_packet()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = Error::Io(io_err);

        let msg = format!("{err}");
        assert!(msg.contains("I/O error"));
        assert!(msg.contains("file not found"));
    }

    #[test]
    fn test_error_io_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset");
        let err: Error = io_err.into();

        assert!(matches!(err, Error::Io(_)));
        let msg = format!("{err}");
        assert!(msg.contains("connection reset"));
    }

    #[test]
    fn test_error_timeout() {
        let err = Error::Timeout;

        let msg = format!("{err}");
        assert_eq!(msg, "Connection timeout");
    }

    #[test]
    fn test_error_protocol() {
        let err = Error::Protocol("invalid packet type".to_string());

        let msg = format!("{err}");
        assert!(msg.contains("Protocol error"));
        assert!(msg.contains("invalid packet type"));
    }

    #[test]
    fn test_error_worker() {
        let err = Error::Worker("worker crashed".to_string());

        let msg = format!("{err}");
        assert!(msg.contains("Worker error"));
        assert!(msg.contains("worker crashed"));
    }

    #[test]
    fn test_error_debug() {
        let err = Error::Timeout;
        let debug_str = format!("{err:?}");
        assert!(debug_str.contains("Timeout"));

        let err = Error::Protocol("test".to_string());
        let debug_str = format!("{err:?}");
        assert!(debug_str.contains("Protocol"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn routine_disconnects_are_classified_as_routine() {
        assert!(Error::Timeout.is_routine_disconnect());
        assert!(Error::ClientClosed.is_routine_disconnect());
        assert!(Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "any",
        ))
        .is_routine_disconnect());
        assert!(
            Error::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "any",))
                .is_routine_disconnect()
        );
    }

    #[test]
    fn truncated_packet_error_is_recognised_and_stays_non_routine() {
        let truncated = Error::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            crate::codec::mqtt::TruncatedPacket,
        ));
        assert!(truncated.is_truncated_packet());
        assert!(!truncated.is_routine_disconnect());

        let plain_eof = Error::Io(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        assert!(!plain_eof.is_truncated_packet());

        assert!(!Error::ClientClosed.is_truncated_packet());
    }

    #[test]
    fn non_routine_errors_are_not_routine() {
        assert!(
            !Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "x"))
                .is_routine_disconnect()
        );
        assert!(!Error::Protocol("x".into()).is_routine_disconnect());
        assert!(!Error::Worker("x".into()).is_routine_disconnect());
    }

    #[test]
    fn codec_errors_are_not_routine() {
        assert!(!Error::Decode(DecodeError::MalformedPacket).is_routine_disconnect());
        assert!(!Error::Decode(DecodeError::InvalidClientId).is_routine_disconnect());
        assert!(!Error::Encode(EncodeError::MalformedPacket).is_routine_disconnect());
    }

    #[test]
    fn near_miss_io_kinds_are_not_routine() {
        // Only ConnectionReset and BrokenPipe count as routine. UnexpectedEof
        // in particular must stay non-routine: a truncated frame is an
        // anomaly, not a clean hangup (same boundary as AC-12).
        for kind in [
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::ConnectionRefused,
            std::io::ErrorKind::WouldBlock,
            std::io::ErrorKind::Other,
        ] {
            assert!(
                !Error::Io(std::io::Error::new(kind, "x")).is_routine_disconnect(),
                "{kind:?} must not be classified as a routine disconnect"
            );
        }
    }

    #[test]
    fn test_error_client_closed() {
        let err = Error::ClientClosed;

        let msg = format!("{err}");
        assert_eq!(msg, "Client closed connection during handshake");
    }

    /// The seam function the benchmark twin calls is the only way an
    /// external crate reaches the classifier, so it must publish the
    /// same verdict the shipped worker logs by.
    #[cfg(feature = "runtime-seam")]
    #[test]
    fn the_seam_classifier_publishes_the_method_verdict() {
        let cases = [
            (Error::Timeout, true),
            (Error::ClientClosed, true),
            (
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "x",
                )),
                true,
            ),
            (
                Error::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "x")),
                true,
            ),
            (
                Error::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "x")),
                false,
            ),
            (
                Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "x")),
                false,
            ),
            (Error::Decode(DecodeError::MalformedPacket), false),
            (Error::Encode(EncodeError::MalformedPacket), false),
            (Error::Protocol("reset closed Timeout".into()), false),
            (Error::Worker("x".into()), false),
        ];
        for (err, expected) in &cases {
            assert_eq!(
                is_routine_disconnect(err),
                *expected,
                "seam classified {err:?} wrongly"
            );
            assert_eq!(
                is_routine_disconnect(err),
                err.is_routine_disconnect(),
                "seam disagrees with the method for {err:?}"
            );
        }
    }

    /// AC-29 — the twin's `join` decides on the seam function, so it must
    /// publish the same verdict as the method for every error shape, and it
    /// must not widen to a plain `UnexpectedEof` or to the routine class.
    #[cfg(feature = "runtime-seam")]
    #[test]
    fn the_truncation_seam_publishes_the_method_verdict() {
        let cases = [
            (
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    crate::codec::mqtt::TruncatedPacket,
                )),
                true,
            ),
            (
                Error::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "x")),
                false,
            ),
            (
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    DecodeError::MalformedPacket,
                )),
                false,
            ),
            (Error::ClientClosed, false),
            (Error::Timeout, false),
            (Error::Decode(DecodeError::MalformedPacket), false),
            (
                Error::Protocol("connection closed mid-packet".into()),
                false,
            ),
        ];
        for (err, expected) in &cases {
            assert_eq!(
                is_truncated_packet(err),
                *expected,
                "seam classified {err:?} wrongly"
            );
            assert_eq!(
                is_truncated_packet(err),
                err.is_truncated_packet(),
                "seam disagrees with the method for {err:?}"
            );
        }
    }

    #[test]
    fn misleading_messages_do_not_fool_classification() {
        // The exact strings the old string-matching filter used to match.
        assert!(!Error::Protocol("reset closed Timeout".into()).is_routine_disconnect());
        // Io kind governs classification regardless of message.
        assert!(Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "totally unrelated message",
        ))
        .is_routine_disconnect());
    }
}
