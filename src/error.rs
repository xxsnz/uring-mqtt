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
