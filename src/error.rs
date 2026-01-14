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

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Worker error: {0}")]
    Worker(String),
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
}
