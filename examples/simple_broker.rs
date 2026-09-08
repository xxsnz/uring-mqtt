//! Simple MQTT broker example using uring-mqtt.
//!
//! Logging: `RUST_LOG` controls verbosity (default `info`). Use
//! `RUST_LOG=uring_mqtt=debug` for the broker's timeout diagnostics
//! (a bare `RUST_LOG=debug` also works but is noisier).
//!
//! Test with the Python test client in the parent project's test/ directory.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uring_mqtt::{BrokerConfig, Event, MqttBroker};

/// Build the example's log filter from a `RUST_LOG`-style spec. An empty
/// spec defaults to `info` so an unset environment is never silent; a bare
/// `RUST_LOG=debug` is honored (the old add_directive shape silently
/// replaced it with `info`).
fn env_filter(spec: &str) -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .parse_lossy(spec)
}

/// Read `RUST_LOG` (unset → empty spec) and build the example's filter.
fn env_filter_from_env() -> tracing_subscriber::EnvFilter {
    env_filter(&std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV).unwrap_or_default())
}

fn main() -> Result<(), uring_mqtt::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(env_filter_from_env())
        .init();

    // Track events across all workers
    let event_count = Arc::new(AtomicU64::new(0));

    // Configure broker
    let config = BrokerConfig::new("0.0.0.0:1883")
        .max_connections_per_worker(1000)
        .connection_timeout_secs(10)
        .idle_timeout_secs(300);

    tracing::info!("Starting MQTT broker on 0.0.0.0:1883");

    // Create event callback
    let counter = event_count.clone();
    let callback = Arc::new(move |event: Event| {
        let count = counter.fetch_add(1, Ordering::Relaxed);
        if count.is_multiple_of(100) {
            match event {
                Event::SensorV1 {
                    temperature,
                    pressure,
                } => {
                    tracing::info!(
                        "Event #{}: temp={:.2}°C, pressure={}hPa",
                        count,
                        f32::from(temperature) / 100.0,
                        pressure
                    );
                }
            }
        }
    });

    // Start broker (returns once startup completes; handle owns the lifecycle)
    let handle = MqttBroker::start_with_callback(config, Some(callback))?;
    let trigger = handle.shutdown_handle();

    // One-shot per server instance: ctrlc registers once per process, and a used
    // ShutdownHandle is a permanent no-op — restart the process to restart the server.
    // Registered before the readiness lines: a launcher reacting to them could otherwise
    // signal while the default action still terminates the process, skipping the drain.
    ctrlc::set_handler(move || trigger.shutdown())
        .map_err(|e| uring_mqtt::Error::Worker(format!("install ctrl-c handler: {e}")))?;

    tracing::info!("Press Ctrl+C to stop");
    tracing::info!("Set RUST_LOG=uring_mqtt=debug for connection diagnostics");

    handle.join()
}

#[cfg(test)]
mod tests {
    use super::{env_filter, env_filter_from_env};

    type LogSink = std::sync::Arc<std::sync::Mutex<Vec<u8>>>;

    #[derive(Clone)]
    struct SinkWriter(LogSink);

    impl std::io::Write for SinkWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("sink poisoned").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SinkWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn captured_with(filter: tracing_subscriber::EnvFilter, f: impl FnOnce()) -> String {
        let sink: LogSink = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_writer(SinkWriter(sink.clone()))
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        let bytes = sink.lock().expect("sink poisoned").clone();
        String::from_utf8(bytes).expect("utf8 logs")
    }

    fn captured_with_spec(spec: &str, f: impl FnOnce()) -> String {
        captured_with(env_filter(spec), f)
    }

    /// Copied from `src/broker/handler.rs:1042-1046`.
    fn has_line_at(logs: &str, level: &str, needles: &[&str]) -> bool {
        logs.lines().any(|l| {
            l.split_whitespace().nth(1) == Some(level) && needles.iter().all(|n| l.contains(n))
        })
    }

    struct RustLogGuard;

    impl Drop for RustLogGuard {
        fn drop(&mut self) {
            std::env::remove_var("RUST_LOG");
        }
    }

    #[test]
    fn bare_debug_spec_admits_crate_debug_lines() {
        let logs = captured_with_spec("debug", || {
            tracing::debug!(target: "uring_mqtt", "debug probe line");
        });
        assert!(
            has_line_at(&logs, "DEBUG", &["debug probe line"]),
            "expected DEBUG line with probe in captured logs, got:\n{logs}"
        );
    }

    #[test]
    fn empty_spec_defaults_to_info() {
        let logs = captured_with_spec("", || {
            tracing::info!(target: "uring_mqtt", "info probe line");
            tracing::debug!(target: "uring_mqtt", "debug probe line");
        });
        assert!(
            has_line_at(&logs, "INFO", &["info probe line"]),
            "expected INFO line with probe in captured logs, got:\n{logs}"
        );
        assert!(
            !has_line_at(&logs, "DEBUG", &["debug probe line"]),
            "expected NO DEBUG line with probe, got:\n{logs}"
        );
    }

    #[test]
    fn targeted_spec_admits_crate_debug_only() {
        let logs = captured_with_spec("uring_mqtt=debug", || {
            tracing::debug!(target: "uring_mqtt", "debug probe line");
            tracing::debug!(target: "other_target", "foreign debug probe line");
        });
        assert!(
            has_line_at(&logs, "DEBUG", &["debug probe line"]),
            "expected DEBUG line with crate probe, got:\n{logs}"
        );
        assert!(
            !has_line_at(&logs, "DEBUG", &["foreign debug probe line"]),
            "expected NO DEBUG line with foreign probe, got:\n{logs}"
        );
    }

    #[test]
    fn invalid_directive_does_not_discard_valid_spec() {
        let logs = captured_with_spec("debug,other_target=not_a_level", || {
            tracing::debug!(target: "uring_mqtt", "debug probe line");
        });
        assert!(
            has_line_at(&logs, "DEBUG", &["debug probe line"]),
            "expected DEBUG line with probe despite invalid directive, got:\n{logs}"
        );
    }

    #[test]
    fn fully_invalid_spec_falls_back_to_info() {
        let logs = captured_with_spec("other_target=not_a_level", || {
            tracing::info!(target: "uring_mqtt", "info probe line");
            tracing::debug!(target: "uring_mqtt", "debug probe line");
        });
        assert!(
            has_line_at(&logs, "INFO", &["info probe line"]),
            "expected INFO line with probe after fallback, got:\n{logs}"
        );
        assert!(
            !has_line_at(&logs, "DEBUG", &["debug probe line"]),
            "expected NO DEBUG line after fallback, got:\n{logs}"
        );
    }

    #[test]
    fn explicit_warn_spec_suppresses_info() {
        let logs = captured_with_spec("warn", || {
            tracing::warn!(target: "uring_mqtt", "warn probe line");
            tracing::info!(target: "uring_mqtt", "info probe line");
        });
        assert!(
            has_line_at(&logs, "WARN", &["warn probe line"]),
            "expected WARN line with probe, got:\n{logs}"
        );
        assert!(
            !has_line_at(&logs, "INFO", &["info probe line"]),
            "expected NO INFO line when the spec asks for warn, got:\n{logs}"
        );
    }

    #[test]
    fn main_filter_reads_rust_log_from_environment() {
        let _guard = RustLogGuard;

        std::env::set_var("RUST_LOG", "debug");
        let logs = captured_with(env_filter_from_env(), || {
            tracing::debug!(target: "uring_mqtt", "debug probe line");
        });
        assert!(
            has_line_at(&logs, "DEBUG", &["debug probe line"]),
            "expected DEBUG line when RUST_LOG=debug, got:\n{logs}"
        );

        std::env::remove_var("RUST_LOG");
        let logs = captured_with(env_filter_from_env(), || {
            tracing::info!(target: "uring_mqtt", "info probe line");
            tracing::debug!(target: "uring_mqtt", "debug probe line");
        });
        assert!(
            has_line_at(&logs, "INFO", &["info probe line"]),
            "expected INFO line when RUST_LOG unset, got:\n{logs}"
        );
        assert!(
            !has_line_at(&logs, "DEBUG", &["debug probe line"]),
            "expected NO DEBUG line when RUST_LOG unset, got:\n{logs}"
        );

        std::env::set_var("RUST_LOG", "");
        let logs = captured_with(env_filter_from_env(), || {
            tracing::info!(target: "uring_mqtt", "info probe line");
            tracing::debug!(target: "uring_mqtt", "debug probe line");
        });
        assert!(
            has_line_at(&logs, "INFO", &["info probe line"]),
            "expected INFO line when RUST_LOG is empty, got:\n{logs}"
        );
        assert!(
            !has_line_at(&logs, "DEBUG", &["debug probe line"]),
            "expected NO DEBUG line when RUST_LOG is empty, got:\n{logs}"
        );
    }
}
