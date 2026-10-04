//! Simple MQTT ingest example using uring-mqtt: decodes SensorV1 readings from every PUBLISH payload.
//!
//! Logging: `RUST_LOG` controls verbosity (default `info`). Use
//! `RUST_LOG=uring_mqtt=debug` for the broker's timeout diagnostics
//! (a bare `RUST_LOG=debug` also works but is noisier).
//!
//! Test with the Python test client in the parent project's test/ directory.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uring_mqtt::{BrokerConfig, MqttBroker, Publish};

/// SensorV1 reading (temperature in 0.01°C, pressure in hPa).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SensorV1 {
    temperature: i16,
    pressure: u16,
}

/// Exact SensorV1 payload width. The length is the whole format: there is no
/// version byte and no length prefix, so a payload of any other width is a
/// different format this parser cannot identify.
const SENSOR_V1_LEN: usize = 4;

/// Log the first and every LOG_EVERY-th reading, and warn on the first and
/// every LOG_EVERY-th rejected payload.
const LOG_EVERY: u64 = 100;

/// Scale from centi-degrees to degrees (SensorV1 stores temperature as i16 in 0.01°C).
const CENTI_DEGREES_PER_DEGREE: f32 = 100.0;

/// Parse binary sensor data (SensorV1 format).
///
/// Format: exactly `SENSOR_V1_LEN` bytes big-endian
/// - bytes 0-1: temperature (i16, scale 0.01°C)
/// - bytes 2-3: pressure (u16, scale 1.0 hPa)
///
/// A longer payload is rejected rather than truncated: its first four bytes
/// may not be a reading at all, and the parser cannot tell, so accepting
/// them could report a reading the publisher never sent.
#[inline(always)]
fn parse_sensor_data(data: &[u8]) -> Option<SensorV1> {
    if data.len() != SENSOR_V1_LEN {
        return None;
    }
    Some(SensorV1 {
        temperature: i16::from_be_bytes([data[0], data[1]]),
        pressure: u16::from_be_bytes([data[2], data[3]]),
    })
}

/// Decode one PUBLISH payload: count successful readings, count and warn on
/// anything else so an acknowledged payload never disappears silently at
/// default log levels.
fn record_payload(topic: &str, payload: &[u8], readings: &AtomicU64, rejected: &AtomicU64) {
    if let Some(r) = parse_sensor_data(payload) {
        // `fetch_add` returns the previous count, so add 1 to report the
        // running total: the first reading is #1, not #0.
        let total = readings.fetch_add(1, Ordering::Relaxed) + 1;
        if total == 1 || total.is_multiple_of(LOG_EVERY) {
            tracing::info!(
                "Reading #{}: temp={:.2}°C, pressure={}hPa",
                total,
                f32::from(r.temperature) / CENTI_DEGREES_PER_DEGREE,
                r.pressure
            );
        }
    } else {
        let total = rejected.fetch_add(1, Ordering::Relaxed) + 1;
        if total == 1 || total.is_multiple_of(LOG_EVERY) {
            // `{:?}` renders the topic debug-escaped: a client-chosen topic
            // carrying CR/LF would otherwise forge what looks like a second
            // log entry.
            tracing::warn!(
                "not a SensorV1 reading on topic {topic:?}: {} bytes ({total} rejected total)",
                payload.len()
            );
        }
    }
}

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

    // Track readings and rejected payloads across all workers
    let readings = Arc::new(AtomicU64::new(0));
    let rejected = Arc::new(AtomicU64::new(0));

    // Configure broker
    let config = BrokerConfig::new("0.0.0.0:1883")
        .max_connections_per_worker(1000)
        .connection_timeout_secs(10)
        .idle_timeout_secs(300);

    tracing::info!("Starting MQTT ingest server on 0.0.0.0:1883");

    // Create the publish callback
    let callback = Arc::new(move |publish: &Publish| {
        record_payload(publish.topic(), publish.payload(), &readings, &rejected);
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
    use super::{
        env_filter, env_filter_from_env, parse_sensor_data, record_payload, SensorV1, SENSOR_V1_LEN,
    };
    use std::sync::atomic::{AtomicU64, Ordering};

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

    #[test]
    fn parse_sensor_data_accepts_only_the_exact_sensor_v1_width() {
        assert_eq!(
            parse_sensor_data(&[0x09, 0xC4, 0x03, 0xF5]),
            Some(SensorV1 {
                temperature: 2500,
                pressure: 1013
            })
        );
        // 0xFF38 as i16 big-endian is -200.
        assert_eq!(
            parse_sensor_data(&[0xFF, 0x38, 0x00, 0x00]),
            Some(SensorV1 {
                temperature: -200,
                pressure: 0
            })
        );
        for len in 0..SENSOR_V1_LEN {
            assert_eq!(
                parse_sensor_data(&vec![0u8; len]),
                None,
                "len={len} must not parse"
            );
        }
        assert_eq!(
            parse_sensor_data(&[0u8; SENSOR_V1_LEN + 1]),
            None,
            "len {} must not parse",
            SENSOR_V1_LEN + 1
        );
    }

    #[test]
    fn non_sensor_payload_warns_on_first_and_every_hundredth() {
        const FORGING_TOPIC: &str = "other/t\r\nforged";
        const PAYLOAD: [u8; 5] = [1, 2, 3, 4, 5];

        let readings = AtomicU64::new(0);
        let rejected = AtomicU64::new(0);
        let logs = captured_with_spec("info", || {
            for _ in 0..100 {
                record_payload(FORGING_TOPIC, &PAYLOAD, &readings, &rejected);
            }
        });

        assert_eq!(rejected.load(Ordering::Relaxed), 100, "rejected count");
        assert_eq!(readings.load(Ordering::Relaxed), 0, "readings count");

        let warn_lines: Vec<&str> = logs
            .lines()
            .filter(|l| l.split_whitespace().nth(1) == Some("WARN"))
            .collect();
        assert!(
            warn_lines
                .iter()
                .any(|l| l.contains(r#""other/t\r\nforged""#)),
            "first warn must contain the debug-escaped topic in quotes, got:\n{logs}"
        );
        assert!(
            warn_lines.iter().any(|l| l.contains("5 bytes")),
            "first warn must name the payload length, got:\n{logs}"
        );
        assert!(
            warn_lines.iter().any(|l| l.contains("(1 rejected total)")),
            "first warn must show 1 rejected total, got:\n{logs}"
        );
        assert!(
            warn_lines
                .iter()
                .all(|l| !l.trim_start().starts_with("forged")),
            "no captured line may start with the unescaped topic, got:\n{logs}"
        );
        assert!(
            warn_lines
                .iter()
                .any(|l| l.contains("(100 rejected total)")),
            "100th warn must show 100 rejected total, got:\n{logs}"
        );
        let rejected_total_lines = warn_lines
            .iter()
            .filter(|l| l.contains("rejected total"))
            .count();
        assert_eq!(
            rejected_total_lines, 2,
            "exactly the 1st and 100th warns, got {rejected_total_lines}:\n{logs}"
        );
    }

    #[test]
    fn sensor_v1_payload_is_a_reading_not_a_rejection() {
        const PAYLOAD: [u8; 4] = [0x09, 0xC4, 0x03, 0xF5];

        let readings = AtomicU64::new(0);
        let rejected = AtomicU64::new(0);
        let logs = captured_with_spec("info", || {
            record_payload("t", &PAYLOAD, &readings, &rejected);
        });

        assert_eq!(readings.load(Ordering::Relaxed), 1, "readings count");
        assert_eq!(rejected.load(Ordering::Relaxed), 0, "rejected count");
        assert!(
            has_line_at(&logs, "INFO", &["temp=25.00°C, pressure=1013hPa"]),
            "expected INFO line with the reading, got:\n{logs}"
        );
        assert!(
            !has_line_at(&logs, "WARN", &["rejected total"]),
            "expected no WARN line for a valid reading, got:\n{logs}"
        );
    }

    #[test]
    fn reading_logs_on_first_and_every_hundredth_with_the_running_total() {
        const PAYLOAD: [u8; 4] = [0x09, 0xC4, 0x03, 0xF5];
        // Two full cadence periods, so the 200th reading must log as well.
        const READINGS: u64 = 200;

        let readings = AtomicU64::new(0);
        let rejected = AtomicU64::new(0);
        let logs = captured_with_spec("info", || {
            for _ in 0..READINGS {
                record_payload("t", &PAYLOAD, &readings, &rejected);
            }
        });

        assert_eq!(readings.load(Ordering::Relaxed), READINGS, "readings count");
        assert_eq!(rejected.load(Ordering::Relaxed), 0, "rejected count");

        let labels: Vec<&str> = logs
            .lines()
            .filter(|l| l.split_whitespace().nth(1) == Some("INFO"))
            .filter_map(|l| l.split("Reading #").nth(1))
            .filter_map(|rest| rest.split(':').next())
            .collect();
        assert_eq!(
            labels,
            ["1", "100", "200"],
            "expected exactly the 1st, 100th and 200th readings, labelled with the running total, got:\n{logs}"
        );
    }
}
