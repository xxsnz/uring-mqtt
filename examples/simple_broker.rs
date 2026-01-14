//! Simple MQTT broker example using uring-mqtt.
//!
//! Run with: cargo run --example simple_broker
//!
//! Test with the Python test client in the parent project's test/ directory.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uring_mqtt::{BrokerConfig, Event, MqttBroker};

fn main() -> Result<(), uring_mqtt::Error> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    // Track events across all workers
    let event_count = Arc::new(AtomicU64::new(0));

    // Configure broker
    let config = BrokerConfig::new("0.0.0.0:1883")
        .max_connections_per_worker(1000)
        .connection_timeout_secs(10)
        .idle_timeout_secs(300);

    tracing::info!("Starting MQTT broker on 0.0.0.0:1883");
    tracing::info!("Press Ctrl+C to stop");

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

    // Run broker (blocks until shutdown)
    MqttBroker::run_with_callback(config, Some(callback))
}
