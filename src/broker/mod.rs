mod handler;
pub(crate) mod worker;

use crate::error::Error;
use std::sync::Arc;

/// Configuration for the MQTT broker.
#[derive(Clone)]
pub struct BrokerConfig {
    /// Address to bind to (e.g., "0.0.0.0:1883")
    pub bind_addr: String,
    /// Maximum connections per worker
    pub max_connections_per_worker: usize,
    /// Connection handshake timeout in seconds
    pub connection_timeout_secs: u64,
    /// Idle timeout in seconds
    pub idle_timeout_secs: u64,
    /// Number of worker threads (defaults to CPU count)
    pub num_workers: Option<usize>,
    /// TCP listen backlog
    pub backlog: i32,
}

impl BrokerConfig {
    pub fn new(bind_addr: impl Into<String>) -> Self {
        Self {
            bind_addr: bind_addr.into(),
            max_connections_per_worker: 1000,
            connection_timeout_secs: 10,
            idle_timeout_secs: 300,
            num_workers: None,
            backlog: 1024,
        }
    }

    pub fn max_connections_per_worker(mut self, max: usize) -> Self {
        self.max_connections_per_worker = max;
        self
    }

    pub fn connection_timeout_secs(mut self, secs: u64) -> Self {
        self.connection_timeout_secs = secs;
        self
    }

    pub fn idle_timeout_secs(mut self, secs: u64) -> Self {
        self.idle_timeout_secs = secs;
        self
    }

    pub fn num_workers(mut self, workers: usize) -> Self {
        self.num_workers = Some(workers);
        self
    }

    pub fn backlog(mut self, backlog: i32) -> Self {
        self.backlog = backlog;
        self
    }
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self::new("0.0.0.0:1883")
    }
}

/// Event emitted by the broker when sensor data is received.
#[derive(Debug, Clone, Copy)]
pub enum Event {
    /// Sensor data received (temperature in 0.01°C, pressure in hPa)
    SensorV1 { temperature: i16, pressure: u16 },
}

/// Callback type for handling events per worker.
pub type EventCallback = Arc<dyn Fn(Event) + Send + Sync>;

/// High-performance MQTT broker using Monoio io_uring runtime.
pub struct MqttBroker;

impl MqttBroker {
    /// Run the MQTT broker with the given configuration.
    ///
    /// This function spawns one worker thread per CPU core (or as configured),
    /// each with its own io_uring event loop. Connections are distributed
    /// across workers via SO_REUSEPORT.
    ///
    /// # Arguments
    /// * `config` - Broker configuration
    ///
    /// # Returns
    /// Returns when all workers have exited (typically never in normal operation)
    pub fn run(config: BrokerConfig) -> Result<(), Error> {
        Self::run_with_callback(config, None)
    }

    /// Run the MQTT broker with an event callback.
    ///
    /// # Arguments
    /// * `config` - Broker configuration
    /// * `callback` - Optional callback invoked for each event (called from worker thread)
    pub fn run_with_callback(
        config: BrokerConfig,
        callback: Option<EventCallback>,
    ) -> Result<(), Error> {
        let num_workers = config.num_workers.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(std::num::NonZero::get)
                .unwrap_or(4)
        });

        tracing::info!(
            "Starting MQTT broker on {} with {} workers",
            config.bind_addr,
            num_workers
        );

        let handles: Vec<_> = (0..num_workers)
            .map(|worker_id| {
                let config = config.clone();
                let callback = callback.clone();

                std::thread::Builder::new()
                    .name(format!("mqtt-worker-{worker_id}"))
                    .spawn(move || {
                        let mut runtime = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                            .enable_timer()
                            .build()
                            .expect("Failed to build monoio runtime");

                        runtime.block_on(worker::run_worker(worker_id, config, callback))
                    })
                    .expect("Failed to spawn worker thread")
            })
            .collect();

        for handle in handles {
            if let Err(e) = handle.join() {
                tracing::error!("Worker thread panicked: {:?}", e);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_config_new() {
        let config = BrokerConfig::new("127.0.0.1:1883");

        assert_eq!(config.bind_addr, "127.0.0.1:1883");
        assert_eq!(config.max_connections_per_worker, 1000);
        assert_eq!(config.connection_timeout_secs, 10);
        assert_eq!(config.idle_timeout_secs, 300);
        assert!(config.num_workers.is_none());
        assert_eq!(config.backlog, 1024);
    }

    #[test]
    fn test_broker_config_default() {
        let config = BrokerConfig::default();

        assert_eq!(config.bind_addr, "0.0.0.0:1883");
    }

    #[test]
    fn test_broker_config_builder_pattern() {
        let config = BrokerConfig::new("0.0.0.0:1883")
            .max_connections_per_worker(500)
            .connection_timeout_secs(5)
            .idle_timeout_secs(120)
            .num_workers(4)
            .backlog(512);

        assert_eq!(config.max_connections_per_worker, 500);
        assert_eq!(config.connection_timeout_secs, 5);
        assert_eq!(config.idle_timeout_secs, 120);
        assert_eq!(config.num_workers, Some(4));
        assert_eq!(config.backlog, 512);
    }

    #[test]
    fn test_broker_config_clone() {
        let config1 = BrokerConfig::new("localhost:1883").num_workers(2);
        let config2 = config1.clone();

        assert_eq!(config1.bind_addr, config2.bind_addr);
        assert_eq!(config1.num_workers, config2.num_workers);
    }

    #[test]
    fn test_event_sensor_v1() {
        let event = Event::SensorV1 {
            temperature: 2500, // 25.00°C
            pressure: 1013,    // 1013 hPa
        };

        match event {
            Event::SensorV1 {
                temperature,
                pressure,
            } => {
                assert_eq!(temperature, 2500);
                assert_eq!(pressure, 1013);
            }
        }
    }

    #[test]
    fn test_event_clone_copy() {
        let event1 = Event::SensorV1 {
            temperature: 100,
            pressure: 500,
        };

        // Test Copy - event1 remains valid after assignment
        let event2 = event1;
        let event3 = event1;
        let event4 = event1;

        match (event2, event3, event4) {
            (
                Event::SensorV1 {
                    temperature: t2, ..
                },
                Event::SensorV1 {
                    temperature: t3, ..
                },
                Event::SensorV1 {
                    temperature: t4, ..
                },
            ) => {
                assert_eq!(t2, 100);
                assert_eq!(t3, 100);
                assert_eq!(t4, 100);
            }
        }
    }

    #[test]
    fn test_event_debug() {
        let event = Event::SensorV1 {
            temperature: -500,
            pressure: 900,
        };

        let debug_str = format!("{event:?}");
        assert!(debug_str.contains("SensorV1"));
        assert!(debug_str.contains("-500"));
        assert!(debug_str.contains("900"));
    }
}
