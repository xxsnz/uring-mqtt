# uring-mqtt

High-performance MQTT broker using Monoio's `io_uring` runtime for Linux systems.

## Features

- **`io_uring`-based I/O** - Modern Linux async I/O with fewer syscalls than epoll
- **Thread-per-core architecture** - No work-stealing, better cache locality
- **`SO_REUSEPORT`** - Kernel-level load balancing across worker threads
- **MQTT v3.1.1 and v5.0** - Full protocol support via rmqtt-codec
- **Two-tier buffer pool** - Thread-local cache + global pool for efficient memory reuse
- **Per-worker events** - Local channels without cross-thread synchronization

## Quick Start

```rust
use uring_mqtt::{BrokerConfig, MqttBroker};

fn main() -> Result<(), uring_mqtt::Error> {
    let config = BrokerConfig::new("0.0.0.0:1883")
        .num_workers(4)
        .max_connections_per_worker(1000);

    MqttBroker::run(config)
}
```

### With Event Callback

```rust
use std::sync::Arc;
use uring_mqtt::{BrokerConfig, Event, MqttBroker};

fn main() -> Result<(), uring_mqtt::Error> {
    let config = BrokerConfig::new("0.0.0.0:1883");

    let callback = Arc::new(|event: Event| {
        match event {
            Event::SensorV1 { temperature, pressure } => {
                println!("T: {:.2}°C, P: {} hPa",
                    f32::from(temperature) / 100.0,
                    pressure);
            }
        }
    });

    MqttBroker::run_with_callback(config, Some(callback))
}
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `bind_addr` | `0.0.0.0:1883` | Address to bind |
| `num_workers` | CPU count | Worker threads |
| `max_connections_per_worker` | 1000 | Connection limit per worker |
| `connection_timeout_secs` | 10 | CONNECT handshake timeout |
| `idle_timeout_secs` | 300 | Idle connection timeout |
| `backlog` | 1024 | TCP listen backlog |

## Architecture

```
Main Thread
    │
    ├── Worker 0 ─── monoio runtime ─── TcpListener (SO_REUSEPORT)
    │                                        └── handle connections
    ├── Worker 1 ─── monoio runtime ─── TcpListener (SO_REUSEPORT)
    │                                        └── handle connections
    └── Worker N ─── monoio runtime ─── TcpListener (SO_REUSEPORT)
                                             └── handle connections
```

Each worker:
1. Creates its own monoio runtime with `io_uring`
2. Binds to the same address with `SO_REUSEPORT` (kernel distributes connections)
3. Runs completely independently (no shared state except buffer pool)

