use std::time::Instant;

/// Lightweight connection state to minimize memory per connection.
///
/// Tracks the essential state for each MQTT client connection including
/// client identity, keep-alive settings, and activity timestamps.
#[derive(Debug)]
pub struct ConnectionState {
    /// MQTT client identifier (set after CONNECT packet).
    pub client_id: Option<String>,
    /// Keep-alive interval in seconds.
    pub keep_alive: u16,
    /// Timestamp of last packet received.
    pub last_packet_time: Instant,
}

impl ConnectionState {
    /// Create a new connection state with default values.
    pub fn new() -> Self {
        Self {
            client_id: None,
            keep_alive: 60,
            last_packet_time: Instant::now(),
        }
    }

    /// Update the last activity timestamp to now.
    pub fn update_activity(&mut self) {
        self.last_packet_time = Instant::now();
    }

    /// Check if the connection has been idle longer than the timeout.
    pub fn is_idle(&self, timeout_secs: u64) -> bool {
        self.last_packet_time.elapsed().as_secs() > timeout_secs
    }
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self::new()
    }
}

/// Memory statistics for monitoring.
///
/// Provides estimated memory usage based on connection count and buffer pool size.
/// Useful for monitoring and capacity planning.
#[allow(dead_code)] // Public API for user monitoring
pub struct MemoryStats {
    /// Number of currently active connections.
    pub active_connections: usize,
    /// Number of buffers in the pool.
    pub buffer_pool_size: usize,
    /// Estimated total memory usage in megabytes.
    pub estimated_memory_mb: f32,
}

#[allow(dead_code)] // Public API for user monitoring
impl MemoryStats {
    /// Calculate memory statistics from connection and pool counts.
    pub fn calculate(connections: usize, pool_size: usize) -> Self {
        let per_connection_kb = 2.0;
        let buffer_pool_kb = pool_size as f32;
        let channel_overhead_kb = connections as f32 * 0.1;

        let total_mb =
            (connections as f32 * per_connection_kb + buffer_pool_kb + channel_overhead_kb)
                / 1024.0;

        Self {
            active_connections: connections,
            buffer_pool_size: pool_size,
            estimated_memory_mb: total_mb,
        }
    }

    /// Log the statistics using tracing.
    pub fn log_stats(&self) {
        tracing::info!(
            "Memory stats - Connections: {}, Pool: {}, Est. Memory: {:.1}MB",
            self.active_connections,
            self.buffer_pool_size,
            self.estimated_memory_mb
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_connection_state_new() {
        let state = ConnectionState::new();
        assert!(state.client_id.is_none());
        assert_eq!(state.keep_alive, 60);
    }

    #[test]
    fn test_connection_state_default() {
        let state = ConnectionState::default();
        assert!(state.client_id.is_none());
        assert_eq!(state.keep_alive, 60);
    }

    #[test]
    fn test_connection_state_update_activity() {
        let mut state = ConnectionState::new();
        let initial_time = state.last_packet_time;

        // Wait a tiny bit
        thread::sleep(Duration::from_millis(10));
        state.update_activity();

        assert!(state.last_packet_time > initial_time);
    }

    #[test]
    fn test_connection_state_is_idle_false() {
        let state = ConnectionState::new();
        // Just created, should not be idle
        assert!(!state.is_idle(1));
    }

    #[test]
    fn test_connection_state_is_idle_true() {
        let mut state = ConnectionState::new();
        // Manually set last_packet_time to the past
        state.last_packet_time = Instant::now().checked_sub(Duration::from_secs(10)).unwrap();
        assert!(state.is_idle(5));
    }

    #[test]
    fn test_memory_stats_calculate() {
        let stats = MemoryStats::calculate(1000, 250);

        assert_eq!(stats.active_connections, 1000);
        assert_eq!(stats.buffer_pool_size, 250);

        // 1000 * 2KB + 250KB + 1000 * 0.1KB = 2000 + 250 + 100 = 2350KB = ~2.3MB
        assert!(stats.estimated_memory_mb > 2.0);
        assert!(stats.estimated_memory_mb < 3.0);
    }

    #[test]
    fn test_memory_stats_zero_connections() {
        let stats = MemoryStats::calculate(0, 0);

        assert_eq!(stats.active_connections, 0);
        assert_eq!(stats.buffer_pool_size, 0);
        assert!(stats.estimated_memory_mb.abs() < f32::EPSILON);
    }
}
