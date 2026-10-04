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
}
