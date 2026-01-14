use monoio::net::{ListenerConfig, TcpListener};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::handler::handle_client;
use super::{BrokerConfig, Event, EventCallback};
use crate::error::Error;
use crate::pool::BufferPool;

/// Per-worker state (thread-local, no Send/Sync required)
struct WorkerState {
    worker_id: usize,
    active_connections: AtomicUsize,
    max_connections: usize,
    #[allow(dead_code)] // Reserved for future buffer reuse optimization
    buffer_pool: BufferPool,
}

impl WorkerState {
    fn new(worker_id: usize, max_connections: usize) -> Self {
        Self {
            worker_id,
            active_connections: AtomicUsize::new(0),
            max_connections,
            buffer_pool: BufferPool::new(2048, max_connections / 4),
        }
    }

    fn try_acquire(&self) -> bool {
        let current = self.active_connections.load(Ordering::Relaxed);
        if current >= self.max_connections {
            return false;
        }
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        true
    }

    fn release(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    fn active_count(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    fn id(&self) -> usize {
        self.worker_id
    }
}

/// Run a worker thread with its own io_uring event loop.
pub async fn run_worker(
    worker_id: usize,
    config: BrokerConfig,
    callback: Option<EventCallback>,
) -> Result<(), Error> {
    tracing::info!("Worker {} starting", worker_id);

    // Create listener with SO_REUSEPORT for kernel load balancing
    let listener_config = ListenerConfig::new()
        .reuse_addr(true)
        .reuse_port(true)
        .backlog(config.backlog);

    let listener = TcpListener::bind_with_config(&config.bind_addr, &listener_config)?;

    tracing::info!(
        "Worker {} listening on {} (max {} connections)",
        worker_id,
        config.bind_addr,
        config.max_connections_per_worker
    );

    // Thread-local worker state (Rc, not Arc - no Send/Sync needed)
    let state = Rc::new(WorkerState::new(
        worker_id,
        config.max_connections_per_worker,
    ));

    // Thread-local event channel
    let (event_tx, event_rx) = local_channel::<Event>(1024);

    // Spawn event processor task
    let callback_clone = callback.clone();
    monoio::spawn(async move {
        process_events(event_rx, callback_clone).await;
    });

    // Accept loop
    let mut connection_counter: u64 = 0;

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                if !state.try_acquire() {
                    tracing::warn!(
                        "Worker {}: connection limit reached, rejecting {}",
                        worker_id,
                        addr
                    );
                    drop(stream);
                    continue;
                }

                connection_counter += 1;
                if connection_counter.is_multiple_of(100) {
                    tracing::info!(
                        "Worker {}: {} active connections (total accepted: {})",
                        state.id(),
                        state.active_count(),
                        connection_counter
                    );
                }

                // Set TCP_NODELAY for low latency
                let _ = stream.set_nodelay(true);

                let state_clone = state.clone();
                let event_tx_clone = event_tx.clone();
                let connection_timeout = config.connection_timeout_secs;
                let idle_timeout = config.idle_timeout_secs;

                // Spawn connection handler (local task, stays on this core)
                monoio::spawn(async move {
                    if let Err(e) =
                        handle_client(stream, event_tx_clone, connection_timeout, idle_timeout)
                            .await
                    {
                        // Only log non-routine errors
                        let err_str = e.to_string();
                        if !err_str.contains("reset")
                            && !err_str.contains("closed")
                            && !err_str.contains("Timeout")
                        {
                            tracing::debug!("Client {} error: {:?}", addr, e);
                        }
                    }
                    state_clone.release();
                });
            }
            Err(e) => {
                tracing::error!("Worker {}: accept error: {}", worker_id, e);
                monoio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

/// Process events from the local channel.
async fn process_events(mut rx: LocalReceiver<Event>, callback: Option<EventCallback>) {
    while let Some(event) = rx.recv().await {
        if let Some(ref cb) = callback {
            cb(event);
        } else {
            tracing::trace!("Event: {:?}", event);
        }
    }
}

// Simple thread-local channel implementation (since we don't need cross-thread sync)
struct LocalChannel<T> {
    buffer: RefCell<std::collections::VecDeque<T>>,
    capacity: usize,
}

impl<T> LocalChannel<T> {
    fn new(capacity: usize) -> Rc<Self> {
        Rc::new(Self {
            buffer: RefCell::new(std::collections::VecDeque::with_capacity(capacity)),
            capacity,
        })
    }
}

#[derive(Clone)]
pub struct LocalSender<T> {
    channel: Rc<LocalChannel<T>>,
}

impl<T> LocalSender<T> {
    pub fn send(&self, item: T) -> bool {
        let mut buffer = self.channel.buffer.borrow_mut();
        if buffer.len() >= self.channel.capacity {
            return false;
        }
        buffer.push_back(item);
        true
    }
}

struct LocalReceiver<T> {
    channel: Rc<LocalChannel<T>>,
}

impl<T> LocalReceiver<T> {
    async fn recv(&mut self) -> Option<T> {
        loop {
            {
                let mut buffer = self.channel.buffer.borrow_mut();
                if let Some(item) = buffer.pop_front() {
                    return Some(item);
                }
            }
            // Yield to allow other tasks to send
            monoio::time::sleep(std::time::Duration::from_micros(100)).await;
        }
    }
}

fn local_channel<T>(capacity: usize) -> (LocalSender<T>, LocalReceiver<T>) {
    let channel = LocalChannel::new(capacity);
    (
        LocalSender {
            channel: channel.clone(),
        },
        LocalReceiver { channel },
    )
}
