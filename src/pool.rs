use bytes::BytesMut;
use parking_lot::Mutex;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;

const LOCAL_CACHE_SIZE: usize = 8;

thread_local! {
    static LOCAL_BUFFER_CACHE: RefCell<Vec<BytesMut>> = RefCell::new(Vec::with_capacity(LOCAL_CACHE_SIZE));
}

/// Buffer pool to reuse buffers across connections.
///
/// Uses a two-tier strategy:
/// 1. Thread-local cache for fast, lock-free access
/// 2. Global pool for overflow/underflow balancing
pub struct BufferPool {
    pool: Arc<Mutex<VecDeque<BytesMut>>>,
    buffer_size: usize,
    max_pool_size: usize,
}

impl BufferPool {
    /// Create a new buffer pool with the specified buffer size and maximum pool capacity.
    pub fn new(buffer_size: usize, max_pool_size: usize) -> Self {
        Self {
            pool: Arc::new(Mutex::new(VecDeque::with_capacity(max_pool_size))),
            buffer_size,
            max_pool_size,
        }
    }

    /// Get a buffer from the pool, creating a new one if necessary.
    ///
    /// Tries the thread-local cache first for lock-free access,
    /// then falls back to the global pool.
    pub fn get(&self) -> BytesMut {
        let from_local = LOCAL_BUFFER_CACHE.with(|cache| cache.borrow_mut().pop());

        if let Some(buffer) = from_local {
            return buffer;
        }

        let mut pool = self.pool.lock();
        pool.pop_front()
            .unwrap_or_else(|| BytesMut::with_capacity(self.buffer_size))
    }

    /// Return a buffer to the pool for reuse.
    ///
    /// The buffer is cleared before being stored. Prefers the thread-local
    /// cache for lock-free access, overflowing to the global pool.
    pub fn put(&self, mut buffer: BytesMut) {
        buffer.clear();

        let local_has_space =
            LOCAL_BUFFER_CACHE.with(|cache| cache.borrow().len() < LOCAL_CACHE_SIZE);

        if local_has_space {
            LOCAL_BUFFER_CACHE.with(|cache| {
                cache.borrow_mut().push(buffer);
            });
            return;
        }

        let mut pool = self.pool.lock();
        if pool.len() < self.max_pool_size {
            pool.push_back(buffer);
        }
    }

    /// Return current pool size for monitoring.
    pub fn pool_size(&self) -> usize {
        self.pool.lock().len()
    }
}

impl Clone for BufferPool {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            buffer_size: self.buffer_size,
            max_pool_size: self.max_pool_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_new() {
        let pool = BufferPool::new(1024, 10);
        assert_eq!(pool.pool_size(), 0);
    }

    #[test]
    fn test_buffer_pool_get_creates_new_buffer() {
        let pool = BufferPool::new(1024, 10);
        let buf = pool.get();
        assert!(buf.capacity() >= 1024);
    }

    #[test]
    fn test_buffer_pool_put_and_get_reuses_buffer() {
        let pool = BufferPool::new(1024, 10);

        // Get a buffer and write some data
        let mut buf = pool.get();
        buf.extend_from_slice(b"test data");

        // Return it to the pool
        pool.put(buf);

        // Get it back - should be cleared
        let buf2 = pool.get();
        assert!(buf2.is_empty());
        assert!(buf2.capacity() >= 1024);
    }

    #[test]
    fn test_buffer_pool_respects_max_size() {
        let pool = BufferPool::new(64, 2);

        // Fill up local cache first (8 buffers)
        for _ in 0..LOCAL_CACHE_SIZE {
            let buf = BytesMut::with_capacity(64);
            pool.put(buf);
        }

        // Now overflow to global pool
        let buf1 = BytesMut::with_capacity(64);
        let buf2 = BytesMut::with_capacity(64);
        let buf3 = BytesMut::with_capacity(64);

        pool.put(buf1);
        pool.put(buf2);
        pool.put(buf3); // This one should be dropped (max_pool_size = 2)

        assert_eq!(pool.pool_size(), 2);
    }

    #[test]
    fn test_buffer_pool_clone_shares_global_pool() {
        let pool1 = BufferPool::new(1024, 10);
        let pool2 = pool1.clone();

        // Fill local cache
        for _ in 0..LOCAL_CACHE_SIZE {
            pool1.put(BytesMut::with_capacity(1024));
        }

        // Put to global pool via pool1
        pool1.put(BytesMut::with_capacity(1024));
        assert_eq!(pool1.pool_size(), 1);

        // pool2 should see the same global pool
        assert_eq!(pool2.pool_size(), 1);
    }
}
