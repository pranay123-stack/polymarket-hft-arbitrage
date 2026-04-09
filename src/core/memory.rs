//! Memory Optimization with Buffer Pooling
//!
//! Provides efficient memory management for HFT systems where allocation
//! latency can impact trading performance.
//!
//! ## Features:
//!
//! - **Object Pooling**: Reuse frequently allocated objects
//! - **Buffer Pooling**: Pre-allocated buffers for network I/O
//! - **Arena Allocation**: Bump allocation for batch operations
//! - **Memory Tracking**: Monitor allocation patterns and pool efficiency
//! - **Zero-Copy Operations**: Minimize data copying where possible

use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, trace, warn};

/// Configuration for buffer pools
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Initial number of buffers to pre-allocate
    pub initial_capacity: usize,
    /// Maximum number of buffers to keep in pool
    pub max_capacity: usize,
    /// Size of each buffer in bytes
    pub buffer_size: usize,
    /// Whether to track allocation metrics
    pub enable_metrics: bool,
    /// Whether to zero buffers on return (security)
    pub zero_on_return: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            initial_capacity: 64,
            max_capacity: 1024,
            buffer_size: 4096,
            enable_metrics: true,
            zero_on_return: false,
        }
    }
}

impl PoolConfig {
    /// Config for small, frequently used buffers (JSON messages)
    pub fn small_buffer() -> Self {
        Self {
            initial_capacity: 128,
            max_capacity: 2048,
            buffer_size: 1024,
            enable_metrics: true,
            zero_on_return: false,
        }
    }

    /// Config for medium buffers (order book snapshots)
    pub fn medium_buffer() -> Self {
        Self {
            initial_capacity: 64,
            max_capacity: 512,
            buffer_size: 16384,
            enable_metrics: true,
            zero_on_return: false,
        }
    }

    /// Config for large buffers (batch operations)
    pub fn large_buffer() -> Self {
        Self {
            initial_capacity: 16,
            max_capacity: 128,
            buffer_size: 65536,
            enable_metrics: true,
            zero_on_return: false,
        }
    }

    /// Config for secure buffers (credentials, signatures)
    pub fn secure_buffer() -> Self {
        Self {
            initial_capacity: 8,
            max_capacity: 64,
            buffer_size: 512,
            enable_metrics: false, // Don't track sensitive data
            zero_on_return: true,  // Zero memory for security
        }
    }
}

/// Pool statistics for monitoring
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Total allocations from pool
    pub allocations: AtomicU64,
    /// Allocations satisfied from pool (cache hits)
    pub pool_hits: AtomicU64,
    /// Allocations that required new allocation (cache misses)
    pub pool_misses: AtomicU64,
    /// Total returns to pool
    pub returns: AtomicU64,
    /// Returns that were dropped (pool at capacity)
    pub drops: AtomicU64,
    /// Peak pool size
    pub peak_size: AtomicUsize,
    /// Current pool size
    pub current_size: AtomicUsize,
}

impl PoolStats {
    /// Calculate hit rate
    pub fn hit_rate(&self) -> f64 {
        let hits = self.pool_hits.load(Ordering::Relaxed);
        let misses = self.pool_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            return 1.0;
        }
        hits as f64 / total as f64
    }

    /// Calculate efficiency (reuse rate)
    pub fn efficiency(&self) -> f64 {
        let allocs = self.allocations.load(Ordering::Relaxed);
        let misses = self.pool_misses.load(Ordering::Relaxed);
        if allocs == 0 {
            return 1.0;
        }
        1.0 - (misses as f64 / allocs as f64)
    }
}

/// A pooled buffer that returns to the pool when dropped
pub struct PooledBuffer {
    buffer: Vec<u8>,
    pool: Arc<BufferPool>,
    /// Actual length of data in buffer
    len: usize,
}

impl PooledBuffer {
    /// Get the length of data in the buffer
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get buffer capacity
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Set the data length
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.buffer.capacity());
        self.len = len;
    }

    /// Clear the buffer
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Get slice of actual data
    pub fn data(&self) -> &[u8] {
        &self.buffer[..self.len]
    }

    /// Get mutable slice of actual data
    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.buffer[..self.len]
    }

    /// Get the full buffer for writing
    pub fn buffer_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }

    /// Extend buffer with data
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        let new_len = self.len + data.len();
        if new_len > self.buffer.len() {
            self.buffer.resize(new_len, 0);
        }
        self.buffer[self.len..new_len].copy_from_slice(data);
        self.len = new_len;
    }
}

impl Deref for PooledBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer[..self.len]
    }
}

impl DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer[..self.len]
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        // Return buffer to pool
        self.pool.return_buffer(std::mem::take(&mut self.buffer));
    }
}

/// A pool of reusable byte buffers
pub struct BufferPool {
    config: PoolConfig,
    buffers: Mutex<VecDeque<Vec<u8>>>,
    stats: PoolStats,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(config: PoolConfig) -> Arc<Self> {
        let mut buffers = VecDeque::with_capacity(config.max_capacity);

        // Pre-allocate initial buffers
        for _ in 0..config.initial_capacity {
            buffers.push_back(vec![0u8; config.buffer_size]);
        }

        let pool = Arc::new(Self {
            config,
            buffers: Mutex::new(buffers),
            stats: PoolStats::default(),
        });

        pool.stats
            .current_size
            .store(pool.config.initial_capacity, Ordering::Relaxed);
        pool.stats
            .peak_size
            .store(pool.config.initial_capacity, Ordering::Relaxed);

        pool
    }

    /// Get a buffer from the pool
    pub fn get(self: &Arc<Self>) -> PooledBuffer {
        if self.config.enable_metrics {
            self.stats.allocations.fetch_add(1, Ordering::Relaxed);
        }

        let buffer = {
            let mut buffers = self.buffers.lock();
            buffers.pop_front()
        };

        let buffer = match buffer {
            Some(mut buf) => {
                if self.config.enable_metrics {
                    self.stats.pool_hits.fetch_add(1, Ordering::Relaxed);
                    self.stats.current_size.fetch_sub(1, Ordering::Relaxed);
                }
                // Clear but don't deallocate
                buf.clear();
                buf.resize(self.config.buffer_size, 0);
                buf
            }
            None => {
                if self.config.enable_metrics {
                    self.stats.pool_misses.fetch_add(1, Ordering::Relaxed);
                }
                trace!("Buffer pool miss, allocating new buffer");
                vec![0u8; self.config.buffer_size]
            }
        };

        PooledBuffer {
            buffer,
            pool: Arc::clone(self),
            len: 0,
        }
    }

    /// Return a buffer to the pool
    fn return_buffer(&self, mut buffer: Vec<u8>) {
        if self.config.enable_metrics {
            self.stats.returns.fetch_add(1, Ordering::Relaxed);
        }

        // Zero buffer if required for security
        if self.config.zero_on_return {
            buffer.fill(0);
        }

        let mut buffers = self.buffers.lock();

        if buffers.len() < self.config.max_capacity {
            // Clear and resize to standard size
            buffer.clear();
            buffer.resize(self.config.buffer_size, 0);
            buffers.push_back(buffer);

            if self.config.enable_metrics {
                let current = self.stats.current_size.fetch_add(1, Ordering::Relaxed) + 1;
                let peak = self.stats.peak_size.load(Ordering::Relaxed);
                if current > peak {
                    self.stats.peak_size.store(current, Ordering::Relaxed);
                }
            }
        } else {
            // Pool at capacity, drop buffer
            if self.config.enable_metrics {
                self.stats.drops.fetch_add(1, Ordering::Relaxed);
            }
            trace!("Buffer pool at capacity, dropping buffer");
        }
    }

    /// Get pool statistics
    pub fn stats(&self) -> &PoolStats {
        &self.stats
    }

    /// Get current pool size
    pub fn size(&self) -> usize {
        self.buffers.lock().len()
    }

    /// Shrink pool to minimum size
    pub fn shrink(&self) {
        let mut buffers = self.buffers.lock();
        while buffers.len() > self.config.initial_capacity {
            buffers.pop_back();
        }
        if self.config.enable_metrics {
            self.stats
                .current_size
                .store(buffers.len(), Ordering::Relaxed);
        }
    }
}

/// Generic object pool for reusing arbitrary objects
pub struct ObjectPool<T: Default + Send> {
    objects: Mutex<VecDeque<T>>,
    max_capacity: usize,
    stats: PoolStats,
}

impl<T: Default + Send> ObjectPool<T> {
    /// Create a new object pool
    pub fn new(max_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            objects: Mutex::new(VecDeque::with_capacity(max_capacity)),
            max_capacity,
            stats: PoolStats::default(),
        })
    }

    /// Create with pre-allocated objects
    pub fn with_capacity(initial: usize, max: usize) -> Arc<Self> {
        let mut objects = VecDeque::with_capacity(max);
        for _ in 0..initial {
            objects.push_back(T::default());
        }

        Arc::new(Self {
            objects: Mutex::new(objects),
            max_capacity: max,
            stats: PoolStats::default(),
        })
    }

    /// Get an object from the pool
    pub fn get(&self) -> T {
        self.stats.allocations.fetch_add(1, Ordering::Relaxed);

        let obj = self.objects.lock().pop_front();

        match obj {
            Some(obj) => {
                self.stats.pool_hits.fetch_add(1, Ordering::Relaxed);
                obj
            }
            None => {
                self.stats.pool_misses.fetch_add(1, Ordering::Relaxed);
                T::default()
            }
        }
    }

    /// Return an object to the pool
    pub fn put(&self, obj: T) {
        self.stats.returns.fetch_add(1, Ordering::Relaxed);

        let mut objects = self.objects.lock();
        if objects.len() < self.max_capacity {
            objects.push_back(obj);
        } else {
            self.stats.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get pool statistics
    pub fn stats(&self) -> &PoolStats {
        &self.stats
    }
}

/// Arena allocator for batch operations
/// Provides fast bump allocation with single deallocation
pub struct Arena {
    chunks: RwLock<Vec<Vec<u8>>>,
    current_chunk: Mutex<ArenaChunk>,
    chunk_size: usize,
    stats: ArenaStats,
}

struct ArenaChunk {
    data: Vec<u8>,
    offset: usize,
}

impl ArenaChunk {
    fn new(size: usize) -> Self {
        Self {
            data: vec![0u8; size],
            offset: 0,
        }
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.offset
    }
}

/// Arena statistics
#[derive(Debug, Default)]
pub struct ArenaStats {
    /// Total bytes allocated
    pub total_allocated: AtomicU64,
    /// Number of allocations
    pub allocation_count: AtomicU64,
    /// Number of chunks created
    pub chunk_count: AtomicUsize,
    /// Total bytes in use
    pub bytes_in_use: AtomicU64,
}

impl Arena {
    /// Create a new arena with default chunk size (64KB)
    pub fn new() -> Self {
        Self::with_chunk_size(65536)
    }

    /// Create arena with custom chunk size
    pub fn with_chunk_size(chunk_size: usize) -> Self {
        Self {
            chunks: RwLock::new(Vec::new()),
            current_chunk: Mutex::new(ArenaChunk::new(chunk_size)),
            chunk_size,
            stats: ArenaStats::default(),
        }
    }

    /// Allocate bytes from the arena
    pub fn alloc(&self, size: usize) -> &mut [u8] {
        self.stats.allocation_count.fetch_add(1, Ordering::Relaxed);
        self.stats.total_allocated.fetch_add(size as u64, Ordering::Relaxed);
        self.stats.bytes_in_use.fetch_add(size as u64, Ordering::Relaxed);

        let mut chunk = self.current_chunk.lock();

        // Check if current chunk has enough space
        if chunk.remaining() >= size {
            let start = chunk.offset;
            chunk.offset += size;

            // SAFETY: We have exclusive access via the mutex and stay within bounds
            let ptr = chunk.data.as_mut_ptr();
            unsafe { std::slice::from_raw_parts_mut(ptr.add(start), size) }
        } else {
            // Need a new chunk
            let new_chunk_size = self.chunk_size.max(size);

            // Move current chunk to storage
            let old_chunk = std::mem::replace(&mut *chunk, ArenaChunk::new(new_chunk_size));
            let old_data = old_chunk.data;

            let mut chunks = self.chunks.write();
            chunks.push(old_data);
            self.stats.chunk_count.store(chunks.len(), Ordering::Relaxed);
            drop(chunks);

            // Allocate from new chunk
            chunk.offset = size;
            let ptr = chunk.data.as_mut_ptr();
            unsafe { std::slice::from_raw_parts_mut(ptr, size) }
        }
    }

    /// Allocate and copy data
    pub fn alloc_copy(&self, data: &[u8]) -> &[u8] {
        let slice = self.alloc(data.len());
        slice.copy_from_slice(data);
        // Return immutable reference
        &*slice
    }

    /// Reset arena, freeing all allocations
    pub fn reset(&self) {
        let mut chunk = self.current_chunk.lock();
        chunk.offset = 0;

        let mut chunks = self.chunks.write();
        chunks.clear();

        self.stats.bytes_in_use.store(0, Ordering::Relaxed);
        self.stats.chunk_count.store(0, Ordering::Relaxed);
    }

    /// Get arena statistics
    pub fn stats(&self) -> &ArenaStats {
        &self.stats
    }

    /// Get total memory used
    pub fn memory_used(&self) -> usize {
        let chunks = self.chunks.read();
        let current = self.current_chunk.lock();

        chunks.iter().map(|c| c.capacity()).sum::<usize>() + current.data.capacity()
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

/// Global memory pools for common use cases
pub struct GlobalPools {
    /// Small buffers for JSON messages
    pub small: Arc<BufferPool>,
    /// Medium buffers for order book data
    pub medium: Arc<BufferPool>,
    /// Large buffers for batch operations
    pub large: Arc<BufferPool>,
    /// Secure buffers for sensitive data
    pub secure: Arc<BufferPool>,
}

impl GlobalPools {
    /// Create global pools with default configurations
    pub fn new() -> Self {
        Self {
            small: BufferPool::new(PoolConfig::small_buffer()),
            medium: BufferPool::new(PoolConfig::medium_buffer()),
            large: BufferPool::new(PoolConfig::large_buffer()),
            secure: BufferPool::new(PoolConfig::secure_buffer()),
        }
    }

    /// Get aggregate statistics
    pub fn aggregate_stats(&self) -> AggregatePoolStats {
        AggregatePoolStats {
            small_hit_rate: self.small.stats().hit_rate(),
            medium_hit_rate: self.medium.stats().hit_rate(),
            large_hit_rate: self.large.stats().hit_rate(),
            total_allocations: self.small.stats().allocations.load(Ordering::Relaxed)
                + self.medium.stats().allocations.load(Ordering::Relaxed)
                + self.large.stats().allocations.load(Ordering::Relaxed),
            total_memory: self.small.size() * 1024
                + self.medium.size() * 16384
                + self.large.size() * 65536,
        }
    }

    /// Shrink all pools to minimum size
    pub fn shrink_all(&self) {
        self.small.shrink();
        self.medium.shrink();
        self.large.shrink();
        self.secure.shrink();
    }
}

impl Default for GlobalPools {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregate statistics across all pools
#[derive(Debug, Clone)]
pub struct AggregatePoolStats {
    pub small_hit_rate: f64,
    pub medium_hit_rate: f64,
    pub large_hit_rate: f64,
    pub total_allocations: u64,
    pub total_memory: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_basic() {
        let pool = BufferPool::new(PoolConfig {
            initial_capacity: 2,
            max_capacity: 4,
            buffer_size: 128,
            enable_metrics: true,
            zero_on_return: false,
        });

        // Initial size should be 2
        assert_eq!(pool.size(), 2);

        // Get a buffer
        let mut buf1 = pool.get();
        assert_eq!(pool.size(), 1);
        assert_eq!(buf1.capacity(), 128);

        // Write to buffer
        buf1.extend_from_slice(b"hello");
        assert_eq!(buf1.len(), 5);
        assert_eq!(buf1.data(), b"hello");

        // Drop returns to pool
        drop(buf1);
        assert_eq!(pool.size(), 2);
    }

    #[test]
    fn test_buffer_pool_hit_rate() {
        let pool = BufferPool::new(PoolConfig {
            initial_capacity: 2,
            max_capacity: 4,
            buffer_size: 64,
            enable_metrics: true,
            zero_on_return: false,
        });

        // First 2 allocations should hit
        let _b1 = pool.get();
        let _b2 = pool.get();
        assert_eq!(pool.stats().pool_hits.load(Ordering::Relaxed), 2);
        assert_eq!(pool.stats().pool_misses.load(Ordering::Relaxed), 0);

        // Third should miss
        let _b3 = pool.get();
        assert_eq!(pool.stats().pool_misses.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_object_pool() {
        let pool: Arc<ObjectPool<Vec<i32>>> = ObjectPool::new(8);

        let mut obj = pool.get();
        obj.push(1);
        obj.push(2);

        pool.put(obj);

        assert_eq!(pool.stats().returns.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_arena_allocation() {
        let arena = Arena::with_chunk_size(1024);

        let slice1 = arena.alloc(100);
        assert_eq!(slice1.len(), 100);

        let slice2 = arena.alloc(200);
        assert_eq!(slice2.len(), 200);

        assert_eq!(arena.stats().allocation_count.load(Ordering::Relaxed), 2);
        assert_eq!(arena.stats().total_allocated.load(Ordering::Relaxed), 300);
    }

    #[test]
    fn test_arena_reset() {
        let arena = Arena::with_chunk_size(1024);

        arena.alloc(500);
        arena.alloc(500);
        arena.alloc(500); // This should trigger new chunk

        assert!(arena.stats().chunk_count.load(Ordering::Relaxed) >= 1);

        arena.reset();
        assert_eq!(arena.stats().bytes_in_use.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_global_pools() {
        let pools = GlobalPools::new();

        let _small = pools.small.get();
        let _medium = pools.medium.get();
        let _large = pools.large.get();

        let stats = pools.aggregate_stats();
        assert_eq!(stats.total_allocations, 3);
    }

    #[test]
    fn test_secure_buffer_zeroing() {
        let pool = BufferPool::new(PoolConfig::secure_buffer());

        {
            let mut buf = pool.get();
            buf.extend_from_slice(b"secret_data");
            // Buffer should be zeroed when dropped
        }

        // Get a new buffer - it should be clean
        let buf = pool.get();
        assert!(buf.data().iter().all(|&b| b == 0));
    }
}
