//! Metrics for tracking write amplification and compression effectiveness.
//!
//! These metrics help answer:
//! - How much write amplification are we seeing? (guest bytes vs S3 bytes)
//! - How effective is compression? (raw bytes vs compressed bytes)
//! - How well is the cache coalescing writes? (guest writes vs S3 writes)

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

/// Metrics for a single export's I/O operations.
#[derive(Debug, Default)]
pub struct ExportMetrics {
    /// Total bytes written by guest (actual NBD write payload)
    pub guest_bytes_written: AtomicU64,

    /// Total number of NBD write commands from guest
    pub guest_write_ops: AtomicU64,

    /// Total bytes read by guest
    pub guest_bytes_read: AtomicU64,

    /// Total number of NBD read commands from guest
    pub guest_read_ops: AtomicU64,

    /// Total bytes written to S3 (after compression/encryption)
    pub s3_bytes_written: AtomicU64,

    /// Total number of blocks written to S3 (within batches)
    pub s3_write_ops: AtomicU64,

    /// Total number of batches written to S3
    pub batches_written: AtomicU64,

    /// Total bytes read from S3 (before decompression)
    pub s3_bytes_read: AtomicU64,

    /// Total number of blocks read from S3
    pub s3_read_ops: AtomicU64,

    /// Total uncompressed bytes before encoding (for compression ratio)
    pub uncompressed_bytes: AtomicU64,

    /// Total compressed bytes after encoding (for compression ratio)
    pub compressed_bytes: AtomicU64,

    /// Cache hits (reads served from local cache)
    pub cache_hits: AtomicU64,

    /// Cache misses (reads that required S3 fetch)
    pub cache_misses: AtomicU64,
}

impl ExportMetrics {
    /// Create new metrics with all counters at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a guest write operation.
    #[inline]
    pub fn record_guest_write(&self, bytes: u64) {
        self.guest_bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.guest_write_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a guest read operation.
    #[inline]
    pub fn record_guest_read(&self, bytes: u64) {
        self.guest_bytes_read.fetch_add(bytes, Ordering::Relaxed);
        self.guest_read_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an S3 batch write operation.
    #[inline]
    pub fn record_batch_write(&self, compressed_bytes: u64, uncompressed_bytes: u64) {
        self.s3_bytes_written
            .fetch_add(compressed_bytes, Ordering::Relaxed);
        self.batches_written.fetch_add(1, Ordering::Relaxed);
        self.uncompressed_bytes
            .fetch_add(uncompressed_bytes, Ordering::Relaxed);
        self.compressed_bytes
            .fetch_add(compressed_bytes, Ordering::Relaxed);
    }

    /// Record blocks synced within a batch.
    #[inline]
    #[allow(dead_code)] // Used in tests, part of metrics API
    pub fn record_blocks_synced(&self, count: u64) {
        self.s3_write_ops.fetch_add(count, Ordering::Relaxed);
    }

    /// Record an S3 read operation.
    #[inline]
    pub fn record_s3_read(&self, bytes: u64) {
        self.s3_bytes_read.fetch_add(bytes, Ordering::Relaxed);
        self.s3_read_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache hit.
    #[inline]
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss.
    #[inline]
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a snapshot of current metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let guest_bytes_written = self.guest_bytes_written.load(Ordering::Relaxed);
        let guest_write_ops = self.guest_write_ops.load(Ordering::Relaxed);
        let s3_bytes_written = self.s3_bytes_written.load(Ordering::Relaxed);
        let s3_write_ops = self.s3_write_ops.load(Ordering::Relaxed);
        let batches_written = self.batches_written.load(Ordering::Relaxed);
        let uncompressed_bytes = self.uncompressed_bytes.load(Ordering::Relaxed);
        let compressed_bytes = self.compressed_bytes.load(Ordering::Relaxed);
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);

        // Calculate derived metrics
        let write_amplification = if guest_bytes_written > 0 {
            s3_bytes_written as f64 / guest_bytes_written as f64
        } else {
            0.0
        };

        let compression_ratio = if compressed_bytes > 0 {
            uncompressed_bytes as f64 / compressed_bytes as f64
        } else {
            1.0
        };

        // Coalesce ratio: guest writes per S3 batch write
        let coalesce_ratio = if batches_written > 0 {
            guest_write_ops as f64 / batches_written as f64
        } else {
            0.0
        };

        let cache_hit_rate = if cache_hits + cache_misses > 0 {
            cache_hits as f64 / (cache_hits + cache_misses) as f64
        } else {
            0.0
        };

        MetricsSnapshot {
            guest_bytes_written,
            guest_write_ops,
            guest_bytes_read: self.guest_bytes_read.load(Ordering::Relaxed),
            guest_read_ops: self.guest_read_ops.load(Ordering::Relaxed),
            s3_bytes_written,
            s3_write_ops,
            batches_written,
            s3_bytes_read: self.s3_bytes_read.load(Ordering::Relaxed),
            s3_read_ops: self.s3_read_ops.load(Ordering::Relaxed),
            uncompressed_bytes,
            compressed_bytes,
            cache_hits,
            cache_misses,
            write_amplification,
            compression_ratio,
            coalesce_ratio,
            cache_hit_rate,
        }
    }
}

/// Point-in-time snapshot of metrics with derived calculations.
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    // Raw counters
    pub guest_bytes_written: u64,
    pub guest_write_ops: u64,
    pub guest_bytes_read: u64,
    pub guest_read_ops: u64,
    pub s3_bytes_written: u64,
    pub s3_write_ops: u64,
    pub batches_written: u64,
    pub s3_bytes_read: u64,
    pub s3_read_ops: u64,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,

    // Derived metrics
    /// S3 bytes written / guest bytes written
    /// < 1.0 = compression helping, > 1.0 = write amplification
    pub write_amplification: f64,

    /// Uncompressed bytes / compressed bytes
    /// > 1.0 = compression effective
    pub compression_ratio: f64,

    /// Guest write ops / batches written
    /// > 1.0 = batching coalescing multiple guest writes into fewer S3 batch writes
    pub coalesce_ratio: f64,

    /// Cache hits / (hits + misses)
    pub cache_hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_snapshot() {
        let metrics = ExportMetrics::new();

        // Simulate: guest writes 10KB across 10 ops
        for _ in 0..10 {
            metrics.record_guest_write(1024);
        }

        // Simulate: batch write containing 10 blocks (compressed from 128KB to 100KB)
        metrics.record_batch_write(100_000, 128_000);
        metrics.record_blocks_synced(10);

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.guest_bytes_written, 10 * 1024);
        assert_eq!(snapshot.guest_write_ops, 10);
        assert_eq!(snapshot.s3_bytes_written, 100_000);
        assert_eq!(snapshot.batches_written, 1);
        assert_eq!(snapshot.s3_write_ops, 10);

        // Write amplification: 100KB written to S3 for 10KB guest writes
        assert!((snapshot.write_amplification - 9.765).abs() < 0.1);

        // Compression ratio: 128KB uncompressed → 100KB compressed
        assert!((snapshot.compression_ratio - 1.28).abs() < 0.01);

        // Coalesce ratio: 10 guest writes → 1 batch write
        assert!((snapshot.coalesce_ratio - 10.0).abs() < 0.01);
    }

    #[test]
    fn test_cache_hit_rate() {
        let metrics = ExportMetrics::new();

        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        let snapshot = metrics.snapshot();
        assert!((snapshot.cache_hit_rate - 0.75).abs() < 0.01);
    }
}
