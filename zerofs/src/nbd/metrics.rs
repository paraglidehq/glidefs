//! Metrics for tracking write amplification and cache effectiveness.
//!
//! These metrics help answer:
//! - How much write amplification are we seeing? (guest bytes vs S3 bytes)
//! - How well is the cache coalescing writes? (guest writes vs S3 writes)
//! - How effective is read caching? (cache hits vs misses)

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

    /// Total bytes written to S3
    pub s3_bytes_written: AtomicU64,

    /// Total number of blocks written to S3 (within batches)
    pub s3_write_ops: AtomicU64,

    /// Total number of batches written to S3
    pub batches_written: AtomicU64,

    /// Total bytes read from S3
    pub s3_bytes_read: AtomicU64,

    /// Total number of blocks read from S3
    pub s3_read_ops: AtomicU64,

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
    pub fn record_batch_write(&self, bytes: u64) {
        self.s3_bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.batches_written.fetch_add(1, Ordering::Relaxed);
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
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);

        // Calculate derived metrics
        let write_amplification = if guest_bytes_written > 0 {
            s3_bytes_written as f64 / guest_bytes_written as f64
        } else {
            0.0
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
            cache_hits,
            cache_misses,
            dirty_blocks: None,
            syncing_blocks: None,
            write_amplification,
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
    pub cache_hits: u64,
    pub cache_misses: u64,

    // Cache state (populated by router)
    /// Number of dirty blocks waiting to be synced to S3
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dirty_blocks: Option<u64>,
    /// Number of blocks currently being synced to S3
    #[serde(skip_serializing_if = "Option::is_none")]
    pub syncing_blocks: Option<u64>,

    // Derived metrics
    /// S3 bytes written / guest bytes written
    /// - `> 1.0` = write amplification (S3 batching overhead)
    /// - `< 1.0` = unlikely without compression
    pub write_amplification: f64,

    /// Guest write ops / batches written
    /// > 1.0 = batching coalescing multiple guest writes into fewer S3 batch writes
    pub coalesce_ratio: f64,

    /// Cache hits / (hits + misses)
    pub cache_hit_rate: f64,
}

impl MetricsSnapshot {
    /// Add cache state to the snapshot.
    pub fn with_cache_state(mut self, dirty_blocks: u64, syncing_blocks: u64) -> Self {
        self.dirty_blocks = Some(dirty_blocks);
        self.syncing_blocks = Some(syncing_blocks);
        self
    }

    /// Format this snapshot as Prometheus metrics text for a single export.
    pub fn to_prometheus(&self, export_name: &str) -> String {
        use std::fmt::Write;
        let mut out = String::new();

        // Helper to write a metric line
        let label = format!("export=\"{}\"", export_name);

        // Guest I/O counters
        writeln!(out, "zerofs_guest_bytes_written{{{label}}} {}", self.guest_bytes_written).unwrap();
        writeln!(out, "zerofs_guest_write_ops{{{label}}} {}", self.guest_write_ops).unwrap();
        writeln!(out, "zerofs_guest_bytes_read{{{label}}} {}", self.guest_bytes_read).unwrap();
        writeln!(out, "zerofs_guest_read_ops{{{label}}} {}", self.guest_read_ops).unwrap();

        // S3 I/O counters
        writeln!(out, "zerofs_s3_bytes_written{{{label}}} {}", self.s3_bytes_written).unwrap();
        writeln!(out, "zerofs_s3_write_ops{{{label}}} {}", self.s3_write_ops).unwrap();
        writeln!(out, "zerofs_s3_batches_written{{{label}}} {}", self.batches_written).unwrap();
        writeln!(out, "zerofs_s3_bytes_read{{{label}}} {}", self.s3_bytes_read).unwrap();
        writeln!(out, "zerofs_s3_read_ops{{{label}}} {}", self.s3_read_ops).unwrap();

        // Cache counters
        writeln!(out, "zerofs_cache_hits{{{label}}} {}", self.cache_hits).unwrap();
        writeln!(out, "zerofs_cache_misses{{{label}}} {}", self.cache_misses).unwrap();

        // Cache state (gauges)
        if let Some(dirty) = self.dirty_blocks {
            writeln!(out, "zerofs_dirty_blocks{{{label}}} {dirty}").unwrap();
        }
        if let Some(syncing) = self.syncing_blocks {
            writeln!(out, "zerofs_syncing_blocks{{{label}}} {syncing}").unwrap();
        }

        // Derived metrics (gauges)
        writeln!(out, "zerofs_write_amplification{{{label}}} {:.6}", self.write_amplification).unwrap();
        writeln!(out, "zerofs_coalesce_ratio{{{label}}} {:.6}", self.coalesce_ratio).unwrap();
        writeln!(out, "zerofs_cache_hit_rate{{{label}}} {:.6}", self.cache_hit_rate).unwrap();

        out
    }
}

/// Generate Prometheus metrics header (TYPE and HELP lines).
/// Call once before iterating exports.
pub fn prometheus_header() -> &'static str {
    r#"# HELP zerofs_guest_bytes_written Total bytes written by guest
# TYPE zerofs_guest_bytes_written counter
# HELP zerofs_guest_write_ops Total NBD write commands from guest
# TYPE zerofs_guest_write_ops counter
# HELP zerofs_guest_bytes_read Total bytes read by guest
# TYPE zerofs_guest_bytes_read counter
# HELP zerofs_guest_read_ops Total NBD read commands from guest
# TYPE zerofs_guest_read_ops counter
# HELP zerofs_s3_bytes_written Total bytes written to S3
# TYPE zerofs_s3_bytes_written counter
# HELP zerofs_s3_write_ops Total blocks written to S3
# TYPE zerofs_s3_write_ops counter
# HELP zerofs_s3_batches_written Total S3 batch objects written
# TYPE zerofs_s3_batches_written counter
# HELP zerofs_s3_bytes_read Total bytes read from S3
# TYPE zerofs_s3_bytes_read counter
# HELP zerofs_s3_read_ops Total S3 read operations
# TYPE zerofs_s3_read_ops counter
# HELP zerofs_cache_hits Cache hits (reads served from local cache)
# TYPE zerofs_cache_hits counter
# HELP zerofs_cache_misses Cache misses (reads requiring S3 fetch)
# TYPE zerofs_cache_misses counter
# HELP zerofs_dirty_blocks Blocks waiting to sync to S3
# TYPE zerofs_dirty_blocks gauge
# HELP zerofs_syncing_blocks Blocks currently syncing to S3
# TYPE zerofs_syncing_blocks gauge
# HELP zerofs_write_amplification S3 bytes / guest bytes written
# TYPE zerofs_write_amplification gauge
# HELP zerofs_coalesce_ratio Guest write ops / S3 batch writes
# TYPE zerofs_coalesce_ratio gauge
# HELP zerofs_cache_hit_rate Fraction of reads served from cache
# TYPE zerofs_cache_hit_rate gauge
"#
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

        // Simulate: batch write of 100KB containing 10 blocks
        metrics.record_batch_write(100_000);
        metrics.record_blocks_synced(10);

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.guest_bytes_written, 10 * 1024);
        assert_eq!(snapshot.guest_write_ops, 10);
        assert_eq!(snapshot.s3_bytes_written, 100_000);
        assert_eq!(snapshot.batches_written, 1);
        assert_eq!(snapshot.s3_write_ops, 10);

        // Write amplification: 100KB written to S3 for 10KB guest writes
        assert!((snapshot.write_amplification - 9.765).abs() < 0.1);

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
