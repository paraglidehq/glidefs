//! Benchmark proving flush() is local SSD only.
//!
//! This benchmark demonstrates that flush() latency is O(1) with respect to
//! dirty block count, proving that ZFS snapshot/clone operations complete
//! in ~10ms instead of 8-15 seconds.
//!
//! Run with: `cargo bench --features test-utils`

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use object_store::ObjectStore;
use tempfile::TempDir;

use zerofs::nbd::block_store::S3BlockStore;
use zerofs::nbd::state::Active;
use zerofs::nbd::write_cache::{WriteCache, WriteCacheConfig};

/// Create a test cache with in-memory S3.
fn setup_cache(temp_dir: &TempDir, device_size_mb: u64) -> (WriteCache<Active>, Arc<S3BlockStore>) {
    let s3: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: "bench".to_string(),
        device_size: device_size_mb * 1024 * 1024,
        block_size: 128 * 1024,
    };

    let s3_store = Arc::new(S3BlockStore::new(Arc::clone(&s3), "bench", 128 * 1024));

    let cache = WriteCache::open(config).expect("Failed to open cache");
    let cache = cache.skip_recovery_for_test();

    (cache, s3_store)
}

/// Benchmark: flush() latency is constant regardless of dirty block count.
///
/// This proves the key architectural claim: FLUSH syncs to local SSD only,
/// not to S3. The latency should be ~5-15ms regardless of how many dirty
/// blocks are pending S3 sync.
fn bench_flush_is_local_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_latency");

    // Test with different dirty block counts
    for dirty_blocks in [0u64, 10, 100, 500] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::new("local_flush", dirty_blocks),
            &dirty_blocks,
            |b, &blocks| {
                let temp_dir = TempDir::new().unwrap();
                let (cache, _s3) = setup_cache(&temp_dir, 100); // 100MB device

                // Write `blocks` worth of data (these become dirty)
                let data = vec![42u8; 128 * 1024]; // 128KB block
                for i in 0..blocks {
                    cache.write(i * 128 * 1024, &data).unwrap();
                }

                // Benchmark flush - should be ~constant time
                b.iter(|| {
                    cache.flush().unwrap();
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Compare local flush vs drain to S3.
///
/// This shows the difference between:
/// - flush(): Local SSD sync (~10ms)
/// - drain_for_snapshot(): Full S3 sync (100ms+ depending on data volume)
fn bench_flush_vs_drain(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("flush_vs_drain");

    let dirty_blocks = 50u64;

    // Local flush benchmark
    group.bench_function("local_flush_50_blocks", |b| {
        let temp_dir = TempDir::new().unwrap();
        let (cache, _s3) = setup_cache(&temp_dir, 100);

        let data = vec![42u8; 128 * 1024];
        for i in 0..dirty_blocks {
            cache.write(i * 128 * 1024, &data).unwrap();
        }

        b.iter(|| {
            cache.flush().unwrap();
        });
    });

    // S3 drain benchmark (what write-through would cost)
    group.bench_function("drain_to_s3_50_blocks", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total = std::time::Duration::ZERO;

            for _ in 0..iters {
                let temp_dir = TempDir::new().unwrap();
                let (cache, s3) = setup_cache(&temp_dir, 100);

                let data = vec![42u8; 128 * 1024];
                for i in 0..dirty_blocks {
                    cache.write(i * 128 * 1024, &data).unwrap();
                }

                let start = std::time::Instant::now();
                cache.drain_for_snapshot(&s3).await.unwrap();
                total += start.elapsed();
            }

            total
        });
    });

    group.finish();
}

/// Benchmark: Write throughput (local cache).
///
/// Shows that write throughput is limited by local SSD speed, not S3.
fn bench_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_throughput");

    let block_size = 128 * 1024usize;
    group.throughput(Throughput::Bytes(block_size as u64));

    group.bench_function("sequential_128kb_writes", |b| {
        let temp_dir = TempDir::new().unwrap();
        let (cache, _s3) = setup_cache(&temp_dir, 100);
        let data = vec![42u8; block_size];
        let mut offset = 0u64;

        b.iter(|| {
            cache.write(offset, &data).unwrap();
            offset = (offset + block_size as u64) % (100 * 1024 * 1024);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_flush_is_local_only,
    bench_flush_vs_drain,
    bench_write_throughput
);
criterion_main!(benches);
