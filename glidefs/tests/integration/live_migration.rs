//! Integration tests for live migration protocol.
//!
//! These tests verify:
//! 1. Readonly staging rejects writes
//! 2. Drain ensures all data is in S3
//! 3. Lease handoff prevents dual-writer
//! 4. Full migration protocol works end-to-end
//!
//! Run with: `cargo test --features test-utils --test integration`

use std::sync::Arc;
use tempfile::TempDir;

use glidefs::nbd::block_store::S3BlockStore;
use glidefs::nbd::handler::NBDBlockHandler;
use glidefs::nbd::lease::LeaseManager;
use glidefs::nbd::metrics::ExportMetrics;
use glidefs::nbd::state::Active;
use glidefs::nbd::write_cache::{WriteCache, WriteCacheConfig};

/// Helper to create a test cache.
fn create_test_cache(
    temp_dir: &TempDir,
    name: &str,
    s3: Arc<object_store::memory::InMemory>,
) -> (Arc<WriteCache<Active>>, Arc<S3BlockStore>, Arc<ExportMetrics>) {
    let config = WriteCacheConfig {
        cache_dir: temp_dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: 10 * 1024 * 1024, // 10MB
        block_size: 128 * 1024,        // 128KB
    };

    let metrics = Arc::new(ExportMetrics::new());
    let s3_store = Arc::new(
        S3BlockStore::new(Arc::clone(&s3) as Arc<dyn object_store::ObjectStore>, "test", 128 * 1024)
            .with_blocks_per_batch(10)
            .with_metrics(Arc::clone(&metrics)),
    );

    let cache = WriteCache::open(config).expect("Failed to open cache");
    let cache = cache.skip_recovery_for_test();

    (Arc::new(cache), s3_store, metrics)
}

/// Helper to create handler.
fn create_handler(
    cache: Arc<WriteCache<Active>>,
    s3_store: Arc<S3BlockStore>,
    metrics: Arc<ExportMetrics>,
    readonly: bool,
) -> NBDBlockHandler {
    NBDBlockHandler::new(cache, s3_store, 10 * 1024 * 1024, readonly, metrics)
}

/// Test: Readonly export rejects writes.
///
/// During migration staging, the destination creates a readonly export.
/// This must reject all write operations.
#[tokio::test]
async fn test_readonly_rejects_writes() {
    let s3 = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, s3_store, metrics) = create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    let handler = create_handler(cache, s3_store, metrics, true);

    // All write operations should fail
    let data = vec![42u8; 4096];
    assert!(handler.write(0, &data, false).is_err());
    assert!(handler.trim(0, 4096, false).is_err());
    assert!(handler.write_zeroes(0, 4096, false).is_err());
}

/// Test: Readonly export allows reads.
#[tokio::test]
async fn test_readonly_allows_reads() {
    let s3 = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, s3_store, metrics) = create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    let handler = create_handler(cache, s3_store, metrics, true);

    // Reads should work
    let result = handler.read(0, 4096).await;
    assert!(result.is_ok());
}

/// Test: Promote changes readonly to read-write.
#[tokio::test]
async fn test_promote_enables_writes() {
    let s3 = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, s3_store, metrics) = create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    let handler = create_handler(cache, s3_store, metrics, true);

    // Initially readonly
    assert!(handler.is_readonly());
    let data = vec![42u8; 4096];
    assert!(handler.write(0, &data, false).is_err());

    // Promote to read-write
    handler.set_readonly(false);

    // Now writes work
    assert!(!handler.is_readonly());
    assert!(handler.write(0, &data, false).is_ok());
}

/// Test: Lease prevents two nodes from being read-write simultaneously.
#[tokio::test]
async fn test_lease_prevents_dual_writer() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

    let manager_a = LeaseManager::new(Arc::clone(&s3), "test", "node-a").with_ttl(300);
    let manager_b = LeaseManager::new(Arc::clone(&s3), "test", "node-b").with_ttl(300);

    // Node A acquires lease
    let handle_a = manager_a.acquire("vol1").await.unwrap();
    assert_eq!(handle_a.lease.owner, "node-a");
    assert_eq!(handle_a.lease.generation, 1);

    // Node B tries to acquire - should fail
    let result_b = manager_b.acquire("vol1").await;
    assert!(
        result_b.is_err(),
        "Node B should not be able to acquire lease held by Node A"
    );
}

/// Test: Lease release allows another node to acquire.
#[tokio::test]
async fn test_lease_release_enables_handoff() {
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

    let manager_a = LeaseManager::new(Arc::clone(&s3), "test", "node-a").with_ttl(300);
    let manager_b = LeaseManager::new(Arc::clone(&s3), "test", "node-b").with_ttl(300);

    // Node A acquires and releases
    let handle_a = manager_a.acquire("vol1").await.unwrap();
    manager_a.release("vol1", &handle_a).await.unwrap();

    // Node B can now acquire
    let handle_b = manager_b.acquire("vol1").await.unwrap();
    assert_eq!(handle_b.lease.owner, "node-b");
    // Generation should be: 1 (acquire) + 1 (release) + 1 (new acquire) = 3
    assert_eq!(handle_b.lease.generation, 3);
}

/// Test: Full live migration protocol.
///
/// Simulates the complete migration workflow:
/// 1. Source node is running with lease
/// 2. Destination stages readonly export
/// 3. Source drains to S3
/// 4. Source releases lease
/// 5. Destination promotes and acquires lease
/// 6. Destination can read all data and write new data
#[tokio::test]
async fn test_full_migration_protocol() {
    let s3 = Arc::new(object_store::memory::InMemory::new());
    let s3_obj: Arc<dyn object_store::ObjectStore> = Arc::clone(&s3) as _;

    // === SOURCE NODE ===
    let source_dir = TempDir::new().unwrap();
    let (source_cache, source_s3, source_metrics) =
        create_test_cache(&source_dir, "vol1", Arc::clone(&s3));
    let source_handler = create_handler(
        Arc::clone(&source_cache),
        Arc::clone(&source_s3),
        Arc::clone(&source_metrics),
        false,
    );
    let source_lease_mgr = LeaseManager::new(Arc::clone(&s3_obj), "test", "source");

    // Source acquires lease
    let source_lease = source_lease_mgr.acquire("vol1").await.unwrap();

    // Source writes data (simulating VM activity)
    for i in 0..10 {
        let data: Vec<u8> = vec![i as u8; 128 * 1024];
        source_handler
            .write(i as u64 * 128 * 1024, &data, false)
            .unwrap();
    }

    // === DESTINATION NODE: Stage readonly ===
    let dest_dir = TempDir::new().unwrap();
    let (dest_cache, dest_s3, dest_metrics) = create_test_cache(&dest_dir, "vol1", Arc::clone(&s3));
    let dest_handler = create_handler(
        Arc::clone(&dest_cache),
        Arc::clone(&dest_s3),
        Arc::clone(&dest_metrics),
        true, // readonly during staging
    );
    let dest_lease_mgr = LeaseManager::new(Arc::clone(&s3_obj), "test", "dest");

    // Destination should reject writes during staging
    assert!(dest_handler.write(0, &[1, 2, 3], false).is_err());

    // === MIGRATION POINT ===

    // 1. Source drains all dirty blocks to S3
    source_cache.drain_for_snapshot(&source_s3).await.unwrap();

    // 2. Source releases lease
    source_lease_mgr
        .release("vol1", &source_lease)
        .await
        .unwrap();

    // 3. Destination acquires lease and promotes to read-write
    let dest_lease = dest_lease_mgr.acquire("vol1").await.unwrap();
    assert_eq!(dest_lease.lease.owner, "dest");
    dest_handler.set_readonly(false);

    // === VERIFY DESTINATION CAN READ ALL DATA ===
    for i in 0..10 {
        let data = dest_handler
            .read(i as u64 * 128 * 1024, 128 * 1024)
            .await
            .unwrap();
        let expected = vec![i as u8; 128 * 1024];
        assert_eq!(data.as_ref(), &expected[..], "Block {} mismatch", i);
    }

    // === VERIFY DESTINATION CAN WRITE NEW DATA ===
    let new_data = vec![0xFFu8; 128 * 1024];
    dest_handler.write(0, &new_data, false).unwrap();

    let read_back = dest_handler.read(0, 128 * 1024).await.unwrap();
    assert_eq!(read_back.as_ref(), &new_data[..]);
}

/// Test: Concurrent write during sync transitions block back to Dirty.
///
/// This verifies the CAS-based state machine handles writes during sync correctly.
#[tokio::test]
async fn test_write_during_sync_stays_dirty() {
    let s3 = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, _s3_store, _metrics) = create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write data
    let data = vec![42u8; 128 * 1024];
    cache.write(0, &data).unwrap();

    // Claim the block for syncing (Dirty → Syncing)
    let claimed = cache.claim_dirty_blocks(1);
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0], 0);

    // Write again while syncing (Syncing → Dirty)
    let new_data = vec![99u8; 128 * 1024];
    cache.write(0, &new_data).unwrap();

    // Block should be dirty again
    assert_eq!(cache.dirty_block_count(), 1, "Block should be dirty again");

    // The new data should be what we read
    let read = cache.read_local(0, 128 * 1024).unwrap();
    assert_eq!(read.as_ref(), &new_data[..]);
}

/// Test: Drain ensures zero dirty blocks.
#[tokio::test]
async fn test_drain_ensures_all_synced() {
    let s3 = Arc::new(object_store::memory::InMemory::new());
    let temp_dir = TempDir::new().unwrap();
    let (cache, s3_store, _metrics) = create_test_cache(&temp_dir, "vol1", Arc::clone(&s3));

    // Write many blocks
    for i in 0..50 {
        let data: Vec<u8> = vec![i as u8; 128 * 1024];
        cache.write(i as u64 * 128 * 1024, &data).unwrap();
    }

    assert!(cache.dirty_block_count() > 0, "Should have dirty blocks");

    // Drain to S3
    cache.drain_for_snapshot(&s3_store).await.unwrap();

    // All blocks should be synced
    assert_eq!(cache.dirty_block_count(), 0, "Should have zero dirty blocks");
    assert_eq!(
        cache.syncing_block_count(),
        0,
        "Should have zero syncing blocks"
    );
}
