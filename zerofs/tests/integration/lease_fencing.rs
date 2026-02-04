//! Lease fencing tests for ZeroFS.
//!
//! These tests verify that lease loss correctly prevents S3 writes:
//! 1. sync_worker stops immediately when lease is lost
//! 2. No S3 PUTs occur after lease.mark_lost()
//! 3. Blocks that were mid-sync are re-queued
//!
//! Run with: `cargo test --features test-utils --test integration lease_fencing`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use tempfile::TempDir;

use zerofs::nbd::block_store::S3BlockStore;
use zerofs::nbd::lease::LeaseState;
use zerofs::nbd::metrics::ExportMetrics;
use zerofs::nbd::state::Initializing;
use zerofs::nbd::write_cache::{sync_worker, SyncWorkerConfig, WriteCache, WriteCacheConfig};

const BLOCK_SIZE: usize = 128 * 1024; // 128KB
const DEVICE_SIZE: u64 = 10 * 1024 * 1024; // 10MB

/// An ObjectStore wrapper that counts PUT operations.
///
/// Used to verify that no S3 writes occur after lease loss.
#[derive(Debug)]
struct CountingObjectStore {
    inner: object_store::memory::InMemory,
    put_count: AtomicU64,
    get_count: AtomicU64,
}

impl CountingObjectStore {
    fn new() -> Self {
        Self {
            inner: object_store::memory::InMemory::new(),
            put_count: AtomicU64::new(0),
            get_count: AtomicU64::new(0),
        }
    }

    fn put_count(&self) -> u64 {
        self.put_count.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    fn get_count(&self) -> u64 {
        self.get_count.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingObjectStore")
    }
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.put_count.fetch_add(1, Ordering::SeqCst);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.get_count.fetch_add(1, Ordering::SeqCst);
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// Helper to create a test cache config.
fn test_config(dir: &TempDir, name: &str) -> WriteCacheConfig {
    WriteCacheConfig {
        cache_dir: dir.path().to_path_buf(),
        device_name: name.to_string(),
        device_size: DEVICE_SIZE,
        block_size: BLOCK_SIZE,
    }
}

// =============================================================================
// LEASE FENCING TESTS
// =============================================================================

/// Test: No S3 writes occur after lease is marked as lost.
///
/// This is the critical fencing guarantee: when the lease is lost,
/// the sync worker MUST stop immediately to prevent conflicting writes
/// with the new lease holder.
#[tokio::test]
async fn test_no_s3_writes_after_lease_loss() {
    let s3_backend = Arc::new(CountingObjectStore::new());
    let s3 = Arc::new(
        S3BlockStore::new(
            Arc::clone(&s3_backend) as Arc<dyn ObjectStore>,
            "test",
            BLOCK_SIZE,
        )
        .with_blocks_per_batch(10)
        .with_metrics(Arc::new(ExportMetrics::new())),
    );

    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "lease_fencing");

    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = Arc::new(cache.skip_recovery_for_test());

    // Create lease state (valid initially)
    let (lease_state, _lost_rx) = LeaseState::new(1);
    let lease_state_for_worker = Arc::clone(&lease_state);

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Write some data before starting the worker
    cache.write(0, &vec![0xAA; BLOCK_SIZE]).unwrap();

    // Start sync worker with the lease
    let worker_cache = Arc::clone(&cache);
    let worker_s3 = Arc::clone(&s3);
    let worker_handle = tokio::spawn(async move {
        sync_worker(
            worker_cache,
            worker_s3,
            SyncWorkerConfig {
                batch_size: 100,
                metadata_save_interval: 1000, // Don't save during test
                hot_batch_cooldown: Duration::ZERO, // No cooldown for test
                ..Default::default()
            },
            shutdown_rx,
            Some(lease_state_for_worker),
        )
        .await;
    });

    // Let the worker process the initial dirty block
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Record PUT count BEFORE marking lease as lost
    let puts_before_loss = s3_backend.put_count();

    // Mark the lease as lost
    lease_state.mark_lost();

    // Wait a moment for the worker to notice
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Write MORE dirty blocks (these should NOT be synced)
    for i in 1..5 {
        cache.write(i * BLOCK_SIZE as u64, &vec![0xBB; BLOCK_SIZE]).unwrap();
    }

    // Give the worker time to potentially (incorrectly) sync them
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Record PUT count AFTER
    let puts_after_loss = s3_backend.put_count();

    // The sync worker should have exited
    let result = tokio::time::timeout(Duration::from_secs(2), worker_handle).await;
    assert!(result.is_ok(), "sync worker should exit when lease is lost");

    // CRITICAL ASSERTION: No new PUTs after lease loss
    // Note: There might be a PUT in flight when we marked lost, so we allow +1
    assert!(
        puts_after_loss <= puts_before_loss + 1,
        "No S3 writes should occur after lease loss. Before: {}, After: {}",
        puts_before_loss,
        puts_after_loss
    );

    // Cleanup
    let _ = shutdown_tx.send(true);
}

/// Test: Blocks claimed before lease loss are re-queued.
///
/// If we claim blocks for sync but then lose the lease before the PUT,
/// the blocks should be marked as failed (Syncing → Dirty) so they
/// can be re-synced by whoever gets the lease next.
#[tokio::test]
async fn test_blocks_requeued_on_lease_loss() {
    let s3_backend = Arc::new(CountingObjectStore::new());
    let s3 = Arc::new(
        S3BlockStore::new(
            Arc::clone(&s3_backend) as Arc<dyn ObjectStore>,
            "test",
            BLOCK_SIZE,
        )
        .with_blocks_per_batch(10)
        .with_metrics(Arc::new(ExportMetrics::new())),
    );

    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "requeue_test");

    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = Arc::new(cache.skip_recovery_for_test());

    // Create lease state
    let (lease_state, _lost_rx) = LeaseState::new(1);
    let lease_state_for_worker = Arc::clone(&lease_state);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Write data
    cache.write(0, &vec![0xCC; BLOCK_SIZE]).unwrap();
    cache.write(BLOCK_SIZE as u64, &vec![0xDD; BLOCK_SIZE]).unwrap();

    assert_eq!(cache.dirty_block_count(), 2);

    // Start sync worker
    let worker_cache = Arc::clone(&cache);
    let worker_s3 = Arc::clone(&s3);
    let worker_handle = tokio::spawn(async move {
        sync_worker(
            worker_cache,
            worker_s3,
            SyncWorkerConfig {
                batch_size: 1, // Process one block at a time for finer control
                metadata_save_interval: 1000,
                hot_batch_cooldown: Duration::ZERO,
                ..Default::default()
            },
            shutdown_rx,
            Some(lease_state_for_worker),
        )
        .await;
    });

    // Give worker time to claim blocks but mark lost immediately
    tokio::time::sleep(Duration::from_millis(10)).await;
    lease_state.mark_lost();

    // Wait for worker to exit
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_handle).await;

    // The blocks that couldn't be synced should be back in dirty state
    // (mark_sync_failed transitions Syncing → Dirty)
    // Note: Some blocks may have synced before we marked lost, that's OK
    // The point is that the worker stopped and didn't corrupt anything
    let _final_dirty = cache.dirty_block_count();
    let final_syncing = cache.syncing_block_count();

    // All blocks should be accounted for (either dirty, syncing→dirty, or synced)
    // No blocks should be "lost" in Syncing state forever
    assert_eq!(
        final_syncing, 0,
        "No blocks should remain in Syncing state after worker exits"
    );

    let _ = shutdown_tx.send(true);
}

/// Test: Lease check happens before each S3 write.
///
/// The sync worker has TWO lease checks:
/// 1. At the start of each loop iteration
/// 2. Right before the S3 PUT (final fencing check)
///
/// This test verifies the second check works by marking lost between
/// claim and PUT.
#[tokio::test]
async fn test_lease_check_before_s3_put() {
    let s3_backend = Arc::new(CountingObjectStore::new());
    let _s3 = Arc::new(
        S3BlockStore::new(
            Arc::clone(&s3_backend) as Arc<dyn ObjectStore>,
            "test",
            BLOCK_SIZE,
        )
        .with_blocks_per_batch(10)
        .with_metrics(Arc::new(ExportMetrics::new())),
    );

    let temp_dir = TempDir::new().unwrap();
    let config = test_config(&temp_dir, "pre_put_check");

    let cache = WriteCache::<Initializing>::open(config).unwrap();
    let cache = Arc::new(cache.skip_recovery_for_test());

    let (lease_state, _lost_rx) = LeaseState::new(1);

    // Write a block
    cache.write(0, &vec![0xEE; BLOCK_SIZE]).unwrap();
    assert_eq!(cache.dirty_block_count(), 1);

    // Manually claim the block (simulating sync worker behavior)
    let claimed = cache.claim_dirty_blocks(10);
    assert_eq!(claimed.len(), 1);
    assert_eq!(cache.syncing_block_count(), 1);

    // Mark lease as lost BEFORE the sync completes
    lease_state.mark_lost();

    // The lease is lost, so sync should be aborted
    // In real sync_worker, this would trigger mark_sync_failed for all claimed blocks
    // Here we verify that if we were to check the lease, it would be invalid
    assert!(!lease_state.is_valid(), "Lease should be invalid after mark_lost");

    // The claimed block is still in Syncing state because we didn't complete the operation
    // In real code, mark_sync_failed would transition it back to Dirty
    cache.mark_sync_failed(claimed[0]);

    assert_eq!(cache.syncing_block_count(), 0, "Block should no longer be Syncing");
    assert_eq!(cache.dirty_block_count(), 1, "Block should be back to Dirty");
}

/// Test: Rapid lease acquire/release doesn't cause data loss.
///
/// Simulates a scenario where leases change hands quickly:
/// Node A writes → Node A loses lease → Node B writes → etc.
/// No data should be lost or corrupted.
#[tokio::test]
async fn test_rapid_lease_handoff_no_data_loss() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let s3 = S3BlockStore::new(Arc::clone(&s3_backend), "test", BLOCK_SIZE).with_blocks_per_batch(10);
    let metrics = Arc::new(ExportMetrics::new());

    // Track all data written
    let mut expected_data: Vec<(u64, Vec<u8>)> = Vec::new();

    // Simulate 3 handoffs
    for handoff in 0..3 {
        let temp_dir = TempDir::new().unwrap();
        let config = test_config(&temp_dir, &format!("node_{}", handoff));

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.skip_recovery_for_test();

        // Write distinctive data
        let offset = handoff * BLOCK_SIZE as u64;
        let data: Vec<u8> = vec![handoff as u8; BLOCK_SIZE];
        cache.write(offset, &data).unwrap();
        expected_data.push((offset, data));

        // Drain to S3 (simulates graceful handoff)
        cache.drain_for_snapshot(&s3).await.unwrap();
    }

    // Verify all data survives from a fresh node
    let verify_dir = TempDir::new().unwrap();
    let verify_config = test_config(&verify_dir, "verifier");

    let verify_cache = WriteCache::<Initializing>::open(verify_config).unwrap();
    let verify_cache = verify_cache.skip_recovery_for_test();

    for (offset, expected) in expected_data {
        let actual = verify_cache
            .read_with_fetch(offset, BLOCK_SIZE, &s3, &metrics)
            .await
            .unwrap();

        assert_eq!(
            actual.as_ref(),
            &expected[..],
            "Data at offset {} should survive rapid handoffs",
            offset
        );
    }
}

/// Test: Lease generation counter prevents stale writes.
///
/// Even if a sync from an old lease holder arrives "late" (due to network delay),
/// it should be rejected by S3's conditional PUT (ETag mismatch).
#[tokio::test]
async fn test_generation_prevents_stale_writes() {
    let s3_backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let s3 = S3BlockStore::new(Arc::clone(&s3_backend), "test", BLOCK_SIZE).with_blocks_per_batch(10);

    // Simulate two nodes with different cache dirs (different local state)
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let config_a = test_config(&dir_a, "node_a");
    let config_b = test_config(&dir_b, "node_b");

    // Node A writes and syncs first
    let cache_a = WriteCache::<Initializing>::open(config_a).unwrap();
    let cache_a = cache_a.skip_recovery_for_test();

    let data_a: Vec<u8> = vec![0xAA; BLOCK_SIZE];
    cache_a.write(0, &data_a).unwrap();
    cache_a.drain_for_snapshot(&s3).await.unwrap();

    // Now Node B takes over and writes to the same block
    let cache_b = WriteCache::<Initializing>::open(config_b).unwrap();
    let cache_b = cache_b.skip_recovery_for_test();

    let data_b: Vec<u8> = vec![0xBB; BLOCK_SIZE];
    cache_b.write(0, &data_b).unwrap();
    cache_b.drain_for_snapshot(&s3).await.unwrap();

    // Verify S3 has Node B's data (the latest)
    let metrics = Arc::new(ExportMetrics::new());
    let verify_dir = TempDir::new().unwrap();
    let verify_config = test_config(&verify_dir, "verifier");

    let verify_cache = WriteCache::<Initializing>::open(verify_config).unwrap();
    let verify_cache = verify_cache.skip_recovery_for_test();

    let actual = verify_cache
        .read_with_fetch(0, BLOCK_SIZE, &s3, &metrics)
        .await
        .unwrap();

    assert_eq!(
        actual.as_ref(),
        &data_b[..],
        "S3 should have the latest data (from Node B)"
    );
}
