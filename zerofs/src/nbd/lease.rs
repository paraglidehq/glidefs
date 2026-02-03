//! Distributed lease management using S3 conditional writes.
//!
//! Provides mutual exclusion for exports without external coordination services.
//! Uses S3's If-Match conditional PUTs to implement compare-and-swap semantics.
//!
//! # Protocol
//!
//! Each export has a lease file at `{prefix}/lease.json`:
//! ```json
//! {
//!   "owner": "node-abc123",
//!   "generation": 42,
//!   "acquired_at": 1706900000,
//!   "ttl_seconds": 300
//! }
//! ```
//!
//! - **Acquire**: GET lease → check expiry/ownership → conditional PUT with If-Match
//! - **Renew**: Same as acquire, but only if we already own it
//! - **Release**: Conditional PUT with generation+1 and empty owner (or just let it expire)
//!
//! The generation counter provides fencing: any operation can verify the generation
//! hasn't changed since the lease was acquired.

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::{debug, info, warn};

/// Default lease TTL in seconds.
pub const DEFAULT_LEASE_TTL_SECONDS: u64 = 300; // 5 minutes

/// Errors that can occur during lease operations.
#[derive(Error, Debug)]
pub enum LeaseError {
    #[error("Lease held by another node: {owner} (generation {generation})")]
    LeaseHeldByOther { owner: String, generation: u64 },

    #[error("Lost lease race (concurrent acquisition)")]
    LostRace,

    #[allow(dead_code)] // Used by renew() which is part of public API
    #[error("Lease not held (must acquire before renewing)")]
    NotHeld,

    #[allow(dead_code)] // Used by renew() which is part of public API
    #[error("Lease generation mismatch: expected {expected}, got {actual}")]
    GenerationMismatch { expected: u64, actual: u64 },

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("System time error: {0}")]
    Time(#[from] std::time::SystemTimeError),
}

/// A lease record stored in S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lease {
    /// Node ID that holds the lease.
    pub owner: String,

    /// Monotonically increasing generation counter.
    /// Incremented on each acquisition; used for fencing.
    pub generation: u64,

    /// Unix timestamp when the lease was acquired.
    pub acquired_at: u64,

    /// Time-to-live in seconds.
    pub ttl_seconds: u64,
}

impl Lease {
    /// Check if the lease is expired.
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        now > self.acquired_at + self.ttl_seconds
    }

    /// Check if the lease is held by the given node.
    pub fn is_held_by(&self, node_id: &str) -> bool {
        self.owner == node_id && !self.is_expired()
    }

    /// Serialize to JSON bytes.
    pub fn to_json(&self) -> Result<Bytes, LeaseError> {
        Ok(Bytes::from(serde_json::to_vec(self)?))
    }

    /// Deserialize from JSON bytes.
    pub fn from_json(data: &[u8]) -> Result<Self, LeaseError> {
        Ok(serde_json::from_slice(data)?)
    }
}

/// Result of a lease operation.
#[derive(Debug, Clone)]
pub struct LeaseHandle {
    /// The lease we hold.
    pub lease: Lease,
}

/// Manages leases for exports using S3 as the coordination primitive.
pub struct LeaseManager {
    object_store: Arc<dyn ObjectStore>,
    prefix: String,
    node_id: String,
    ttl_seconds: u64,
}

impl LeaseManager {
    /// Create a new lease manager.
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
        node_id: impl Into<String>,
    ) -> Self {
        Self {
            object_store,
            prefix: prefix.into(),
            node_id: node_id.into(),
            ttl_seconds: DEFAULT_LEASE_TTL_SECONDS,
        }
    }

    /// Set the lease TTL.
    #[allow(dead_code)] // Used by tests, part of builder API
    pub fn with_ttl(mut self, ttl_seconds: u64) -> Self {
        self.ttl_seconds = ttl_seconds;
        self
    }

    /// Get the TTL in seconds.
    pub fn ttl_seconds(&self) -> u64 {
        self.ttl_seconds
    }

    /// Get the S3 path for an export's lease file.
    fn lease_path(&self, export: &str) -> Path {
        Path::from(format!("{}/nbd/{}/lease.json", self.prefix, export))
    }

    /// Get the current time as Unix timestamp.
    fn now() -> Result<u64, LeaseError> {
        Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
    }

    /// Try to acquire the lease for an export.
    ///
    /// Returns `Ok(LeaseHandle)` if the lease was acquired.
    /// Returns `Err(LeaseHeldByOther)` if another node holds a valid lease.
    /// Returns `Err(LostRace)` if we lost a race with another node.
    ///
    /// **Idempotent**: If we already hold the lease, this renews it.
    pub async fn acquire(&self, export: &str) -> Result<LeaseHandle, LeaseError> {
        let path = self.lease_path(export);
        let now = Self::now()?;

        // Try to read existing lease
        let (existing, etag) = match self.object_store.get(&path).await {
            Ok(resp) => {
                let etag = resp.meta.e_tag.clone();
                let data = resp.bytes().await?;
                let lease = Lease::from_json(&data)?;
                (Some(lease), etag)
            }
            Err(object_store::Error::NotFound { .. }) => (None, None),
            Err(e) => return Err(e.into()),
        };

        // Determine if we can acquire
        let (can_acquire, prev_generation) = match &existing {
            Some(lease) => {
                if lease.is_held_by(&self.node_id) {
                    // We already hold it - this is a renewal
                    debug!(export, "renewing existing lease (generation {})", lease.generation);
                    (true, lease.generation)
                } else if lease.owner.is_empty() {
                    // Released (empty owner) - we can take it
                    info!(
                        export,
                        "taking over released lease (was generation {})",
                        lease.generation
                    );
                    (true, lease.generation)
                } else if lease.is_expired() {
                    // Expired - we can take it
                    info!(
                        export,
                        prev_owner = lease.owner,
                        "taking over expired lease (was generation {})",
                        lease.generation
                    );
                    (true, lease.generation)
                } else {
                    // Someone else holds it
                    return Err(LeaseError::LeaseHeldByOther {
                        owner: lease.owner.clone(),
                        generation: lease.generation,
                    });
                }
            }
            None => {
                // No lease exists - we can create it
                info!(export, "creating new lease");
                (true, 0)
            }
        };

        if !can_acquire {
            unreachable!("Logic error: can_acquire should be true here");
        }

        // Create the new lease
        let new_lease = Lease {
            owner: self.node_id.clone(),
            generation: prev_generation + 1,
            acquired_at: now,
            ttl_seconds: self.ttl_seconds,
        };

        // Try to write with conditional PUT
        let put_opts = match etag {
            Some(ref e) => PutOptions {
                mode: PutMode::Update(object_store::UpdateVersion {
                    e_tag: Some(e.clone()),
                    version: None,
                }),
                ..Default::default()
            },
            None => PutOptions {
                mode: PutMode::Create, // Only create if doesn't exist
                ..Default::default()
            },
        };

        match self
            .object_store
            .put_opts(&path, new_lease.to_json()?.into(), put_opts)
            .await
        {
            Ok(_result) => {
                info!(
                    export,
                    generation = new_lease.generation,
                    "lease acquired successfully"
                );
                Ok(LeaseHandle { lease: new_lease })
            }
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => {
                warn!(export, "lost lease race with another node");
                Err(LeaseError::LostRace)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Renew an existing lease.
    ///
    /// This is a convenience wrapper around `acquire` that verifies we hold the lease.
    #[allow(dead_code)] // Part of public API, used by tests
    pub async fn renew(&self, export: &str, current: &LeaseHandle) -> Result<LeaseHandle, LeaseError> {
        let new_handle = self.acquire(export).await?;

        // Verify generation is what we expect (we should have incremented it)
        if new_handle.lease.generation != current.lease.generation + 1 {
            return Err(LeaseError::GenerationMismatch {
                expected: current.lease.generation + 1,
                actual: new_handle.lease.generation,
            });
        }

        Ok(new_handle)
    }

    /// Release the lease.
    ///
    /// **Idempotent**: If we don't hold the lease, returns Ok.
    pub async fn release(&self, export: &str, current: &LeaseHandle) -> Result<(), LeaseError> {
        let path = self.lease_path(export);

        // Read current lease to verify we still hold it
        let (existing, etag) = match self.object_store.get(&path).await {
            Ok(resp) => {
                let etag = resp.meta.e_tag.clone();
                let data = resp.bytes().await?;
                let lease = Lease::from_json(&data)?;
                (lease, etag)
            }
            Err(object_store::Error::NotFound { .. }) => {
                // Lease doesn't exist - nothing to release
                debug!(export, "lease already gone, nothing to release");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        // Check if we still hold it
        if existing.owner != self.node_id {
            // Someone else owns it now
            debug!(
                export,
                current_owner = existing.owner,
                "lease owned by another node, nothing to release"
            );
            return Ok(());
        }

        if existing.generation != current.lease.generation {
            // Generation changed - we lost it at some point
            debug!(
                export,
                expected_gen = current.lease.generation,
                actual_gen = existing.generation,
                "lease generation mismatch, treating as released"
            );
            return Ok(());
        }

        // Release by writing a lease with empty owner and incremented generation
        // This allows another node to immediately acquire without waiting for TTL
        let released_lease = Lease {
            owner: String::new(), // Empty owner = released
            generation: existing.generation + 1,
            acquired_at: Self::now()?,
            ttl_seconds: 0,
        };

        let put_opts = match etag {
            Some(ref e) => PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: Some(e.clone()),
                    version: None,
                }),
                ..Default::default()
            },
            None => PutOptions::default(),
        };

        match self
            .object_store
            .put_opts(&path, released_lease.to_json()?.into(), put_opts)
            .await
        {
            Ok(_) => {
                info!(export, generation = released_lease.generation, "lease released");
                Ok(())
            }
            Err(object_store::Error::Precondition { .. }) => {
                // Someone else modified it - they own it now
                debug!(export, "lease already modified by another node");
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Check if we hold the lease and the generation matches.
    ///
    /// This is a read-only operation for fencing checks before critical operations.
    #[allow(dead_code)] // Part of public API, used by tests
    pub async fn verify(&self, export: &str, expected_generation: u64) -> Result<bool, LeaseError> {
        let path = self.lease_path(export);

        match self.object_store.get(&path).await {
            Ok(resp) => {
                let data = resp.bytes().await?;
                let lease = Lease::from_json(&data)?;

                if lease.owner != self.node_id {
                    debug!(
                        export,
                        owner = lease.owner,
                        "lease held by another node"
                    );
                    return Ok(false);
                }

                if lease.generation != expected_generation {
                    debug!(
                        export,
                        expected = expected_generation,
                        actual = lease.generation,
                        "lease generation mismatch"
                    );
                    return Ok(false);
                }

                if lease.is_expired() {
                    debug!(export, "our lease has expired");
                    return Ok(false);
                }

                Ok(true)
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!(export, "lease doesn't exist");
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Get the current lease state for an export (read-only).
    #[allow(dead_code)] // Part of public API for inspecting lease state
    pub async fn get_lease(&self, export: &str) -> Result<Option<Lease>, LeaseError> {
        let path = self.lease_path(export);

        match self.object_store.get(&path).await {
            Ok(resp) => {
                let data = resp.bytes().await?;
                Ok(Some(Lease::from_json(&data)?))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

// ============================================================================
// LeaseGuard - Runtime lease verification and renewal
// ============================================================================

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::watch;

/// Shared state for lease coordination between sync worker and renewal task.
///
/// This provides:
/// - Atomic generation tracking for fencing checks
/// - Valid flag that's set to false when lease is lost
/// - Notification when lease is lost
pub struct LeaseState {
    /// Current lease generation (for fencing checks).
    generation: AtomicU64,

    /// Whether the lease is currently valid.
    valid: AtomicBool,

    /// Channel to notify when lease is lost.
    lost_tx: watch::Sender<bool>,
}

impl LeaseState {
    /// Create new lease state with the given generation.
    pub fn new(generation: u64) -> (Arc<Self>, watch::Receiver<bool>) {
        let (lost_tx, lost_rx) = watch::channel(false);
        let state = Arc::new(Self {
            generation: AtomicU64::new(generation),
            valid: AtomicBool::new(true),
            lost_tx,
        });
        (state, lost_rx)
    }

    /// Check if the lease is still valid.
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }

    /// Get the current generation (for logging/debugging).
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Update generation after successful renewal.
    pub fn update_generation(&self, new_gen: u64) {
        self.generation.store(new_gen, Ordering::Release);
    }

    /// Mark the lease as lost.
    pub fn mark_lost(&self) {
        self.valid.store(false, Ordering::Release);
        let _ = self.lost_tx.send(true);
    }
}

/// Background task that renews the lease periodically.
///
/// Renews at 50% of TTL. If renewal fails, marks the lease as lost.
pub async fn lease_renewal_task(
    manager: Arc<LeaseManager>,
    export: String,
    state: Arc<LeaseState>,
    mut shutdown: watch::Receiver<bool>,
) {
    let renewal_interval = Duration::from_secs(manager.ttl_seconds() / 2);
    info!(
        export = %export,
        interval_secs = renewal_interval.as_secs(),
        "lease renewal task started"
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep(renewal_interval) => {
                // Time to renew
                match manager.acquire(&export).await {
                    Ok(handle) => {
                        state.update_generation(handle.lease.generation);
                        debug!(
                            export = %export,
                            generation = handle.lease.generation,
                            "lease renewed"
                        );
                    }
                    Err(e) => {
                        warn!(
                            export = %export,
                            error = %e,
                            "lease renewal failed, marking as lost"
                        );
                        state.mark_lost();
                        return;
                    }
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!(export = %export, "lease renewal task shutting down");
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn test_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn test_manager(node_id: &str) -> LeaseManager {
        LeaseManager::new(test_store(), "test", node_id).with_ttl(10) // Short TTL for tests
    }

    #[tokio::test]
    async fn test_acquire_new_lease() {
        let manager = test_manager("node-a");
        let handle = manager.acquire("export1").await.unwrap();

        assert_eq!(handle.lease.owner, "node-a");
        assert_eq!(handle.lease.generation, 1);
    }

    #[tokio::test]
    async fn test_acquire_idempotent() {
        let manager = test_manager("node-a");

        let handle1 = manager.acquire("export1").await.unwrap();
        let handle2 = manager.acquire("export1").await.unwrap();

        assert_eq!(handle1.lease.owner, handle2.lease.owner);
        // Generation should increment on each acquire (renewal)
        assert_eq!(handle2.lease.generation, handle1.lease.generation + 1);
    }

    #[tokio::test]
    async fn test_acquire_blocked_by_other() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manager_a = LeaseManager::new(Arc::clone(&store), "test", "node-a").with_ttl(300);
        let manager_b = LeaseManager::new(Arc::clone(&store), "test", "node-b").with_ttl(300);

        // Node A acquires
        let _handle_a = manager_a.acquire("export1").await.unwrap();

        // Node B should fail
        let result = manager_b.acquire("export1").await;
        assert!(matches!(result, Err(LeaseError::LeaseHeldByOther { .. })));
    }

    #[tokio::test]
    async fn test_release_and_reacquire() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manager_a = LeaseManager::new(Arc::clone(&store), "test", "node-a").with_ttl(300);
        let manager_b = LeaseManager::new(Arc::clone(&store), "test", "node-b").with_ttl(300);

        // Node A acquires
        let handle_a = manager_a.acquire("export1").await.unwrap();
        assert_eq!(handle_a.lease.generation, 1);

        // Node A releases
        manager_a.release("export1", &handle_a).await.unwrap();

        // Node B can now acquire
        let handle_b = manager_b.acquire("export1").await.unwrap();
        assert_eq!(handle_b.lease.owner, "node-b");
        assert_eq!(handle_b.lease.generation, 3); // 1 (acquire) + 1 (release) + 1 (new acquire)
    }

    #[tokio::test]
    async fn test_verify_lease() {
        let manager = test_manager("node-a");

        let handle = manager.acquire("export1").await.unwrap();

        // Should verify successfully
        assert!(manager.verify("export1", handle.lease.generation).await.unwrap());

        // Wrong generation should fail
        assert!(!manager.verify("export1", 999).await.unwrap());
    }

    #[tokio::test]
    async fn test_release_idempotent() {
        let manager = test_manager("node-a");
        let handle = manager.acquire("export1").await.unwrap();

        // Release twice should succeed
        manager.release("export1", &handle).await.unwrap();
        manager.release("export1", &handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_lease_state_coordination() {
        // Create lease state with generation 1
        let (state, mut lost_rx) = LeaseState::new(1);

        // Initially valid
        assert!(state.is_valid());
        assert_eq!(state.generation(), 1);

        // Update generation
        state.update_generation(2);
        assert_eq!(state.generation(), 2);
        assert!(state.is_valid());

        // Mark as lost
        state.mark_lost();
        assert!(!state.is_valid());

        // Should receive notification
        lost_rx.changed().await.unwrap();
        assert!(*lost_rx.borrow());
    }
}
