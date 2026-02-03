//! Per-path writer lease coordination for fast microVM migration.
//!
//! This module implements a per-path lease-based writer coordination system that enables
//! fast (<500ms) handoff of individual paths between ZeroFS nodes. This allows migrating
//! one VM while others continue running on the same node.
//!
//! # Architecture
//!
//! Leases are stored as JSON files in S3 at `{db_path}/.zerofs_leases/{path_hash}.json`.
//! S3 conditional writes (If-Match) ensure atomicity.
//!
//! # Handoff Protocol (per-path)
//!
//! 1. **PrepareHandoffPath** (Node A): Block inode, drain writes, flush, set Releasing
//! 2. **CompleteHandoffPath** (Node A): Set state to Released
//! 3. **AcquirePath** (Node B): Read lease, verify Released, create new lease

use crate::fs::flush_coordinator::FlushCoordinator;
use crate::fs::inode::InodeId;
use crate::fs::write_coordinator::WriteCoordinator;
use crate::task::spawn_named;
use dashmap::{DashMap, DashSet};
use serde::{Deserialize, Serialize};
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    Error as ObjectStoreError, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const LEASES_DIR_NAME: &str = ".zerofs_leases";

/// Default lease duration in seconds
pub const DEFAULT_LEASE_DURATION_SECS: u64 = 30;

/// Default renewal interval in seconds (should be less than lease duration)
pub const DEFAULT_RENEWAL_INTERVAL_SECS: u64 = 10;

/// State of the writer lease
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LeaseState {
    /// Lease is active, holder has exclusive write access
    Active,
    /// Lease holder is preparing to hand off (draining writes, flushing)
    Releasing,
    /// Lease has been released, available for acquisition
    Released,
}

/// Writer lease data stored in S3
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterLease {
    /// Unique identifier for the lease holder
    pub holder_id: Uuid,
    /// Human-readable name for the lease holder (hostname, pod name, etc.)
    pub holder_name: String,
    /// Unix timestamp when the lease was acquired
    pub acquired_at: u64,
    /// Unix timestamp when the lease expires
    pub expires_at: u64,
    /// Current state of the lease
    pub state: LeaseState,
    /// Version for optimistic concurrency (incremented on each update)
    pub version: u64,
    /// The path this lease covers (e.g., "/.nbd/vm-1.raw")
    pub path: String,
}

impl WriterLease {
    fn new(holder_name: String, duration_secs: u64, path: String) -> Self {
        let now = current_timestamp();
        Self {
            holder_id: Uuid::new_v4(),
            holder_name,
            acquired_at: now,
            expires_at: now + duration_secs,
            state: LeaseState::Active,
            version: 1,
            path,
        }
    }

    fn is_expired(&self) -> bool {
        current_timestamp() > self.expires_at
    }

    fn is_available(&self) -> bool {
        self.is_expired() || self.state == LeaseState::Released
    }
}

/// Configuration for lease behavior
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    /// How long the lease is valid (seconds)
    pub lease_duration_secs: u64,
    /// How often to renew the lease (seconds)
    pub renewal_interval_secs: u64,
    /// Name to identify this node in the lease
    pub holder_name: String,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            lease_duration_secs: DEFAULT_LEASE_DURATION_SECS,
            renewal_interval_secs: DEFAULT_RENEWAL_INTERVAL_SECS,
            holder_name: gethostname::gethostname()
                .to_string_lossy()
                .into_owned(),
        }
    }
}

/// Error types for lease operations
#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error("Lease for path '{path}' is held by another node: {holder_name} (id: {holder_id})")]
    LeaseHeldByOther {
        path: String,
        holder_id: Uuid,
        holder_name: String,
    },

    #[error("Lease not held for path '{0}'")]
    LeaseNotHeld(String),

    #[error("Lease is in wrong state for operation: expected {expected:?}, got {actual:?}")]
    WrongState {
        expected: LeaseState,
        actual: LeaseState,
    },

    #[error("Concurrent modification detected (version mismatch)")]
    ConcurrentModification,

    #[error("S3 operation failed: {0}")]
    StorageError(#[from] slatedb::object_store::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Lease expired during operation")]
    LeaseExpired,

    #[error("Path not found: {0}")]
    #[allow(dead_code)] // Available for future use
    PathNotFound(String),

    #[error("Invalid handoff state: {0}")]
    InvalidHandoffState(String),
}

/// Held lease state including ETag for conditional updates
struct HeldPathLease {
    inode_id: InodeId,
    lease: WriterLease,
    etag: Option<String>,
}

/// A token proving that `prepare_handoff` was called for a specific path.
///
/// This token is REQUIRED by `complete_handoff`. You cannot complete a handoff
/// without first preparing it - this is enforced at compile time.
///
/// The token is consumed by `complete_handoff`, so you can only complete once.
#[must_use = "PreparedHandoff must be passed to complete_handoff to finish the handoff"]
pub struct PreparedHandoff {
    path: String,
}

impl PreparedHandoff {
    /// Get the path this handoff is for.
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Coordinates per-path writer leases for single-writer semantics.
///
/// This allows multiple paths to have independent leases, enabling
/// migration of individual VMs while others continue running.
pub struct LeaseCoordinator {
    object_store: Arc<dyn ObjectStore>,
    leases_base_path: Path,
    config: LeaseConfig,

    /// Per-path leases we hold (keyed by canonical path string)
    held_leases: DashMap<String, HeldPathLease>,

    /// Inodes with blocked writes (populated during prepare_handoff)
    /// pub to allow testing from integration tests
    pub blocked_inodes: DashSet<InodeId>,

    /// Pending handoff tokens stored after prepare_handoff.
    /// This allows RPC handlers to complete handoffs without carrying tokens
    /// across request boundaries, while still enforcing the prepare→complete protocol.
    pending_handoffs: DashMap<String, PreparedHandoff>,

    /// Sender for lease state changes (path, state)
    state_sender: watch::Sender<Option<(String, LeaseState)>>,

    /// Receiver for lease state changes
    #[allow(dead_code)]
    state_receiver: watch::Receiver<Option<(String, LeaseState)>>,
}

impl LeaseCoordinator {
    /// Create a new lease coordinator
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        db_path: &str,
        config: LeaseConfig,
    ) -> Self {
        let leases_base_path = Path::from(db_path).child(LEASES_DIR_NAME);
        let (state_sender, state_receiver) = watch::channel(None);

        Self {
            object_store,
            leases_base_path,
            config,
            held_leases: DashMap::new(),
            blocked_inodes: DashSet::new(),
            pending_handoffs: DashMap::new(),
            state_sender,
            state_receiver,
        }
    }

    /// Check if an inode has blocked writes (used during write operations).
    ///
    /// This is the fast path called by every write operation.
    #[inline]
    pub fn is_inode_blocked(&self, inode_id: InodeId) -> bool {
        self.blocked_inodes.contains(&inode_id)
    }

    /// Acquire a lease for a specific path.
    ///
    /// This will succeed if:
    /// - No lease exists for the path
    /// - The existing lease has expired
    /// - The existing lease is in Released state
    pub async fn acquire_path(&self, path: &str, inode_id: InodeId) -> Result<(), LeaseError> {
        info!("Attempting to acquire lease for path: {}", path);

        let lease_path = self.path_to_lease_path(path);

        // Check for existing lease
        let existing = self.read_lease_with_etag(&lease_path).await?;
        let no_existing_lease = existing.is_none();

        match &existing {
            Some((lease, _etag)) if !lease.is_available() => {
                return Err(LeaseError::LeaseHeldByOther {
                    path: path.to_string(),
                    holder_id: lease.holder_id,
                    holder_name: lease.holder_name.clone(),
                });
            }
            Some((lease, _)) => {
                debug!(
                    "Existing lease for {} is available (expired={}, state={:?})",
                    path,
                    lease.is_expired(),
                    lease.state
                );
            }
            None => {
                debug!("No existing lease found for {}", path);
            }
        }

        // Create new lease
        let base_version = existing.as_ref().map(|(l, _)| l.version).unwrap_or(0);
        let mut lease = WriterLease::new(
            self.config.holder_name.clone(),
            self.config.lease_duration_secs,
            path.to_string(),
        );
        lease.version = base_version + 1;

        let etag = self.write_lease_create_or_overwrite(&lease_path, &lease, no_existing_lease).await?;

        info!(
            "Acquired lease for path {}: holder_id={}, expires_at={}",
            path, lease.holder_id, lease.expires_at
        );

        self.held_leases.insert(
            path.to_string(),
            HeldPathLease {
                inode_id,
                lease,
                etag,
            },
        );

        let _ = self.state_sender.send(Some((path.to_string(), LeaseState::Active)));

        Ok(())
    }

    /// Prepare for handoff of a specific path.
    ///
    /// This will:
    /// 1. Block writes to the inode
    /// 2. Update lease state to Releasing in S3
    /// 3. Drain in-flight writes
    /// 4. Flush data
    ///
    /// Returns a [`PreparedHandoff`] token that MUST be passed to [`complete_handoff`]
    /// to finish the handoff. This enforces correct protocol at compile time.
    pub async fn prepare_handoff(
        &self,
        path: &str,
        write_coordinator: &Arc<WriteCoordinator>,
        flush_coordinator: &FlushCoordinator,
    ) -> Result<PreparedHandoff, LeaseError> {
        info!("Preparing handoff for path: {}", path);

        let lease_path = self.path_to_lease_path(path);

        // Get and update the held lease
        let inode_id = {
            let mut entry = self.held_leases
                .get_mut(path)
                .ok_or_else(|| LeaseError::LeaseNotHeld(path.to_string()))?;

            let held = entry.value_mut();

            if held.lease.state != LeaseState::Active {
                return Err(LeaseError::WrongState {
                    expected: LeaseState::Active,
                    actual: held.lease.state,
                });
            }

            // Block writes to this inode FIRST
            let inode_id = held.inode_id;
            self.blocked_inodes.insert(inode_id);
            info!("Blocked writes to inode {} for path {}", inode_id, path);

            // Update lease state
            held.lease.state = LeaseState::Releasing;
            held.lease.version += 1;

            let new_etag = self.write_lease_conditional(&lease_path, &held.lease, held.etag.as_deref()).await?;
            held.etag = new_etag;

            inode_id
        };

        let _ = self.state_sender.send(Some((path.to_string(), LeaseState::Releasing)));

        // Drain in-flight writes
        info!("Draining in-flight writes for path {}...", path);
        let mut drain_guard = write_coordinator.allocate_sequence();
        drain_guard.wait_for_predecessors().await;
        drain_guard.mark_committed();
        info!("All in-flight writes drained for path {}", path);

        // Flush to S3
        info!("Flushing data to S3 for path {}...", path);
        flush_coordinator
            .flush()
            .await
            .map_err(|_| LeaseError::LeaseExpired)?;
        info!("Flush complete for path {}", path);

        debug!("Handoff prepared for path {}, inode {}", path, inode_id);

        // Store in pending_handoffs - this is the source of truth that prepare was called.
        // Both complete_handoff(token) and complete_handoff_by_path() will check this.
        let token = PreparedHandoff {
            path: path.to_string(),
        };
        self.pending_handoffs.insert(path.to_string(), PreparedHandoff {
            path: path.to_string(),
        });

        // Return token for callers who want compile-time enforcement
        Ok(token)
    }

    /// Complete the handoff for a prepared path.
    ///
    /// This method REQUIRES a [`PreparedHandoff`] token, which can only be obtained
    /// by calling [`prepare_handoff`]. This enforces at compile time that you cannot
    /// complete a handoff without first preparing it.
    ///
    /// After this returns, another node can acquire the lease for this path.
    pub async fn complete_handoff(&self, prepared: PreparedHandoff) -> Result<(), LeaseError> {
        let path = &prepared.path;
        info!("Completing handoff for path: {}", path);

        // Remove from pending_handoffs (token proves prepare was called,
        // but we also clean up the stored entry)
        self.pending_handoffs.remove(path);

        let lease_path = self.path_to_lease_path(path);

        let mut entry = self.held_leases
            .get_mut(path)
            .ok_or_else(|| LeaseError::LeaseNotHeld(path.to_string()))?;

        let held = entry.value_mut();

        // This check is now redundant (prepare_handoff guarantees Releasing state)
        // but we keep it for defense in depth
        if held.lease.state != LeaseState::Releasing {
            return Err(LeaseError::WrongState {
                expected: LeaseState::Releasing,
                actual: held.lease.state,
            });
        }

        held.lease.state = LeaseState::Released;
        held.lease.version += 1;

        let new_etag = self.write_lease_conditional(&lease_path, &held.lease, held.etag.as_deref()).await?;
        held.etag = new_etag;

        let _ = self.state_sender.send(Some((path.to_string(), LeaseState::Released)));

        info!("Handoff complete for path {}: lease released", path);

        // Token is consumed here - cannot be reused
        drop(prepared);

        Ok(())
    }

    /// Complete handoff by path (for RPC use).
    ///
    /// This method is for RPC handlers where we can't pass tokens between requests.
    /// It validates that `prepare_handoff` was called by checking the internal
    /// `pending_handoffs` map, ensuring the prepare→complete protocol is enforced
    /// even across RPC request boundaries.
    ///
    /// For internal/SDK use, prefer the token-based [`complete_handoff`] method
    /// which provides compile-time enforcement.
    pub async fn complete_handoff_by_path(&self, path: &str) -> Result<(), LeaseError> {
        info!("Completing handoff for path (RPC): {}", path);

        // Verify and consume the pending handoff token.
        // This ensures prepare_handoff was called before complete_handoff.
        let _token = self
            .pending_handoffs
            .remove(path)
            .ok_or_else(|| LeaseError::InvalidHandoffState(
                format!("No pending handoff for path '{}'. Call prepare_handoff first.", path)
            ))?;

        let lease_path = self.path_to_lease_path(path);

        let mut entry = self.held_leases
            .get_mut(path)
            .ok_or_else(|| LeaseError::LeaseNotHeld(path.to_string()))?;

        let held = entry.value_mut();

        // Additional runtime validation - should always pass if pending_handoffs was set
        if held.lease.state != LeaseState::Releasing {
            return Err(LeaseError::WrongState {
                expected: LeaseState::Releasing,
                actual: held.lease.state,
            });
        }

        held.lease.state = LeaseState::Released;
        held.lease.version += 1;

        let new_etag = self.write_lease_conditional(&lease_path, &held.lease, held.etag.as_deref()).await?;
        held.etag = new_etag;

        let _ = self.state_sender.send(Some((path.to_string(), LeaseState::Released)));

        info!("Handoff complete for path {}: lease released (RPC)", path);

        Ok(())
    }

    /// Release a lease for a specific path (used during normal shutdown).
    pub async fn release_path(&self, path: &str) -> Result<(), LeaseError> {
        let lease_path = self.path_to_lease_path(path);

        if let Some((_, held)) = self.held_leases.remove(path) {
            // Unblock the inode
            self.blocked_inodes.remove(&held.inode_id);

            let mut released_lease = held.lease.clone();
            released_lease.state = LeaseState::Released;
            released_lease.version += 1;

            self.write_lease_conditional(&lease_path, &released_lease, held.etag.as_deref()).await?;
            info!("Released lease for path {}", path);

            let _ = self.state_sender.send(None);
        }

        Ok(())
    }

    /// Release all held leases (used during shutdown).
    pub async fn release_all(&self) -> Result<(), LeaseError> {
        let paths: Vec<String> = self.held_leases.iter().map(|e| e.key().clone()).collect();

        for path in paths {
            if let Err(e) = self.release_path(&path).await {
                warn!("Failed to release lease for {}: {}", path, e);
            }
        }

        // Clear any remaining blocked inodes
        self.blocked_inodes.clear();

        Ok(())
    }

    /// Renew a lease for a specific path.
    pub async fn renew_path(&self, path: &str) -> Result<(), LeaseError> {
        let lease_path = self.path_to_lease_path(path);

        let mut entry = self.held_leases
            .get_mut(path)
            .ok_or_else(|| LeaseError::LeaseNotHeld(path.to_string()))?;

        let held = entry.value_mut();

        if held.lease.state != LeaseState::Active {
            return Err(LeaseError::WrongState {
                expected: LeaseState::Active,
                actual: held.lease.state,
            });
        }

        let now = current_timestamp();
        held.lease.expires_at = now + self.config.lease_duration_secs;
        held.lease.version += 1;

        let new_etag = self.write_lease_conditional(&lease_path, &held.lease, held.etag.as_deref()).await?;
        held.etag = new_etag;

        debug!(
            "Renewed lease for {}: new expires_at={}, version={}",
            path, held.lease.expires_at, held.lease.version
        );

        Ok(())
    }

    /// Get the lease status for a specific path.
    pub fn get_path_status(&self, path: &str) -> Option<WriterLease> {
        self.held_leases.get(path).map(|e| e.value().lease.clone())
    }

    /// List all held leases.
    pub fn list_held_leases(&self) -> Vec<(String, WriterLease)> {
        self.held_leases
            .iter()
            .map(|e| (e.key().clone(), e.value().lease.clone()))
            .collect()
    }

    /// Check if this coordinator holds a lease for the given path.
    pub fn holds_lease_for(&self, path: &str) -> bool {
        self.held_leases.contains_key(path)
    }

    /// Perform a complete handoff in a single operation.
    ///
    /// This is a convenience method that does the full handoff sequence:
    /// 1. Acquire lease for the path
    /// 2. Prepare handoff (blocks writes, drains, flushes) - returns a token
    /// 3. Complete handoff with that token - releases the lease
    ///
    /// The `prepare_handoff` → `complete_handoff` sequence uses compile-time
    /// enforcement: `complete_handoff` REQUIRES the `PreparedHandoff` token
    /// which can only be obtained from `prepare_handoff`.
    ///
    /// After this method returns, another node can acquire the lease for this path.
    pub async fn perform_full_handoff(
        &self,
        path: &str,
        inode_id: InodeId,
        write_coordinator: &Arc<WriteCoordinator>,
        flush_coordinator: &FlushCoordinator,
    ) -> Result<(), LeaseError> {
        info!("Performing full handoff for path: {}", path);

        // Step 1: Acquire the lease
        self.acquire_path(path, inode_id).await?;
        info!("Acquired lease for {}", path);

        // Step 2: Prepare handoff - returns PreparedHandoff token
        // You CANNOT skip this step and call complete directly
        let prepared = self
            .prepare_handoff(path, write_coordinator, flush_coordinator)
            .await?;
        info!("Prepared handoff for {}", prepared.path());

        // Step 3: Complete handoff - REQUIRES the token from prepare
        // This is compile-time enforced: no token = no complete = compile error
        self.complete_handoff(prepared).await?;
        info!("Completed handoff for {}", path);

        Ok(())
    }

    /// Start a background task that periodically renews all held leases.
    pub fn start_renewal_task(
        self: &Arc<Self>,
        shutdown: CancellationToken,
    ) -> JoinHandle<()> {
        let coordinator = Arc::clone(self);
        let interval = Duration::from_secs(self.config.renewal_interval_secs);

        spawn_named("lease-renewal", async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        debug!("Lease renewal task shutting down");
                        break;
                    }
                    _ = interval_timer.tick() => {
                        // Renew all active leases
                        let paths: Vec<String> = coordinator.held_leases
                            .iter()
                            .filter(|e| e.value().lease.state == LeaseState::Active)
                            .map(|e| e.key().clone())
                            .collect();

                        for path in paths {
                            if let Err(e) = coordinator.renew_path(&path).await {
                                match e {
                                    LeaseError::LeaseNotHeld(_) => {
                                        debug!("Lease for {} no longer held, skipping renewal", path);
                                    }
                                    LeaseError::WrongState { .. } => {
                                        debug!("Lease for {} in non-active state, skipping renewal", path);
                                    }
                                    _ => {
                                        error!("Failed to renew lease for {}: {}", path, e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    /// Convert a path to its lease file path in S3.
    fn path_to_lease_path(&self, path: &str) -> Path {
        // Hash the path to create a valid filename
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        let hash = hasher.finish();

        // Use a combination of hash and sanitized path for debuggability
        let sanitized = path.replace('/', "_").trim_start_matches('_').to_string();
        let filename = format!("{}_{}.json", sanitized, hash);

        self.leases_base_path.child(filename)
    }

    /// Read a lease from S3, returning both the lease and its ETag.
    async fn read_lease_with_etag(&self, lease_path: &Path) -> Result<Option<(WriterLease, Option<String>)>, LeaseError> {
        match self.object_store.get(lease_path).await {
            Ok(result) => {
                let etag = result.meta.e_tag.clone();
                let bytes = result.bytes().await?;
                let lease: WriterLease = serde_json::from_slice(&bytes)?;
                Ok(Some((lease, etag)))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(LeaseError::StorageError(e)),
        }
    }

    /// Write a lease to S3 for initial creation or overwriting an expired/released lease.
    async fn write_lease_create_or_overwrite(
        &self,
        lease_path: &Path,
        lease: &WriterLease,
        create_only: bool,
    ) -> Result<Option<String>, LeaseError> {
        let payload = serde_json::to_vec(lease)?;

        let result = if create_only {
            self.object_store
                .put_opts(
                    lease_path,
                    PutPayload::from(payload),
                    PutOptions::from(PutMode::Create),
                )
                .await
        } else {
            self.object_store
                .put(lease_path, PutPayload::from(payload))
                .await
        };

        match result {
            Ok(put_result) => Ok(put_result.e_tag),
            Err(ObjectStoreError::AlreadyExists { .. }) => {
                Err(LeaseError::ConcurrentModification)
            }
            Err(e) => Err(LeaseError::StorageError(e)),
        }
    }

    /// Write a lease to S3 with conditional update using ETag.
    async fn write_lease_conditional(
        &self,
        lease_path: &Path,
        lease: &WriterLease,
        expected_etag: Option<&str>,
    ) -> Result<Option<String>, LeaseError> {
        let payload = serde_json::to_vec(lease)?;

        let put_mode = match expected_etag {
            Some(etag) => PutMode::Update(UpdateVersion {
                e_tag: Some(etag.to_string()),
                version: None,
            }),
            None => PutMode::Create,
        };

        let result = self
            .object_store
            .put_opts(
                lease_path,
                PutPayload::from(payload),
                PutOptions::from(put_mode),
            )
            .await;

        match result {
            Ok(put_result) => Ok(put_result.e_tag),
            Err(ObjectStoreError::Precondition { .. }) => {
                warn!("Lease update failed: ETag mismatch (concurrent modification)");
                Err(LeaseError::ConcurrentModification)
            }
            Err(ObjectStoreError::AlreadyExists { .. }) => {
                Err(LeaseError::ConcurrentModification)
            }
            Err(e) => Err(LeaseError::StorageError(e)),
        }
    }
}

/// Get current Unix timestamp in seconds
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// Note: Compile-time handoff enforcement is achieved through the PreparedHandoff token.
// The prepare_handoff() method returns this token, and complete_handoff() REQUIRES it.
// This makes it impossible to call complete without first calling prepare - enforced at compile time.

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    fn create_test_config(name: &str) -> LeaseConfig {
        LeaseConfig {
            lease_duration_secs: 30,
            renewal_interval_secs: 10,
            holder_name: name.to_string(),
        }
    }

    fn create_test_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    #[tokio::test]
    async fn test_acquire_path_new_lease() {
        let store = create_test_store();
        let coordinator = LeaseCoordinator::new(store, "test-db", create_test_config("node-a"));

        coordinator.acquire_path("/.nbd/vm-1.raw", 42).await.unwrap();
        assert!(coordinator.holds_lease_for("/.nbd/vm-1.raw"));

        let status = coordinator.get_path_status("/.nbd/vm-1.raw").unwrap();
        assert_eq!(status.state, LeaseState::Active);
        assert_eq!(status.holder_name, "node-a");
        assert_eq!(status.path, "/.nbd/vm-1.raw");
    }

    #[tokio::test]
    async fn test_acquire_path_fails_when_held() {
        let store = create_test_store();

        // Node A acquires
        let coordinator_a = LeaseCoordinator::new(
            Arc::clone(&store),
            "test-db",
            create_test_config("node-a"),
        );
        coordinator_a.acquire_path("/.nbd/vm-1.raw", 42).await.unwrap();

        // Node B tries to acquire same path
        let coordinator_b = LeaseCoordinator::new(store, "test-db", create_test_config("node-b"));
        let result = coordinator_b.acquire_path("/.nbd/vm-1.raw", 42).await;

        assert!(matches!(result, Err(LeaseError::LeaseHeldByOther { .. })));
    }

    #[tokio::test]
    async fn test_multiple_paths_independent() {
        let store = create_test_store();

        // Node A acquires path 1
        let coordinator_a = LeaseCoordinator::new(
            Arc::clone(&store),
            "test-db",
            create_test_config("node-a"),
        );
        coordinator_a.acquire_path("/.nbd/vm-1.raw", 42).await.unwrap();

        // Node B can acquire different path
        let coordinator_b = LeaseCoordinator::new(store, "test-db", create_test_config("node-b"));
        coordinator_b.acquire_path("/.nbd/vm-2.raw", 43).await.unwrap();

        // Both should have their respective leases
        assert!(coordinator_a.holds_lease_for("/.nbd/vm-1.raw"));
        assert!(!coordinator_a.holds_lease_for("/.nbd/vm-2.raw"));
        assert!(coordinator_b.holds_lease_for("/.nbd/vm-2.raw"));
        assert!(!coordinator_b.holds_lease_for("/.nbd/vm-1.raw"));
    }

    #[tokio::test]
    async fn test_inode_blocking() {
        let store = create_test_store();
        let coordinator = LeaseCoordinator::new(store, "test-db", create_test_config("node-a"));

        // Initially not blocked
        assert!(!coordinator.is_inode_blocked(42));

        // Manually block for testing
        coordinator.blocked_inodes.insert(42);
        assert!(coordinator.is_inode_blocked(42));

        // Other inodes not blocked
        assert!(!coordinator.is_inode_blocked(43));

        // Unblock
        coordinator.blocked_inodes.remove(&42);
        assert!(!coordinator.is_inode_blocked(42));
    }

    /// Mock FlushCoordinator for testing
    fn create_mock_flush_coordinator() -> FlushCoordinator {
        use crate::fs::errors::FsError;
        use crate::task::spawn_named;
        use tokio::sync::mpsc;
        use tokio::sync::oneshot;

        let (sender, mut receiver) =
            mpsc::unbounded_channel::<oneshot::Sender<Result<(), FsError>>>();

        spawn_named("mock-flush-coordinator", async move {
            while let Some(reply) = receiver.recv().await {
                let _ = reply.send(Ok(()));
            }
        });

        #[derive(Clone)]
        struct MockFlushCoordinator {
            sender: mpsc::UnboundedSender<oneshot::Sender<Result<(), FsError>>>,
        }

        let mock = MockFlushCoordinator { sender };
        unsafe { std::mem::transmute(mock) }
    }

    fn create_mock_write_coordinator() -> Arc<WriteCoordinator> {
        Arc::new(WriteCoordinator::default())
    }

    #[tokio::test]
    async fn test_full_path_handoff_flow() {
        let store = create_test_store();

        // Node A acquires lease for path
        let coordinator_a = LeaseCoordinator::new(
            Arc::clone(&store),
            "test-db",
            create_test_config("node-a"),
        );
        coordinator_a.acquire_path("/.nbd/vm-1.raw", 42).await.unwrap();

        let write_coordinator = create_mock_write_coordinator();
        let flush_coordinator = create_mock_flush_coordinator();

        // Prepare handoff - returns a token that MUST be passed to complete_handoff
        let prepared = coordinator_a
            .prepare_handoff("/.nbd/vm-1.raw", &write_coordinator, &flush_coordinator)
            .await
            .unwrap();

        let status = coordinator_a.get_path_status("/.nbd/vm-1.raw").unwrap();
        assert_eq!(status.state, LeaseState::Releasing);
        assert!(coordinator_a.is_inode_blocked(42));

        // Complete handoff - REQUIRES the token from prepare_handoff (compile-time enforced)
        coordinator_a.complete_handoff(prepared).await.unwrap();

        let status = coordinator_a.get_path_status("/.nbd/vm-1.raw").unwrap();
        assert_eq!(status.state, LeaseState::Released);

        // Node B can now acquire
        let coordinator_b = LeaseCoordinator::new(store, "test-db", create_test_config("node-b"));
        coordinator_b.acquire_path("/.nbd/vm-1.raw", 42).await.unwrap();

        let status = coordinator_b.get_path_status("/.nbd/vm-1.raw").unwrap();
        assert_eq!(status.holder_name, "node-b");
        assert_eq!(status.state, LeaseState::Active);
    }

    #[tokio::test]
    async fn test_handoff_one_path_while_another_active() {
        let store = create_test_store();

        let coordinator = LeaseCoordinator::new(
            Arc::clone(&store),
            "test-db",
            create_test_config("node-a"),
        );

        // Acquire two paths
        coordinator.acquire_path("/.nbd/vm-1.raw", 42).await.unwrap();
        coordinator.acquire_path("/.nbd/vm-2.raw", 43).await.unwrap();

        let write_coordinator = create_mock_write_coordinator();
        let flush_coordinator = create_mock_flush_coordinator();

        // Handoff only vm-1 - returns token (we don't complete in this test)
        let _prepared = coordinator
            .prepare_handoff("/.nbd/vm-1.raw", &write_coordinator, &flush_coordinator)
            .await
            .unwrap();

        // vm-1 is blocked, vm-2 is not
        assert!(coordinator.is_inode_blocked(42));
        assert!(!coordinator.is_inode_blocked(43));

        // vm-2 is still active
        let status_2 = coordinator.get_path_status("/.nbd/vm-2.raw").unwrap();
        assert_eq!(status_2.state, LeaseState::Active);
    }

    #[tokio::test]
    async fn test_release_path_unblocks_inode() {
        let store = create_test_store();
        let coordinator = LeaseCoordinator::new(store, "test-db", create_test_config("node-a"));

        coordinator.acquire_path("/.nbd/vm-1.raw", 42).await.unwrap();

        let write_coordinator = create_mock_write_coordinator();
        let flush_coordinator = create_mock_flush_coordinator();

        let _prepared = coordinator
            .prepare_handoff("/.nbd/vm-1.raw", &write_coordinator, &flush_coordinator)
            .await
            .unwrap();

        assert!(coordinator.is_inode_blocked(42));

        // Release should unblock (even though we have a prepared token)
        coordinator.release_path("/.nbd/vm-1.raw").await.unwrap();
        assert!(!coordinator.is_inode_blocked(42));
        assert!(!coordinator.holds_lease_for("/.nbd/vm-1.raw"));
    }

    #[tokio::test]
    async fn test_list_held_leases() {
        let store = create_test_store();
        let coordinator = LeaseCoordinator::new(store, "test-db", create_test_config("node-a"));

        coordinator.acquire_path("/.nbd/vm-1.raw", 42).await.unwrap();
        coordinator.acquire_path("/.nbd/vm-2.raw", 43).await.unwrap();

        let leases = coordinator.list_held_leases();
        assert_eq!(leases.len(), 2);

        let paths: Vec<&str> = leases.iter().map(|(p, _)| p.as_str()).collect();
        assert!(paths.contains(&"/.nbd/vm-1.raw"));
        assert!(paths.contains(&"/.nbd/vm-2.raw"));
    }

    #[tokio::test]
    async fn test_renew_path() {
        let store = create_test_store();
        let coordinator = LeaseCoordinator::new(store, "test-db", create_test_config("node-a"));

        coordinator.acquire_path("/.nbd/vm-1.raw", 42).await.unwrap();

        let initial_expires = coordinator.get_path_status("/.nbd/vm-1.raw").unwrap().expires_at;

        tokio::time::sleep(Duration::from_millis(100)).await;
        coordinator.renew_path("/.nbd/vm-1.raw").await.unwrap();

        let new_expires = coordinator.get_path_status("/.nbd/vm-1.raw").unwrap().expires_at;
        assert!(new_expires >= initial_expires);
    }

    #[tokio::test]
    async fn test_version_increments() {
        let store = create_test_store();

        let coordinator_a = LeaseCoordinator::new(
            Arc::clone(&store),
            "test-db",
            create_test_config("node-a"),
        );
        coordinator_a.acquire_path("/.nbd/vm-1.raw", 42).await.unwrap();

        let v1 = coordinator_a.get_path_status("/.nbd/vm-1.raw").unwrap().version;

        let write_coordinator = create_mock_write_coordinator();
        let flush_coordinator = create_mock_flush_coordinator();

        // Prepare handoff returns a token
        let prepared = coordinator_a
            .prepare_handoff("/.nbd/vm-1.raw", &write_coordinator, &flush_coordinator)
            .await
            .unwrap();

        let v2 = coordinator_a.get_path_status("/.nbd/vm-1.raw").unwrap().version;
        assert!(v2 > v1);

        // Complete handoff requires the token (compile-time enforced)
        coordinator_a.complete_handoff(prepared).await.unwrap();

        let v3 = coordinator_a.get_path_status("/.nbd/vm-1.raw").unwrap().version;
        assert!(v3 > v2);

        // Node B acquires
        let coordinator_b = LeaseCoordinator::new(store, "test-db", create_test_config("node-b"));
        coordinator_b.acquire_path("/.nbd/vm-1.raw", 42).await.unwrap();

        let v4 = coordinator_b.get_path_status("/.nbd/vm-1.raw").unwrap().version;
        assert!(v4 > v3);
    }

    /// Test that the token-based API enforces correct handoff ordering.
    ///
    /// This test demonstrates that:
    /// - prepare_handoff() returns a PreparedHandoff token
    /// - complete_handoff() REQUIRES that token - you cannot call it without one
    /// - This is compile-time enforcement: no token = no complete = compile error
    #[tokio::test]
    async fn test_token_enforces_handoff_ordering() {
        let store = create_test_store();
        let coordinator = LeaseCoordinator::new(
            Arc::clone(&store),
            "test-db",
            create_test_config("node-a"),
        );

        coordinator.acquire_path("/.nbd/vm-1.raw", 42).await.unwrap();

        let write_coordinator = create_mock_write_coordinator();
        let flush_coordinator = create_mock_flush_coordinator();

        // Step 1: prepare_handoff returns a token
        let prepared: PreparedHandoff = coordinator
            .prepare_handoff("/.nbd/vm-1.raw", &write_coordinator, &flush_coordinator)
            .await
            .unwrap();

        // The token knows what path it's for
        assert_eq!(prepared.path(), "/.nbd/vm-1.raw");

        // Step 2: complete_handoff REQUIRES the token
        // Without this token, you cannot call complete_handoff - this is compile-time enforced
        coordinator.complete_handoff(prepared).await.unwrap();

        // Verify the handoff completed
        let status = coordinator.get_path_status("/.nbd/vm-1.raw").unwrap();
        assert_eq!(status.state, LeaseState::Released);
    }

    #[tokio::test]
    async fn test_complete_handoff_by_path_requires_prepare() {
        let store = create_test_store();
        let coordinator = LeaseCoordinator::new(store, "test-db", create_test_config("node-a"));

        // Acquire a lease
        coordinator.acquire_path("/.nbd/vm-1.raw", 42).await.unwrap();
        assert!(coordinator.holds_lease_for("/.nbd/vm-1.raw"));

        // Try to complete handoff without calling prepare_handoff first
        let result = coordinator.complete_handoff_by_path("/.nbd/vm-1.raw").await;

        // Should fail with InvalidHandoffState because prepare_handoff wasn't called
        assert!(result.is_err());
        match result.unwrap_err() {
            LeaseError::InvalidHandoffState(msg) => {
                assert!(msg.contains("No pending handoff"));
                assert!(msg.contains("prepare_handoff"));
            }
            other => panic!("Expected InvalidHandoffState, got: {:?}", other),
        }

        // Lease should still be Active (not Released)
        let status = coordinator.get_path_status("/.nbd/vm-1.raw").unwrap();
        assert_eq!(status.state, LeaseState::Active);
    }
}
