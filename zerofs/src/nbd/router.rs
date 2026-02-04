//! Multi-tenant export router for NBD server.
//!
//! Manages multiple NBD exports, each with its own write cache and S3 storage.
//! Supports dynamic export creation/removal for microVM scale-to-zero and live migration.

use crate::config::ExportConfig;
use crate::nbd::block_store::S3BlockStore;
use crate::nbd::handler::NBDBlockHandler;
use crate::nbd::lease::{lease_renewal_task, LeaseHandle, LeaseManager, LeaseState};
use crate::nbd::metrics::{ExportMetrics, MetricsSnapshot};
use crate::nbd::state::Active;
use crate::nbd::write_cache::{sync_worker, CacheError, WriteCache, WriteCacheConfig};
use crate::task::spawn_named;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Errors that can occur during export operations.
#[derive(Error, Debug)]
pub enum RouterError {
    #[allow(dead_code)] // create_export is idempotent, but API layer still matches this
    #[error("Export '{0}' already exists")]
    ExportExists(String),

    #[error("Export '{0}' not found")]
    ExportNotFound(String),

    #[error("Lease error: {0}")]
    Lease(#[from] super::lease::LeaseError),

    #[error("Cache error: {0}")]
    Cache(#[from] CacheError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Information about an export for NBD protocol.
#[derive(Clone, Debug)]
pub struct ExportInfo {
    pub name: String,
    pub size: u64,
    pub readonly: bool,
}

/// State for a single export.
pub struct ExportState {
    pub handler: Arc<NBDBlockHandler>,
    pub cache: Arc<WriteCache<Active>>,
    pub s3_store: Arc<S3BlockStore>,
    pub readonly: bool,
    pub metrics: Arc<ExportMetrics>,
    /// Lease handle when export is read-write (None if readonly or no lease required)
    pub lease: Option<LeaseHandle>,
    /// Lease state for coordination with sync worker (None if readonly)
    /// Stored to keep the Arc alive; sync_worker and renewal task hold clones.
    #[allow(dead_code)]
    lease_state: Option<Arc<LeaseState>>,
    sync_shutdown: watch::Sender<bool>,
    sync_handle: JoinHandle<()>,
    /// Lease renewal task handle (None if readonly)
    lease_renewal_handle: Option<JoinHandle<()>>,
}

impl ExportState {
    /// Drain all dirty blocks to S3.
    pub async fn drain(&self) -> Result<(), RouterError> {
        self.cache
            .drain_for_snapshot(&self.s3_store)
            .await
            .map_err(RouterError::Cache)
    }
}

/// Multi-tenant export router.
///
/// Manages multiple NBD exports, each with independent storage and caching.
/// Uses S3 leases for distributed coordination during migrations.
pub struct ExportRouter {
    /// Active exports: name → state
    exports: RwLock<HashMap<String, ExportState>>,

    /// Shared object store (S3/MinIO/etc)
    object_store: Arc<dyn ObjectStore>,

    /// Base S3 path prefix
    db_path: String,

    /// Local cache directory
    cache_dir: PathBuf,

    /// Block size for all exports (default, can be overridden per-export)
    block_size: usize,

    /// Number of blocks per S3 batch
    blocks_per_batch: u64,

    /// Sync delay in milliseconds (time to coalesce writes before S3 upload)
    sync_delay_ms: u64,

    /// Lease manager for distributed coordination (Arc for sharing with renewal tasks)
    lease_manager: Arc<LeaseManager>,
}

impl ExportRouter {
    /// Create a new export router.
    ///
    /// # Arguments
    /// * `object_store` - S3/MinIO/etc backend
    /// * `db_path` - Base path prefix in object store
    /// * `cache_dir` - Local directory for write cache
    /// * `block_size` - Block size in bytes
    /// * `blocks_per_batch` - Number of blocks per S3 batch object
    /// * `sync_delay_ms` - Delay before syncing writes to S3
    /// * `node_id` - Unique identifier for this node (for lease coordination)
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        db_path: String,
        cache_dir: PathBuf,
        block_size: usize,
        blocks_per_batch: u64,
        sync_delay_ms: u64,
        node_id: String,
    ) -> Self {
        let lease_manager = Arc::new(LeaseManager::new(
            Arc::clone(&object_store),
            db_path.clone(),
            node_id,
        ));

        Self {
            exports: RwLock::new(HashMap::new()),
            object_store,
            db_path,
            cache_dir,
            block_size,
            blocks_per_batch,
            sync_delay_ms,
            lease_manager,
        }
    }

    /// Create a new export.
    ///
    /// If `readonly` is true, the export will reject writes. Used during live migration to
    /// pre-stage the export on the destination node before promoting it to read-write.
    ///
    /// **Idempotent**: If export already exists, returns Ok(()) without error.
    pub async fn create_export(
        &self,
        config: ExportConfig,
        readonly: bool,
    ) -> Result<(), RouterError> {
        let name = config.name.clone();

        // Check if export already exists - idempotent: return success if already exists
        {
            let exports = self.exports.read().await;
            if exports.contains_key(&name) {
                info!("Export '{}' already exists, skipping creation", name);
                return Ok(());
            }
        }

        info!(
            "Creating export '{}': size={}GB, s3_prefix={}, readonly={}",
            name,
            config.size_gb,
            config.s3_prefix(),
            readonly
        );

        // Create metrics for this export (created first so S3BlockStore can use it)
        let metrics = Arc::new(ExportMetrics::new());

        // Use per-export block size if specified, otherwise use global default
        let block_size = config.block_size_or(self.block_size);

        // Create S3 block store for this export
        // Note: S3BlockStore no longer uses client-side encryption - S3 SSE handles it
        let s3_prefix = format!("{}/nbd/{}", self.db_path, config.s3_prefix());
        let s3_store = Arc::new(
            S3BlockStore::new(Arc::clone(&self.object_store), s3_prefix, block_size)
                .with_blocks_per_batch(self.blocks_per_batch)
                .with_metrics(Arc::clone(&metrics)),
        );

        // Create write cache for this export
        let cache_config = WriteCacheConfig {
            cache_dir: self.cache_dir.clone(),
            device_name: name.clone(),
            device_size: config.size_bytes(),
            block_size,
        };

        let cache = WriteCache::open(cache_config)?;

        info!("Recovering write cache for export '{}'...", name);
        let cache = cache.finish_recovery(&s3_store).await?;
        let cache = Arc::new(cache);
        info!("Export '{}' cache ready", name);

        // Acquire lease FIRST if this is a read-write export
        // This determines whether we can actually write, and we need the generation
        // for fencing coordination with the sync worker
        let (lease, lease_state, actual_readonly) = if !readonly {
            match self.lease_manager.acquire(&name).await {
                Ok(handle) => {
                    info!(
                        "Acquired lease for export '{}' (generation {})",
                        name, handle.lease.generation
                    );
                    // Create lease state for sync worker coordination
                    let (state, _lost_rx) = LeaseState::new(handle.lease.generation);
                    (Some(handle), Some(state), false)
                }
                Err(e) => {
                    warn!(
                        "Failed to acquire lease for export '{}': {}. Starting as readonly.",
                        name, e
                    );
                    (None, None, true)
                }
            }
        } else {
            (None, None, true)
        };

        // Create handler with S3 store for read-through caching
        let handler = Arc::new(NBDBlockHandler::new(
            Arc::clone(&cache),
            Arc::clone(&s3_store),
            config.size_bytes(),
            actual_readonly,
            Arc::clone(&metrics),
        ));

        // Start sync worker for this export
        let (sync_shutdown_tx, sync_shutdown_rx) = watch::channel(false);
        let sync_cache = Arc::clone(&cache);
        let sync_s3 = Arc::clone(&s3_store);
        let export_name = name.clone();
        let sync_config = super::write_cache::SyncWorkerConfig {
            hot_batch_cooldown: std::time::Duration::from_millis(self.sync_delay_ms),
            ..Default::default()
        };
        let sync_lease_state = lease_state.clone();
        let sync_handle = spawn_named(&format!("sync-{}", name), async move {
            sync_worker(sync_cache, sync_s3, sync_config, sync_shutdown_rx, sync_lease_state).await;
            info!("Sync worker for export '{}' stopped", export_name);
        });

        // Start lease renewal task if we have a lease
        let lease_renewal_handle = if let Some(ref state) = lease_state {
            let manager = Arc::clone(&self.lease_manager);
            let export_name = name.clone();
            let state_clone = Arc::clone(state);
            let shutdown_rx = sync_shutdown_tx.subscribe();
            Some(spawn_named(&format!("lease-renew-{}", name), async move {
                lease_renewal_task(manager, export_name, state_clone, shutdown_rx).await;
            }))
        } else {
            None
        };

        // Store export state
        let state = ExportState {
            handler,
            cache,
            s3_store,
            readonly: actual_readonly,
            metrics,
            lease,
            lease_state,
            sync_shutdown: sync_shutdown_tx,
            sync_handle,
            lease_renewal_handle,
        };

        let mut exports = self.exports.write().await;
        exports.insert(name.clone(), state);

        info!("Export '{}' created successfully (readonly={})", name, actual_readonly);
        Ok(())
    }

    /// Get handler for an export (used during NBD negotiation).
    pub async fn get_handler(&self, name: &str) -> Option<Arc<NBDBlockHandler>> {
        let exports = self.exports.read().await;
        exports.get(name).map(|s| Arc::clone(&s.handler))
    }

    /// Check if an export is readonly.
    #[allow(dead_code)]
    pub async fn is_readonly(&self, name: &str) -> Option<bool> {
        let exports = self.exports.read().await;
        exports.get(name).map(|s| s.readonly)
    }

    /// List all exports.
    pub async fn list_exports(&self) -> Vec<ExportInfo> {
        let exports = self.exports.read().await;
        exports
            .iter()
            .map(|(name, state)| ExportInfo {
                name: name.clone(),
                size: state.handler.device_size(),
                readonly: state.readonly,
            })
            .collect()
    }

    /// Get export names.
    pub async fn list_export_names(&self) -> Vec<String> {
        let exports = self.exports.read().await;
        exports.keys().cloned().collect()
    }

    /// Get metrics snapshot for an export.
    pub async fn get_export_metrics(&self, name: &str) -> Option<MetricsSnapshot> {
        let exports = self.exports.read().await;
        exports.get(name).map(|s| {
            s.metrics.snapshot().with_cache_state(
                s.cache.dirty_block_count(),
                s.cache.syncing_block_count(),
            )
        })
    }

    /// Drain an export's dirty blocks to S3.
    pub async fn drain_export(&self, name: &str) -> Result<(), RouterError> {
        let exports = self.exports.read().await;
        let state = exports
            .get(name)
            .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;

        info!("Draining export '{}'...", name);
        state.drain().await?;
        info!("Export '{}' drained successfully", name);
        Ok(())
    }

    /// Drain all exports.
    pub async fn drain_all(&self) -> Result<(), RouterError> {
        let names = self.list_export_names().await;
        for name in names {
            if let Err(e) = self.drain_export(&name).await {
                warn!("Failed to drain export '{}': {}", name, e);
            }
        }
        Ok(())
    }

    /// Promote a readonly export to read-write.
    ///
    /// This acquires the distributed lease before promoting, ensuring only one
    /// node can be the writer for this export at a time.
    pub async fn promote_export(&self, name: &str) -> Result<(), RouterError> {
        // First, acquire the lease (before taking the write lock)
        let lease_handle = self.lease_manager.acquire(name).await?;
        info!(
            "Acquired lease for export '{}' (generation {})",
            name, lease_handle.lease.generation
        );

        let mut exports = self.exports.write().await;
        let state = exports
            .get_mut(name)
            .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;

        if !state.readonly {
            info!("Export '{}' is already read-write", name);
            // We acquired a lease but export was already read-write
            // This is fine - we just renewed/took over the lease
            state.lease = Some(lease_handle);
            return Ok(());
        }

        // Update state with the lease and promote to read-write
        state.lease = Some(lease_handle);
        state.readonly = false;
        state.handler.set_readonly(false);
        info!("Export '{}' promoted to read-write", name);
        Ok(())
    }

    /// Remove an export.
    ///
    /// If `purge` is true, also delete the local cache files.
    /// Properly transitions the cache through Draining state.
    ///
    /// **Idempotent**: If export doesn't exist, returns Ok(()) without error.
    pub async fn remove_export(&self, name: &str, purge: bool) -> Result<(), RouterError> {
        let state = {
            let mut exports = self.exports.write().await;
            match exports.remove(name) {
                Some(state) => state,
                None => {
                    info!("Export '{}' doesn't exist, nothing to remove", name);
                    // Still purge local files if requested (idempotent cleanup)
                    if purge {
                        let cache_file = self.cache_dir.join(format!("{}.cache", name));
                        let meta_file = self.cache_dir.join(format!("{}.meta", name));
                        let _ = std::fs::remove_file(&cache_file);
                        let _ = std::fs::remove_file(&meta_file);
                    }
                    return Ok(());
                }
            }
        };

        info!("Removing export '{}'...", name);

        // 1. Signal sync worker and lease renewal to stop FIRST
        // (must stop renewal task before releasing lease to avoid race condition)
        let _ = state.sync_shutdown.send(true);

        // 2. Wait for lease renewal task to exit (if any)
        // This prevents the renewal task from racing with lease release
        if let Some(handle) = state.lease_renewal_handle
            && let Err(e) = handle.await {
                warn!("Lease renewal task for '{}' panicked: {}", name, e);
            }

        // 3. Now release the lease (renewal task is stopped, no race condition)
        if let Some(ref lease_handle) = state.lease
            && let Err(e) = self.lease_manager.release(name, lease_handle).await {
                warn!("Failed to release lease for '{}': {}", name, e);
                // Continue with removal - lease will expire eventually
            }

        // 4. Wait for sync worker to exit (releases its Arc clone)
        if let Err(e) = state.sync_handle.await {
            warn!("Sync worker for '{}' panicked: {}", name, e);
        }

        // 5. Drop the handler (releases its Arc clone)
        drop(state.handler);

        // 6. Unwrap the Arc and transition through Draining state
        match Arc::try_unwrap(state.cache) {
            Ok(cache) => {
                match cache.shutdown(&state.s3_store).await {
                    Ok(draining) => {
                        draining.finish();
                        info!("Export '{}' drained and removed cleanly", name);
                    }
                    Err(e) => {
                        warn!("Failed to drain export '{}': {}", name, e);
                    }
                }
            }
            Err(arc) => {
                // Fallback if someone still holds a reference
                warn!(
                    "Export '{}' has {} references, using fallback drain",
                    name,
                    Arc::strong_count(&arc)
                );
                if let Err(e) = arc.drain_for_snapshot(&state.s3_store).await {
                    warn!("Failed to drain export '{}': {}", name, e);
                }
            }
        }

        if purge {
            let cache_file = self.cache_dir.join(format!("{}.cache", name));
            let meta_file = self.cache_dir.join(format!("{}.meta", name));
            let _ = std::fs::remove_file(&cache_file);
            let _ = std::fs::remove_file(&meta_file);
            info!("Purged cache files for export '{}'", name);
        }

        info!("Export '{}' removed", name);
        Ok(())
    }

    /// Shutdown all exports gracefully.
    ///
    /// This properly transitions each cache through the typestate:
    /// Active → Draining → finished
    pub async fn shutdown(&self) -> Result<(), RouterError> {
        info!("Shutting down all exports...");

        // Take ownership of all exports
        let mut exports = self.exports.write().await;
        let export_list: Vec<_> = exports.drain().collect();
        drop(exports); // Release the lock

        for (name, state) in export_list {
            info!("Shutting down export '{}'...", name);

            // 1. Signal sync worker and lease renewal to stop FIRST
            // (must stop renewal task before releasing lease to avoid race condition)
            let _ = state.sync_shutdown.send(true);

            // 2. Wait for lease renewal task to exit (if any)
            // This prevents the renewal task from racing with lease release
            if let Some(handle) = state.lease_renewal_handle
                && let Err(e) = handle.await {
                    warn!("Lease renewal task for '{}' panicked: {}", name, e);
                }

            // 3. Now release the lease (renewal task is stopped, no race condition)
            if let Some(ref lease_handle) = state.lease
                && let Err(e) = self.lease_manager.release(&name, lease_handle).await {
                    warn!("Failed to release lease for '{}': {}", name, e);
                    // Continue with shutdown - lease will expire eventually
                }

            // 4. Wait for sync worker to exit (releases its Arc clone)
            if let Err(e) = state.sync_handle.await {
                warn!("Sync worker for '{}' panicked: {}", name, e);
            }

            // 5. Drop the handler (releases its Arc clone)
            drop(state.handler);

            // 6. Now we should be the only holder - unwrap the Arc
            match Arc::try_unwrap(state.cache) {
                Ok(cache) => {
                    // 7. Transition Active → Draining via shutdown()
                    match cache.shutdown(&state.s3_store).await {
                        Ok(draining) => {
                            // 8. Finish draining
                            draining.finish();
                            info!("Export '{}' shut down cleanly", name);
                        }
                        Err(e) => {
                            warn!("Failed to drain export '{}': {}", name, e);
                        }
                    }
                }
                Err(arc) => {
                    // Someone still holds a reference - fall back to drain_for_snapshot
                    warn!(
                        "Export '{}' has {} references, using fallback drain",
                        name,
                        Arc::strong_count(&arc)
                    );
                    if let Err(e) = arc.drain_for_snapshot(&state.s3_store).await {
                        warn!("Failed to drain export '{}': {}", name, e);
                    }
                }
            }
        }

        info!("All exports shut down");
        Ok(())
    }
}
