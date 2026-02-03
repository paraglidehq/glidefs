//! Multi-tenant export router for NBD server.
//!
//! Manages multiple NBD exports, each with its own write cache and S3 storage.
//! Supports dynamic export creation/removal for microVM scale-to-zero and live migration.

use crate::block_transformer::ZeroFsBlockTransformer;
use crate::config::ExportConfig;
use crate::nbd::block_store::S3BlockStore;
use crate::nbd::handler::NBDBlockHandler;
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
    #[error("Export '{0}' already exists")]
    ExportExists(String),

    #[error("Export '{0}' not found")]
    ExportNotFound(String),

    #[allow(dead_code)]
    #[error("Export '{0}' is readonly")]
    ExportReadonly(String),

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
    sync_shutdown: watch::Sender<bool>,
    sync_handle: JoinHandle<()>,
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
pub struct ExportRouter {
    /// Active exports: name → state
    exports: RwLock<HashMap<String, ExportState>>,

    /// Shared object store (S3/MinIO/etc)
    object_store: Arc<dyn ObjectStore>,

    /// Base S3 path prefix
    db_path: String,

    /// Local cache directory
    cache_dir: PathBuf,

    /// Block size for all exports
    block_size: usize,

    /// Encryption/compression transformer
    transformer: Option<Arc<ZeroFsBlockTransformer>>,
}

impl ExportRouter {
    /// Create a new export router.
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        db_path: String,
        cache_dir: PathBuf,
        block_size: usize,
        transformer: Option<Arc<ZeroFsBlockTransformer>>,
    ) -> Self {
        Self {
            exports: RwLock::new(HashMap::new()),
            object_store,
            db_path,
            cache_dir,
            block_size,
            transformer,
        }
    }

    /// Create a new export.
    ///
    /// If `readonly` is true, the export will reject writes (used for pre-warming during migration).
    pub async fn create_export(
        &self,
        config: ExportConfig,
        readonly: bool,
    ) -> Result<(), RouterError> {
        let name = config.name.clone();

        // Check if export already exists
        {
            let exports = self.exports.read().await;
            if exports.contains_key(&name) {
                return Err(RouterError::ExportExists(name));
            }
        }

        info!(
            "Creating export '{}': size={}GB, s3_prefix={}, readonly={}",
            name,
            config.size_gb,
            config.s3_prefix(),
            readonly
        );

        // Create S3 block store for this export
        let s3_prefix = format!("{}/nbd/{}", self.db_path, config.s3_prefix());
        let s3_store = Arc::new(S3BlockStore::new(
            Arc::clone(&self.object_store),
            s3_prefix,
            self.block_size,
            self.transformer.clone(),
        ));

        // Create write cache for this export
        let cache_config = WriteCacheConfig {
            cache_dir: self.cache_dir.clone(),
            device_name: name.clone(),
            device_size: config.size_bytes(),
            block_size: self.block_size,
        };

        let cache = WriteCache::open(cache_config, self.transformer.clone())?;

        info!("Recovering write cache for export '{}'...", name);
        let cache = cache.finish_recovery(&s3_store).await?;
        let cache = Arc::new(cache);
        info!("Export '{}' cache ready", name);

        // Create handler with S3 store for read-through caching
        let handler = Arc::new(NBDBlockHandler::new(
            Arc::clone(&cache),
            Arc::clone(&s3_store),
            config.size_bytes(),
            readonly,
        ));

        // Start sync worker for this export
        let (sync_shutdown_tx, sync_shutdown_rx) = watch::channel(false);
        let sync_cache = Arc::clone(&cache);
        let sync_s3 = Arc::clone(&s3_store);
        let export_name = name.clone();
        let sync_handle = spawn_named(&format!("sync-{}", name), async move {
            sync_worker(
                sync_cache,
                sync_s3,
                super::write_cache::SyncWorkerConfig::default(),
                sync_shutdown_rx,
            )
            .await;
            info!("Sync worker for export '{}' stopped", export_name);
        });

        // Store export state
        let state = ExportState {
            handler,
            cache,
            s3_store,
            readonly,
            sync_shutdown: sync_shutdown_tx,
            sync_handle,
        };

        let mut exports = self.exports.write().await;
        exports.insert(name.clone(), state);

        info!("Export '{}' created successfully", name);
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
    pub async fn promote_export(&self, name: &str) -> Result<(), RouterError> {
        let mut exports = self.exports.write().await;
        let state = exports
            .get_mut(name)
            .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?;

        if !state.readonly {
            info!("Export '{}' is already read-write", name);
            return Ok(());
        }

        // Update both the state and the handler's readonly flag
        state.readonly = false;
        state.handler.set_readonly(false);
        info!("Export '{}' promoted to read-write", name);
        Ok(())
    }

    /// Remove an export.
    ///
    /// If `purge` is true, also delete the local cache files.
    /// Properly transitions the cache through Draining state.
    pub async fn remove_export(&self, name: &str, purge: bool) -> Result<(), RouterError> {
        let state = {
            let mut exports = self.exports.write().await;
            exports
                .remove(name)
                .ok_or_else(|| RouterError::ExportNotFound(name.to_string()))?
        };

        info!("Removing export '{}'...", name);

        // 1. Signal sync worker to stop
        let _ = state.sync_shutdown.send(true);

        // 2. Wait for sync worker to exit (releases its Arc clone)
        if let Err(e) = state.sync_handle.await {
            warn!("Sync worker for '{}' panicked: {}", name, e);
        }

        // 3. Drop the handler (releases its Arc clone)
        drop(state.handler);

        // 4. Unwrap the Arc and transition through Draining state
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

            // 1. Signal sync worker to stop
            let _ = state.sync_shutdown.send(true);

            // 2. Wait for sync worker to exit (releases its Arc clone)
            if let Err(e) = state.sync_handle.await {
                warn!("Sync worker for '{}' panicked: {}", name, e);
            }

            // 3. Drop the handler (releases its Arc clone)
            drop(state.handler);

            // 4. Now we should be the only holder - unwrap the Arc
            match Arc::try_unwrap(state.cache) {
                Ok(cache) => {
                    // 5. Transition Active → Draining via shutdown()
                    match cache.shutdown(&state.s3_store).await {
                        Ok(draining) => {
                            // 6. Finish draining
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
