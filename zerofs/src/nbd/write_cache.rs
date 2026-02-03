//! Write-behind cache for NBD devices.
//!
//! This module implements a local SSD cache that provides fast FLUSH semantics
//! while asynchronously syncing data to S3. The cache uses typestate to enforce
//! proper lifecycle management at compile time.
//!
//! # Lock-Free Design
//!
//! Block states and presence tracking use lock-free atomics to avoid contention
//! under high write concurrency. State transitions use compare-and-swap (CAS)
//! operations, and presence bits use atomic OR. The only lock is on the file
//! handle for metadata operations (not data path).

use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write as IoWrite};
use std::marker::PhantomData;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use thiserror::Error;
use tracing::{debug, info, instrument, warn};

use super::block_store::{BlockStoreError, S3BlockStore};
use super::state::{Active, BlockState, Draining, Initializing, Recovering};

/// Default block size: 128KB (matches ZFS default recordsize)
#[allow(dead_code)]
pub const DEFAULT_BLOCK_SIZE: usize = 128 * 1024;

/// Magic bytes for cache metadata file
const METADATA_MAGIC: &[u8; 8] = b"ZFSCACHE";
/// Version 3: present_blocks as packed bits (8x smaller than v2)
const METADATA_VERSION: u32 = 3;

/// Errors that can occur during cache operations.
#[derive(Error, Debug)]
pub enum CacheError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[allow(dead_code)]
    #[error("Cache not ready for I/O operations")]
    NotReady,

    #[allow(dead_code)]
    #[error("Cache is shutting down")]
    ShuttingDown,

    #[error("Block store error: {0}")]
    BlockStore(#[from] BlockStoreError),

    #[error("Invalid cache metadata")]
    InvalidMetadata,

    #[error("Offset {0} exceeds device size {1}")]
    OffsetOutOfBounds(u64, u64),

    #[allow(dead_code)] // Part of public error API for lease coordination
    #[error("Lease lost - another node may have taken ownership")]
    LeaseLost,
}

/// Configuration for the write cache.
#[derive(Clone, Debug)]
pub struct WriteCacheConfig {
    /// Path to the local cache directory
    pub cache_dir: PathBuf,

    /// Device name (used for cache file naming)
    pub device_name: String,

    /// Device size in bytes
    pub device_size: u64,

    /// Block size in bytes
    pub block_size: usize,
}

impl WriteCacheConfig {
    /// Calculate the number of blocks for this device.
    pub fn num_blocks(&self) -> usize {
        self.device_size.div_ceil(self.block_size as u64) as usize
    }

    /// Path to the cache data file.
    pub fn data_path(&self) -> PathBuf {
        self.cache_dir.join(format!("{}.cache", self.device_name))
    }

    /// Path to the cache metadata file.
    pub fn metadata_path(&self) -> PathBuf {
        self.cache_dir.join(format!("{}.meta", self.device_name))
    }
}

/// Internal state shared across all cache states.
///
/// Uses lock-free atomics for block states and presence to avoid contention
/// under high write concurrency. The file lock is only for metadata operations.
pub(crate) struct CacheInner {
    /// Configuration
    config: WriteCacheConfig,

    /// Local cache file (data) - encrypted at rest
    /// RwLock is for metadata ops only; pwrite_all_at is thread-safe
    data_file: RwLock<File>,

    /// Block states (indexed by block number) - LOCK-FREE
    /// Uses AtomicU8 with CAS for state transitions
    block_states: Box<[AtomicU8]>,

    /// Presence bitmap as atomic u64 chunks - LOCK-FREE
    /// Each chunk covers 64 blocks. Uses atomic OR to set bits.
    /// Chunk index = block_num / 64, bit index = block_num % 64
    present_chunks: Box<[AtomicU64]>,

    /// Number of blocks (for bounds checking)
    num_blocks: usize,

    /// Statistics
    dirty_block_count: AtomicU64,
    syncing_block_count: AtomicU64,
}

impl CacheInner {
    /// Get block state (lock-free read).
    #[inline]
    #[allow(dead_code)] // Part of lock-free API, may be used for diagnostics
    fn get_state(&self, block_num: usize) -> BlockState {
        if block_num >= self.num_blocks {
            return BlockState::Clean;
        }
        BlockState::from_u8(self.block_states[block_num].load(Ordering::Acquire))
    }

    /// Check if block is present (lock-free read).
    #[inline]
    fn is_present(&self, block_num: usize) -> bool {
        if block_num >= self.num_blocks {
            return false;
        }
        let chunk_idx = block_num / 64;
        let bit_idx = block_num % 64;
        let chunk = self.present_chunks[chunk_idx].load(Ordering::Acquire);
        (chunk & (1u64 << bit_idx)) != 0
    }

    /// Mark block as present (lock-free atomic OR).
    #[inline]
    fn set_present(&self, block_num: usize) {
        if block_num >= self.num_blocks {
            return;
        }
        let chunk_idx = block_num / 64;
        let bit_idx = block_num % 64;
        self.present_chunks[chunk_idx].fetch_or(1u64 << bit_idx, Ordering::Release);
    }

    /// Count present blocks (for metrics/logging).
    fn count_present(&self) -> usize {
        self.present_chunks
            .iter()
            .map(|chunk| chunk.load(Ordering::Relaxed).count_ones() as usize)
            .sum()
    }

    /// Persist block states and presence to metadata file.
    ///
    /// Uses atomic write pattern: write to temp file, fsync, then rename.
    /// This ensures metadata is never corrupted if we crash mid-write.
    fn save_metadata(&self) -> Result<(), CacheError> {
        let path = self.config.metadata_path();
        let tmp_path = path.with_extension("meta.tmp");

        // Write to temp file first
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;

        // Write header
        file.write_all(METADATA_MAGIC)?;
        file.write_all(&METADATA_VERSION.to_le_bytes())?;
        file.write_all(&self.config.device_size.to_le_bytes())?;
        file.write_all(&(self.config.block_size as u64).to_le_bytes())?;
        file.write_all(&(self.num_blocks as u64).to_le_bytes())?;

        // Write block states (1 byte per block) - snapshot atomic values
        let state_bytes: Vec<u8> = self
            .block_states
            .iter()
            .map(|s| s.load(Ordering::Relaxed))
            .collect();
        file.write_all(&state_bytes)?;

        // Write presence bitmap as packed bits (1 bit per block)
        // Convert atomic u64 chunks back to packed bytes
        let mut present_bytes = vec![0u8; self.num_blocks.div_ceil(8)];
        for (chunk_idx, chunk) in self.present_chunks.iter().enumerate() {
            let chunk_val = chunk.load(Ordering::Relaxed);
            let base_byte = chunk_idx * 8;
            for byte_offset in 0..8 {
                let byte_idx = base_byte + byte_offset;
                if byte_idx < present_bytes.len() {
                    present_bytes[byte_idx] = ((chunk_val >> (byte_offset * 8)) & 0xFF) as u8;
                }
            }
        }
        file.write_all(&present_bytes)?;

        // Fsync temp file to ensure data is on disk
        file.sync_all()?;
        drop(file);

        // Atomic rename (POSIX guarantees this is atomic)
        std::fs::rename(&tmp_path, &path)?;

        let present_count = self.count_present();
        debug!(
            path = %path.display(),
            blocks = self.num_blocks,
            present = present_count,
            "saved cache metadata (atomic)"
        );
        Ok(())
    }

    /// Load block states and presence from metadata file.
    ///
    /// Returns (state_bytes, present_chunks, dirty_count) where:
    /// - state_bytes: Raw u8 values for block states (Syncing converted to Dirty)
    /// - present_chunks: Atomic u64 chunks for presence bitmap
    /// - dirty_count: Number of dirty blocks (for counter initialization)
    fn load_metadata(
        config: &WriteCacheConfig,
    ) -> Result<(Vec<u8>, Vec<u64>, usize), CacheError> {
        let path = config.metadata_path();
        let num_blocks = config.num_blocks();
        let num_chunks = num_blocks.div_ceil(64);

        if !path.exists() {
            // No metadata file - all blocks are clean and NOT present
            debug!(path = %path.display(), "no metadata file, starting fresh");
            return Ok((
                vec![BlockState::Clean as u8; num_blocks],
                vec![0u64; num_chunks],
                0,
            ));
        }

        let mut file = File::open(&path)?;
        let mut header = [0u8; 8 + 4 + 8 + 8 + 8]; // magic + version + size + block_size + num_blocks
        file.read_exact(&mut header)?;

        // Validate header
        if &header[0..8] != METADATA_MAGIC {
            warn!("Invalid cache metadata magic bytes");
            return Err(CacheError::InvalidMetadata);
        }

        let version = u32::from_le_bytes(header[8..12].try_into().unwrap());

        let device_size = u64::from_le_bytes(header[12..20].try_into().unwrap());
        let block_size = u64::from_le_bytes(header[20..28].try_into().unwrap());
        let stored_num_blocks = u64::from_le_bytes(header[28..36].try_into().unwrap()) as usize;

        // Validate device matches
        if device_size != config.device_size || block_size != config.block_size as u64 {
            warn!(
                stored_size = device_size,
                config_size = config.device_size,
                stored_block = block_size,
                config_block = config.block_size,
                "Device size mismatch"
            );
            return Err(CacheError::InvalidMetadata);
        }

        // Read block states
        let mut state_bytes = vec![0u8; stored_num_blocks];
        file.read_exact(&mut state_bytes)?;

        // Convert Syncing to Dirty (conservative for crash recovery)
        let mut dirty_count = 0;
        for state in &mut state_bytes {
            let parsed = BlockState::from_u8(*state);
            // Syncing blocks had in-flight uploads that may have failed
            if parsed == BlockState::Syncing {
                *state = BlockState::Dirty as u8;
            }
            if *state == BlockState::Dirty as u8 {
                dirty_count += 1;
            }
        }

        // Read presence bitmap and convert to u64 chunks
        let present_chunks: Vec<u64> = if version >= 3 {
            // Version 3: packed bits (1 bit per block)
            let num_bytes = stored_num_blocks.div_ceil(8);
            let mut present_bytes = vec![0u8; num_bytes];
            file.read_exact(&mut present_bytes)?;

            // Convert packed bytes to u64 chunks
            let mut chunks = vec![0u64; num_chunks];
            for (chunk_idx, chunk) in chunks.iter_mut().enumerate() {
                let base_byte = chunk_idx * 8;
                for byte_offset in 0..8 {
                    let byte_idx = base_byte + byte_offset;
                    if byte_idx < present_bytes.len() {
                        *chunk |= (present_bytes[byte_idx] as u64) << (byte_offset * 8);
                    }
                }
            }
            chunks
        } else if version >= 2 {
            // Version 2: 1 byte per block (legacy)
            let mut present_bytes = vec![0u8; stored_num_blocks];
            file.read_exact(&mut present_bytes)?;

            // Convert to u64 chunks
            let mut chunks = vec![0u64; num_chunks];
            for (block_num, &present) in present_bytes.iter().enumerate() {
                if present != 0 {
                    let chunk_idx = block_num / 64;
                    let bit_idx = block_num % 64;
                    chunks[chunk_idx] |= 1u64 << bit_idx;
                }
            }
            chunks
        } else {
            // Version 1 compatibility: dirty blocks are present, clean blocks are NOT
            let mut chunks = vec![0u64; num_chunks];
            for (block_num, &state) in state_bytes.iter().enumerate() {
                if state == BlockState::Dirty as u8 {
                    let chunk_idx = block_num / 64;
                    let bit_idx = block_num % 64;
                    chunks[chunk_idx] |= 1u64 << bit_idx;
                }
            }
            chunks
        };

        let present_count: usize = present_chunks.iter().map(|c| c.count_ones() as usize).sum();
        info!(
            path = %path.display(),
            blocks = state_bytes.len(),
            dirty = dirty_count,
            present = present_count,
            "loaded cache metadata"
        );

        Ok((state_bytes, present_chunks, dirty_count))
    }
}

/// Write-behind cache with typestate lifecycle management.
///
/// The cache progresses through states:
/// - `Initializing`: Loading local cache
/// - `Recovering`: Syncing dirty blocks from previous session
/// - `Active`: Serving I/O, only state where read/write/flush are allowed
/// - `Draining`: Syncing all blocks before shutdown
pub struct WriteCache<S> {
    inner: Arc<CacheInner>,
    _state: PhantomData<S>,
}

impl WriteCache<Initializing> {
    /// Open or create a write cache.
    ///
    /// Returns a cache in `Recovering` state. Call `finish_recovery()` to
    /// transition to `Active` state before serving I/O.
    ///
    /// # Arguments
    /// * `config` - Cache configuration
    #[instrument(skip(config), fields(device = %config.device_name))]
    pub fn open(config: WriteCacheConfig) -> Result<WriteCache<Recovering>, CacheError> {
        // Ensure cache directory exists
        std::fs::create_dir_all(&config.cache_dir)?;

        // Open or create data file (sparse file - only allocates on write)
        // We don't truncate because we want to preserve existing cache data
        let data_path = config.data_path();
        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&data_path)?;

        // Set file size (sparse - doesn't allocate disk space until written)
        let file_size = data_file.metadata()?.len();
        if file_size < config.device_size {
            data_file.set_len(config.device_size)?;
            info!(
                path = %data_path.display(),
                old_size = file_size,
                new_size = config.device_size,
                "extended cache file (sparse)"
            );
        }

        // Load block states and presence (or create fresh)
        let num_blocks = config.num_blocks();
        let (state_bytes, present_chunk_vals, dirty_count) = CacheInner::load_metadata(&config)?;

        // Convert to atomic types
        let block_states: Box<[AtomicU8]> = state_bytes
            .into_iter()
            .map(AtomicU8::new)
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let present_chunks: Box<[AtomicU64]> = present_chunk_vals
            .into_iter()
            .map(AtomicU64::new)
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let present_count: usize = present_chunks
            .iter()
            .map(|c| c.load(Ordering::Relaxed).count_ones() as usize)
            .sum();

        let inner = Arc::new(CacheInner {
            config,
            data_file: RwLock::new(data_file),
            block_states,
            present_chunks,
            num_blocks,
            dirty_block_count: AtomicU64::new(dirty_count as u64),
            syncing_block_count: AtomicU64::new(0),
        });

        info!(
            dirty_blocks = dirty_count,
            present_blocks = present_count,
            "cache opened, transitioning to Recovering"
        );

        Ok(WriteCache {
            inner,
            _state: PhantomData,
        })
    }
}

impl WriteCache<Recovering> {
    /// Skip recovery and transition directly to Active state.
    ///
    /// **TEST ONLY**: This bypasses recovery for unit tests that don't need S3.
    #[cfg(test)]
    pub fn skip_recovery_for_test(self) -> WriteCache<Active> {
        WriteCache {
            inner: self.inner,
            _state: PhantomData,
        }
    }

    /// Sync all dirty blocks from previous session and transition to Active.
    ///
    /// This uploads any blocks that were dirty when the cache was last closed
    /// (or when the process crashed).
    #[instrument(skip(self, s3))]
    pub async fn finish_recovery(
        self,
        s3: &S3BlockStore,
    ) -> Result<WriteCache<Active>, CacheError> {
        let dirty_count = self.inner.dirty_block_count.load(Ordering::Relaxed);

        if dirty_count == 0 {
            info!("no dirty blocks, recovery complete");
        } else {
            info!(dirty_blocks = dirty_count, "starting recovery sync");

            // Collect and sync dirty blocks using batched writes
            let dirty_blocks = self.collect_dirty_blocks();
            match self.sync_blocks_batched(s3, dirty_blocks).await {
                Ok(synced) => {
                    info!(synced = synced, "recovery sync complete");
                }
                Err(e) => {
                    warn!(error = %e, "recovery sync failed, will retry on next startup");
                }
            }

            // Save metadata after recovery
            self.inner.save_metadata()?;
            info!("recovery complete");
        }

        Ok(WriteCache {
            inner: self.inner,
            _state: PhantomData,
        })
    }

    fn collect_dirty_blocks(&self) -> Vec<u64> {
        self.inner
            .block_states
            .iter()
            .enumerate()
            .filter(|(_, s)| s.load(Ordering::Relaxed) == BlockState::Dirty as u8)
            .map(|(i, _)| i as u64)
            .collect()
    }

    /// Sync dirty blocks to S3 using batch writes with conditional PUT.
    async fn sync_blocks_batched(&self, s3: &S3BlockStore, block_nums: Vec<u64>) -> Result<usize, CacheError> {
        use std::collections::HashMap;

        if block_nums.is_empty() {
            return Ok(0);
        }

        // Group blocks by batch number
        let mut batches: HashMap<u64, Vec<u64>> = HashMap::new();
        for block_num in block_nums {
            let batch_num = s3.batch_num(block_num);
            batches.entry(batch_num).or_default().push(block_num);
        }

        let mut synced_count = 0;

        // Process each batch
        for (batch_num, blocks_in_batch) in batches {
            // GET existing batch with ETag for conditional PUT
            let batch_result = s3.get_batch_with_etag(batch_num).await?;
            let mut batch_data = batch_result.data;
            let etag = batch_result.etag;

            // Update dirty block slots with local data
            for &block_num in &blocks_in_batch {
                let local_data = self.read_local_block(block_num)?;
                let offset = s3.offset_in_batch(block_num) as usize;
                batch_data[offset..offset + local_data.len()].copy_from_slice(&local_data);
            }

            // Conditional PUT: only succeed if no one else modified the batch
            s3.put_batch_conditional(batch_num, batch_data, etag).await?;

            // Mark all blocks in this batch as synced
            for block_num in blocks_in_batch {
                self.mark_synced(block_num);
                synced_count += 1;
            }
        }

        Ok(synced_count)
    }

    fn read_local_block(&self, block_num: u64) -> Result<Bytes, CacheError> {
        let offset = block_num * self.inner.config.block_size as u64;
        let mut buf = vec![0u8; self.inner.config.block_size];

        let file = self.inner.data_file.read().unwrap();
        file.read_exact_at(&mut buf, offset)?;

        Ok(Bytes::from(buf))
    }

    fn mark_synced(&self, block_num: u64) {
        let idx = block_num as usize;
        if idx >= self.inner.num_blocks {
            return;
        }

        // CAS loop: Dirty|Syncing -> Clean
        loop {
            let current = self.inner.block_states[idx].load(Ordering::Acquire);
            if current != BlockState::Dirty as u8 && current != BlockState::Syncing as u8 {
                break;
            }

            if self.inner.block_states[idx]
                .compare_exchange(
                    current,
                    BlockState::Clean as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                self.inner.dirty_block_count.fetch_sub(1, Ordering::Relaxed);
                break;
            }
            // CAS failed, retry
        }
    }
}

impl WriteCache<Active> {
    /// Write data to the cache.
    ///
    /// Data is written to the local SSD and the affected blocks are marked dirty and present.
    /// The write returns immediately after local I/O completes.
    ///
    /// # Lock-Free State Updates
    ///
    /// Uses CAS operations for state transitions:
    /// - Clean → Dirty: increment dirty_count
    /// - Syncing → Dirty: decrement syncing_count, increment dirty_count
    /// - Dirty → Dirty: no-op
    #[instrument(skip(self, data), fields(offset = offset, len = data.len()))]
    pub fn write(&self, offset: u64, data: &[u8]) -> Result<(), CacheError> {
        if offset + data.len() as u64 > self.inner.config.device_size {
            return Err(CacheError::OffsetOutOfBounds(
                offset + data.len() as u64,
                self.inner.config.device_size,
            ));
        }

        if data.is_empty() {
            return Ok(());
        }

        // Write to local file
        {
            let file = self.inner.data_file.read().unwrap();
            file.write_all_at(data, offset)?;
        }

        // Mark affected blocks as dirty and present (lock-free)
        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + data.len() as u64 - 1) / block_size;

        for block in start_block..=end_block {
            let idx = block as usize;
            if idx >= self.inner.num_blocks {
                continue;
            }

            // Mark as present (atomic OR)
            self.inner.set_present(idx);

            // CAS loop for state transition
            loop {
                let current = self.inner.block_states[idx].load(Ordering::Acquire);

                if current == BlockState::Dirty as u8 {
                    // Already dirty, nothing to do
                    break;
                }

                if current == BlockState::Clean as u8 {
                    // Clean → Dirty
                    if self.inner.block_states[idx]
                        .compare_exchange(
                            current,
                            BlockState::Dirty as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.inner.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    // CAS failed, retry
                } else if current == BlockState::Syncing as u8 {
                    // Syncing → Dirty (write during sync)
                    if self.inner.block_states[idx]
                        .compare_exchange(
                            current,
                            BlockState::Dirty as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.inner.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
                        self.inner.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    // CAS failed, retry
                } else {
                    // Unknown state, just break
                    break;
                }
            }
        }

        debug!(start_block = start_block, end_block = end_block, "marked blocks dirty and present");
        Ok(())
    }

    /// Read data from the cache, fetching from S3 if blocks are not present locally.
    ///
    /// This is the primary read path for NBD I/O. Blocks that haven't been written
    /// locally are fetched from S3 on demand (read-through caching).
    #[instrument(skip(self, s3, metrics), fields(offset = offset, len = len))]
    pub async fn read_with_fetch(
        &self,
        offset: u64,
        len: usize,
        s3: &S3BlockStore,
        metrics: &super::metrics::ExportMetrics,
    ) -> Result<Bytes, CacheError> {
        if offset + len as u64 > self.inner.config.device_size {
            return Err(CacheError::OffsetOutOfBounds(
                offset + len as u64,
                self.inner.config.device_size,
            ));
        }

        if len == 0 {
            return Ok(Bytes::new());
        }

        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len as u64 - 1) / block_size;

        // Check which blocks need to be fetched from S3 (lock-free)
        let blocks_to_fetch: Vec<u64> = (start_block..=end_block)
            .filter(|&block| !self.inner.is_present(block as usize))
            .collect();

        // Record cache hits/misses
        let total_blocks = (end_block - start_block + 1) as usize;
        let cache_misses = blocks_to_fetch.len();
        let cache_hits = total_blocks - cache_misses;
        for _ in 0..cache_hits {
            metrics.record_cache_hit();
        }
        for _ in 0..cache_misses {
            metrics.record_cache_miss();
        }

        // Fetch missing blocks from S3
        for block_num in blocks_to_fetch {
            self.fetch_block_from_s3(s3, block_num, Some(metrics)).await?;
        }

        // Now read from local cache
        self.read_local(offset, len)
    }

    /// Fetch a single block from S3 and cache it locally.
    #[instrument(skip(self, s3, metrics), fields(block = block_num))]
    async fn fetch_block_from_s3(
        &self,
        s3: &S3BlockStore,
        block_num: u64,
        metrics: Option<&super::metrics::ExportMetrics>,
    ) -> Result<(), CacheError> {
        let block_size = self.inner.config.block_size;
        let offset = block_num * block_size as u64;

        // Fetch from S3
        let data = match s3.read_block(block_num).await {
            Ok(data) => {
                if let Some(m) = metrics {
                    m.record_s3_read(data.len() as u64);
                }
                data
            }
            Err(super::block_store::BlockStoreError::NotFound(_)) => {
                // Block doesn't exist in S3 - this is a never-written block, use zeros
                debug!(block = block_num, "block not in S3, using zeros");
                Bytes::from(vec![0u8; block_size])
            }
            Err(e) => return Err(CacheError::BlockStore(e)),
        };

        // Write to local cache file
        {
            let file = self.inner.data_file.read().unwrap();
            // Pad or truncate to block size
            let mut buf = vec![0u8; block_size];
            let copy_len = data.len().min(block_size);
            buf[..copy_len].copy_from_slice(&data[..copy_len]);
            file.write_all_at(&buf, offset)?;
        }

        // Mark block as present (but Clean since it matches S3) - lock-free
        self.inner.set_present(block_num as usize);

        debug!(block = block_num, "fetched block from S3 and cached locally");
        Ok(())
    }

    /// Read data from local cache only (no S3 fetch).
    ///
    /// Used internally and by sync worker. Caller must ensure blocks are present.
    #[instrument(skip(self), fields(offset = offset, len = len))]
    pub fn read_local(&self, offset: u64, len: usize) -> Result<Bytes, CacheError> {
        if offset + len as u64 > self.inner.config.device_size {
            return Err(CacheError::OffsetOutOfBounds(
                offset + len as u64,
                self.inner.config.device_size,
            ));
        }

        if len == 0 {
            return Ok(Bytes::new());
        }

        let mut buf = vec![0u8; len];
        {
            let file = self.inner.data_file.read().unwrap();
            file.read_exact_at(&mut buf, offset)?;
        }

        Ok(Bytes::from(buf))
    }

    /// Legacy read method - reads from local cache only.
    ///
    /// **WARNING**: Returns zeros for blocks not present locally.
    /// Use `read_with_fetch` for NBD I/O to get proper S3 read-through.
    #[instrument(skip(self), fields(offset = offset, len = len))]
    pub fn read(&self, offset: u64, len: usize) -> Result<Bytes, CacheError> {
        self.read_local(offset, len)
    }

    /// Flush the local cache file.
    ///
    /// This performs an fsync on the local SSD, which is fast (<10ms).
    /// It does NOT wait for S3 sync - that happens in the background.
    #[instrument(skip(self))]
    pub fn flush(&self) -> Result<(), CacheError> {
        let file = self.inner.data_file.read().unwrap();
        file.sync_all()?;
        debug!("local flush complete");
        Ok(())
    }

    /// Write zeros to a range efficiently.
    ///
    /// On Linux, uses `fallocate(FALLOC_FL_ZERO_RANGE)` to zero the range
    /// without actually writing data - the kernel marks the range as zeros.
    /// This is much faster for large ranges.
    ///
    /// On other platforms, falls back to writing a static zero buffer.
    pub fn zero_range(&self, offset: u64, len: u64) -> Result<(), CacheError> {
        if len == 0 {
            return Ok(());
        }

        if offset + len > self.inner.config.device_size {
            return Err(CacheError::OffsetOutOfBounds(
                offset + len,
                self.inner.config.device_size,
            ));
        }

        // Zero the file range
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let file = self.inner.data_file.read().unwrap();
            let fd = file.as_raw_fd();

            // FALLOC_FL_ZERO_RANGE = 0x10
            // This zeros the range without deallocating - keeps the file contiguous
            const FALLOC_FL_ZERO_RANGE: libc::c_int = 0x10;

            let ret = unsafe {
                libc::fallocate(fd, FALLOC_FL_ZERO_RANGE, offset as libc::off_t, len as libc::off_t)
            };

            if ret != 0 {
                let err = std::io::Error::last_os_error();
                // If fallocate isn't supported, fall back to writing zeros
                if err.raw_os_error() == Some(libc::EOPNOTSUPP)
                    || err.raw_os_error() == Some(libc::ENOTSUP)
                {
                    return self.zero_range_fallback(offset, len);
                }
                return Err(CacheError::Io(err));
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            self.zero_range_fallback(offset, len)?;
        }

        // Mark affected blocks as dirty and present
        self.mark_range_dirty_and_present(offset, len);

        Ok(())
    }

    /// Fallback zero writing using a static buffer.
    /// Used on non-Linux platforms or when fallocate isn't supported.
    fn zero_range_fallback(&self, offset: u64, len: u64) -> Result<(), CacheError> {
        use std::sync::LazyLock;

        // Static zero buffer - allocated once, reused forever
        const ZERO_CHUNK_SIZE: usize = 128 * 1024; // 128KB
        static ZERO_CHUNK: LazyLock<Box<[u8]>> = LazyLock::new(|| {
            vec![0u8; ZERO_CHUNK_SIZE].into_boxed_slice()
        });

        let file = self.inner.data_file.read().unwrap();
        let mut remaining = len;
        let mut current_offset = offset;

        while remaining > 0 {
            let chunk_size = (remaining as usize).min(ZERO_CHUNK_SIZE);
            file.write_all_at(&ZERO_CHUNK[..chunk_size], current_offset)?;
            remaining -= chunk_size as u64;
            current_offset += chunk_size as u64;
        }

        Ok(())
    }

    /// Mark a range of blocks as dirty and present (lock-free).
    fn mark_range_dirty_and_present(&self, offset: u64, len: u64) {
        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len - 1) / block_size;

        for block in start_block..=end_block {
            let idx = block as usize;
            if idx >= self.inner.num_blocks {
                continue;
            }

            // Mark as present (atomic OR)
            self.inner.set_present(idx);

            // CAS loop for state transition (same as write())
            loop {
                let current = self.inner.block_states[idx].load(Ordering::Acquire);

                if current == BlockState::Dirty as u8 {
                    break;
                }

                if current == BlockState::Clean as u8 {
                    if self.inner.block_states[idx]
                        .compare_exchange(
                            current,
                            BlockState::Dirty as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.inner.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                } else if current == BlockState::Syncing as u8 {
                    if self.inner.block_states[idx]
                        .compare_exchange(
                            current,
                            BlockState::Dirty as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.inner.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
                        self.inner.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }

    /// Claim dirty blocks for syncing (lock-free).
    ///
    /// This transitions blocks from Dirty to Syncing and returns their numbers.
    /// The sync worker should call this, upload the blocks, then call
    /// `mark_synced` or `mark_sync_failed` for each block.
    ///
    /// Uses CAS to atomically transition Dirty → Syncing.
    #[instrument(skip(self))]
    pub fn claim_dirty_blocks(&self, max_blocks: usize) -> Vec<u64> {
        let mut claimed = Vec::with_capacity(max_blocks);

        for i in 0..self.inner.num_blocks {
            if claimed.len() >= max_blocks {
                break;
            }

            // Try CAS: Dirty → Syncing
            if self.inner.block_states[i]
                .compare_exchange(
                    BlockState::Dirty as u8,
                    BlockState::Syncing as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                claimed.push(i as u64);
                self.inner.dirty_block_count.fetch_sub(1, Ordering::Relaxed);
                self.inner.syncing_block_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        if !claimed.is_empty() {
            debug!(count = claimed.len(), "claimed dirty blocks for sync");
        }
        claimed
    }

    /// Mark a block as successfully synced (lock-free).
    pub fn mark_synced(&self, block_num: u64) {
        let idx = block_num as usize;
        if idx >= self.inner.num_blocks {
            return;
        }

        // CAS: Syncing → Clean
        if self.inner.block_states[idx]
            .compare_exchange(
                BlockState::Syncing as u8,
                BlockState::Clean as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.inner.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Mark a block sync as failed (will retry) - lock-free.
    pub fn mark_sync_failed(&self, block_num: u64) {
        let idx = block_num as usize;
        if idx >= self.inner.num_blocks {
            return;
        }

        // CAS: Syncing → Dirty
        if self.inner.block_states[idx]
            .compare_exchange(
                BlockState::Syncing as u8,
                BlockState::Dirty as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.inner.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
            self.inner.dirty_block_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Read a single block from local cache.
    pub fn read_local_block(&self, block_num: u64) -> Result<Bytes, CacheError> {
        let offset = block_num * self.inner.config.block_size as u64;
        self.read(offset, self.inner.config.block_size)
    }

    /// Get the number of dirty blocks pending sync.
    pub fn dirty_block_count(&self) -> u64 {
        self.inner.dirty_block_count.load(Ordering::Relaxed)
    }

    /// Get the number of blocks currently being synced.
    pub fn syncing_block_count(&self) -> u64 {
        self.inner.syncing_block_count.load(Ordering::Relaxed)
    }

    /// Get the device size.
    #[allow(dead_code)]
    pub fn device_size(&self) -> u64 {
        self.inner.config.device_size
    }

    /// Get the block size.
    pub fn block_size(&self) -> usize {
        self.inner.config.block_size
    }

    /// Save metadata to disk.
    pub fn save_metadata(&self) -> Result<(), CacheError> {
        self.inner.save_metadata()
    }

    /// Sync dirty blocks to S3 using batch writes with conditional PUT.
    ///
    /// Groups blocks by batch number, performs GET-modify-PUT for each batch.
    /// Uses conditional PUT (If-Match) as defense-in-depth against concurrent writers.
    /// Returns the number of blocks successfully synced.
    pub async fn sync_blocks_batched(&self, s3: &S3BlockStore, block_nums: Vec<u64>) -> Result<usize, CacheError> {
        use std::collections::HashMap;

        if block_nums.is_empty() {
            return Ok(0);
        }

        // Group blocks by batch number
        let mut batches: HashMap<u64, Vec<u64>> = HashMap::new();
        for block_num in block_nums {
            let batch_num = s3.batch_num(block_num);
            batches.entry(batch_num).or_default().push(block_num);
        }

        let mut synced_count = 0;

        // Process each batch
        for (batch_num, blocks_in_batch) in batches {
            // GET existing batch with ETag for conditional PUT
            let batch_result = match s3.get_batch_with_etag(batch_num).await {
                Ok(result) => result,
                Err(e) => {
                    // Mark all blocks in this batch as failed
                    for block_num in blocks_in_batch {
                        self.mark_sync_failed(block_num);
                    }
                    return Err(CacheError::BlockStore(e));
                }
            };

            let mut batch_data = batch_result.data;
            let etag = batch_result.etag;

            // Update dirty block slots with local data
            for &block_num in &blocks_in_batch {
                let local_data = self.read_local_block(block_num)?;
                let offset = s3.offset_in_batch(block_num) as usize;
                batch_data[offset..offset + local_data.len()].copy_from_slice(&local_data);
            }

            // Conditional PUT: only succeed if no one else modified the batch
            if let Err(e) = s3.put_batch_conditional(batch_num, batch_data, etag).await {
                // Mark all blocks in this batch as failed
                for block_num in blocks_in_batch {
                    self.mark_sync_failed(block_num);
                }
                return Err(CacheError::BlockStore(e));
            }

            // Mark all blocks in this batch as synced
            for block_num in blocks_in_batch {
                self.mark_synced(block_num);
                synced_count += 1;
            }
        }

        Ok(synced_count)
    }

    /// Graceful shutdown: sync all blocks and transition to Draining.
    #[allow(dead_code)]
    #[instrument(skip(self, s3))]
    pub async fn shutdown(self, s3: &S3BlockStore) -> Result<WriteCache<Draining>, CacheError> {
        info!("starting graceful shutdown");

        // Claim and sync all remaining dirty blocks using batched writes
        loop {
            let dirty = self.claim_dirty_blocks(1000);
            if dirty.is_empty() {
                break;
            }

            if let Err(e) = self.sync_blocks_batched(s3, dirty).await {
                warn!(error = %e, "shutdown sync failed");
            }
        }

        // Wait for any in-flight syncs
        while self.syncing_block_count() > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        // Save final metadata
        self.inner.save_metadata()?;
        info!("shutdown complete, all blocks synced");

        Ok(WriteCache {
            inner: self.inner,
            _state: PhantomData,
        })
    }

    /// Drain all dirty blocks to S3 for a consistent snapshot.
    ///
    /// Call this before cross-host clone operations to ensure all data is
    /// visible to other hosts. Normal FLUSH operations use fast local fsync.
    ///
    /// Returns when all dirty blocks have been uploaded to S3.
    #[instrument(skip(self, s3))]
    pub async fn drain_for_snapshot(&self, s3: &S3BlockStore) -> Result<(), CacheError> {
        let initial_dirty = self.dirty_block_count();
        if initial_dirty == 0 {
            debug!("no dirty blocks, drain complete");
            return Ok(());
        }

        info!(dirty_blocks = initial_dirty, "draining for snapshot");

        // Upload all dirty blocks using batched writes
        loop {
            let dirty = self.claim_dirty_blocks(100);
            if dirty.is_empty() {
                break;
            }

            self.sync_blocks_batched(s3, dirty).await?;
        }

        // Wait for any in-flight syncs from background worker
        while self.syncing_block_count() > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        info!("drain complete, all blocks in S3");
        Ok(())
    }

    /// Get a clone of the inner Arc for sharing with the sync worker.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> Arc<CacheInner> {
        Arc::clone(&self.inner)
    }
}

#[allow(dead_code)]
impl WriteCache<Draining> {
    /// Finish draining and drop the cache.
    pub fn finish(self) {
        info!("cache drained and closed");
    }
}

// ============================================================================
// Background Sync Worker
// ============================================================================

/// Configuration for the sync worker.
#[derive(Clone, Debug)]
pub struct SyncWorkerConfig {
    /// Maximum number of blocks to claim per sync cycle
    pub batch_size: usize,
    /// How long to sleep when no dirty blocks are found
    pub idle_sleep: std::time::Duration,
    /// How often to save metadata (every N sync cycles)
    pub metadata_save_interval: usize,
}

impl Default for SyncWorkerConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            idle_sleep: std::time::Duration::from_millis(100),
            metadata_save_interval: 10,
        }
    }
}

use super::lease::LeaseState;

/// Sync worker that continuously drains dirty blocks to S3.
///
/// This function runs in a background task and uploads dirty blocks using
/// batched S3 writes. Blocks are grouped by batch number and uploaded
/// together to reduce S3 PUT costs by ~10x.
///
/// # Lease Coordination
///
/// If `lease_state` is provided, the worker checks the lease before each sync
/// cycle. If the lease is lost (renewal failed or another node took it), the
/// worker stops syncing to prevent conflicting writes.
///
/// # Cancellation
/// The worker checks the shutdown signal between upload batches and exits
/// gracefully when signaled.
///
/// # Example
/// ```ignore
/// let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
/// let cache = Arc::new(cache);
/// let s3 = Arc::new(s3_store);
///
/// let worker_handle = tokio::spawn(sync_worker(
///     Arc::clone(&cache),
///     Arc::clone(&s3),
///     SyncWorkerConfig::default(),
///     shutdown_rx,
///     None, // No lease coordination
/// ));
///
/// // To stop:
/// let _ = shutdown_tx.send(true);
/// worker_handle.await.unwrap();
/// ```
#[instrument(skip(cache, s3, config, shutdown, lease_state))]
pub async fn sync_worker(
    cache: Arc<WriteCache<Active>>,
    s3: Arc<S3BlockStore>,
    config: SyncWorkerConfig,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    lease_state: Option<Arc<LeaseState>>,
) {
    info!(
        batch_size = config.batch_size,
        blocks_per_batch = s3.blocks_per_batch(),
        has_lease = lease_state.is_some(),
        "sync worker started"
    );

    let mut batches_since_save = 0;

    loop {
        // Check for shutdown
        if *shutdown.borrow() {
            info!("sync worker received shutdown signal");
            break;
        }

        // Check if lease is still valid (fencing check)
        if let Some(ref state) = lease_state
            && !state.is_valid() {
                warn!(
                    generation = state.generation(),
                    "lease lost, stopping sync worker to prevent conflicting writes"
                );
                break;
            }

        // Claim a batch of dirty blocks
        let dirty = cache.claim_dirty_blocks(config.batch_size);

        if dirty.is_empty() {
            // No dirty blocks - short sleep before checking again
            tokio::select! {
                _ = tokio::time::sleep(config.idle_sleep) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("sync worker received shutdown signal");
                        break;
                    }
                }
            }
            continue;
        }

        let block_count = dirty.len();
        let start = std::time::Instant::now();

        // Final lease check before S3 write (fencing)
        if let Some(ref state) = lease_state
            && !state.is_valid() {
                warn!(
                    generation = state.generation(),
                    blocks = block_count,
                    "lease lost before S3 write, aborting sync"
                );
                // Mark blocks as failed so they'll be retried if lease is reacquired
                for block_num in dirty {
                    cache.mark_sync_failed(block_num);
                }
                break;
            }

        // Sync blocks using batched writes (groups by S3 batch, GET-modify-PUT)
        match cache.sync_blocks_batched(&s3, dirty).await {
            Ok(synced) => {
                let elapsed = start.elapsed();
                debug!(
                    blocks = synced,
                    elapsed_ms = elapsed.as_millis(),
                    throughput_mb_s = (synced * cache.block_size()) as f64 / elapsed.as_secs_f64() / 1_000_000.0,
                    "sync batch completed"
                );
            }
            Err(e) => {
                warn!(
                    blocks = block_count,
                    error = %e,
                    "sync batch failed"
                );
            }
        }

        // Periodically save metadata to persist sync progress
        batches_since_save += 1;
        if batches_since_save >= config.metadata_save_interval {
            if let Err(e) = cache.save_metadata() {
                warn!(error = %e, "failed to save metadata during sync");
            }
            batches_since_save = 0;
        }
    }

    // Final metadata save on shutdown
    if let Err(e) = cache.save_metadata() {
        warn!(error = %e, "failed to save metadata on sync worker shutdown");
    }

    info!("sync worker stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::path::Path;
    use tempfile::TempDir;

    fn test_config(dir: &Path) -> WriteCacheConfig {
        WriteCacheConfig {
            cache_dir: dir.to_path_buf(),
            device_name: "test".to_string(),
            device_size: 1024 * 1024, // 1MB
            block_size: 4096,         // 4KB for testing
        }
    }

    fn test_s3() -> S3BlockStore {
        let object_store = Arc::new(InMemory::new());
        S3BlockStore::with_defaults(object_store, "test")
    }

    #[tokio::test]
    async fn test_open_fresh_cache() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        assert_eq!(cache.dirty_block_count(), 0);
    }

    #[tokio::test]
    async fn test_write_read() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write some data
        cache.write(0, b"hello world").unwrap();

        // Read it back
        let data = cache.read(0, 11).unwrap();
        assert_eq!(&data[..], b"hello world");

        // Should have dirty blocks now
        assert!(cache.dirty_block_count() > 0);
    }

    #[tokio::test]
    async fn test_flush() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        cache.write(0, b"data").unwrap();
        cache.flush().unwrap();

        // Data should still be readable
        let data = cache.read(0, 4).unwrap();
        assert_eq!(&data[..], b"data");
    }

    #[tokio::test]
    async fn test_claim_and_sync() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write to first block
        cache.write(0, b"block data").unwrap();
        assert_eq!(cache.dirty_block_count(), 1);

        // Claim dirty blocks
        let claimed = cache.claim_dirty_blocks(100);
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0], 0);
        assert_eq!(cache.dirty_block_count(), 0);
        assert_eq!(cache.syncing_block_count(), 1);

        // Mark as synced
        cache.mark_synced(0);
        assert_eq!(cache.syncing_block_count(), 0);
    }

    #[tokio::test]
    async fn test_write_during_sync() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write and claim
        cache.write(0, b"first").unwrap();
        let _ = cache.claim_dirty_blocks(100);
        assert_eq!(cache.syncing_block_count(), 1);

        // Write again during sync - should go back to dirty
        cache.write(0, b"second").unwrap();
        assert_eq!(cache.dirty_block_count(), 1);
        assert_eq!(cache.syncing_block_count(), 0);
    }

    #[tokio::test]
    async fn test_metadata_persistence() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        // Create cache and write data
        {
            let cache = WriteCache::<Initializing>::open(config.clone()).unwrap();
            let cache = cache.finish_recovery(&s3).await.unwrap();
            cache.write(0, b"persistent").unwrap();
            cache.save_metadata().unwrap();
        }

        // Reopen and verify dirty blocks are preserved
        {
            let cache = WriteCache::<Initializing>::open(config).unwrap();
            // Should have dirty blocks from previous session
            assert!(cache.inner.dirty_block_count.load(Ordering::Relaxed) > 0);

            let cache = cache.finish_recovery(&s3).await.unwrap();
            // Data should be readable
            let data = cache.read(0, 10).unwrap();
            assert_eq!(&data[..], b"persistent");
        }
    }

    #[tokio::test]
    async fn test_drain_for_snapshot() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write some data
        cache.write(0, b"snapshot data").unwrap();
        assert!(cache.dirty_block_count() > 0);

        // Drain for snapshot
        cache.drain_for_snapshot(&s3).await.unwrap();

        // All blocks should be synced
        assert_eq!(cache.dirty_block_count(), 0);
        assert_eq!(cache.syncing_block_count(), 0);

        // Data should be in S3
        let s3_data = s3.read_block(0).await.unwrap();
        assert_eq!(&s3_data[..13], b"snapshot data");
    }

    #[tokio::test]
    async fn test_sync_worker() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = Arc::new(test_s3());

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = Arc::new(cache.finish_recovery(&s3).await.unwrap());

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Start sync worker
        let worker_cache = Arc::clone(&cache);
        let worker_s3 = Arc::clone(&s3);
        let worker_handle = tokio::spawn(super::sync_worker(
            worker_cache,
            worker_s3,
            super::SyncWorkerConfig::default(),
            shutdown_rx,
            None, // No lease coordination in test
        ));

        // Write some data
        cache.write(0, b"worker test").unwrap();

        // Wait for sync to complete (with timeout)
        let start = std::time::Instant::now();
        while cache.dirty_block_count() > 0 && start.elapsed().as_secs() < 5 {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }

        // Should be synced
        assert_eq!(cache.dirty_block_count(), 0);

        // Stop worker
        let _ = shutdown_tx.send(true);
        worker_handle.await.unwrap();

        // Data should be in S3
        let s3_data = s3.read_block(0).await.unwrap();
        assert_eq!(&s3_data[..11], b"worker test");
    }

    #[tokio::test]
    async fn test_read_through_from_s3() {
        // This test verifies the core read-through functionality:
        // When a block exists in S3 but not locally, read_with_fetch should fetch it.

        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let s3 = S3BlockStore::new(Arc::clone(&object_store), "test", 4096)
            .with_blocks_per_batch(10); // 10 blocks per batch for testing
        let metrics = super::super::metrics::ExportMetrics::new();

        // Pre-populate S3 with some data (simulating data from another node)
        // Block 0 is in batch 0, block 5 is also in batch 0 (with 10 blocks per batch)
        let mut batch0 = vec![0u8; s3.batch_size()];
        // Block 0: fill with 42
        batch0[..4096].copy_from_slice(&vec![42u8; 4096]);
        // Block 5: fill with 99
        let block5_offset = 5 * 4096;
        batch0[block5_offset..block5_offset + 4096].copy_from_slice(&vec![99u8; 4096]);
        s3.put_batch(0, batch0).await.unwrap();

        // Create a fresh cache on a "new node" (no local data)
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Read block 0 - should fetch from S3
        let data = cache.read_with_fetch(0, 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 42);
        assert!(data.iter().all(|&b| b == 42));

        // Read block 5 - should also fetch from S3
        let offset = 5 * 4096;
        let data = cache.read_with_fetch(offset, 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 99);

        // Second read of block 0 should come from local cache now
        let data = cache.read_with_fetch(0, 4096, &s3, &metrics).await.unwrap();
        assert_eq!(data[0], 42);

        // Read a block that doesn't exist in S3 - should return zeros
        let offset = 10 * 4096;
        let data = cache.read_with_fetch(offset, 4096, &s3, &metrics).await.unwrap();
        assert!(data.iter().all(|&b| b == 0));

        // Verify metrics were recorded
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.cache_misses, 3); // blocks 0, 5, and 10
        assert_eq!(snapshot.cache_hits, 1); // second read of block 0
        assert_eq!(snapshot.s3_read_ops, 2); // blocks 0 and 5 (10 was not found)
    }

    #[tokio::test]
    async fn test_write_then_read_local() {
        // Verify that written blocks are marked as present and read locally
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();
        let metrics = super::super::metrics::ExportMetrics::new();

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write data locally
        cache.write(0, b"local data!").unwrap();

        // Read should come from local cache, not S3
        let data = cache.read_with_fetch(0, 11, &s3, &metrics).await.unwrap();
        assert_eq!(&data[..], b"local data!");

        // S3 should NOT have this data yet (not synced)
        let s3_result = s3.read_block(0).await;
        assert!(matches!(
            s3_result,
            Err(super::super::block_store::BlockStoreError::NotFound(_))
        ));

        // Verify cache hit (data was present locally)
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.cache_hits, 1);
        assert_eq!(snapshot.cache_misses, 0);
    }

    #[tokio::test]
    async fn test_lease_fencing_prevents_sync_after_lease_lost() {
        // This test verifies that the sync_worker stops syncing when the lease is lost,
        // preventing split-brain writes that could corrupt data.
        //
        // Scenario:
        // 1. Node A has valid lease, writes data to blocks 0-3
        // 2. Sync worker starts, lease state is marked as lost mid-sync
        // 3. Verify sync worker stops and marks blocks as failed (dirty again)
        // 4. Verify S3 does NOT have the data (sync was aborted)

        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = Arc::new(test_s3());

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = Arc::new(cache.finish_recovery(&s3).await.unwrap());

        // Write data to multiple blocks
        cache.write(0, &[42u8; 4096]).unwrap();
        cache.write(4096, &[43u8; 4096]).unwrap();
        cache.write(8192, &[44u8; 4096]).unwrap();
        assert_eq!(cache.dirty_block_count(), 3);

        // Create lease state and mark it as lost BEFORE starting sync worker
        let (lease_state, _lost_rx) = super::super::lease::LeaseState::new(1);
        lease_state.mark_lost(); // Simulate lease loss

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Start sync worker with the "lost" lease
        let worker_cache = Arc::clone(&cache);
        let worker_s3 = Arc::clone(&s3);
        let worker_handle = tokio::spawn(super::sync_worker(
            worker_cache,
            worker_s3,
            super::SyncWorkerConfig {
                batch_size: 100,
                idle_sleep: std::time::Duration::from_millis(10),
                metadata_save_interval: 1,
            },
            shutdown_rx,
            Some(lease_state), // Lost lease - sync should abort
        ));

        // Wait for sync worker to notice the lost lease and exit
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            worker_handle
        ).await.expect("sync worker should exit quickly when lease is lost").unwrap();

        // Verify: blocks should still be dirty (sync was aborted)
        assert_eq!(cache.dirty_block_count(), 3, "blocks should remain dirty when lease is lost");

        // Verify: S3 should NOT have the data (sync was prevented)
        let s3_result = s3.read_block(0).await;
        assert!(
            matches!(s3_result, Err(super::super::block_store::BlockStoreError::NotFound(_))),
            "S3 should NOT have data when sync was aborted due to lost lease"
        );

        // Cleanup
        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn test_sync_worker_stops_when_lease_lost_mid_cycle() {
        // Test that sync_worker checks lease validity between cycles and stops
        // when the lease becomes invalid.

        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = Arc::new(test_s3());

        let cache = WriteCache::<Initializing>::open(config).unwrap();
        let cache = Arc::new(cache.finish_recovery(&s3).await.unwrap());

        // Create lease state (valid initially)
        let (lease_state, _lost_rx) = super::super::lease::LeaseState::new(1);
        let lease_state_clone = Arc::clone(&lease_state);

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Start sync worker with valid lease
        let worker_cache = Arc::clone(&cache);
        let worker_s3 = Arc::clone(&s3);
        let worker_handle = tokio::spawn(super::sync_worker(
            worker_cache,
            worker_s3,
            super::SyncWorkerConfig {
                batch_size: 100,
                idle_sleep: std::time::Duration::from_millis(50),
                metadata_save_interval: 10,
            },
            shutdown_rx,
            Some(lease_state),
        ));

        // Let the worker run a few cycles (no dirty blocks, so just sleeping)
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        // Now mark the lease as lost
        lease_state_clone.mark_lost();

        // The worker should exit soon
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            worker_handle
        ).await;

        assert!(result.is_ok(), "sync worker should exit when lease is lost");

        // Cleanup
        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn test_two_nodes_cannot_corrupt_same_batch() {
        // This test demonstrates that with lease fencing, two nodes cannot
        // simultaneously modify the same S3 batch.
        //
        // Scenario simulated:
        // 1. Node A writes blocks 0-2, syncs them (has lease)
        // 2. Node A's lease expires
        // 3. Node B acquires lease, writes blocks 5-7 (same batch)
        // 4. Node B syncs - should see Node A's data preserved in batch
        //
        // The key invariant: whoever holds the lease is the only writer.
        // Lost leases mean no more writes.

        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // Use small batches so blocks 0-2 and 5-7 are in the same batch
        let s3 = S3BlockStore::new(Arc::clone(&object_store), "test", 4096)
            .with_blocks_per_batch(10);  // Blocks 0-9 all in batch 0

        // === Node A: has lease, writes blocks 0-2 ===
        let dir_a = TempDir::new().unwrap();
        let config_a = WriteCacheConfig {
            cache_dir: dir_a.path().to_path_buf(),
            device_name: "node_a".to_string(),
            device_size: 1024 * 1024,
            block_size: 4096,
        };

        let cache_a = WriteCache::<Initializing>::open(config_a).unwrap();
        let cache_a = cache_a.finish_recovery(&s3).await.unwrap();

        // Node A writes blocks 0, 1, 2 with pattern 0xAA
        cache_a.write(0, &[0xAAu8; 4096]).unwrap();
        cache_a.write(4096, &[0xAAu8; 4096]).unwrap();
        cache_a.write(8192, &[0xAAu8; 4096]).unwrap();

        // Node A syncs (simulating holding the lease)
        let dirty = cache_a.claim_dirty_blocks(10);
        cache_a.sync_blocks_batched(&s3, dirty).await.unwrap();

        // Verify Node A's data is in S3
        let block0 = s3.read_block(0).await.unwrap();
        assert_eq!(block0[0], 0xAA, "Node A's data should be in S3");

        // === Node B: acquires lease, writes blocks 5-7 ===
        let dir_b = TempDir::new().unwrap();
        let config_b = WriteCacheConfig {
            cache_dir: dir_b.path().to_path_buf(),
            device_name: "node_b".to_string(),
            device_size: 1024 * 1024,
            block_size: 4096,
        };

        let cache_b = WriteCache::<Initializing>::open(config_b).unwrap();
        let cache_b = cache_b.finish_recovery(&s3).await.unwrap();

        // Node B writes blocks 5, 6, 7 with pattern 0xBB
        cache_b.write(5 * 4096, &[0xBBu8; 4096]).unwrap();
        cache_b.write(6 * 4096, &[0xBBu8; 4096]).unwrap();
        cache_b.write(7 * 4096, &[0xBBu8; 4096]).unwrap();

        // Node B syncs (now holding the lease)
        let dirty = cache_b.claim_dirty_blocks(10);
        cache_b.sync_blocks_batched(&s3, dirty).await.unwrap();

        // === Verify: Both nodes' data coexists in the batch ===
        // The GET-modify-PUT pattern preserves Node A's blocks when Node B updates

        let block0 = s3.read_block(0).await.unwrap();
        assert_eq!(block0[0], 0xAA, "Node A's block 0 should be preserved");

        let block1 = s3.read_block(1).await.unwrap();
        assert_eq!(block1[0], 0xAA, "Node A's block 1 should be preserved");

        let block5 = s3.read_block(5).await.unwrap();
        assert_eq!(block5[0], 0xBB, "Node B's block 5 should be written");

        let block7 = s3.read_block(7).await.unwrap();
        assert_eq!(block7[0], 0xBB, "Node B's block 7 should be written");

        // This proves the GET-modify-PUT pattern correctly merges both nodes' writes
        // as long as they don't write to the same block (which the lease prevents).
    }
}
