//! Write-behind cache for NBD devices.
//!
//! This module implements a local SSD cache that provides fast FLUSH semantics
//! while asynchronously syncing data to S3. The cache uses typestate to enforce
//! proper lifecycle management at compile time.

use bitvec::prelude::*;
use bytes::Bytes;
use futures::StreamExt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write as IoWrite};
use std::marker::PhantomData;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use thiserror::Error;
use tracing::{debug, info, instrument, warn};

use super::block_store::{BlockStoreError, S3BlockStore};
use super::state::{Active, BlockState, Draining, Initializing, Recovering};
use crate::block_transformer::ZeroFsBlockTransformer;

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
pub(crate) struct CacheInner {
    /// Configuration
    config: WriteCacheConfig,

    /// Local cache file (data) - encrypted at rest
    data_file: RwLock<File>,

    /// Block states (indexed by block number)
    block_states: RwLock<Vec<BlockState>>,

    /// Which blocks are present locally (have been fetched or written)
    /// A block can be Clean but not present (never accessed on this node)
    /// Uses BitVec<u8, Lsb0> for 8x memory savings vs Vec<bool>
    present_blocks: RwLock<BitVec<u8, Lsb0>>,

    /// Statistics
    dirty_block_count: AtomicU64,
    syncing_block_count: AtomicU64,

    /// Encryption transformer (encrypts before local write)
    #[allow(dead_code)]
    transformer: Option<Arc<ZeroFsBlockTransformer>>,
}

impl CacheInner {
    /// Persist block states and presence to metadata file.
    fn save_metadata(&self) -> Result<(), CacheError> {
        let path = self.config.metadata_path();
        let states = self.block_states.read().unwrap();
        let present = self.present_blocks.read().unwrap();

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        // Write header
        file.write_all(METADATA_MAGIC)?;
        file.write_all(&METADATA_VERSION.to_le_bytes())?;
        file.write_all(&self.config.device_size.to_le_bytes())?;
        file.write_all(&(self.config.block_size as u64).to_le_bytes())?;
        file.write_all(&(states.len() as u64).to_le_bytes())?;

        // Write block states (1 byte per block)
        let state_bytes: Vec<u8> = states.iter().map(|s| *s as u8).collect();
        file.write_all(&state_bytes)?;

        // Write presence bitmap as packed bits (1 bit per block)
        let present_bytes = present.as_raw_slice();
        file.write_all(present_bytes)?;

        file.sync_all()?;

        let present_count = present.count_ones();
        debug!(
            path = %path.display(),
            blocks = states.len(),
            present = present_count,
            "saved cache metadata"
        );
        Ok(())
    }

    /// Load block states and presence from metadata file.
    fn load_metadata(config: &WriteCacheConfig) -> Result<(Vec<BlockState>, BitVec<u8, Lsb0>), CacheError> {
        let path = config.metadata_path();
        let num_blocks = config.num_blocks();

        if !path.exists() {
            // No metadata file - all blocks are clean and NOT present
            debug!(path = %path.display(), "no metadata file, starting fresh");
            return Ok((
                vec![BlockState::Clean; num_blocks],
                bitvec![u8, Lsb0; 0; num_blocks],
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

        // Convert to BlockState, treating Syncing as Dirty (conservative for crash recovery)
        let states: Vec<BlockState> = state_bytes
            .iter()
            .map(|&b| {
                let state = BlockState::from_u8(b);
                // Syncing blocks had in-flight uploads that may have failed
                if state == BlockState::Syncing {
                    BlockState::Dirty
                } else {
                    state
                }
            })
            .collect();

        // Read presence bitmap
        let present: BitVec<u8, Lsb0> = if version >= 3 {
            // Version 3: packed bits (1 bit per block)
            let num_bytes = stored_num_blocks.div_ceil(8);
            let mut present_bytes = vec![0u8; num_bytes];
            file.read_exact(&mut present_bytes)?;
            let mut bv = BitVec::<u8, Lsb0>::from_vec(present_bytes);
            bv.truncate(stored_num_blocks); // Remove padding bits
            bv
        } else if version >= 2 {
            // Version 2: 1 byte per block (legacy)
            let mut present_bytes = vec![0u8; stored_num_blocks];
            file.read_exact(&mut present_bytes)?;
            present_bytes.iter().map(|&b| b != 0).collect()
        } else {
            // Version 1 compatibility: dirty blocks are present, clean blocks are NOT
            // (conservative - forces re-fetch from S3 for clean blocks)
            states.iter().map(|s| *s == BlockState::Dirty).collect()
        };

        let dirty_count = states.iter().filter(|s| **s == BlockState::Dirty).count();
        let present_count = present.count_ones();
        info!(
            path = %path.display(),
            blocks = states.len(),
            dirty = dirty_count,
            present = present_count,
            "loaded cache metadata"
        );

        Ok((states, present))
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
    /// * `transformer` - Optional encryption transformer. If provided, data is
    ///   encrypted before writing to local SSD.
    #[instrument(skip(config, transformer), fields(device = %config.device_name))]
    pub fn open(
        config: WriteCacheConfig,
        transformer: Option<Arc<ZeroFsBlockTransformer>>,
    ) -> Result<WriteCache<Recovering>, CacheError> {
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
                encrypted = transformer.is_some(),
                "extended cache file (sparse)"
            );
        }

        // Load block states and presence (or create fresh)
        let (block_states, present_blocks) = CacheInner::load_metadata(&config)?;

        let dirty_count = block_states
            .iter()
            .filter(|s| **s == BlockState::Dirty)
            .count();

        let present_count = present_blocks.count_ones();

        let inner = Arc::new(CacheInner {
            config,
            data_file: RwLock::new(data_file),
            block_states: RwLock::new(block_states),
            present_blocks: RwLock::new(present_blocks),
            dirty_block_count: AtomicU64::new(dirty_count as u64),
            syncing_block_count: AtomicU64::new(0),
            transformer,
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

            // Collect dirty blocks
            let dirty_blocks = self.collect_dirty_blocks();

            // Upload each block
            for block_num in dirty_blocks {
                match self.upload_block(s3, block_num).await {
                    Ok(()) => {
                        self.mark_synced(block_num);
                    }
                    Err(e) => {
                        warn!(block = block_num, error = %e, "recovery upload failed, will retry");
                        // Leave as Dirty for next sync cycle
                    }
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
        let states = self.inner.block_states.read().unwrap();
        states
            .iter()
            .enumerate()
            .filter(|(_, s)| **s == BlockState::Dirty)
            .map(|(i, _)| i as u64)
            .collect()
    }

    async fn upload_block(&self, s3: &S3BlockStore, block_num: u64) -> Result<(), CacheError> {
        let data = self.read_local_block(block_num)?;
        s3.write_block(block_num, data).await?;
        Ok(())
    }

    fn read_local_block(&self, block_num: u64) -> Result<Bytes, CacheError> {
        let offset = block_num * self.inner.config.block_size as u64;
        let mut buf = vec![0u8; self.inner.config.block_size];

        let file = self.inner.data_file.read().unwrap();
        file.read_exact_at(&mut buf, offset)?;

        Ok(Bytes::from(buf))
    }

    fn mark_synced(&self, block_num: u64) {
        let mut states = self.inner.block_states.write().unwrap();
        if let Some(state) = states.get_mut(block_num as usize)
            && (*state == BlockState::Dirty || *state == BlockState::Syncing)
        {
            *state = BlockState::Clean;
            self.inner.dirty_block_count.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl WriteCache<Active> {
    /// Write data to the cache.
    ///
    /// Data is written to the local SSD and the affected blocks are marked dirty and present.
    /// The write returns immediately after local I/O completes.
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

        // Mark affected blocks as dirty and present
        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + data.len() as u64 - 1) / block_size;

        {
            let mut states = self.inner.block_states.write().unwrap();
            let mut present = self.inner.present_blocks.write().unwrap();

            for block in start_block..=end_block {
                let idx = block as usize;

                // Mark as present
                if idx < present.len() {
                    present.set(idx, true);
                }

                // Update dirty state
                if let Some(state) = states.get_mut(idx) {
                    if *state == BlockState::Clean {
                        *state = BlockState::Dirty;
                        self.inner.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                    } else if *state == BlockState::Syncing {
                        // Write during sync - mark dirty again (will re-upload)
                        *state = BlockState::Dirty;
                        self.inner.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
                        self.inner.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                    }
                    // Already Dirty - no change needed
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
    #[instrument(skip(self, s3), fields(offset = offset, len = len))]
    pub async fn read_with_fetch(
        &self,
        offset: u64,
        len: usize,
        s3: &S3BlockStore,
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

        // Check which blocks need to be fetched from S3
        let blocks_to_fetch: Vec<u64> = {
            let present = self.inner.present_blocks.read().unwrap();
            (start_block..=end_block)
                .filter(|&block| {
                    let idx = block as usize;
                    idx >= present.len() || !present[idx]
                })
                .collect()
        };

        // Fetch missing blocks from S3
        for block_num in blocks_to_fetch {
            self.fetch_block_from_s3(s3, block_num).await?;
        }

        // Now read from local cache
        self.read_local(offset, len)
    }

    /// Fetch a single block from S3 and cache it locally.
    #[instrument(skip(self, s3), fields(block = block_num))]
    async fn fetch_block_from_s3(&self, s3: &S3BlockStore, block_num: u64) -> Result<(), CacheError> {
        let block_size = self.inner.config.block_size;
        let offset = block_num * block_size as u64;

        // Fetch from S3
        let data = match s3.read_block(block_num).await {
            Ok(data) => data,
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
            let mut padded = vec![0u8; block_size];
            let copy_len = data.len().min(block_size);
            padded[..copy_len].copy_from_slice(&data[..copy_len]);
            file.write_all_at(&padded, offset)?;
        }

        // Mark block as present (but Clean since it matches S3)
        {
            let mut present = self.inner.present_blocks.write().unwrap();
            let idx = block_num as usize;
            if idx < present.len() {
                present.set(idx, true);
            }
        }

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

    /// Mark a range of blocks as dirty and present.
    fn mark_range_dirty_and_present(&self, offset: u64, len: u64) {
        let block_size = self.inner.config.block_size as u64;
        let start_block = offset / block_size;
        let end_block = (offset + len - 1) / block_size;

        let mut states = self.inner.block_states.write().unwrap();
        let mut present = self.inner.present_blocks.write().unwrap();

        for block in start_block..=end_block {
            let idx = block as usize;

            // Mark as present
            if idx < present.len() {
                present.set(idx, true);
            }

            // Update dirty state
            if let Some(state) = states.get_mut(idx) {
                if *state == BlockState::Clean {
                    *state = BlockState::Dirty;
                    self.inner.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                } else if *state == BlockState::Syncing {
                    *state = BlockState::Dirty;
                    self.inner.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
                    self.inner.dirty_block_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Claim dirty blocks for syncing.
    ///
    /// This transitions blocks from Dirty to Syncing and returns their numbers.
    /// The sync worker should call this, upload the blocks, then call
    /// `mark_synced` or `mark_sync_failed` for each block.
    #[instrument(skip(self))]
    pub fn claim_dirty_blocks(&self, max_blocks: usize) -> Vec<u64> {
        let mut states = self.inner.block_states.write().unwrap();
        let mut claimed = Vec::with_capacity(max_blocks);

        for (i, state) in states.iter_mut().enumerate() {
            if *state == BlockState::Dirty {
                *state = BlockState::Syncing;
                claimed.push(i as u64);
                self.inner.dirty_block_count.fetch_sub(1, Ordering::Relaxed);
                self.inner.syncing_block_count.fetch_add(1, Ordering::Relaxed);

                if claimed.len() >= max_blocks {
                    break;
                }
            }
        }

        if !claimed.is_empty() {
            debug!(count = claimed.len(), "claimed dirty blocks for sync");
        }
        claimed
    }

    /// Mark a block as successfully synced.
    pub fn mark_synced(&self, block_num: u64) {
        let mut states = self.inner.block_states.write().unwrap();
        if let Some(state) = states.get_mut(block_num as usize)
            && *state == BlockState::Syncing
        {
            *state = BlockState::Clean;
            self.inner.syncing_block_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Mark a block sync as failed (will retry).
    pub fn mark_sync_failed(&self, block_num: u64) {
        let mut states = self.inner.block_states.write().unwrap();
        if let Some(state) = states.get_mut(block_num as usize)
            && *state == BlockState::Syncing
        {
            *state = BlockState::Dirty;
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

    /// Graceful shutdown: sync all blocks and transition to Draining.
    #[allow(dead_code)]
    #[instrument(skip(self, s3))]
    pub async fn shutdown(self, s3: &S3BlockStore) -> Result<WriteCache<Draining>, CacheError> {
        info!("starting graceful shutdown");

        // Claim and sync all remaining dirty blocks
        loop {
            let dirty = self.claim_dirty_blocks(1000);
            if dirty.is_empty() {
                break;
            }

            for block_num in dirty {
                let data = self.read_local_block(block_num)?;
                match s3.write_block(block_num, data).await {
                    Ok(()) => self.mark_synced(block_num),
                    Err(e) => {
                        warn!(block = block_num, error = %e, "shutdown sync failed");
                        self.mark_sync_failed(block_num);
                    }
                }
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

        // Upload all dirty blocks
        loop {
            let dirty = self.claim_dirty_blocks(100);
            if dirty.is_empty() {
                break;
            }

            for block_num in dirty {
                let data = self.read_local_block(block_num)?;
                match s3.write_block(block_num, data).await {
                    Ok(()) => self.mark_synced(block_num),
                    Err(e) => {
                        warn!(block = block_num, error = %e, "snapshot drain upload failed");
                        self.mark_sync_failed(block_num);
                        return Err(CacheError::BlockStore(e));
                    }
                }
            }
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
    /// Maximum number of blocks to claim per batch
    pub batch_size: usize,
    /// Maximum number of concurrent S3 uploads
    pub max_concurrent_uploads: usize,
    /// How long to sleep when no dirty blocks are found
    pub idle_sleep: std::time::Duration,
    /// How often to save metadata (every N batches)
    pub metadata_save_interval: usize,
}

impl Default for SyncWorkerConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            max_concurrent_uploads: 16,
            idle_sleep: std::time::Duration::from_millis(100),
            metadata_save_interval: 10,
        }
    }
}

/// Sync worker that continuously drains dirty blocks to S3.
///
/// This function runs in a background task and uploads dirty blocks as quickly
/// as possible using parallel uploads, keeping the dirty set small. This improves:
/// - Cross-host snapshot latency (less data to drain)
/// - Data durability (less data at risk on crash)
/// - Throughput (parallel S3 uploads)
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
/// ));
///
/// // To stop:
/// let _ = shutdown_tx.send(true);
/// worker_handle.await.unwrap();
/// ```
#[instrument(skip(cache, s3, config, shutdown))]
pub async fn sync_worker(
    cache: Arc<WriteCache<Active>>,
    s3: Arc<S3BlockStore>,
    config: SyncWorkerConfig,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    info!(
        batch_size = config.batch_size,
        max_concurrent = config.max_concurrent_uploads,
        "sync worker started"
    );

    let mut batches_since_save = 0;

    loop {
        // Check for shutdown
        if *shutdown.borrow() {
            info!("sync worker received shutdown signal");
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

        let upload_count = dirty.len();
        let start = std::time::Instant::now();

        // Prepare upload tasks - read blocks first (sync I/O)
        let mut upload_tasks: Vec<(u64, Bytes)> = Vec::with_capacity(dirty.len());
        let mut read_failures = 0;

        for block_num in dirty {
            match cache.read_local_block(block_num) {
                Ok(data) => upload_tasks.push((block_num, data)),
                Err(e) => {
                    warn!(block = block_num, error = %e, "failed to read block for sync");
                    cache.mark_sync_failed(block_num);
                    read_failures += 1;
                }
            }
        }

        // Upload in parallel with bounded concurrency
        let upload_futures: Vec<_> = upload_tasks
            .into_iter()
            .map(|(block_num, data)| {
                let s3 = Arc::clone(&s3);
                let cache = Arc::clone(&cache);
                async move {
                    match s3.write_block(block_num, data).await {
                        Ok(()) => {
                            cache.mark_synced(block_num);
                            (true, block_num)
                        }
                        Err(e) => {
                            warn!(block = block_num, error = %e, "sync upload failed");
                            cache.mark_sync_failed(block_num);
                            (false, block_num)
                        }
                    }
                }
            })
            .collect();

        let results: Vec<_> = futures::stream::iter(upload_futures)
            .buffer_unordered(config.max_concurrent_uploads)
            .collect()
            .await;

        let success_count = results.iter().filter(|(ok, _)| *ok).count();
        let failed_count = results.iter().filter(|(ok, _)| !*ok).count() + read_failures;
        let elapsed = start.elapsed();

        if failed_count > 0 {
            warn!(
                success = success_count,
                failed = failed_count,
                elapsed_ms = elapsed.as_millis(),
                "sync batch completed with failures"
            );
        } else {
            debug!(
                blocks = upload_count,
                elapsed_ms = elapsed.as_millis(),
                throughput_mb_s = (upload_count * cache.block_size()) as f64 / elapsed.as_secs_f64() / 1_000_000.0,
                "sync batch completed"
            );
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
        S3BlockStore::with_defaults(object_store, "test", None)
    }

    #[tokio::test]
    async fn test_open_fresh_cache() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config, None).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        assert_eq!(cache.dirty_block_count(), 0);
    }

    #[tokio::test]
    async fn test_write_read() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config, None).unwrap();
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

        let cache = WriteCache::<Initializing>::open(config, None).unwrap();
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

        let cache = WriteCache::<Initializing>::open(config, None).unwrap();
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

        let cache = WriteCache::<Initializing>::open(config, None).unwrap();
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
            let cache = WriteCache::<Initializing>::open(config.clone(), None).unwrap();
            let cache = cache.finish_recovery(&s3).await.unwrap();
            cache.write(0, b"persistent").unwrap();
            cache.save_metadata().unwrap();
        }

        // Reopen and verify dirty blocks are preserved
        {
            let cache = WriteCache::<Initializing>::open(config, None).unwrap();
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

        let cache = WriteCache::<Initializing>::open(config, None).unwrap();
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

        let cache = WriteCache::<Initializing>::open(config, None).unwrap();
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
        let s3 = S3BlockStore::new(Arc::clone(&object_store), "test", 4096, None);

        // Pre-populate S3 with some data (simulating data from another node)
        let s3_data = Bytes::from(vec![42u8; 4096]);
        s3.write_block(0, s3_data.clone()).await.unwrap();
        s3.write_block(5, Bytes::from(vec![99u8; 4096])).await.unwrap();

        // Create a fresh cache on a "new node" (no local data)
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let cache = WriteCache::<Initializing>::open(config, None).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Read block 0 - should fetch from S3
        let data = cache.read_with_fetch(0, 4096, &s3).await.unwrap();
        assert_eq!(data[0], 42);
        assert!(data.iter().all(|&b| b == 42));

        // Read block 5 - should also fetch from S3
        let offset = 5 * 4096;
        let data = cache.read_with_fetch(offset, 4096, &s3).await.unwrap();
        assert_eq!(data[0], 99);

        // Second read of block 0 should come from local cache now
        let data = cache.read_with_fetch(0, 4096, &s3).await.unwrap();
        assert_eq!(data[0], 42);

        // Read a block that doesn't exist in S3 - should return zeros
        let offset = 10 * 4096;
        let data = cache.read_with_fetch(offset, 4096, &s3).await.unwrap();
        assert!(data.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn test_write_then_read_local() {
        // Verify that written blocks are marked as present and read locally
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let s3 = test_s3();

        let cache = WriteCache::<Initializing>::open(config, None).unwrap();
        let cache = cache.finish_recovery(&s3).await.unwrap();

        // Write data locally
        cache.write(0, b"local data!").unwrap();

        // Read should come from local cache, not S3
        let data = cache.read_with_fetch(0, 11, &s3).await.unwrap();
        assert_eq!(&data[..], b"local data!");

        // S3 should NOT have this data yet (not synced)
        let s3_result = s3.read_block(0).await;
        assert!(matches!(
            s3_result,
            Err(super::super::block_store::BlockStoreError::NotFound(_))
        ));
    }
}
