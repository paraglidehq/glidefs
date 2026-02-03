//! Direct block-to-S3 storage mapping for NBD devices.
//!
//! This module provides a thin layer between block I/O and S3 object storage,
//! mapping each block directly to an S3 object. This avoids LSM tree overhead
//! for block-level workloads.

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, instrument, warn};

use crate::block_transformer::{BlockTransformer, ZeroFsBlockTransformer};

/// Default block size: 128KB (matches ZFS default recordsize)
#[allow(dead_code)]
pub const DEFAULT_BLOCK_SIZE: usize = 128 * 1024;

/// Errors that can occur during block store operations.
#[derive(Error, Debug)]
pub enum BlockStoreError {
    #[error("S3 operation failed: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Encryption/decryption failed: {0}")]
    Crypto(String),

    #[error("Block not found: {0}")]
    NotFound(u64),
}

/// Direct block-to-S3-object storage.
///
/// Each block is stored as a separate S3 object with the key format:
/// `{prefix}/blocks/{block_number:016x}`
///
/// This provides:
/// - O(1) block lookup (no LSM tree traversal)
/// - Simple recovery (just list objects)
/// - Natural parallelization (independent objects)
pub struct S3BlockStore {
    /// Underlying object store (S3, MinIO, etc.)
    object_store: Arc<dyn ObjectStore>,

    /// Path prefix for all blocks (e.g., "zerofs/myvolume")
    prefix: String,

    /// Block size in bytes
    #[allow(dead_code)]
    block_size: usize,

    /// Optional transformer for encryption/compression
    transformer: Option<Arc<ZeroFsBlockTransformer>>,
}

impl S3BlockStore {
    /// Create a new block store.
    ///
    /// # Arguments
    /// * `object_store` - The underlying S3-compatible object store
    /// * `prefix` - Path prefix for all blocks (e.g., "zerofs/myvolume")
    /// * `block_size` - Block size in bytes (default: 128KB)
    /// * `transformer` - Optional encryption/compression transformer
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
        block_size: usize,
        transformer: Option<Arc<ZeroFsBlockTransformer>>,
    ) -> Self {
        Self {
            object_store,
            prefix: prefix.into(),
            block_size,
            transformer,
        }
    }

    /// Create a new block store with default block size.
    #[allow(dead_code)]
    pub fn with_defaults(
        object_store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
        transformer: Option<Arc<ZeroFsBlockTransformer>>,
    ) -> Self {
        Self::new(object_store, prefix, DEFAULT_BLOCK_SIZE, transformer)
    }

    /// Get the block size.
    #[inline]
    #[allow(dead_code)]
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Generate the S3 key for a block number.
    ///
    /// Format: `{prefix}/blocks/{block_number:016x}`
    #[inline]
    fn block_key(&self, block_num: u64) -> Path {
        Path::from(format!("{}/blocks/{:016x}", self.prefix, block_num))
    }

    /// Read a single block from S3.
    ///
    /// Returns `BlockStoreError::NotFound` if the block doesn't exist.
    #[instrument(skip(self), fields(block = block_num))]
    pub async fn read_block(&self, block_num: u64) -> Result<Bytes, BlockStoreError> {
        let key = self.block_key(block_num);

        let result = self.object_store.get(&key).await;

        match result {
            Ok(response) => {
                let data = response.bytes().await?;

                // Decrypt if transformer is configured
                if let Some(transformer) = &self.transformer {
                    let decrypted = transformer
                        .decode(data)
                        .await
                        .map_err(|e| BlockStoreError::Crypto(e.to_string()))?;
                    debug!(block = block_num, size = decrypted.len(), "read block");
                    Ok(decrypted)
                } else {
                    debug!(block = block_num, size = data.len(), "read block (unencrypted)");
                    Ok(data)
                }
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!(block = block_num, "block not found, returning zeros");
                Err(BlockStoreError::NotFound(block_num))
            }
            Err(e) => Err(BlockStoreError::ObjectStore(e)),
        }
    }

    /// Write a single block to S3.
    #[instrument(skip(self, data), fields(block = block_num, size = data.len()))]
    pub async fn write_block(&self, block_num: u64, data: Bytes) -> Result<(), BlockStoreError> {
        let key = self.block_key(block_num);

        // Encrypt if transformer is configured
        let payload = if let Some(transformer) = &self.transformer {
            let encrypted = transformer
                .encode(data)
                .await
                .map_err(|e| BlockStoreError::Crypto(e.to_string()))?;
            PutPayload::from(encrypted)
        } else {
            PutPayload::from(data)
        };

        self.object_store.put(&key, payload).await?;
        debug!(block = block_num, "wrote block");

        Ok(())
    }

    /// Write multiple blocks to S3 in parallel.
    ///
    /// Returns the number of blocks successfully written.
    /// Continues on individual block failures but logs warnings.
    #[allow(dead_code)]
    #[instrument(skip(self, blocks), fields(count = blocks.len()))]
    pub async fn write_blocks(
        &self,
        blocks: Vec<(u64, Bytes)>,
    ) -> Result<usize, BlockStoreError> {
        if blocks.is_empty() {
            return Ok(0);
        }

        let mut futures: FuturesUnordered<_> = blocks
            .into_iter()
            .map(|(block_num, data)| async move {
                let result = self.write_block(block_num, data).await;
                (block_num, result)
            })
            .collect();

        let mut success_count = 0;
        let mut last_error = None;

        while let Some((block_num, result)) = futures.next().await {
            match result {
                Ok(()) => success_count += 1,
                Err(e) => {
                    warn!(block = block_num, error = %e, "failed to write block");
                    last_error = Some(e);
                }
            }
        }

        // If all writes failed, return the last error
        if success_count == 0 && let Some(e) = last_error {
            return Err(e);
        }

        debug!(success = success_count, "batch write complete");
        Ok(success_count)
    }

    /// Delete a block from S3.
    ///
    /// This is a no-op if the block doesn't exist.
    #[allow(dead_code)]
    #[instrument(skip(self), fields(block = block_num))]
    pub async fn delete_block(&self, block_num: u64) -> Result<(), BlockStoreError> {
        let key = self.block_key(block_num);
        self.object_store.delete(&key).await?;
        debug!(block = block_num, "deleted block");
        Ok(())
    }

    /// List all block numbers in the store.
    ///
    /// This can be used for recovery to find which blocks exist in S3.
    #[allow(dead_code)]
    #[instrument(skip(self))]
    pub async fn list_blocks(&self) -> Result<Vec<u64>, BlockStoreError> {
        let prefix = Path::from(format!("{}/blocks/", self.prefix));

        let mut block_nums = Vec::new();
        let mut stream = self.object_store.list(Some(&prefix));

        while let Some(result) = stream.next().await {
            let meta = result?;
            // Parse block number from path: prefix/blocks/{block_num:016x}
            if let Some(filename) = meta.location.filename()
                && let Ok(block_num) = u64::from_str_radix(filename, 16)
            {
                block_nums.push(block_num);
            }
        }

        block_nums.sort_unstable();
        debug!(count = block_nums.len(), "listed blocks");
        Ok(block_nums)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn test_store() -> S3BlockStore {
        let object_store = Arc::new(InMemory::new());
        S3BlockStore::with_defaults(object_store, "test", None)
    }

    #[tokio::test]
    async fn test_write_read_block() {
        let store = test_store();
        let data = Bytes::from(vec![42u8; 128 * 1024]);

        store.write_block(0, data.clone()).await.unwrap();
        let read_data = store.read_block(0).await.unwrap();

        assert_eq!(read_data, data);
    }

    #[tokio::test]
    async fn test_read_nonexistent_block() {
        let store = test_store();

        let result = store.read_block(999).await;
        assert!(matches!(result, Err(BlockStoreError::NotFound(999))));
    }

    #[tokio::test]
    async fn test_write_blocks_batch() {
        let store = test_store();

        let blocks: Vec<_> = (0..10)
            .map(|i| (i, Bytes::from(vec![i as u8; 1024])))
            .collect();

        let written = store.write_blocks(blocks).await.unwrap();
        assert_eq!(written, 10);

        // Verify all blocks
        for i in 0..10 {
            let data = store.read_block(i).await.unwrap();
            assert_eq!(data[0], i as u8);
        }
    }

    #[tokio::test]
    async fn test_list_blocks() {
        let store = test_store();

        // Write some blocks (not contiguous)
        store.write_block(0, Bytes::from("a")).await.unwrap();
        store.write_block(5, Bytes::from("b")).await.unwrap();
        store.write_block(100, Bytes::from("c")).await.unwrap();

        let blocks = store.list_blocks().await.unwrap();
        assert_eq!(blocks, vec![0, 5, 100]);
    }

    #[tokio::test]
    async fn test_delete_block() {
        let store = test_store();

        store.write_block(0, Bytes::from("data")).await.unwrap();
        assert!(store.read_block(0).await.is_ok());

        store.delete_block(0).await.unwrap();
        assert!(matches!(
            store.read_block(0).await,
            Err(BlockStoreError::NotFound(0))
        ));
    }

    #[tokio::test]
    async fn test_block_key_format() {
        let store = test_store();

        // Block 0
        assert_eq!(
            store.block_key(0).to_string(),
            "test/blocks/0000000000000000"
        );

        // Block with max u64
        assert_eq!(
            store.block_key(u64::MAX).to_string(),
            "test/blocks/ffffffffffffffff"
        );
    }
}
