//! Database wrapper for SlateDB.
//!
//! This provides a unified interface for both read-write and read-only database access.
//! Encryption is handled at the SlateDB level via BlockTransformer, so this wrapper
//! just passes through operations.

use crate::fs::errors::FsError;
use crate::fs::write_coordinator::{SequenceGuard, WriteCoordinator};
use anyhow::Result;
use arc_swap::ArcSwap;
use bytes::Bytes;
use slatedb::config::{DurabilityLevel, PutOptions, ReadOptions, ScanOptions, WriteOptions};
use slatedb::{DbReader, WriteBatch};
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;

/// Wrapper for SlateDB handle that can be either read-write or read-only.
pub enum SlateDbHandle {
    ReadWrite(Arc<slatedb::Db>),
    ReadOnly(ArcSwap<DbReader>),
}

impl Clone for SlateDbHandle {
    fn clone(&self) -> Self {
        match self {
            SlateDbHandle::ReadWrite(db) => SlateDbHandle::ReadWrite(db.clone()),
            SlateDbHandle::ReadOnly(reader) => {
                SlateDbHandle::ReadOnly(ArcSwap::new(reader.load_full()))
            }
        }
    }
}

impl SlateDbHandle {
    pub fn is_read_only(&self) -> bool {
        matches!(self, SlateDbHandle::ReadOnly(_))
    }
}

/// Fatal handler for SlateDB write errors.
/// After a write failure, the database state is unknown. Exit and let
/// the eventual orchestrator restart the service to rebuild from a known-good state.
pub fn exit_on_write_error(err: impl std::fmt::Display) -> ! {
    tracing::error!("Fatal write error, exiting: {}", err);
    std::process::exit(1)
}

/// Trait for types that can accumulate transaction mutations.
///
/// This allows store methods to work with both `Txn<Open>` (for sequenced
/// filesystem operations) and `WriteBatch` (for unsequenced operations like GC).
pub trait TransactionMut {
    fn put_bytes(&mut self, key: &Bytes, value: Bytes);
    fn delete_bytes(&mut self, key: &Bytes);
}

impl TransactionMut for WriteBatch {
    fn put_bytes(&mut self, key: &Bytes, value: Bytes) {
        self.put(key, &value);
    }

    fn delete_bytes(&mut self, key: &Bytes) {
        self.delete(key);
    }
}

// ============================================================================
// Typestate Transaction API
// ============================================================================

/// Transaction state: open for mutations (put/delete operations).
pub struct Open {
    coordinator: Arc<WriteCoordinator>,
}

/// Transaction state: sequenced and ready to commit (no more mutations allowed).
pub struct Sequenced {
    guard: SequenceGuard,
}

/// A typestate transaction that combines WriteBatch with WriteCoordinator sequencing.
///
/// This provides compile-time enforcement of the transaction lifecycle:
/// - `Txn<Open>`: Can call `put_bytes()`, `delete_bytes()`, then `sequence()`
/// - `Txn<Sequenced>`: Can only call `commit()` (or drop to auto-abandon)
///
/// # Example
/// ```ignore
/// let mut txn = fs.begin_txn()?;
/// store.save(&mut txn, id, &data)?;
/// txn.sequence().commit(&db).await?;
/// ```
pub struct Txn<S> {
    batch: WriteBatch,
    db: Arc<Db>,
    state: S,
}

impl Txn<Open> {
    /// Create a new transaction in the Open state.
    pub(crate) fn new(db: Arc<Db>, coordinator: Arc<WriteCoordinator>) -> Self {
        Self {
            batch: WriteBatch::new(),
            db,
            state: Open { coordinator },
        }
    }

    /// Acquire a sequence number and transition to Sequenced state.
    ///
    /// After calling this, no more mutations are allowed. The transaction
    /// must be committed or it will be automatically abandoned on drop.
    pub fn sequence(self) -> Txn<Sequenced> {
        let guard = self.state.coordinator.allocate_sequence();
        Txn {
            batch: self.batch,
            db: self.db,
            state: Sequenced { guard },
        }
    }
}

impl TransactionMut for Txn<Open> {
    fn put_bytes(&mut self, key: &Bytes, value: Bytes) {
        self.batch.put(key, &value);
    }

    fn delete_bytes(&mut self, key: &Bytes) {
        self.batch.delete(key);
    }
}

impl Txn<Sequenced> {
    /// Wait for predecessors and commit the transaction.
    ///
    /// This consumes the transaction to prevent use after commit.
    /// On success, the sequence is marked committed.
    /// On error (or if dropped without calling commit), the sequence is marked abandoned.
    pub async fn commit(mut self) -> Result<(), FsError> {
        // Wait for all preceding sequences to complete
        self.state.guard.wait_for_predecessors().await;

        // Write batch to database
        self.db
            .write_with_options(
                std::mem::take(&mut self.batch),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| FsError::IoError)?;

        // Mark committed - this prevents Drop from marking abandoned
        self.state.guard.mark_committed();

        Ok(())
    }
}

// Note: We don't need a custom Drop for Txn<Sequenced> because SequenceGuard
// already handles marking as abandoned if not committed.

/// Database wrapper providing a unified interface for SlateDB operations.
///
/// With BlockTransformer handling encryption at the SlateDB level, this wrapper
/// simply passes through operations without additional encryption/decryption.
pub struct Db {
    inner: SlateDbHandle,
}

impl Db {
    pub fn new(db: Arc<slatedb::Db>) -> Self {
        Self {
            inner: SlateDbHandle::ReadWrite(db),
        }
    }

    pub fn new_read_only(db_reader: ArcSwap<DbReader>) -> Self {
        Self {
            inner: SlateDbHandle::ReadOnly(db_reader),
        }
    }

    pub fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    pub fn swap_reader(&self, new_reader: Arc<DbReader>) -> Result<()> {
        match &self.inner {
            SlateDbHandle::ReadOnly(reader_swap) => {
                reader_swap.store(new_reader);
                Ok(())
            }
            SlateDbHandle::ReadWrite(_) => Err(anyhow::anyhow!(
                "Cannot swap reader on a read-write database"
            )),
        }
    }

    pub async fn get_bytes(&self, key: &Bytes) -> Result<Option<Bytes>> {
        let read_options = ReadOptions {
            durability_filter: DurabilityLevel::Memory,
            cache_blocks: true,
            ..Default::default()
        };

        let result = match &self.inner {
            SlateDbHandle::ReadWrite(db) => db.get_with_options(key, &read_options).await?,
            SlateDbHandle::ReadOnly(reader_swap) => {
                let reader = reader_swap.load();
                reader.get_with_options(key, &read_options).await?
            }
        };

        Ok(result)
    }

    pub async fn scan<R: RangeBounds<Bytes> + Clone + Send + Sync + 'static>(
        &self,
        range: R,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<(Bytes, Bytes)>> + Send + '_>>> {
        let scan_options = ScanOptions {
            durability_filter: DurabilityLevel::Memory,
            read_ahead_bytes: 1024 * 1024,
            cache_blocks: true,
            max_fetch_tasks: 8,
            ..Default::default()
        };

        let iter = match &self.inner {
            SlateDbHandle::ReadWrite(db) => db.scan_with_options(range, &scan_options).await?,
            SlateDbHandle::ReadOnly(reader_swap) => {
                let reader = reader_swap.load();
                reader.scan_with_options(range, &scan_options).await?
            }
        };

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<(Bytes, Bytes)>>(32);

        tokio::spawn(async move {
            let mut iter = iter;
            while let Ok(Some(kv)) = iter.next().await {
                if tx.send(Ok((kv.key, kv.value))).await.is_err() {
                    break;
                }
            }
        });

        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    pub async fn write_with_options(
        &self,
        batch: WriteBatch,
        options: &WriteOptions,
    ) -> Result<()> {
        if self.is_read_only() {
            return Err(FsError::ReadOnlyFilesystem.into());
        }

        match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                if let Err(e) = db.write_with_options(batch, options).await {
                    exit_on_write_error(e);
                }
            }
            SlateDbHandle::ReadOnly(_) => unreachable!(),
        }

        Ok(())
    }

    pub async fn put_with_options(
        &self,
        key: &Bytes,
        value: &[u8],
        put_options: &PutOptions,
        write_options: &WriteOptions,
    ) -> Result<()> {
        if self.is_read_only() {
            return Err(FsError::ReadOnlyFilesystem.into());
        }

        match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                if let Err(e) = db
                    .put_with_options(key, value, put_options, write_options)
                    .await
                {
                    exit_on_write_error(e);
                }
            }
            SlateDbHandle::ReadOnly(_) => unreachable!(),
        }

        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        if self.is_read_only() {
            return Err(FsError::ReadOnlyFilesystem.into());
        }

        match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                if let Err(e) = db.flush().await {
                    exit_on_write_error(e);
                }
            }
            SlateDbHandle::ReadOnly(_) => unreachable!(),
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                if let Err(e) = db.close().await {
                    exit_on_write_error(e);
                }
            }
            SlateDbHandle::ReadOnly(reader_swap) => {
                let reader = reader_swap.load();
                reader.close().await?
            }
        }
        Ok(())
    }
}
