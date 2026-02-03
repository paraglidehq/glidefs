# ZeroFS Architecture

A high-performance block storage system that uses Amazon S3 (or S3-compatible storage) as durable storage while exposing local-speed block devices via NBD for ZFS.

## Design Philosophy

- **FLUSH = local durability** - Fast path for ZFS operations
- **Background sync to S3** - Eventual durability (continuous drain)
- **Leverage ZFS** - Let ZFS handle CoW, snapshots, compression
- **Minimal abstraction** - Direct block-to-S3 mapping

## Data Flow

### Write Path

```
ZFS write ─► NBD ─► Local SSD (fsync) ─► return (<10ms)
                          │
               Background sync ─► S3 (async)
```

The key insight: FLUSH returns after local SSD fsync, not after S3 sync. This makes ZFS snapshot/clone operations fast (~100ms instead of ~10s).

### Read Path

```
ZFS read ─► NBD ─► Local SSD cache (hit) ─► return (<1ms)
                          │ (miss)
                          ▼
                         S3 ─► decrypt ─► Local SSD ─► return (50-300ms)
```

### ZFS Snapshot (Target: <100ms)

```
zfs snapshot ─► NBD FLUSH ─► Local SSD fsync (<10ms) ─► return
```

Compare to the old architecture where FLUSH blocked on S3:
- Old: 8-15 seconds per snapshot
- New: <100ms per snapshot (420x improvement)

## State Machines

### Block State (Runtime)

```
Clean ◄──── sync complete ───── Syncing
  │                               ▲
  │ write                  claim dirty
  ▼                               │
Dirty ───── sync start ──────────┘
  ▲
  └── write during sync / sync failure
```

| State | Meaning |
|-------|---------|
| Clean | Block matches S3, safe to evict from local cache |
| Dirty | Local has newer data than S3 |
| Syncing | Upload in progress |

### Device Lifecycle (Typestate)

Compile-time enforcement ensures I/O only happens in the correct state:

```
WriteCache<Initializing>
         │ load cache
         ▼
WriteCache<Recovering>
         │ finish_recovery()
         ▼
WriteCache<Active>       ◄─ Only this state can serve I/O
         │ shutdown()
         ▼
WriteCache<Draining>
         │ finish()
         ▼
       [Dropped]
```

| State | Operations Allowed |
|-------|-------------------|
| Initializing | Load local cache file |
| Recovering | Sync dirty/syncing blocks to S3 |
| Active | read(), write(), flush() |
| Draining | Sync remaining blocks, reject new writes |

## Core Components

### WriteCache (`nbd/write_cache.rs`)

Local SSD write-behind cache with typestate lifecycle:

```rust
// Only Active cache can serve I/O - compiler enforced
impl WriteCache<Active> {
    pub async fn write(&self, offset: u64, data: &[u8]) -> Result<()>;
    pub fn flush(&self) -> Result<()>;  // Local fsync only!
    pub async fn read(&self, offset: u64, len: usize) -> Result<Bytes>;
}
```

### S3BlockStore (`nbd/block_store.rs`)

Direct block-to-S3 object mapping:

```rust
// Key format: {prefix}/blocks/{block_number:016x}
// Example: zerofs/nbd/device1/blocks/0000000000000042

impl S3BlockStore {
    pub async fn read_block(&self, block: u64) -> Result<Bytes>;
    pub async fn write_block(&self, block: u64, data: Bytes) -> Result<()>;
}
```

No LSM tree overhead - O(1) block lookup in S3.

### NBDBlockHandler (`nbd/handler.rs`)

Thin handler that delegates all I/O to WriteCache:

```rust
pub struct NBDBlockHandler {
    cache: Arc<WriteCache<Active>>,  // Typestate: only Active cache
    device_size: u64,
}

impl NBDBlockHandler {
    pub fn flush(&self) -> CommandResult<()> {
        self.cache.flush()  // Local fsync - returns in <10ms
    }
}
```

### Background Sync Worker

Continuous drain pattern keeps dirty set small:

```rust
async fn sync_worker(cache, s3_store, shutdown) {
    loop {
        let dirty = cache.claim_dirty_blocks(BATCH_SIZE);
        if dirty.is_empty() {
            sleep(Duration::from_millis(100)).await;
        } else {
            upload_to_s3(dirty).await;
        }
    }
}
```

Benefits:
- Cross-host snapshots only wait for current batch
- Dirty set stays bounded
- Network failures retry individual blocks

## Configuration

| Variable | Default | Rationale |
|----------|---------|-----------|
| `nbd.device_size_gb` | 100.0 | Virtual device size |
| `nbd.block_size` | 128KB | Match ZFS recordsize |
| `cache.dir` | ~/.cache/zerofs | Local SSD cache location |
| `cache.disk_size_gb` | 10.0 | Local cache size |

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| ZFS snapshot | <100ms | Local fsync only |
| ZFS clone | <100ms | ZFS metadata operation |
| Write latency | <1ms | Local SSD |
| Read latency (cached) | <1ms | Local SSD |
| Read latency (cold) | 50-300ms | S3 fetch |

## Security Model

### What We Verify

- Password-derived key via Argon2id (128-bit salt, 256-bit key)
- XChaCha20-Poly1305 authenticated encryption on all blocks
- Data encrypted both on local SSD and in S3

### What We Do NOT Verify

- **S3 access control** - Anyone with bucket access sees encrypted blobs
- **Network security** - NBD protocol is not encrypted (use VPN/firewall)

### Why This Is Acceptable

- S3 bucket policies provide coarse access control
- Local SSD encryption provides at-rest protection
- NBD intended for local/trusted network (localhost or VM hypervisor)

## Failure Modes

| Failure | Impact | Recovery |
|---------|--------|----------|
| Crash before sync | Up to N seconds of writes in local cache | Recovered on restart, synced to S3 |
| S3 unavailable | Writes continue locally | Sync resumes when S3 returns |
| Local SSD failure | Data loss for unsynced blocks | Restore from S3 (cold) |
| Graceful shutdown | None | All dirty blocks synced before exit |

### Data Loss Window

Write-behind means some writes may be on local SSD but not yet in S3.

**Mitigations:**
1. Continuous drain keeps dirty set small
2. Sync on graceful shutdown (flush all dirty blocks)
3. NVMe with power loss protection recommended for production
4. On crash: dirty blocks in local cache are recovered on restart
5. ZFS `sync=disabled` users already accept similar trade-offs

## Package Structure

| File | Purpose |
|------|---------|
| `nbd/server.rs` | NBD protocol server |
| `nbd/handler.rs` | Block I/O handler |
| `nbd/write_cache.rs` | Write-behind cache with typestate |
| `nbd/block_store.rs` | Direct S3 block storage |
| `nbd/state.rs` | BlockState, lifecycle state types |
| `block_transformer.rs` | Encryption/decryption |
| `config.rs` | Configuration loading |
| `cli/server.rs` | Server initialization and lifecycle |
| `rpc/server.rs` | Admin RPC API |

## Design Decisions

### Why Write-Behind Over Write-Through?

Write-through (old architecture):
```
NBD FLUSH → SlateDB.flush() → S3 PUT → return (2-15 seconds)
```

Write-behind (new architecture):
```
NBD FLUSH → Local SSD fsync → return (10ms)
                ↓
         Background → S3 PUT (async)
```

ZFS snapshot/clone operations are **metadata-only** (copy-on-write pointers). But NBD FLUSH was making them block on S3 network I/O - a 420x slowdown.

### Why Sparse Files for Cache?

- 100GB device with 10GB written = ~10GB on disk
- File grows as blocks are written
- OS handles sparse allocation transparently
- No pre-allocation overhead

### Why Direct Block-to-S3 (No LSM)?

The old architecture used SlateDB (LSM tree):
- Overhead: WAL + memtable + compaction
- Good for: Small key-value operations
- Bad for: Large sequential block I/O

Direct block-to-S3:
- One S3 object per block
- O(1) lookup: block 42 → `{prefix}/blocks/000000000000002a`
- No compaction overhead
- Simpler crash recovery

### Why Typestate for Device Lifecycle?

```rust
// Compile-time: impossible to serve I/O during recovery
WriteCache<Recovering> → finish_recovery() → WriteCache<Active>

// Runtime alternative (rejected):
cache.read()  // panics if not ready - bug found at runtime
```

Benefits:
- **Zero runtime overhead** - No state checks in hot path
- **Bugs found at compile time** - Not in production
- **Self-documenting API** - Types encode valid transitions

### Why Continuous Drain Over Periodic Sync?

Polling pattern:
```rust
loop { sleep(5s); sync_all_dirty(); }
```

Continuous drain pattern:
```rust
loop {
    let batch = claim_dirty(100);
    if batch.empty() { sleep(100ms) }
    else { upload(batch) }
}
```

Benefits:
- Dirty set stays small (most blocks synced within ~1s)
- Cross-host snapshots wait for current batch, not all dirty blocks
- More even S3 write traffic

## Limits

| Limit | Value | Constraint |
|-------|-------|------------|
| Max device size | 16 EiB | 64-bit block indices |
| Block size | 128 KB | Configurable, matches ZFS recordsize |
| Local cache | Disk space | Sparse file grows as needed |
