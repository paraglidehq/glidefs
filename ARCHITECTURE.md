# ZeroFS Architecture

A high-performance NBD (Network Block Device) server backed by S3-compatible object storage. Enables microVM storage with scale-to-zero and live migration capabilities.

## Design Philosophy

- **Performance is a feature**: Minimize allocations, do less work, measure before optimizing
- **FLUSH = local durability**: Fast path for ZFS operations
- **Background sync to S3**: Eventual durability via continuous drain
- **Idempotent operations**: Safe to retry after failures
- **Atomic state transitions**: Either fully succeed or leave system in recoverable state
- **S3 as source of truth**: Local cache is ephemeral, S3 is durable
- **Minimal abstraction**: Direct block-to-S3 mapping, no LSM overhead

## Deployment Model

```
Compute Node (ONE zerofs process):
  zerofs daemon
    ├── Export "vm-001" → /dev/nbd0 → MicroVM 1
    │     ├── S3: s3://bucket/path/vm-001/
    │     └── Cache: /var/cache/zerofs/<bucket-id>/vm-001/
    │
    ├── Export "vm-002" → /dev/nbd1 → MicroVM 2
    │     ├── S3: s3://bucket/path/vm-002/
    │     └── Cache: /var/cache/zerofs/<bucket-id>/vm-002/
    │
    └── Export "vm-003" → /dev/nbd2 → MicroVM 3
          ├── S3: s3://bucket/path/vm-003/
          └── Cache: /var/cache/zerofs/<bucket-id>/vm-003/
```

**Key invariant**: One zerofs daemon per compute node, multiple exports per daemon (one per microVM).

## Data Flow

### Write Path

```
Guest write → NBD handler → WriteCache (local SSD)
                               ├── Write to local file (page cache + disk)
                               ├── Mark block present (atomic OR)
                               ├── Mark block dirty (CAS)
                               ├── Push to dirty_queue (lock-free)
                               ├── Notify sync worker (wake if sleeping)
                               └── return (<1ms)
                                      │
                           (background) → S3BlockStore → S3
```

The key insight: FLUSH returns after local SSD fsync, not after S3 sync. This makes ZFS snapshot/clone operations fast (~100ms instead of ~10s).

### Read Path (Read-Through Caching)

```
Guest read → NBD handler → WriteCache.read_with_fetch()
                             │
                             ├── Block present locally?
                             │     └── Yes → Read from local SSD → Return (<1ms)
                             │
                             └── No → S3BlockStore.read_block()
                                        │
                                        ├── Block in S3 → Fetch, cache locally, mark present (50-300ms)
                                        │
                                        └── Not in S3 → Return zeros (never-written block)
```

This enables **wake anywhere**: VMs can start on any node and read their data on-demand from S3.

### Flush Path

```
Guest FLUSH → NBD handler → WriteCache.flush()
                               ↓
                           fsync(local_ssd)
                               ↓
                           Return success (<10ms)

(S3 sync happens asynchronously in background)
```

Compare to blocking on S3 (write-through):
- Write-through: 8-15 seconds per snapshot
- Write-behind: <100ms per snapshot (420x faster)

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

**Block presence tracking**:
- `present_chunks`: `Box<[AtomicU64]>` bitmap tracks which blocks exist locally
- Fresh cache on new node: all blocks marked NOT present
- On read: if block not present, fetch from S3 and cache locally
- On write: mark block as present (atomic OR) and dirty

## Core Components

### ExportRouter (`nbd/router.rs`)

Central coordinator for multi-tenant operation. Manages the lifecycle of all exports.

```rust
pub struct ExportRouter {
    exports: RwLock<HashMap<String, ExportState>>,
    object_store: Arc<dyn ObjectStore>,
    db_path: String,              // Base S3 path prefix
    cache_dir: PathBuf,
    block_size: usize,
    blocks_per_batch: u64,
    sync_delay_ms: u64,           // Write coalescing delay
    lease_manager: Arc<LeaseManager>,
}
```

**Responsibilities**:
- Create/remove exports dynamically
- Route NBD connections to correct handler
- Coordinate drain operations for migration
- Handle graceful shutdown
- Manage distributed leases for single-writer enforcement

### NBDServer (`nbd/server.rs`)

Handles NBD protocol negotiation and session management. Supports both TCP and Unix socket transports.

**Protocol flow**:
1. Client connects, negotiates NBD protocol
2. Client requests export by name (`NBD_OPT_GO`)
3. Server looks up handler via router
4. Transmission phase: read/write/flush/trim commands

### WriteCache (`nbd/write_cache.rs`)

Local SSD write-behind cache with typestate lifecycle:

```rust
// Only Active cache can serve I/O - compiler enforced
impl WriteCache<Active> {
    pub fn write(&self, offset: u64, data: &[u8]) -> Result<()>;
    pub fn flush(&self) -> Result<()>;  // Local fsync only!
    pub async fn read_with_fetch(&self, ...) -> Result<Bytes>;
}
```

**Key guarantees**:
- `FLUSH` = fsync to local SSD (not S3 round-trip)
- Background sync pushes dirty blocks to S3
- Drain blocks until all dirty blocks synced
- **Read-through**: Blocks not present locally are fetched from S3 on demand

**Lock-free internals**:
- `block_states`: `AtomicU8` array with CAS for state transitions
- `present_chunks`: `AtomicU64` bitmap with atomic OR
- `dirty_queue`: Lock-free `SegQueue<u64>` for O(1) dirty block tracking
- `dirty_notify`: `Notify` to wake sync worker on writes

### S3BlockStore (`nbd/block_store.rs`)

Batched block storage backed by S3. Blocks are grouped into fixed-size batches to reduce PUT costs.

```
s3://bucket/path/{export_name}/batches/{batch_num:012x}
```

**Batch format**:
- Each batch contains `blocks_per_batch` consecutive blocks (default: 100)
- Fixed size: 100 blocks × 128KB = 12.8MB per batch object
- Reads use S3 range requests to fetch individual blocks from batches
- Writes use GET-modify-PUT to update batch contents

**Cost reduction**: ~10x fewer S3 PUTs compared to individual block storage.

**No LSM tree** - direct batch addressing for O(1) predictable latency.

### Background Sync Worker

Event-driven sync with O(1) dirty block tracking, hot batch deferral, and back-pressure:

```rust
loop {
    // Fencing check: stop if we lost the lease
    if let Some(lease_state) = &lease_state {
        if !lease_state.is_valid() {
            warn!("lease lost, stopping sync");
            break;
        }
    }

    // O(1) pop from lock-free dirty queue
    dirty_blocks = cache.claim_dirty_blocks(batch_size);

    if dirty_blocks.is_empty() {
        // Sleep until write path wakes us (or safety timeout)
        // All sleeps are interruptible by shutdown signal
        select! {
            _ = cache.wait_for_dirty() => {}
            _ = sleep(1s) => {}
            _ = shutdown.changed() => break
        }
        continue;
    }

    // Back-pressure: warn/error if dirty queue grows too large
    if dirty_queue_size > critical_threshold {
        error!("dirty queue critical: {} blocks", dirty_queue_size);
    }

    // Group by S3 batch, defer hot batches
    for (batch_num, blocks) in group_by_batch(dirty_blocks) {
        if recently_synced(batch_num, hot_batch_cooldown) {
            // Defer: mark blocks dirty again, sync later
            for block in blocks { mark_dirty(block); }
        } else {
            // Sync: GET-modify-PUT
            batch_data = s3.get_batch_or_empty(batch_num);
            for block in blocks {
                // Read from local disk (likely still in page cache)
                batch_data[offset] = read_local_block(block);
            }
            s3.put_batch(batch_num, batch_data);
            mark_hot(batch_num);  // Track for cooldown
        }
    }
}
```

**Design**:
- **Event-driven**: Sleeps when idle, wakes instantly on writes
- **O(1) dirty tracking**: Lock-free queue (`SegQueue`) - no scanning
- **Page cache friendly**: Recently written blocks are read from page cache
- **Lease fencing**: Stops immediately if lease lost
- **Hot batch deferral**: Recently-synced batches are deferred to reduce write amplification
- **Back-pressure**: Warnings/errors when dirty queue exceeds thresholds
- **Shutdown-responsive**: All sleeps are interruptible by shutdown signals

**Benefits**:
- Cross-host snapshots only wait for current batch
- Dirty set stays bounded (most blocks synced within ~1s)
- More even S3 write traffic
- Network failures retry individual blocks
- Zero CPU for idle VMs
- Reduced write amplification via hot batch tracking

### NBDBlockHandler (`nbd/handler.rs`)

Thin handler that delegates all I/O to WriteCache:

```rust
pub struct NBDBlockHandler {
    cache: Arc<WriteCache<Active>>,  // Typestate: only Active cache
    s3_store: Arc<S3BlockStore>,     // For read-through caching
    device_size: u64,
    readonly: AtomicBool,            // Reject writes when true (migration)
    metrics: Arc<ExportMetrics>,     // I/O statistics
}

impl NBDBlockHandler {
    pub fn flush(&self) -> CommandResult<()> {
        self.cache.flush()  // Local fsync - returns in <10ms
    }
}
```

## HTTP API (`nbd/api.rs`)

REST API for orchestrator integration.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/exports` | GET | List all exports |
| `/api/exports` | POST | Create new export |
| `/api/exports/{name}` | GET | Get export info |
| `/api/exports/{name}` | DELETE | Remove export |
| `/api/exports/{name}/drain` | POST | Drain to S3 |
| `/api/exports/{name}/promote` | POST | Readonly → read-write |
| `/api/exports/{name}/metrics` | GET | Get I/O metrics |
| `/health` | GET | Health check |

## Metrics (`nbd/metrics.rs`)

Per-export I/O metrics for monitoring write amplification and cache effectiveness.

**Available via**: `GET /api/exports/{name}/metrics`

| Metric | Description |
|--------|-------------|
| `guest_bytes_written` | Total bytes written by VM |
| `guest_write_ops` | Number of NBD write commands |
| `guest_bytes_read` | Total bytes read by VM |
| `guest_read_ops` | Number of NBD read commands |
| `s3_bytes_written` | Total bytes written to S3 |
| `s3_write_ops` | Number of blocks synced to S3 |
| `s3_bytes_read` | Total bytes read from S3 |
| `s3_read_ops` | Number of blocks fetched from S3 |
| `batches_written` | Number of S3 batch objects written |
| `cache_hits` / `cache_misses` | Read-through cache effectiveness |

**Derived metrics**:

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| `write_amplification` | s3_bytes / guest_bytes | > 1.0 = batching overhead, < 1.0 = zero-block optimization helping |
| `coalesce_ratio` | guest_ops / batches_written | > 1.0 = batching effective |
| `cache_hit_rate` | hits / (hits + misses) | Higher = fewer S3 fetches |

Note: Block-level compression is intentionally not implemented. ZFS handles compression at its layer.

## Operational Scenarios

### Scale to Zero

```
1. VM stops (graceful or timeout)
2. Orchestrator: POST /api/exports/vm-001/drain
3. zerofs drains dirty blocks to S3 (blocking)
4. Orchestrator: DELETE /api/exports/vm-001
5. zerofs releases export (cache preserved for fast re-attach)

Result: vm-001 has zero local resources, all data in S3
        Other VMs unaffected
```

### Wake Anywhere

```
1. Orchestrator: POST nodeB:8080/api/exports
   { "name": "vm-001", "size_gb": 50 }
2. zerofs creates export, loads existing cache if present
3. nbd-client nodeB /dev/nbd0 -N vm-001
4. VM starts, reads pull from S3 on demand

First read: 50-300ms (S3 fetch)
Subsequent: <1ms (local cache)
```

### Live Migration

```
Node A                              Node B
──────                              ──────
VM running

1. POST B/api/exports
   { "name": "vm-001", "readonly": true }
                                    2. Creates read-only export
                                       Pre-warms cache from S3

3. POST A/api/exports/vm-001/drain
   Drains dirty blocks (blocking)

4. Pause VM

5. DELETE A/api/exports/vm-001

                                    6. POST B/api/exports/vm-001/promote
                                       readonly → read-write

                                    7. Resume VM

Downtime: Steps 4-7 (~100-500ms)
```

### Graceful Shutdown

```
SIGTERM/SIGINT:
1. Cancel all servers
2. Wait for connections to close
3. Drain all exports to S3
4. Exit

SIGUSR1 (node maintenance):
1. Drain all exports to S3
2. Continue running (for planned maintenance)
```

## Configuration

```toml
[storage]
url = "s3://bucket/path"
encryption_password = "$ZEROFS_PASSWORD"  # XChaCha20-Poly1305 key derivation

[cache]
dir = "/var/cache/zerofs"
disk_size_gb = 100.0
memory_size_gb = 10.0    # Optional in-memory cache

[servers.nbd]
addresses = ["0.0.0.0:10809"]
unix_socket = "/run/zerofs/nbd.sock"
api_address = "127.0.0.1:8080"
block_size = 131072      # 128KB (default)
blocks_per_batch = 100   # S3 batching: 100 blocks × 128KB = 12.8MB per PUT
sync_delay_ms = 8000     # Write coalescing delay (maps to hot batch cooldown, default 8s)

[[servers.nbd.exports]]
name = "vm-001"
size_gb = 50.0

[[servers.nbd.exports]]
name = "vm-002"
size_gb = 100.0
s3_prefix = "custom-prefix"  # Optional, defaults to name
block_size = 16384           # Optional, 16KB for database workloads
```

**TOML Configuration Options**:

| Variable | Default | Rationale |
|----------|---------|-----------|
| `block_size` | 128KB | Match ZFS recordsize |
| `blocks_per_batch` | 100 | Balance PUT cost vs latency |
| `sync_delay_ms` | 8000ms | Write coalescing / hot batch cooldown (8s reduces S3 PUTs via coalescing) |
| `cache.dir` | ~/.cache/zerofs | Local SSD cache location |
| `cache.disk_size_gb` | required | Cache size on disk |
| `cache.memory_size_gb` | none | Optional in-memory cache |

**Internal Defaults** (not configurable via TOML):

| Variable | Default | Rationale |
|----------|---------|-----------|
| `dirty_queue_warn_threshold` | 1000 | Warn when dirty queue exceeds (128MB @ 128KB blocks) |
| `dirty_queue_critical_threshold` | 10000 | Error when dirty queue exceeds (1.28GB @ 128KB blocks) |

## Write Amplification & S3 Cost Mitigations

### 1. Block Batching (S3 Cost Reduction)

Blocks are grouped into batches to reduce S3 PUT operations:

**Cost impact** (for 10GB daily writes at 128KB blocks):

| Approach | PUTs/day | PUT Cost/month |
|----------|----------|----------------|
| Individual blocks | ~115,000 | ~$17.40 |
| Batched (100:1) | ~1,200 | ~$1.80 |

**Trade-off**: Slightly higher read latency for first access to a batch (fetches 12.8MB vs 128KB), but S3's range request support means subsequent reads within the same batch are fast.

### 2. Zero-Block Detection

Blocks that are all zeros are **not written to S3**. Since missing blocks return zeros by default, this is safe and helps with:
- Fresh VMs (mostly zeros)
- TRIM/discard operations
- Sparse filesystems

### 3. Per-Export Block Size

Tune block size for your workload:

```toml
[[servers.nbd.exports]]
name = "database-vm"
size_gb = 50.0
block_size = 16384   # 16KB - less amplification for 8KB database pages

[[servers.nbd.exports]]
name = "media-vm"
size_gb = 500.0
block_size = 262144  # 256KB - better throughput for sequential I/O
```

**Trade-off**: Smaller blocks = less amplification but more batches to update.

### 4. Hot Batch Cooldown

Batches that were recently synced are marked "hot" and deferred:

```
Write to block 42 (batch 0) → sync batch 0 to S3 → mark batch 0 hot
Write to block 43 (batch 0) → batch 0 is hot → defer block 43
...3 seconds later...
batch 0 cools down → sync block 43 (and any others accumulated)
```

**Impact**: If a VM writes to blocks 0-99 (all in batch 0) sequentially:
- Without cooldown: 100 GET-modify-PUT operations
- With cooldown (100ms): ~1-2 GET-modify-PUT operations

**Trade-off**: Slightly delayed sync for recently-written batches, but dramatically reduced write amplification for sequential/localized writes.

## Security Model

### Encryption at Rest

ZeroFS relies on **S3 Server-Side Encryption (SSE)** for data at rest. This is configured at the bucket level:

```bash
aws s3api put-bucket-encryption \
    --bucket your-bucket \
    --server-side-encryption-configuration '{
        "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
    }'
```

**Why S3 SSE instead of client-side encryption?**

Client-side AEAD encryption (like ChaCha20-Poly1305) requires the full ciphertext to decrypt. This is fundamentally incompatible with S3 range requests for reading individual blocks from batched objects. S3 SSE provides transparent encryption that works with range requests.

### What We Do NOT Provide

- **Client-side encryption** - Blocks are stored as plaintext in S3 (S3 SSE encrypts at rest)
- **Network encryption** - NBD protocol is not encrypted (use VPN/firewall)

### Why This Is Acceptable

- S3 SSE provides encryption at rest with AWS-managed keys
- S3 bucket policies provide access control
- For local SSD encryption, use OS-level solutions (LUKS, FileVault)
- NBD intended for local/trusted network (localhost or VM hypervisor)
- API should bind to localhost only in production

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

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| ZFS snapshot | <100ms | Local fsync only |
| ZFS clone | <100ms | ZFS metadata operation |
| Write latency | <1ms | Local SSD |
| Read latency (cached) | <1ms | Local SSD |
| Read latency (cold) | 50-300ms | S3 fetch |

## Durability Semantics

**Critical**: ZeroFS makes an explicit trade-off between latency and durability. Understanding this is essential for operating ZeroFS safely.

### The Trade-off

| Event | Data Durability |
|-------|-----------------|
| `NBD FLUSH` returns | Data on local SSD only (durable to local failure) |
| `sync_worker` uploads batch | Data in S3 (durable to node loss) |
| `drain_for_snapshot()` returns | All dirty data in S3 (fully durable) |

### When Data Is At Risk

1. **After FLUSH, before S3 sync**: Data is on local SSD but not in S3. If the node dies (hardware failure, power loss without UPS), this data is lost.

2. **During migration without drain**: If you stop a VM and move to another node without calling `drain`, the new node won't see recent writes.

### Guaranteed Durability Points

Use these for operations that require full durability:

```bash
# Before live migration - wait for all data to reach S3
curl -X POST http://localhost:8080/api/exports/vm-001/drain

# Before scale-to-zero - ensure all data persisted
curl -X POST http://localhost:8080/api/exports/vm-001/drain
curl -X DELETE http://localhost:8080/api/exports/vm-001
```

### Safe Operating Procedures

| Scenario | Required Action |
|----------|-----------------|
| Live migration | Drain source before promoting destination |
| Scale to zero | Drain before removing export |
| Node maintenance | Send SIGUSR1 to drain all, or drain individual exports |
| Backup/snapshot | Call drain, then proceed with external backup |

### Who Should Use This?

This architecture is appropriate when:

- ZFS snapshots/clones need to be instant (<100ms)
- MicroVM fork speed is critical
- Users understand ZFS `sync=disabled` trade-offs
- Production nodes have battery-backed NVRAM/NVMe

This architecture may NOT be appropriate when:

- You need synchronous S3 replication on every write
- You cannot tolerate any data loss on node failure
- Your workload has very few writes (write-through would be fine)

### Comparison to Other Systems

| System | FLUSH Behavior | Trade-off |
|--------|----------------|-----------|
| ZeroFS | Local fsync (~10ms) | Data at risk until S3 sync |
| NFS (sync) | Network round-trip (~50-200ms) | Always durable, slower |
| EBS | Network round-trip (~1-5ms) | Always durable, AWS-only |
| Local disk | Local fsync (~1-10ms) | Lost if node dies |

ZeroFS behaves like local disk with background replication - similar to DRBD async mode or MySQL async replication.

## Design Decisions

### Why Write-Behind Over Write-Through?

Write-through would block on S3:
```
NBD FLUSH → S3 PUT → return (2-15 seconds)
```

Write-behind (what we do):
```
NBD FLUSH → Local SSD fsync → return (10ms)
                ↓
         Background → S3 PUT (async)
```

ZFS snapshot/clone operations are **metadata-only** (copy-on-write pointers). Blocking FLUSH on S3 network I/O would cause a 420x slowdown.

### Why Sparse Files for Cache?

- 100GB device with 10GB written = ~10GB on disk
- File grows as blocks are written
- OS handles sparse allocation transparently
- No pre-allocation overhead

### Why Direct Block-to-S3 (No LSM)?

LSM trees (like RocksDB/SlateDB) add overhead:
- WAL + memtable + compaction
- Good for: Small key-value operations
- Bad for: Large sequential block I/O

Direct block-to-S3 (what we do):
- One S3 batch per N blocks
- O(1) lookup: block 42 → batch 0, offset 42
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

### Why Event-Driven Sync?

```rust
loop {
    let batch = claim_dirty(100);  // O(1) pop from dirty queue
    if batch.empty() {
        wait_for_dirty().await;    // Zero CPU when idle
    } else {
        upload(batch)
    }
}
```

**Design**:
- **O(1) dirty block tracking**: Write path pushes to lock-free queue, sync pops
- **Zero CPU when idle**: Sync worker sleeps until write path notifies
- **Instant response**: Wakes immediately when writes happen

Benefits:
- Dirty set stays small (most blocks synced within ~1s)
- Cross-host snapshots wait for current batch, not all dirty blocks
- More even S3 write traffic
- No CPU overhead for idle VMs

## Distributed Coordination

### S3-Based Leases (`nbd/lease.rs`)

ZeroFS uses S3 conditional writes to implement distributed leases for single-writer enforcement. No external coordination service (etcd, Zookeeper) is required.

**Lease file**: `s3://bucket/path/nbd/{export}/lease.json`

```json
{
  "owner": "node-abc123",
  "generation": 42,
  "acquired_at": 1706900000,
  "ttl_seconds": 300
}
```

**Protocol**:
- **Acquire**: GET lease → check expiry/ownership → conditional PUT with If-Match
- **Renew**: Re-acquire with generation+1 (every 2.5 minutes for 5 minute TTL)
- **Release**: PUT with empty owner and generation+1

**Fencing**: The generation counter provides fencing. Sync workers verify the lease is still valid before each S3 write. If a node loses its lease (renewal fails, another node takes over), the sync worker stops immediately.

**Conditional Batch Writes**: As defense-in-depth, batch writes use S3 conditional PUT (If-Match with ETag). Even if leases fail to prevent concurrent writers, the conditional PUT will fail if another writer modified the batch since we read it. This provides a second layer of protection against data corruption.

```
GET batch → ETag: "abc123"
modify batch locally
PUT batch (If-Match: "abc123") → succeeds only if batch unchanged
```

### What zerofs handles

- **Single-writer enforcement**: S3 leases prevent concurrent writers
- **Automatic lease renewal**: Background task renews at 50% TTL
- **Fencing checks**: Sync worker verifies lease before each batch write
- **Graceful handoff**: Lease released on export removal/shutdown
- **Readonly mode**: Safe pre-warming during migration
- **Per-export drain/sync**

### What the orchestrator must handle

- Coordinating migration sequence (create readonly → drain → remove → promote)
- Tracking which node serves which export
- Health monitoring and failover decisions

### Lease Flow

```
create_export(readonly=false):
  1. Acquire lease from S3 (generation N)
  2. Create LeaseState for coordination
  3. Start sync_worker with LeaseState
  4. Start lease_renewal_task

While running:
  - Renewal task wakes every 2.5 minutes
  - Calls acquire() → generation N+1
  - Updates LeaseState
  - If renewal fails → marks lease lost → sync worker stops

remove_export() / shutdown():
  1. Release lease (allows immediate reacquisition)
  2. Signal shutdown to sync worker and renewal task
  3. Wait for both to complete
```

### Failure Modes

| Failure | Behavior |
|---------|----------|
| Lease renewal fails | Sync worker stops, export becomes readonly |
| Node crashes | Lease expires after TTL (5 min), other nodes can acquire |
| Network partition | Same as crash - lease expires |
| Two nodes try to acquire | One succeeds, other gets `LeaseHeldByOther` error |
| Concurrent batch modification | Conditional PUT fails, blocks marked dirty for retry |

## Package Structure

```
src/
├── main.rs                    # Entry point: CLI command dispatching
├── lib.rs                     # Library re-exports
├── config.rs                  # TOML configuration parsing
├── key_management.rs          # XChaCha20-Poly1305 encryption, Argon2 key derivation
├── bucket_identity.rs         # Bucket UUID tracking for cache isolation
├── parse_object_store.rs      # Object store factory (S3/Azure/GCP)
├── storage_compatibility.rs   # Storage backend utilities
├── task.rs                    # Named task spawning for debugging
├── deku_bytes.rs              # Binary serialization helpers
├── cli/
│   ├── mod.rs                 # CLI struct and subcommand routing
│   ├── server.rs              # `zerofs run` command, signal handling
│   └── password.rs            # `zerofs change-password` command
└── nbd/
    ├── mod.rs                 # Module exports
    ├── server.rs              # NBD protocol server (TCP/Unix socket)
    ├── router.rs              # Multi-tenant export router
    ├── api.rs                 # HTTP API for dynamic export management
    ├── handler.rs             # NBD command handler (read/write/flush/trim)
    ├── block_store.rs         # S3 block storage backend (batched)
    ├── write_cache.rs         # Write-behind cache with typestate
    ├── lease.rs               # S3-based distributed lease coordination
    ├── state.rs               # Device state machines and typestate markers
    ├── metrics.rs             # Per-export I/O metrics
    ├── protocol.rs            # NBD protocol types
    └── error.rs               # Error types
```

## Limits

| Limit | Value | Constraint |
|-------|-------|------------|
| Max device size | 16 EiB | 64-bit block indices |
| Block size | 128 KB | Configurable, matches ZFS recordsize |
| Local cache | Disk space | Sparse file grows as needed |
